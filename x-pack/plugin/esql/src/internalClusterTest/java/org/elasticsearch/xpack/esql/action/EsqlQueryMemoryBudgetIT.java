/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that the per-query memory budget (RM) is enforced for ES|QL data-node execution: with a tiny
 * {@code search.admission_control.query_memory} budget, a query's own allocations trip its per-query breaker and the
 * query fails with a {@link CircuitBreakingException} — and the admission slots are still fully released afterwards.
 */
public class EsqlQueryMemoryBudgetIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Enable admission control, and give each shard of work a 1-byte memory budget so any real allocation
            // immediately exceeds the per-query budget.
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 4)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_QUERY_MEMORY.getKey(), "1b")
            .build();
    }

    public void testQueryExceedingItsMemoryBudgetFails() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1)));
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        client().admin().indices().prepareRefresh("test").get();

        Exception e = expectThrows(Exception.class, () -> {
            try (EsqlQueryResponse ignored = run("FROM test | STATS s = sum(v)")) {
                // should not get here
            }
        });
        // The failure is a circuit break from the per-query memory budget, not a node-wide trip.
        assertThat(ExceptionsHelper.unwrap(e, CircuitBreakingException.class), notNullValue());

        // The admission slots reserved for the failed query were released on the failure path.
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertEquals(0, service.searchAdmissionStats().currentUsedSlots());
        }
    }
}
