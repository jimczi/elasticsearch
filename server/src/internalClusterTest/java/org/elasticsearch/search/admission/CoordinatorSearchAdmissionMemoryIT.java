/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies execution-under-lease memory enforcement (RD3): with coordinator admission and a tiny per-query memory
 * budget, the <em>whole</em> shard — query phase and aggregations alike — runs under the budget the coordinator reserved
 * on each node, so even a plain query fails with a query-scoped {@link CircuitBreakingException}; and no leases or slots
 * leak afterwards.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class CoordinatorSearchAdmissionMemoryIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 4)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_COORDINATOR_ENABLED.getKey(), true)
            // 1-byte per-query budget: any aggregation allocation exceeds the lease budget.
            .put(SearchService.SEARCH_ADMISSION_CONTROL_QUERY_MEMORY.getKey(), "1b")
            .build();
    }

    public void testWholeShardRunsUnderLeaseBudget() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
        );
        for (int i = 0; i < 50; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        indicesAdmin().prepareRefresh("test").get();
        ensureGreen("test");

        // Even a plain query phase (no aggregations) is bounded by the per-query lease budget, so it trips the breaker.
        Exception plain = expectThrows(Exception.class, () -> client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).get());
        assertThat(ExceptionsHelper.unwrap(plain, CircuitBreakingException.class), notNullValue());

        // An aggregation search is bounded by the same budget.
        Exception agg = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").addAggregation(AggregationBuilders.terms("by_v").field("v")).get()
        );
        assertThat(ExceptionsHelper.unwrap(agg, CircuitBreakingException.class), notNullValue());

        // The failed searches release their leases asynchronously after settling, so poll until everything is freed.
        assertBusy(() -> {
            for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, searchService.searchAdmissionStats().currentUsedSlots());
            }
            for (SearchAdmissionService admission : internalCluster().getInstances(SearchAdmissionService.class)) {
                assertEquals(0, admission.openLeaseCount());
            }
        });
    }
}
