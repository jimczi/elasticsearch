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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies execution-under-lease memory enforcement (RD3): with coordinator admission and a tiny per-query memory
 * budget, the query's shard aggregations run under the budget the coordinator reserved on each node and fail with a
 * query-scoped {@link CircuitBreakingException}; a non-aggregation search is unaffected; and no leases or slots leak.
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

    public void testAggregationFailsUnderLeaseBudgetButPlainSearchSucceeds() {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
        );
        for (int i = 0; i < 50; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        indicesAdmin().prepareRefresh("test").get();
        ensureGreen("test");

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").addAggregation(AggregationBuilders.terms("by_v").field("v")).get()
        );
        assertThat(ExceptionsHelper.unwrap(e, CircuitBreakingException.class), notNullValue());

        // A non-aggregation search builds no aggregation context, so the per-query budget does not apply.
        assertNoFailures(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()));

        // The failed and the successful searches both released their leases.
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            assertEquals(0, searchService.searchAdmissionStats().currentUsedSlots());
        }
        for (SearchAdmissionService admission : internalCluster().getInstances(SearchAdmissionService.class)) {
            assertEquals(0, admission.openLeaseCount());
        }
    }
}
