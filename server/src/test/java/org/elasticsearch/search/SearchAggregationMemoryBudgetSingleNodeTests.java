/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end check that the per-query memory budget (RM) is enforced for classic {@code _search} aggregations: with a
 * tiny {@code search.admission_control.query_memory} budget, an aggregation search trips its per-query breaker and fails
 * with a {@link CircuitBreakingException}, while a non-aggregation search (which never builds an aggregation context) is
 * unaffected.
 */
public class SearchAggregationMemoryBudgetSingleNodeTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 4)
            // 1-byte per-query budget: any aggregation allocation immediately exceeds it.
            .put(SearchService.SEARCH_ADMISSION_CONTROL_QUERY_MEMORY.getKey(), "1b")
            .build();
    }

    public void testAggregationExceedingMemoryBudgetFails() {
        createIndex("test");
        for (int i = 0; i < 50; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).setRefreshPolicy(IMMEDIATE).get();
        }

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").addAggregation(AggregationBuilders.terms("by_v").field("v")).get()
        );
        assertThat(ExceptionsHelper.unwrap(e, CircuitBreakingException.class), notNullValue());
    }

    public void testPlainSearchIsUnaffectedByTheBudget() {
        createIndex("plain");
        for (int i = 0; i < 10; i++) {
            prepareIndex("plain").setId(Integer.toString(i)).setSource("v", i).setRefreshPolicy(IMMEDIATE).get();
        }
        // No aggregation context is built, so the per-query memory breaker does not apply.
        assertResponse(
            client().prepareSearch("plain").setQuery(QueryBuilders.matchAllQuery()),
            response -> assertEquals(0, response.getFailedShards())
        );
    }
}
