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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.resource.Reservation;
import org.elasticsearch.common.resource.ResourcePool;
import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end checks that shard-level search admission control ({@code search.admission_control.*}) is actually consulted
 * by {@link SearchService#runAsync} and that reservations are fully released on every path. The reject/queue/timeout
 * semantics of the underlying pool are unit-tested separately in {@code ResourcePoolTests}; here we only prove the
 * wiring and the no-leak guarantee against a real node.
 */
public class SearchServiceAdmissionControlSingleNodeTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 2)
            // No queue + short timeout so a request that cannot be admitted is rejected promptly rather than waiting.
            .put(SearchService.SEARCH_ADMISSION_CONTROL_MAX_QUEUE_LENGTH.getKey(), 0)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_ACQUIRE_TIMEOUT.getKey(), TimeValue.timeValueMillis(100))
            .build();
    }

    public void testAdmissionStatsTrackAcquireAndRelease() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        ResourcePoolStats before = service.searchAdmissionStats();
        assertThat(before, notNullValue());

        assertResponse(
            client().prepareSearch("index").setQuery(QueryBuilders.matchAllQuery()),
            response -> assertEquals(0, response.getFailedShards())
        );

        ResourcePoolStats after = service.searchAdmissionStats();
        // The shard query (and fetch) tasks went through the admission pool...
        assertThat(after.totalAcquired(), greaterThan(before.totalAcquired()));
        // ...and every acquired slot was released, leaving the pool empty.
        assertEquals(0, after.currentUsedSlots());
        assertEquals(after.totalAcquired(), after.totalReleased());
    }

    public void testSearchRejectedWhenNoSlotsAvailable() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        ResourcePool pool = service.searchAdmissionPool();
        assertThat(pool, notNullValue());

        // Hold the entire node slot budget so no shard task can be admitted.
        try (Reservation held = pool.acquire(pool.stats().slotCapacity(), 0, ResourcePriority.NORMAL)) {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("index").setQuery(QueryBuilders.matchAllQuery()).get()
            );
            // The single shard failed because it could not be admitted; the root cause is an admission rejection.
            assertThat(ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class), notNullValue());
        }

        // Releasing the held budget must leave the pool empty again.
        assertEquals(0, service.searchAdmissionStats().currentUsedSlots());
    }
}
