/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.resource.ResourceLaneStats;
import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Verifies that the configured lane strategy ({@code search.admission_control.lane_strategy}) actually routes shard
 * searches into admission lanes: with the {@code system} strategy a search over a regular user index is admitted into
 * the {@link ResourcePriority#NORMAL} lane (and never the {@link ResourcePriority#SYSTEM} lane). The system-index → SYSTEM
 * mapping itself is unit-tested in {@code SearchLaneResolverTests}.
 */
public class SearchServiceLaneStrategySingleNodeTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 2)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_LANE_STRATEGY.getKey(), "system")
            .build();
    }

    public void testUserIndexSearchRoutesToNormalLane() {
        createIndex("index");
        prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchService service = getInstanceFromNode(SearchService.class);
        long normalBefore = laneAcquired(service.searchAdmissionStats(), ResourcePriority.NORMAL);
        long systemBefore = laneAcquired(service.searchAdmissionStats(), ResourcePriority.SYSTEM);

        assertResponse(
            client().prepareSearch("index").setQuery(QueryBuilders.matchAllQuery()),
            response -> assertEquals(0, response.getFailedShards())
        );

        ResourcePoolStats after = service.searchAdmissionStats();
        assertTrue("user-index search should acquire in the NORMAL lane", laneAcquired(after, ResourcePriority.NORMAL) > normalBefore);
        assertEquals("user-index search must not touch the SYSTEM lane", systemBefore, laneAcquired(after, ResourcePriority.SYSTEM));
    }

    private static long laneAcquired(ResourcePoolStats stats, ResourcePriority lane) {
        for (ResourceLaneStats laneStats : stats.lanes()) {
            if (laneStats.lane() == lane) {
                return laneStats.totalAcquired();
            }
        }
        throw new AssertionError("no stats for lane " + lane);
    }
}
