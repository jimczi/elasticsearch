/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end check that the {@code _search_admission/stats} action reports this node's live admission pool snapshot.
 */
public class SearchAdmissionStatsSingleNodeTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 2).build();
    }

    public void testStatsReportsLivePoolSnapshot() {
        SearchAdmissionStatsResponse response = client().execute(
            TransportSearchAdmissionStatsAction.TYPE,
            new SearchAdmissionStatsRequest()
        ).actionGet();

        assertEquals(1, response.getNodes().size());
        NodeSearchAdmissionStats node = response.getNodes().get(0);
        assertEquals(0, node.openLeases());
        ResourcePoolStats stats = node.poolStats();
        assertThat(stats, notNullValue());
        assertThat(stats.slotCapacity(), greaterThan(0L));
        // every priority lane is represented in the per-lane breakdown
        assertEquals(org.elasticsearch.common.resource.ResourcePriority.values().length, stats.lanes().size());
    }
}
