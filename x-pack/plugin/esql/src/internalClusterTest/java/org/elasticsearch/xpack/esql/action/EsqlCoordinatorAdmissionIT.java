/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.admission.SearchAdmissionService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that when coordinator distributed admission is enabled, an ES|QL query reserves capacity on the participating
 * data nodes before dispatching its compute (the data nodes then run under that lease instead of acquiring per batch),
 * runs to completion, and releases every per-node lease afterwards (no leaked slots or leases).
 */
public class EsqlCoordinatorAdmissionIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 4)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_COORDINATOR_ENABLED.getKey(), true)
            .build();
    }

    public void testCoordinatorReservesOnDataNodesAndReleases() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
        );
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        client().admin().indices().prepareRefresh("test").get();
        ensureGreen("test");

        long acquiredBefore = sumAcquired();
        try (EsqlQueryResponse resp = run("FROM test | STATS s = sum(v)")) {
            assertNotNull(resp);
        }

        // The coordinator reserved data-node capacity through the admission pool for the query...
        assertThat(sumAcquired(), greaterThan(acquiredBefore));
        // ...and every per-node lease (and the slots it held) was released once the query settled.
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals("admission pool leaked slots on a node", 0, service.searchAdmissionStats().currentUsedSlots());
            }
            for (SearchAdmissionService admission : internalCluster().getInstances(SearchAdmissionService.class)) {
                assertEquals("leaked a coordinator lease on a node", 0, admission.openLeaseCount());
            }
        });
    }

    private long sumAcquired() {
        long total = 0;
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            total += service.searchAdmissionStats().totalAcquired();
        }
        return total;
    }
}
