/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.admission.SearchAdmissionService;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Closes the RD4 reserve-rejection gap: when coordinator admission is enabled and a participating data node cannot be
 * reserved (its budget is exhausted), the ES|QL query must fail <em>fast and cleanly</em> rather than hang. Verified
 * with a bounded request timeout so a hang surfaces as a test failure rather than a clean rejection.
 */
public class EsqlCoordinatorAdmissionRejectionIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 1)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_MAX_QUEUE_LENGTH.getKey(), 0) // node-side reserve rejects, never waits
            .put(SearchService.SEARCH_ADMISSION_CONTROL_ACQUIRE_TIMEOUT.getKey(), TimeValue.timeValueMillis(100))
            .put(SearchService.SEARCH_ADMISSION_CONTROL_COORDINATOR_ENABLED.getKey(), true)
            .build();
    }

    public void testQueryFailsFastWhenADataNodeCannotBeReserved() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
        );
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        client().admin().indices().prepareRefresh("test").get();
        ensureGreen("test");

        // Exhaust every node's admission budget so the coordinator's per-node reserve is rejected.
        saturateAllNodes("hold");

        // With allow_partial_results=false (the integ-test default), an un-reservable node must fail the whole query, and
        // it must do so promptly: a bounded timeout turns a hang into an AssertionError("timeout") rather than passing.
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM test | STATS s = sum(v)");
        expectThrows(Exception.class, () -> run(request, TimeValue.timeValueSeconds(20)).close());

        // Once capacity frees up, the same query runs to completion.
        releaseAllNodes("hold");
        try (EsqlQueryResponse resp = run("FROM test | STATS s = sum(v)")) {
            assertNotNull(resp);
        }

        // No leases or slots are leaked after either path.
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.searchAdmissionStats().currentUsedSlots());
            }
            for (SearchAdmissionService admission : internalCluster().getInstances(SearchAdmissionService.class)) {
                assertEquals(0, admission.openLeaseCount());
            }
        });
    }

    private void saturateAllNodes(String leasePrefix) {
        for (String node : internalCluster().getNodeNames()) {
            SearchService searchService = internalCluster().getInstance(SearchService.class, node);
            SearchAdmissionService admission = internalCluster().getInstance(SearchAdmissionService.class, node);
            int capacity = Math.toIntExact(searchService.searchAdmissionStats().slotCapacity());
            PlainActionFuture<Void> held = new PlainActionFuture<>();
            admission.reserveLocally(capacity, ResourcePriority.NORMAL, null, leasePrefix + "-" + node, held);
            held.actionGet();
        }
    }

    private void releaseAllNodes(String leasePrefix) {
        for (String node : internalCluster().getNodeNames()) {
            internalCluster().getInstance(SearchAdmissionService.class, node).releaseLocally(leasePrefix + "-" + node);
        }
    }
}
