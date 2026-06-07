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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.resource.ResourceRejectedException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies the coordinator "accept = guaranteed to run" contract end to end: when participating data nodes have no
 * admission capacity, a fan-out search is rejected <em>up front</em> at acceptance (not mid-flight); once capacity frees
 * up the same search is admitted and runs, and all leases are released afterwards.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class CoordinatorSearchAdmissionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 1)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_MAX_QUEUE_LENGTH.getKey(), 0) // node-side reserve never waits
            .put(SearchService.SEARCH_ADMISSION_CONTROL_COORDINATOR_ENABLED.getKey(), true)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_COORDINATOR_ACCEPT_TIMEOUT.getKey(), "500ms")
            .put(SearchService.SEARCH_ADMISSION_CONTROL_COORDINATOR_RETRY_INTERVAL.getKey(), "50ms")
            .build();
    }

    public void testSearchRejectedUpFrontWhenSaturatedThenAdmittedWhenFreed() {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
        );
        for (int i = 0; i < 50; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("v", i).get();
        }
        indicesAdmin().prepareRefresh("test").get();
        ensureGreen("test");

        // Saturate every node's admission budget with a separate held lease.
        saturateAllNodes("hold");

        // With no capacity anywhere, the fan-out search is rejected at acceptance.
        Exception rejected = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).get()
        );
        assertThat(ExceptionsHelper.unwrap(rejected, ResourceRejectedException.class), notNullValue());

        // Free the budget; the same search is now admitted and runs to completion.
        releaseAllNodes("hold");
        assertNoFailures(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()));

        // No leases or slots are leaked after the admitted search completes.
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            assertEquals(0, searchService.searchAdmissionStats().currentUsedSlots());
        }
        for (SearchAdmissionService admission : internalCluster().getInstances(SearchAdmissionService.class)) {
            assertEquals(0, admission.openLeaseCount());
        }
    }

    private void saturateAllNodes(String leasePrefix) {
        // Hold every slot on every node so no further reservation can be granted anywhere.
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
