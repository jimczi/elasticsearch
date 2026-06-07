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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.resource.ResourceRejectedException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.notNullValue;

/**
 * Exercises {@link SearchAdmissionService} against a real node: node-local reserve/release, a transport round-trip to
 * the local node, and rejection when the node budget is exhausted. The cross-node and execution-under-lease behaviour
 * is covered later; here we prove the lease lifecycle and transport plumbing.
 */
public class SearchAdmissionServiceSingleNodeTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(SearchService.SEARCH_ADMISSION_CONTROL_SLOTS_PER_THREAD.getKey(), 4)
            // No queue + short timeout so an over-capacity reservation is rejected promptly rather than waiting.
            .put(SearchService.SEARCH_ADMISSION_CONTROL_MAX_QUEUE_LENGTH.getKey(), 0)
            .put(SearchService.SEARCH_ADMISSION_CONTROL_ACQUIRE_TIMEOUT.getKey(), "100ms")
            .build();
    }

    public void testReserveAndReleaseLocally() {
        SearchAdmissionService admission = getInstanceFromNode(SearchAdmissionService.class);
        SearchService searchService = getInstanceFromNode(SearchService.class);

        int leasesBefore = admission.openLeaseCount();
        long slotsBefore = searchService.searchAdmissionStats().currentUsedSlots();
        String leaseId = "local-" + randomAlphaOfLength(8);

        PlainActionFuture<Void> reserved = new PlainActionFuture<>();
        admission.reserveLocally(3, ResourcePriority.NORMAL, null, leaseId, reserved);
        reserved.actionGet();

        assertEquals(leasesBefore + 1, admission.openLeaseCount());
        assertEquals(slotsBefore + 3, searchService.searchAdmissionStats().currentUsedSlots());

        assertTrue(admission.releaseLocally(leaseId));
        assertEquals(leasesBefore, admission.openLeaseCount());
        assertEquals(slotsBefore, searchService.searchAdmissionStats().currentUsedSlots());

        // Releasing again is a no-op.
        assertFalse(admission.releaseLocally(leaseId));
    }

    public void testReserveAndReleaseOverTransportToSelf() {
        SearchAdmissionService admission = getInstanceFromNode(SearchAdmissionService.class);
        DiscoveryNode localNode = getInstanceFromNode(ClusterService.class).localNode();

        int leasesBefore = admission.openLeaseCount();
        String leaseId = "remote-" + randomAlphaOfLength(8);

        PlainActionFuture<Void> reserved = new PlainActionFuture<>();
        admission.reserve(localNode, leaseId, 2, ResourcePriority.NORMAL, reserved);
        reserved.actionGet();
        assertEquals(leasesBefore + 1, admission.openLeaseCount());

        PlainActionFuture<Void> released = new PlainActionFuture<>();
        admission.release(localNode, leaseId, released);
        released.actionGet();
        assertEquals(leasesBefore, admission.openLeaseCount());
    }

    public void testReserveRejectedWhenBudgetExhausted() {
        SearchAdmissionService admission = getInstanceFromNode(SearchAdmissionService.class);
        SearchService searchService = getInstanceFromNode(SearchService.class);

        int available = Math.toIntExact(searchService.searchAdmissionStats().currentAvailableSlots());
        String holdLease = "hold-" + randomAlphaOfLength(8);
        PlainActionFuture<Void> hold = new PlainActionFuture<>();
        admission.reserveLocally(available, ResourcePriority.NORMAL, null, holdLease, hold);
        hold.actionGet();

        try {
            PlainActionFuture<Void> overflow = new PlainActionFuture<>();
            admission.reserveLocally(1, ResourcePriority.NORMAL, null, "overflow-" + randomAlphaOfLength(8), overflow);
            Exception e = expectThrows(Exception.class, overflow::actionGet);
            assertThat(ExceptionsHelper.unwrap(e, ResourceRejectedException.class), notNullValue());
        } finally {
            assertTrue(admission.releaseLocally(holdLease));
        }
    }
}
