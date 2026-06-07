/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.resource.ResourceRejectedException;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CoordinatorSearchAdmissionTests extends ESTestCase {

    private static final DiscoveryNode NODE_A = DiscoveryNodeUtils.create("node-a");
    private static final DiscoveryNode NODE_B = DiscoveryNodeUtils.create("node-b");
    private static final DiscoveryNode NODE_C = DiscoveryNodeUtils.create("node-c");

    public void testEmptyDemandSucceedsInline() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        CoordinatorSearchAdmission admission = controller(new FakeClient(), taskQueue.getThreadPool());
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        admission.admit("lease", Map.of(), ResourcePriority.NORMAL, future);
        assertTrue(future.isDone());
        assertNotNull(future.actionGet());
    }

    public void testAllGrantSucceedsAndReleases() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        FakeClient client = new FakeClient();
        CoordinatorSearchAdmission admission = controller(client, taskQueue.getThreadPool());

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        admission.admit("lease", demand(NODE_A, NODE_B, NODE_C), ResourcePriority.NORMAL, future);

        assertTrue(future.isDone());
        assertEquals(Set.of(NODE_A, NODE_B, NODE_C), client.held);
        assertEquals(0, admission.queuedCount());

        future.actionGet().close();
        assertTrue(client.held.isEmpty()); // releasing frees every node lease
    }

    public void testRollbackThenRetrySucceeds() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        FakeClient client = new FakeClient();
        client.failNodeForAttempts(NODE_B, 1); // B rejects the first attempt, grants the second
        CoordinatorSearchAdmission admission = controller(client, taskQueue.getThreadPool());

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        admission.admit("lease", demand(NODE_A, NODE_B, NODE_C), ResourcePriority.NORMAL, future);

        // First attempt rolled back (A, C reserved then released) and the search is now queued for retry.
        assertFalse(future.isDone());
        assertTrue(client.held.isEmpty());
        assertEquals(1, admission.queuedCount());

        taskQueue.runAllTasksInTimeOrder();

        assertTrue(future.isDone());
        assertNotNull(future.actionGet());
        assertEquals(Set.of(NODE_A, NODE_B, NODE_C), client.held);
        assertEquals(0, admission.queuedCount());
    }

    public void testDeadlineExceededRejectsAndRollsBack() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        FakeClient client = new FakeClient();
        client.failNodeForAttempts(NODE_B, Integer.MAX_VALUE); // B never grants
        CoordinatorSearchAdmission admission = controller(client, taskQueue.getThreadPool());

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        admission.admit("lease", demand(NODE_A, NODE_B), ResourcePriority.NORMAL, future);

        taskQueue.runAllTasksInTimeOrder();

        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
        assertTrue("all partial reservations must be rolled back", client.held.isEmpty());
        assertEquals(0, admission.queuedCount());
    }

    public void testQueueFullRejectsImmediately() {
        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        FakeClient client = new FakeClient();
        client.failNodeForAttempts(NODE_B, Integer.MAX_VALUE);
        // maxQueuedSearches = 0: a search that cannot be admitted on the first try is rejected rather than queued.
        CoordinatorSearchAdmission admission = new CoordinatorSearchAdmission(
            client,
            taskQueue.getThreadPool(),
            TimeValue.timeValueSeconds(30),
            TimeValue.timeValueMillis(100),
            0
        );

        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        admission.admit("lease", demand(NODE_A, NODE_B), ResourcePriority.NORMAL, future);

        assertTrue(future.isDone());
        expectThrows(ResourceRejectedException.class, future::actionGet);
        assertTrue(client.held.isEmpty());
        assertEquals(0, admission.queuedCount());
    }

    private static CoordinatorSearchAdmission controller(FakeClient client, ThreadPool threadPool) {
        return new CoordinatorSearchAdmission(client, threadPool, TimeValue.timeValueSeconds(5), TimeValue.timeValueMillis(100), 10);
    }

    private static Map<DiscoveryNode, Integer> demand(DiscoveryNode... nodes) {
        Map<DiscoveryNode, Integer> demand = new LinkedHashMap<>();
        for (DiscoveryNode node : nodes) {
            demand.put(node, 1);
        }
        return demand;
    }

    /** In-memory admission client: grants reservations and tracks held leases; can be told to reject some nodes. */
    private static final class FakeClient implements NodeAdmissionClient {
        private final Set<DiscoveryNode> held = new HashSet<>();
        private final Map<DiscoveryNode, Integer> failUntilAttempt = new HashMap<>();
        private final Map<DiscoveryNode, Integer> attempts = new HashMap<>();

        void failNodeForAttempts(DiscoveryNode node, int attempts) {
            failUntilAttempt.put(node, attempts);
        }

        @Override
        public void reserve(DiscoveryNode node, String leaseId, int slots, ResourcePriority priority, ActionListener<Void> listener) {
            int attempt = attempts.merge(node, 1, Integer::sum);
            if (attempt <= failUntilAttempt.getOrDefault(node, 0)) {
                listener.onFailure(new ResourceRejectedException("node [" + node.getId() + "] is full"));
            } else {
                held.add(node);
                listener.onResponse(null);
            }
        }

        @Override
        public void release(DiscoveryNode node, String leaseId, ActionListener<Void> listener) {
            held.remove(node);
            listener.onResponse(null);
        }
    }
}
