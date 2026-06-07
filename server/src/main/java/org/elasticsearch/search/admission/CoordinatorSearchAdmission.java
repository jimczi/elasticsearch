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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.resource.ResourceRejectedException;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator-side distributed admission for a single search: reserves capacity on <em>every</em> participating node
 * before the search runs, so that an accepted search is guaranteed it can run (the "accept = guaranteed to run"
 * contract). Strict all-or-nothing — if any node cannot admit, the partial reservations are rolled back and the search
 * waits in a small coordinator queue, retrying the whole reservation until an accept deadline, then it is rejected.
 *
 * <p>Partial reservations are never held across a wait, so two large searches grabbing disjoint node subsets cannot
 * deadlock — they roll back and retry rather than block each other.
 *
 * <p>The lease id is the coordinator search task id, so the data node can recognise shard work that is already covered
 * by a coordinator lease and skip its own local admission (avoiding double counting).
 */
public class CoordinatorSearchAdmission {

    private final NodeAdmissionClient client;
    private final ThreadPool threadPool;
    private final TimeValue acceptTimeout;
    private final TimeValue retryInterval;
    private final int maxQueuedSearches;

    // Number of searches currently waiting (retrying) for admission; bounds the coordinator queue.
    private final AtomicInteger queued = new AtomicInteger();

    public CoordinatorSearchAdmission(
        NodeAdmissionClient client,
        ThreadPool threadPool,
        TimeValue acceptTimeout,
        TimeValue retryInterval,
        int maxQueuedSearches
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.acceptTimeout = acceptTimeout;
        this.retryInterval = retryInterval;
        this.maxQueuedSearches = maxQueuedSearches;
    }

    /**
     * Admits a search whose per-node slot demand is {@code demand}, completing {@code listener} with a {@link Releasable}
     * that releases every node lease when the search finishes, or failing it with a {@link ResourceRejectedException} if
     * the search cannot be admitted within the accept deadline.
     */
    public void admit(String leaseId, Map<DiscoveryNode, Integer> demand, ResourcePriority priority, ActionListener<Releasable> listener) {
        if (demand.isEmpty()) {
            listener.onResponse(() -> {});
            return;
        }
        long deadlineNanos = threadPool.relativeTimeInNanos() + acceptTimeout.nanos();
        attempt(leaseId, demand, priority, deadlineNanos, false, listener);
    }

    private void attempt(
        String leaseId,
        Map<DiscoveryNode, Integer> demand,
        ResourcePriority priority,
        long deadlineNanos,
        boolean counted,
        ActionListener<Releasable> listener
    ) {
        final List<DiscoveryNode> nodes = List.copyOf(demand.keySet());
        final Set<DiscoveryNode> granted = ConcurrentHashMap.newKeySet();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final CountDown countDown = new CountDown(nodes.size());
        for (DiscoveryNode node : nodes) {
            client.reserve(node, leaseId, demand.get(node), priority, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    granted.add(node);
                    finish();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.compareAndSet(null, e);
                    finish();
                }

                private void finish() {
                    if (countDown.countDown()) {
                        onAllResponded(leaseId, demand, priority, deadlineNanos, counted, listener, granted, failure.get());
                    }
                }
            });
        }
    }

    private void onAllResponded(
        String leaseId,
        Map<DiscoveryNode, Integer> demand,
        ResourcePriority priority,
        long deadlineNanos,
        boolean counted,
        ActionListener<Releasable> listener,
        Set<DiscoveryNode> granted,
        Exception failure
    ) {
        if (failure == null) {
            // Every node admitted: hand back a releasable that frees all the leases when the search finishes.
            if (counted) {
                queued.decrementAndGet();
            }
            listener.onResponse(releasableFor(leaseId, demand.keySet()));
            return;
        }
        // At least one node rejected: roll back the partial reservation, then decide whether to retry or give up.
        releaseAll(leaseId, granted, () -> {
            if (threadPool.relativeTimeInNanos() >= deadlineNanos) {
                if (counted) {
                    queued.decrementAndGet();
                }
                listener.onFailure(rejection(leaseId, "accept timeout exceeded", failure));
                return;
            }
            boolean nowCounted = counted;
            if (counted == false) {
                if (queued.incrementAndGet() > maxQueuedSearches) {
                    queued.decrementAndGet();
                    listener.onFailure(rejection(leaseId, "coordinator admission queue is full", failure));
                    return;
                }
                nowCounted = true;
            }
            final boolean c = nowCounted;
            threadPool.schedule(() -> attempt(leaseId, demand, priority, deadlineNanos, c, listener), retryInterval, threadPool.generic());
        });
    }

    // Releases every granted lease, then runs {@code after}. Release failures are ignored — the node's connection-loss
    // backstop and lease idempotency make a missed release safe.
    private void releaseAll(String leaseId, Collection<DiscoveryNode> nodes, Runnable after) {
        if (nodes.isEmpty()) {
            after.run();
            return;
        }
        final CountDown countDown = new CountDown(nodes.size());
        final Runnable maybeAfter = () -> {
            if (countDown.countDown()) {
                after.run();
            }
        };
        for (DiscoveryNode node : nodes) {
            client.release(node, leaseId, ActionListener.wrap(ignored -> maybeAfter.run(), e -> maybeAfter.run()));
        }
    }

    private Releasable releasableFor(String leaseId, Collection<DiscoveryNode> nodes) {
        final List<DiscoveryNode> snapshot = List.copyOf(nodes);
        return Releasables.releaseOnce(() -> {
            for (DiscoveryNode node : snapshot) {
                client.release(node, leaseId, ActionListener.noop());
            }
        });
    }

    private static ResourceRejectedException rejection(String leaseId, String reason, Exception lastFailure) {
        ResourceRejectedException e = new ResourceRejectedException(
            "search [" + leaseId + "] could not reserve resources on all participating nodes: " + reason
        );
        if (lastFailure != null) {
            e.addSuppressed(lastFailure);
        }
        return e;
    }

    // Visible for testing: searches currently waiting for admission.
    int queuedCount() {
        return queued.get();
    }
}
