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
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchService.SearchAdmission;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Node-level service that lets a coordinator reserve and release shard-search admission capacity on this (or a remote)
 * node ahead of execution — the local substrate of distributed admission (RD). A reservation is held as a <b>lease</b>
 * tracked by id; the coordinator releases it explicitly when the query finishes, and as a leak backstop the node
 * releases a coordinator's leases if that coordinator disconnects.
 *
 * <p>This first version only manages the lease lifecycle over the node-local {@link SearchService#admitSearchWork
 * admission pool}; wiring actual shard/compute execution to run <em>under</em> a lease (instead of acquiring locally) is
 * a later step.
 */
public class SearchAdmissionService implements NodeAdmissionClient {

    public static final String RESERVE_ACTION_NAME = "internal:admission/search/reserve";
    public static final String RELEASE_ACTION_NAME = "internal:admission/search/release";

    private final TransportService transportService;
    private final SearchService searchService;
    private final Executor searchExecutor;
    private final ConcurrentHashMap<String, Lease> leases = new ConcurrentHashMap<>();

    private record Lease(String id, String ownerNodeId, SearchAdmission admission) {}

    public SearchAdmissionService(TransportService transportService, SearchService searchService) {
        this.transportService = transportService;
        this.searchService = searchService;
        this.searchExecutor = transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
        transportService.registerRequestHandler(
            RESERVE_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ReserveSearchResourcesRequest::new,
            this::handleReserve
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ReleaseSearchResourcesRequest::new,
            this::handleRelease
        );
        // Leak backstop: if a coordinator drops, release every lease it was holding here.
        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeDisconnected(DiscoveryNode node, Exception closeException) {
                releaseForNode(node.getId());
            }
        });
    }

    // -- node-local lease lifecycle ----------------------------------------------------------------------------------

    /**
     * Reserves capacity on this node under {@code leaseId}, attributing the lease to {@code ownerNodeId} (for
     * disconnect cleanup; may be {@code null}). Completes the listener once the lease is held, or fails it on rejection.
     */
    public void reserveLocally(int slots, ResourcePriority priority, String ownerNodeId, String leaseId, ActionListener<Void> listener) {
        searchService.admitSearchWork(slots, priority, searchExecutor, listener.delegateFailureAndWrap((l, admission) -> {
            Lease previous = leases.putIfAbsent(leaseId, new Lease(leaseId, ownerNodeId, admission));
            if (previous != null) {
                // A lease with this id already exists; drop the duplicate reservation rather than overwrite/leak it.
                admission.releasable().close();
                l.onFailure(new IllegalStateException("duplicate search admission lease id [" + leaseId + "]"));
                return;
            }
            l.onResponse(null);
        }));
    }

    /** Releases the lease with {@code leaseId}. Idempotent: returns false if there was no such lease. */
    public boolean releaseLocally(String leaseId) {
        Lease lease = leases.remove(leaseId);
        if (lease != null) {
            lease.admission().releasable().close();
            return true;
        }
        return false;
    }

    private void releaseForNode(String ownerNodeId) {
        if (ownerNodeId == null) {
            return;
        }
        leases.values().removeIf(lease -> {
            if (ownerNodeId.equals(lease.ownerNodeId())) {
                lease.admission().releasable().close();
                return true;
            }
            return false;
        });
    }

    /** The number of leases currently held on this node. Visible for testing/metrics. */
    public int openLeaseCount() {
        return leases.size();
    }

    // -- coordinator-side client -------------------------------------------------------------------------------------

    /** Reserves capacity on {@code node} under {@code leaseId}; the listener fails with a rejection if it cannot. */
    @Override
    public void reserve(DiscoveryNode node, String leaseId, int slots, ResourcePriority priority, ActionListener<Void> listener) {
        transportService.sendRequest(
            transportService.getConnection(node),
            RESERVE_ACTION_NAME,
            new ReserveSearchResourcesRequest(leaseId, slots, priority),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener.delegateFailure((l, empty) -> l.onResponse(null)),
                in -> ActionResponse.Empty.INSTANCE,
                TransportResponseHandler.TRANSPORT_WORKER
            )
        );
    }

    /** Releases the lease {@code leaseId} on {@code node}. */
    @Override
    public void release(DiscoveryNode node, String leaseId, ActionListener<Void> listener) {
        transportService.sendRequest(
            transportService.getConnection(node),
            RELEASE_ACTION_NAME,
            new ReleaseSearchResourcesRequest(leaseId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener.delegateFailure((l, empty) -> l.onResponse(null)),
                in -> ActionResponse.Empty.INSTANCE,
                TransportResponseHandler.TRANSPORT_WORKER
            )
        );
    }

    // -- transport handlers ------------------------------------------------------------------------------------------

    private void handleReserve(ReserveSearchResourcesRequest request, TransportChannel channel, Task task) {
        String ownerNodeId = task.getParentTaskId().isSet() ? task.getParentTaskId().getNodeId() : null;
        ChannelActionListener<ActionResponse.Empty> channelListener = new ChannelActionListener<>(channel);
        reserveLocally(
            request.slots(),
            request.priority(),
            ownerNodeId,
            request.leaseId(),
            channelListener.map(ignored -> ActionResponse.Empty.INSTANCE)
        );
    }

    private void handleRelease(ReleaseSearchResourcesRequest request, TransportChannel channel, Task task) {
        releaseLocally(request.leaseId());
        new ChannelActionListener<ActionResponse.Empty>(channel).onResponse(ActionResponse.Empty.INSTANCE);
    }
}
