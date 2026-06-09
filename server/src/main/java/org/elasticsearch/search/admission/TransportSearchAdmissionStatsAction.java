/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Fans out to every node and collects its shard-search admission {@link ResourcePoolStats} plus held-lease count, so the
 * live state of the resource manager can be observed cluster-wide (e.g. by a load-testing dashboard).
 */
public class TransportSearchAdmissionStatsAction extends TransportNodesAction<
    SearchAdmissionStatsRequest,
    SearchAdmissionStatsResponse,
    TransportSearchAdmissionStatsAction.NodeRequest,
    NodeSearchAdmissionStats,
    Void> {

    public static final ActionType<SearchAdmissionStatsResponse> TYPE = new ActionType<>("cluster:monitor/search_admission/stats");

    private final SearchService searchService;
    private final SearchAdmissionService searchAdmissionService;

    @Inject
    public TransportSearchAdmissionStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SearchService searchService,
        SearchAdmissionService searchAdmissionService
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.searchService = searchService;
        this.searchAdmissionService = searchAdmissionService;
    }

    @Override
    protected SearchAdmissionStatsResponse newResponse(
        SearchAdmissionStatsRequest request,
        List<NodeSearchAdmissionStats> responses,
        List<FailedNodeException> failures
    ) {
        return new SearchAdmissionStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(SearchAdmissionStatsRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeSearchAdmissionStats newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeSearchAdmissionStats(in);
    }

    @Override
    protected NodeSearchAdmissionStats nodeOperation(NodeRequest request, Task task) {
        ResourcePoolStats stats = searchService.searchAdmissionStats(); // null when admission is disabled on this node
        return new NodeSearchAdmissionStats(transportService.getLocalNode(), stats, searchAdmissionService.openLeaseCount());
    }

    public static class NodeRequest extends AbstractTransportRequest {
        public NodeRequest() {}

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }
}
