/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.search.admission.SearchAdmissionStatsRequest;
import org.elasticsearch.search.admission.TransportSearchAdmissionStatsAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Read-only endpoint exposing the live state of the shard-search admission pool (slots, memory, lanes, queue, rejections,
 * leases) per node, so the resource manager can be observed cluster-wide.
 *
 * <pre>
 *   GET /_search_admission/stats
 *   GET /_search_admission/stats/{nodeId}
 * </pre>
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSearchAdmissionStatsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "search_admission_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_search_admission/stats"), new Route(GET, "/_search_admission/stats/{nodeId}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String[] nodeIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        SearchAdmissionStatsRequest statsRequest = new SearchAdmissionStatsRequest(nodeIds);
        return channel -> client.execute(TransportSearchAdmissionStatsAction.TYPE, statsRequest, new NodesResponseRestListener<>(channel));
    }
}
