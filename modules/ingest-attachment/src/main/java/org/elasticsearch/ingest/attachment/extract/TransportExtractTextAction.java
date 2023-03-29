/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment.extract;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportExtractTextAction extends HandledTransportAction<ExtractTextRequest, ExtractTextResponse> {
    @Inject
    public TransportExtractTextAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        NodeClient client,
        ActionFilters actionFilters
    ) {
        super(ExtractTextAction.NAME, transportService, actionFilters, ExtractTextRequest::new, ThreadPool.Names.WRITE);
    }

    @Override
    protected void doExecute(Task task, ExtractTextRequest request, ActionListener<ExtractTextResponse> listener) {
        if (request.getContent() instanceof ReleasableBytesReference) {
            System.out.println(request.getContent().getClass());
        }
        try (BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(request.getContent().streamInput(), "content")) {
            input.skip(request.getContent().length());
            listener.onResponse(new ExtractTextResponse(input.getChecksum(), request.getContent().length()));
        } catch (Exception exc) {
            listener.onFailure(exc);;
        }
    }
}


