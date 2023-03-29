/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment.extract;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestExtractTextAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "extract_text";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_extract_text/{name}")
        );
    }
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        ExtractTextRequest transportRequest = new ExtractTextRequest(request.param("name"), request.requiredContent());
        return channel -> client.execute(ExtractTextAction.INSTANCE, transportRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }
}
