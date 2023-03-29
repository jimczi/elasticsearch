/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.attachment.extract;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ExtractTextResponse extends ActionResponse implements StatusToXContentObject {
    private final long contentChecksum;
    private final long contentLen;

    ExtractTextResponse(long contentChecksum, long contentLen) {
        this.contentChecksum = contentChecksum;
        this.contentLen = contentLen;
    }

    ExtractTextResponse(StreamInput in) throws IOException {
        super(in);
        this.contentChecksum = in.readLong();
        this.contentLen = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(contentChecksum);
        out.writeLong(contentLen);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("content_checksum", contentChecksum);
        builder.field("content_len", contentLen);
        builder.endObject();
        return builder;
    }

    @Override
    public RestStatus status() {
        return RestStatus.OK;
    }
}
