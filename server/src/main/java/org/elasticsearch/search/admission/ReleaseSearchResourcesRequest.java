/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

/**
 * Releases the lease previously reserved under {@code leaseId}. Idempotent: releasing an unknown or already-released
 * lease is a no-op.
 */
public class ReleaseSearchResourcesRequest extends AbstractTransportRequest {

    private final String leaseId;

    public ReleaseSearchResourcesRequest(String leaseId) {
        this.leaseId = leaseId;
    }

    public ReleaseSearchResourcesRequest(StreamInput in) throws IOException {
        super(in);
        this.leaseId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(leaseId);
    }

    public String leaseId() {
        return leaseId;
    }
}
