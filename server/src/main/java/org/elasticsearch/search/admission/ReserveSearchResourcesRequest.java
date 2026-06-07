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
import org.elasticsearch.common.resource.ResourcePriority;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

/**
 * Asks a node to reserve {@code slots} units of its shard-search admission budget for a query, under {@code leaseId}.
 * The coordinator generates {@code leaseId} so it can later release the lease idempotently. The reservation succeeds
 * (the transport response is empty) or the node replies with a {@code ResourceRejectedException}.
 */
public class ReserveSearchResourcesRequest extends AbstractTransportRequest {

    private final String leaseId;
    private final int slots;
    private final ResourcePriority priority;

    public ReserveSearchResourcesRequest(String leaseId, int slots, ResourcePriority priority) {
        this.leaseId = leaseId;
        this.slots = slots;
        this.priority = priority;
    }

    public ReserveSearchResourcesRequest(StreamInput in) throws IOException {
        super(in);
        this.leaseId = in.readString();
        this.slots = in.readVInt();
        this.priority = in.readEnum(ResourcePriority.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(leaseId);
        out.writeVInt(slots);
        out.writeEnum(priority);
    }

    public String leaseId() {
        return leaseId;
    }

    public int slots() {
        return slots;
    }

    public ResourcePriority priority() {
        return priority;
    }
}
