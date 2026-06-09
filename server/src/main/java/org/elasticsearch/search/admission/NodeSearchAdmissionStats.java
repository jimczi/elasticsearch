/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * One node's view of the shard-search admission pool: its {@link ResourcePoolStats} snapshot (null when admission is
 * disabled on that node) and the number of distributed-admission leases it currently holds.
 */
public class NodeSearchAdmissionStats extends BaseNodeResponse implements ToXContentFragment {

    @Nullable
    private final ResourcePoolStats poolStats;
    private final int openLeases;

    public NodeSearchAdmissionStats(DiscoveryNode node, @Nullable ResourcePoolStats poolStats, int openLeases) {
        super(node);
        this.poolStats = poolStats;
        this.openLeases = openLeases;
    }

    public NodeSearchAdmissionStats(StreamInput in) throws IOException {
        super(in);
        this.poolStats = in.readOptionalWriteable(ResourcePoolStats::new);
        this.openLeases = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(poolStats);
        out.writeVInt(openLeases);
    }

    @Nullable
    public ResourcePoolStats poolStats() {
        return poolStats;
    }

    public int openLeases() {
        return openLeases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getNode().getId());
        builder.field("name", getNode().getName());
        builder.field("enabled", poolStats != null);
        builder.field("open_leases", openLeases);
        if (poolStats != null) {
            poolStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
