/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-lane slice of a {@link ResourcePoolStats} snapshot.
 *
 * @param lane             the priority lane these counters belong to
 * @param floorSlots       the lane's guaranteed slot floor
 * @param floorMemory      the lane's guaranteed memory floor, in bytes
 * @param usedSlots        slots currently held by live reservations in this lane
 * @param usedMemory       memory entitlement currently held by live reservations in this lane, in bytes
 * @param borrowedSlots    slots currently used above the lane's floor (reclaimable by other lanes)
 * @param borrowedMemory   memory currently used above the lane's floor, in bytes (reclaimable by other lanes)
 * @param totalReclaimed   reservations in this lane whose reclaim hook has been invoked over the pool's lifetime
 * @param totalAcquired    reservations admitted into this lane over the pool's lifetime (monotonic)
 */
public record ResourceLaneStats(
    ResourcePriority lane,
    long floorSlots,
    long floorMemory,
    long usedSlots,
    long usedMemory,
    long borrowedSlots,
    long borrowedMemory,
    long totalReclaimed,
    long totalAcquired
) implements Writeable, ToXContentObject {

    public ResourceLaneStats(StreamInput in) throws IOException {
        this(
            in.readEnum(ResourcePriority.class),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(lane);
        out.writeVLong(floorSlots);
        out.writeVLong(floorMemory);
        out.writeVLong(usedSlots);
        out.writeVLong(usedMemory);
        out.writeVLong(borrowedSlots);
        out.writeVLong(borrowedMemory);
        out.writeVLong(totalReclaimed);
        out.writeVLong(totalAcquired);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("lane", lane.name());
        builder.field("floor_slots", floorSlots);
        builder.field("floor_memory", floorMemory);
        builder.field("used_slots", usedSlots);
        builder.field("used_memory", usedMemory);
        builder.field("borrowed_slots", borrowedSlots);
        builder.field("borrowed_memory", borrowedMemory);
        builder.field("total_reclaimed", totalReclaimed);
        builder.field("total_acquired", totalAcquired);
        builder.endObject();
        return builder;
    }
}
