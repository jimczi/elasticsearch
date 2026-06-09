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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Immutable point-in-time snapshot of a {@link ResourcePool}'s counters across both the slot and memory dimensions.
 *
 * <p>The {@code current*} fields are live gauges (capacity in use right now, requests waiting right now); the
 * {@code total*} fields are monotonic counters accumulated over the pool's lifetime. {@link #lanes} carries the same
 * gauges broken down per priority lane. This is the basic-metrics surface; wiring these into the node's
 * {@code MeterRegistry} is left to a later milestone.
 *
 * @param slotCapacity        total slot capacity configured for the pool
 * @param memoryCapacity      total memory-entitlement capacity configured for the pool, in bytes
 * @param currentUsedSlots    slots currently held by live reservations
 * @param currentUsedMemory   memory entitlement currently held by live reservations, in bytes
 * @param peakUsedSlots       high-water mark of {@code currentUsedSlots} over the pool's lifetime
 * @param peakUsedMemory      high-water mark of {@code currentUsedMemory} over the pool's lifetime, in bytes
 * @param currentQueueLength  requests currently waiting for capacity
 * @param totalAcquired       reservations granted over the pool's lifetime (inline plus drained-from-queue)
 * @param totalReleased       reservations closed over the pool's lifetime
 * @param totalRejected       requests rejected because capacity was unavailable and the request was not (or could not
 *                            be) queued, including requests whose demand exceeds total capacity
 * @param totalQueued         requests that were placed on the waiting queue over the pool's lifetime
 * @param totalQueueRejected  requests rejected specifically because the bounded queue was full
 * @param totalCancelled      queued requests cancelled by the caller before admission
 * @param totalTimedOut       queued requests rejected because they waited longer than their timeout for a slot (a subset
 *                            of {@code totalRejected})
 * @param totalReclaimed      borrowed reservations whose reclaim hook was invoked to restore another lane's floor
 * @param lanes               per-lane breakdown of usage, floors, borrowing, and reclaims
 */
public record ResourcePoolStats(
    long slotCapacity,
    long memoryCapacity,
    long currentUsedSlots,
    long currentUsedMemory,
    long peakUsedSlots,
    long peakUsedMemory,
    int currentQueueLength,
    long totalAcquired,
    long totalReleased,
    long totalRejected,
    long totalQueued,
    long totalQueueRejected,
    long totalCancelled,
    long totalTimedOut,
    long totalReclaimed,
    List<ResourceLaneStats> lanes
) implements Writeable, ToXContentFragment {

    public ResourcePoolStats(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVInt(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readCollectionAsImmutableList(ResourceLaneStats::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(slotCapacity);
        out.writeVLong(memoryCapacity);
        out.writeVLong(currentUsedSlots);
        out.writeVLong(currentUsedMemory);
        out.writeVLong(peakUsedSlots);
        out.writeVLong(peakUsedMemory);
        out.writeVInt(currentQueueLength);
        out.writeVLong(totalAcquired);
        out.writeVLong(totalReleased);
        out.writeVLong(totalRejected);
        out.writeVLong(totalQueued);
        out.writeVLong(totalQueueRejected);
        out.writeVLong(totalCancelled);
        out.writeVLong(totalTimedOut);
        out.writeVLong(totalReclaimed);
        out.writeCollection(lanes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("slot_capacity", slotCapacity);
        builder.field("current_used_slots", currentUsedSlots);
        builder.field("current_available_slots", currentAvailableSlots());
        builder.field("peak_used_slots", peakUsedSlots);
        builder.field("memory_capacity", memoryCapacity);
        builder.field("current_used_memory", currentUsedMemory);
        builder.field("peak_used_memory", peakUsedMemory);
        builder.field("current_queue_length", currentQueueLength);
        builder.field("total_acquired", totalAcquired);
        builder.field("total_released", totalReleased);
        builder.field("total_rejected", totalRejected);
        builder.field("total_queued", totalQueued);
        builder.field("total_queue_rejected", totalQueueRejected);
        builder.field("total_cancelled", totalCancelled);
        builder.field("total_timed_out", totalTimedOut);
        builder.field("total_reclaimed", totalReclaimed);
        builder.xContentList("lanes", lanes);
        return builder;
    }

    /** Slots not currently held by any live reservation. Never negative. */
    public long currentAvailableSlots() {
        return slotCapacity - currentUsedSlots;
    }

    /** Memory-entitlement bytes not currently held by any live reservation. Never negative. */
    public long currentAvailableMemory() {
        return memoryCapacity - currentUsedMemory;
    }
}
