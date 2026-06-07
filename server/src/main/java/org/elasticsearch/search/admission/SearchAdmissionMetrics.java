/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.common.resource.ResourceLaneStats;
import org.elasticsearch.common.resource.ResourcePoolStats;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * Registers node-level gauges and counters for shard-search admission so admins can see what the resource manager is
 * doing — slots/memory in use, queue depth, per-lane usage, and lifetime rejections/timeouts/reclaims. The values are
 * read on each metrics collection from a {@link ResourcePoolStats} supplier; when admission is disabled the supplier
 * yields {@code null} and every gauge reports zero / no lanes.
 */
public class SearchAdmissionMetrics {

    public SearchAdmissionMetrics(MeterRegistry meter, Supplier<ResourcePoolStats> stats) {
        meter.registerLongGauge(
            "es.search.admission.slots.current_used",
            "Shard-search admission slots currently held by live reservations",
            "slots",
            () -> gauge(stats, ResourcePoolStats::currentUsedSlots)
        );
        meter.registerLongGauge(
            "es.search.admission.slots.current_available",
            "Shard-search admission slots currently free",
            "slots",
            () -> gauge(stats, ResourcePoolStats::currentAvailableSlots)
        );
        meter.registerLongGauge(
            "es.search.admission.memory.current_used",
            "Shard-search admission memory entitlement currently held by live reservations",
            "bytes",
            () -> gauge(stats, ResourcePoolStats::currentUsedMemory)
        );
        meter.registerLongGauge(
            "es.search.admission.queue.current_size",
            "Requests currently waiting for a shard-search admission slot",
            "requests",
            () -> gauge(stats, s -> s.currentQueueLength())
        );
        meter.registerLongAsyncCounter(
            "es.search.admission.rejected.total",
            "Shard-search admission requests rejected over the node's lifetime",
            "requests",
            () -> gauge(stats, ResourcePoolStats::totalRejected)
        );
        meter.registerLongAsyncCounter(
            "es.search.admission.timed_out.total",
            "Shard-search admission requests rejected after waiting too long for a slot",
            "requests",
            () -> gauge(stats, ResourcePoolStats::totalTimedOut)
        );
        meter.registerLongAsyncCounter(
            "es.search.admission.reclaimed.total",
            "Borrowed reservations reclaimed to restore another lane's floor over the node's lifetime",
            "reservations",
            () -> gauge(stats, ResourcePoolStats::totalReclaimed)
        );
        meter.registerLongsGauge(
            "es.search.admission.lane.current_used_slots",
            "Shard-search admission slots currently held per priority lane",
            "slots",
            () -> laneGauge(stats)
        );
    }

    private static LongWithAttributes gauge(Supplier<ResourcePoolStats> stats, ToLongFunction<ResourcePoolStats> value) {
        ResourcePoolStats snapshot = stats.get();
        return new LongWithAttributes(snapshot == null ? 0L : value.applyAsLong(snapshot));
    }

    private static Collection<LongWithAttributes> laneGauge(Supplier<ResourcePoolStats> stats) {
        ResourcePoolStats snapshot = stats.get();
        if (snapshot == null) {
            return List.of();
        }
        List<LongWithAttributes> measurements = new ArrayList<>(snapshot.lanes().size());
        for (ResourceLaneStats lane : snapshot.lanes()) {
            measurements.add(new LongWithAttributes(lane.usedSlots(), Map.of("lane", lane.lane().name())));
        }
        return measurements;
    }
}
