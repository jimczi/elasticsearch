/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

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
) {}
