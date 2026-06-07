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
 * The guaranteed floor of a single priority lane in a {@link ResourcePool}: the minimum {@code slots} and
 * {@code memoryBytes} the lane is entitled to even when other lanes are saturated.
 *
 * <p>A lane may use beyond its floor by borrowing idle capacity from other lanes, but capacity used above a lane's floor
 * is reclaimable: when a lane is below its floor and has work waiting, the pool reclaims borrowed capacity from other
 * lanes to restore that floor. A lane's floor itself is never reclaimed, which is what guarantees minimum progress for
 * lower-priority (e.g. unboosted) work.
 */
public record ResourceLaneBudget(long slots, long memoryBytes) {
    public ResourceLaneBudget {
        if (slots < 0) {
            throw new IllegalArgumentException("lane floor slots must be non-negative but was [" + slots + "]");
        }
        if (memoryBytes < 0) {
            throw new IllegalArgumentException("lane floor memory bytes must be non-negative but was [" + memoryBytes + "]");
        }
    }

    public static final ResourceLaneBudget NONE = new ResourceLaneBudget(0, 0);
}
