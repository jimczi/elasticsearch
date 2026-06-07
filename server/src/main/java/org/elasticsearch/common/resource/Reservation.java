/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.core.Releasable;

/**
 * A granted, two-dimensional slice of a {@link ResourcePool}'s capacity: a number of execution {@link #slots()} plus an
 * entitlement to {@link #memoryBytes()} of the pool's memory budget.
 *
 * <p>The memory dimension is an admission-time <em>entitlement</em>, not byte-accurate accounting — the circuit breaker
 * remains the fine-grained enforcer of actual bytes. Reserving the entitlement up front lets the pool reject at
 * admission when memory headroom is gone, instead of letting work start and trip the breaker mid-flight.
 *
 * <p>A reservation is a {@link Releasable}: the holder owns its slots and memory entitlement until it calls
 * {@link #close()}, which returns both and lets the pool admit waiting requests. Closing is idempotent — closing more
 * than once is a no-op rather than an over-release — so it is safe with try-with-resources and best-effort cleanup
 * paths. Every reservation handed to a caller must eventually be closed, including on failure and cancellation, or the
 * pool leaks capacity.
 */
public interface Reservation extends Releasable {

    /** The number of execution slots held by this reservation. Always positive. */
    long slots();

    /** The number of bytes of the pool's memory budget this reservation is entitled to. Never negative. */
    long memoryBytes();

    /** The priority class this reservation was acquired under. */
    ResourcePriority priority();

    @Override
    void close();
}
