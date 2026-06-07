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
 * Priority class attached to a request for capacity from a {@link ResourcePool}.
 *
 * <p>Priority only affects the order in which queued requests are admitted: when capacity is freed, a pool drains its
 * waiting queue strictly in priority order ({@link #HIGH} before {@link #NORMAL} before {@link #LOW}), and in FIFO order
 * within a single priority class. It does <em>not</em> change how much capacity a request consumes, and it does not
 * (yet) reserve a per-class floor or allow one class to borrow another's capacity — those policies are deliberately
 * deferred to a later milestone so the first version stays easy to reason about.
 *
 * <p>The names are intentionally generic. Higher-level callers map their own notion of importance onto these classes
 * (for example a "boosted" index onto {@link #HIGH} and background or unboosted work onto {@link #LOW}); the pool itself
 * stays agnostic about that policy.
 */
public enum ResourcePriority {
    /** Lowest priority. Admitted from the queue only after all waiting {@link #NORMAL} and {@link #HIGH} requests. */
    LOW,
    /** Default priority for ordinary foreground work. */
    NORMAL,
    /** Highest priority. Admitted from the queue ahead of all waiting {@link #NORMAL} and {@link #LOW} requests. */
    HIGH
}
