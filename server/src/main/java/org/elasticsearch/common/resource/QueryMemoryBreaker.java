/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A per-query memory breaker: a {@link CircuitBreaker} with its own limit (the query's reserved memory budget) layered
 * on top of a parent breaker (normally the node-global {@code REQUEST} breaker).
 *
 * <p>This adds the granular, per-query view the global breaker lacks. Every allocation is checked against this query's
 * budget first and then charged through to the parent, so:
 * <ul>
 *   <li>a query that exceeds <em>its own</em> budget fails with a query-scoped {@link CircuitBreakingException} — only
 *       that query, not the node — giving us a place to later spill to disk, compact, or return partial results;
 *   <li>the parent still sees every byte, so the node-global total stays accurate and remains the ultimate backstop, and
 *       there is no double counting.
 * </ul>
 *
 * <p>The breaker pairs with a {@link Reservation}: the budget is the reservation's {@link Reservation#memoryBytes()} and
 * its lifetime matches the reservation's. It is a {@link Releasable}: {@link #close()} returns any bytes still tracked
 * to the parent, so a query that leaks an allocation cannot permanently inflate the parent breaker.
 *
 * <p>Use {@link #wrap} to obtain one: when the budget is non-positive (memory dimension disabled) it returns the parent
 * unchanged, so callers can always route allocations through the returned breaker without special-casing.
 */
public final class QueryMemoryBreaker implements CircuitBreaker, Releasable {

    private final CircuitBreaker parent;
    private final String name;
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong trippedCount = new AtomicLong();
    private volatile long limit;

    private QueryMemoryBreaker(CircuitBreaker parent, String name, long limit) {
        this.parent = parent;
        this.name = name;
        this.limit = limit;
    }

    /**
     * Returns a per-query breaker enforcing {@code budgetBytes} on top of {@code parent}, or {@code parent} itself when
     * {@code budgetBytes <= 0} (the memory dimension is disabled, so there is nothing extra to enforce).
     */
    public static CircuitBreaker wrap(CircuitBreaker parent, String name, long budgetBytes) {
        return budgetBytes <= 0 ? parent : create(parent, name, budgetBytes);
    }

    /** Creates a per-query breaker; {@code budgetBytes} must be positive. */
    public static QueryMemoryBreaker create(CircuitBreaker parent, String name, long budgetBytes) {
        if (budgetBytes <= 0) {
            throw new IllegalArgumentException("query memory budget must be positive but was [" + budgetBytes + "]");
        }
        return new QueryMemoryBreaker(parent, name, budgetBytes);
    }

    @Override
    public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        // Reserve against this query's budget first, so a breach charges nothing and fails only this query.
        long current = used.addAndGet(bytes);
        if (bytes > 0 && current > limit) {
            used.addAndGet(-bytes);
            circuitBreak(label, bytes);
        }
        try {
            parent.addEstimateBytesAndMaybeBreak(bytes, label);
        } catch (CircuitBreakingException e) {
            // The node-global breaker tripped; undo our local reservation and propagate the node-scoped failure.
            used.addAndGet(-bytes);
            throw e;
        }
    }

    @Override
    public void addWithoutBreaking(long bytes) {
        used.addAndGet(bytes);
        parent.addWithoutBreaking(bytes);
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        trippedCount.incrementAndGet();
        throw new CircuitBreakingException(
            "Query memory budget ["
                + limit
                + "] for ["
                + name
                + "] exceeded by allocation of ["
                + bytesNeeded
                + "] bytes for ["
                + fieldName
                + "]",
            bytesNeeded,
            limit,
            Durability.TRANSIENT
        );
    }

    /** Returns any bytes still tracked to the parent so a leaked allocation does not permanently inflate it. */
    @Override
    public void close() {
        long residual = used.getAndSet(0);
        if (residual != 0) {
            parent.addWithoutBreaking(-residual);
        }
    }

    @Override
    public long getUsed() {
        return used.get();
    }

    @Override
    public long getLimit() {
        return limit;
    }

    @Override
    public double getOverhead() {
        return parent.getOverhead();
    }

    @Override
    public long getTrippedCount() {
        return trippedCount.get();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Durability getDurability() {
        return Durability.TRANSIENT;
    }

    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        this.limit = limit;
    }
}
