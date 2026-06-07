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
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

/**
 * A {@link CircuitBreakerService} that substitutes a per-query {@link QueryMemoryBreaker} for the {@code REQUEST}
 * breaker, delegating every other breaker to the node service. Pass it to a {@link org.elasticsearch.common.util.BigArrays}
 * (via {@code withBreakerService}) so that work allocating through that {@code BigArrays} — and anything resolving the
 * {@code REQUEST} breaker from it — is bounded by the query's memory budget rather than charging the shared node
 * breaker directly. Mirrors {@link org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService}; like it, the
 * {@code stats()} methods are unsupported because this is a per-query allocation shim, not a node-level stats source.
 */
public class QueryMemoryBreakerService extends CircuitBreakerService {

    private final CircuitBreakerService next;
    private final CircuitBreaker queryBreaker;

    public QueryMemoryBreakerService(CircuitBreakerService next, CircuitBreaker queryBreaker) {
        this.next = next;
        this.queryBreaker = queryBreaker;
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        return CircuitBreaker.REQUEST.equals(name) ? queryBreaker : next.getBreaker(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        throw new UnsupportedOperationException();
    }
}
