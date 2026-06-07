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
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class QueryMemoryBreakerTests extends ESTestCase {

    public void testWrapReturnsParentWhenBudgetDisabled() {
        CircuitBreaker parent = new LimitedBreaker("parent", 1000);
        assertSame(parent, QueryMemoryBreaker.wrap(parent, "q", 0));
        assertSame(parent, QueryMemoryBreaker.wrap(parent, "q", -1));
    }

    public void testEnforcesOwnBudgetAndFailsOnlyThisQuery() {
        CircuitBreaker parent = new LimitedBreaker("parent", 10_000);
        CircuitBreaker breaker = QueryMemoryBreaker.wrap(parent, "q", 100);

        breaker.addEstimateBytesAndMaybeBreak(100, "ok");
        assertEquals(100, breaker.getUsed());
        assertEquals(100, parent.getUsed());

        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> breaker.addEstimateBytesAndMaybeBreak(1, "over"));
        assertEquals(100, e.getByteLimit());
        // The failed allocation charged nothing, to this breaker or the parent.
        assertEquals(100, breaker.getUsed());
        assertEquals(100, parent.getUsed());
        assertEquals(1, breaker.getTrippedCount());
    }

    public void testChargesAndReleasesParent() {
        CircuitBreaker parent = new LimitedBreaker("parent", 10_000);
        CircuitBreaker breaker = QueryMemoryBreaker.wrap(parent, "q", 1000);

        breaker.addEstimateBytesAndMaybeBreak(400, "a");
        breaker.addWithoutBreaking(100);
        assertEquals(500, breaker.getUsed());
        assertEquals(500, parent.getUsed());

        breaker.addWithoutBreaking(-500);
        assertEquals(0, breaker.getUsed());
        assertEquals(0, parent.getUsed());
    }

    public void testOneQueryExceedingItsBudgetDoesNotStopAnother() {
        CircuitBreaker parent = new LimitedBreaker("parent", 1000);
        CircuitBreaker a = QueryMemoryBreaker.wrap(parent, "a", 100);
        CircuitBreaker b = QueryMemoryBreaker.wrap(parent, "b", 1000);

        a.addEstimateBytesAndMaybeBreak(100, "a-fill");
        expectThrows(CircuitBreakingException.class, () -> a.addEstimateBytesAndMaybeBreak(1, "a-over"));

        // b is unaffected by a's failure and can use the rest of the shared node budget.
        b.addEstimateBytesAndMaybeBreak(900, "b-fill"); // parent now at 1000
        // b only fails at the node-global limit, not its own (still under 1000).
        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> b.addEstimateBytesAndMaybeBreak(1, "b-node"));
        assertEquals(1000, e.getByteLimit()); // node limit, not b's budget
    }

    public void testParentTripPropagatesAndRollsBackLocal() {
        CircuitBreaker parent = new LimitedBreaker("parent", 100);
        CircuitBreaker breaker = QueryMemoryBreaker.wrap(parent, "q", 1000); // budget larger than the node

        expectThrows(CircuitBreakingException.class, () -> breaker.addEstimateBytesAndMaybeBreak(150, "big"));
        // Local reservation was rolled back when the parent tripped.
        assertEquals(0, breaker.getUsed());
        assertEquals(0, parent.getUsed());
    }

    public void testCloseReturnsResidualToParent() {
        CircuitBreaker parent = new LimitedBreaker("parent", 10_000);
        QueryMemoryBreaker breaker = (QueryMemoryBreaker) QueryMemoryBreaker.wrap(parent, "q", 1000);

        breaker.addEstimateBytesAndMaybeBreak(300, "leak"); // never explicitly released
        assertEquals(300, parent.getUsed());

        breaker.close();
        assertEquals(0, breaker.getUsed());
        assertEquals(0, parent.getUsed());
    }

    /** Minimal parent breaker: tracks bytes and trips at a fixed limit, like the node REQUEST breaker. */
    private static final class LimitedBreaker implements CircuitBreaker {
        private final String name;
        private final long limit;
        private final AtomicLong used = new AtomicLong();

        LimitedBreaker(String name, long limit) {
            this.name = name;
            this.limit = limit;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            long current = used.addAndGet(bytes);
            if (bytes > 0 && current > limit) {
                used.addAndGet(-bytes);
                circuitBreak(label, bytes);
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            throw new CircuitBreakingException("parent limit exceeded", bytesNeeded, limit, Durability.TRANSIENT);
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
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
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
        public void setLimitAndOverhead(long limit, double overhead) {}
    }
}
