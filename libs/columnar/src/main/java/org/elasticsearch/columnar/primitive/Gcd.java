/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.primitive;

import org.elasticsearch.columnar.encoder.NumericBlockEncoder;

/**
 * Greatest common divisor utilities for long blocks. Used by GCD-aware
 * {@link NumericBlockEncoder} implementations to detect a common factor across all values in a
 * block (e.g. timestamps at day granularity share 86,400,000) and divide it out before
 * bit-packing, which shrinks the bit width substantially when the common factor is large.
 *
 * <p>Mirrors the role TSDB-style pipelines give to a GCD stage; the primitive lives here so
 * downstream encoders can compose it without depending on the encoder taxonomy.
 */
public final class Gcd {

    public Gcd() {}

    /**
     * Euclidean GCD of two non-negative longs. Returns the absolute value when either input
     * is negative — the result is always non-negative. {@code gcd(0, x) == |x|}.
     */
    public static long gcd(long a, long b) {
        a = Math.abs(a);
        b = Math.abs(b);
        while (b != 0) {
            final long t = b;
            b = a % b;
            a = t;
        }
        return a;
    }

    /**
     * GCD of all values in {@code values[offset, offset + len)}. Returns {@code 0} for an
     * empty input, {@code |values[offset]|} when {@code len == 1}, and stops the iteration
     * early once the running GCD reaches {@code 1} (no further reduction possible).
     *
     * <p>For typical date-at-millisecond data this returns a large value (e.g. {@code
     * 86_400_000} for day-granularity). For random data this falls to {@code 1} after a few
     * elements and the caller should skip the GCD path.
     */
    public static long gcdOfBlock(long[] values, int offset, int len) {
        if (len <= 0) {
            return 0L;
        }
        long g = Math.abs(values[offset]);
        for (int i = 1; i < len && g != 1L; i++) {
            g = gcd(g, values[offset + i]);
        }
        return g;
    }
}
