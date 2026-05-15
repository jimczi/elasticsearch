/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.primitive.Delta;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class DeltaTests extends ESTestCase {

    public void testEncodeFirstOrder() {
        final long[] in = { 10, 13, 17, 22 };
        final long[] out = new long[in.length];
        Delta.encode(in, 0, in.length, out, 0);
        assertArrayEquals(new long[] { 10, 3, 4, 5 }, out);
    }

    public void testDecodeFirstOrder() {
        final long[] in = { 10, 3, 4, 5 };
        final long[] out = new long[in.length];
        Delta.decode(in, 0, in.length, out, 0);
        assertArrayEquals(new long[] { 10, 13, 17, 22 }, out);
    }

    public void testEncodeSecondOrder() {
        final long[] in = { 10, 13, 17, 22 };
        final long[] out = new long[in.length];
        Delta.encode2(in, 0, in.length, out, 0);
        assertArrayEquals(new long[] { 10, 3, 1, 1 }, out);
    }

    public void testDecodeSecondOrder() {
        final long[] in = { 10, 3, 1, 1 };
        final long[] out = new long[in.length];
        Delta.decode2(in, 0, in.length, out, 0);
        assertArrayEquals(new long[] { 10, 13, 17, 22 }, out);
    }

    public void testRoundTripFirstOrderRandom() {
        for (int blockSize : new int[] { 0, 1, 2, 16, 128, 1023 }) {
            final long[] in = randomLongArray(blockSize, -1_000_000L, 1_000_000L);
            assertFirstOrderRoundTrip(in);
        }
    }

    public void testRoundTripSecondOrderRandom() {
        for (int blockSize : new int[] { 0, 1, 2, 3, 16, 128, 1023 }) {
            final long[] in = randomLongArray(blockSize, -1_000_000L, 1_000_000L);
            assertSecondOrderRoundTrip(in);
        }
    }

    public void testInPlaceFirstOrder() {
        final long[] values = { 10, 13, 17, 22 };
        final long[] copy = values.clone();
        Delta.encode(values, 0, values.length, values, 0);
        assertArrayEquals(new long[] { 10, 3, 4, 5 }, values);
        Delta.decode(values, 0, values.length, values, 0);
        assertArrayEquals(copy, values);
    }

    public void testInPlaceSecondOrder() {
        final long[] values = { 10, 13, 17, 22, 28, 35 };
        final long[] copy = values.clone();
        Delta.encode2(values, 0, values.length, values, 0);
        Delta.decode2(values, 0, values.length, values, 0);
        assertArrayEquals(copy, values);
    }

    public void testFirstOrderOnMonotonic() {
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1_700_000_000_000L + i * 1000L;
        }
        final long[] encoded = new long[in.length];
        Delta.encode(in, 0, in.length, encoded, 0);
        // After the base value every delta is exactly 1000.
        for (int i = 1; i < in.length; i++) {
            assertEquals(1000L, encoded[i]);
        }
        assertFirstOrderRoundTrip(in);
    }

    public void testSecondOrderOnMonotonic() {
        final long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1_700_000_000_000L + i * 1000L;
        }
        final long[] encoded = new long[in.length];
        Delta.encode2(in, 0, in.length, encoded, 0);
        // Constant stride collapses to all-zero deltas-of-deltas after the first two elements.
        for (int i = 2; i < in.length; i++) {
            assertEquals(0L, encoded[i]);
        }
        assertSecondOrderRoundTrip(in);
    }

    public void testZigZagSymmetry() {
        for (long v : new long[] { 0, 1, -1, 2, -2, Long.MAX_VALUE, Long.MIN_VALUE + 1, 12345, -12345 }) {
            assertEquals(v, Delta.zigZagDecode(Delta.zigZagEncode(v)));
        }
    }

    public void testZigZagAllRoundTrip() {
        final long[] in = randomLongArray(256, -1_000_000L, 1_000_000L);
        final long[] working = in.clone();
        Delta.zigZagEncodeAll(working, 0, working.length);
        Delta.zigZagDecodeAll(working, 0, working.length);
        assertArrayEquals(in, working);
    }

    private void assertFirstOrderRoundTrip(long[] in) {
        final long[] encoded = new long[in.length];
        Delta.encode(in, 0, in.length, encoded, 0);
        final long[] decoded = new long[in.length];
        Delta.decode(encoded, 0, in.length, decoded, 0);
        assertArrayEquals(in, decoded);
    }

    private void assertSecondOrderRoundTrip(long[] in) {
        final long[] encoded = new long[in.length];
        Delta.encode2(in, 0, in.length, encoded, 0);
        final long[] decoded = new long[in.length];
        Delta.decode2(encoded, 0, in.length, decoded, 0);
        assertArrayEquals(in, decoded);
    }

    private long[] randomLongArray(int len, long minInclusive, long maxInclusive) {
        final long[] out = new long[len];
        for (int i = 0; i < len; i++) {
            out[i] = randomLongBetween(minInclusive, maxInclusive);
        }
        return out;
    }

    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    @SuppressWarnings("unused")
    private static String summarize(long[] a) {
        return Arrays.toString(a);
    }
}
