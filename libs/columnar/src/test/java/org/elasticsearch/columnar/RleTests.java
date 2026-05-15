/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.primitive.Rle;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class RleTests extends ESTestCase {

    public void testEmpty() {
        final long[] outValues = new long[0];
        final int[] outCounts = new int[0];
        final int runs = Rle.encode(new long[0], 0, 0, outValues, 0, outCounts, 0);
        assertEquals(0, runs);
    }

    public void testSingleValue() {
        final long[] outValues = new long[1];
        final int[] outCounts = new int[1];
        final int runs = Rle.encode(new long[] { 42 }, 0, 1, outValues, 0, outCounts, 0);
        assertEquals(1, runs);
        assertEquals(42L, outValues[0]);
        assertEquals(1, outCounts[0]);
    }

    public void testAllSame() {
        final long[] in = new long[16];
        Arrays.fill(in, 7L);
        final long[] outValues = new long[16];
        final int[] outCounts = new int[16];
        final int runs = Rle.encode(in, 0, in.length, outValues, 0, outCounts, 0);
        assertEquals(1, runs);
        assertEquals(7L, outValues[0]);
        assertEquals(16, outCounts[0]);
    }

    public void testAllDistinct() {
        final long[] in = { 1, 2, 3, 4, 5 };
        final long[] outValues = new long[in.length];
        final int[] outCounts = new int[in.length];
        final int runs = Rle.encode(in, 0, in.length, outValues, 0, outCounts, 0);
        assertEquals(5, runs);
        for (int i = 0; i < 5; i++) {
            assertEquals(in[i], outValues[i]);
            assertEquals(1, outCounts[i]);
        }
    }

    public void testMixedRuns() {
        final long[] in = { 5, 5, 5, 7, 7, 9, 9, 9, 9, 2 };
        final long[] outValues = new long[in.length];
        final int[] outCounts = new int[in.length];
        final int runs = Rle.encode(in, 0, in.length, outValues, 0, outCounts, 0);
        assertEquals(4, runs);
        assertArrayEquals(new long[] { 5, 7, 9, 2 }, Arrays.copyOf(outValues, runs));
        assertArrayEquals(new int[] { 3, 2, 4, 1 }, Arrays.copyOf(outCounts, runs));

        final long[] decoded = new long[in.length];
        final int written = Rle.decode(outValues, 0, outCounts, 0, runs, decoded, 0);
        assertEquals(in.length, written);
        assertArrayEquals(in, decoded);
    }

    public void testRandomRoundTrip() {
        for (int blockSize : new int[] { 1, 16, 128, 1024 }) {
            for (int distinctValues : new int[] { 1, 4, 16, 64 }) {
                final long[] in = randomLowCardinalityBlock(blockSize, distinctValues);
                assertRoundTrip(in);
            }
        }
    }

    public void testWorstCaseCapacity() {
        // No adjacent duplicates ⇒ runs == len. Output arrays sized to len must suffice.
        final long[] in = { 1, 2, 1, 2, 1, 2 };
        final long[] outValues = new long[in.length];
        final int[] outCounts = new int[in.length];
        final int runs = Rle.encode(in, 0, in.length, outValues, 0, outCounts, 0);
        assertEquals(in.length, runs);
    }

    public void testDecodeRejectsZeroRunLength() {
        final long[] inValues = { 1L };
        final int[] inCounts = { 0 };
        final long[] out = new long[1];
        expectThrows(IllegalArgumentException.class, () -> Rle.decode(inValues, 0, inCounts, 0, 1, out, 0));
    }

    private void assertRoundTrip(long[] in) {
        final long[] outValues = new long[in.length];
        final int[] outCounts = new int[in.length];
        final int runs = Rle.encode(in, 0, in.length, outValues, 0, outCounts, 0);

        final long[] decoded = new long[in.length];
        final int written = Rle.decode(outValues, 0, outCounts, 0, runs, decoded, 0);
        assertEquals(in.length, written);
        assertArrayEquals(in, decoded);
    }

    private long[] randomLowCardinalityBlock(int blockSize, int distinctValues) {
        final long[] dictionary = new long[distinctValues];
        for (int i = 0; i < distinctValues; i++) {
            dictionary[i] = randomLong();
        }
        final long[] out = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            out[i] = dictionary[randomIntBetween(0, distinctValues - 1)];
        }
        return out;
    }
}
