/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.primitive.BitPacking;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class BitPackingTests extends ESTestCase {

    public void testRequiredLongsRoundsUp() {
        assertEquals(1, BitPacking.requiredLongs(1, 1));
        assertEquals(1, BitPacking.requiredLongs(64, 1));
        assertEquals(2, BitPacking.requiredLongs(65, 1));
        assertEquals(2, BitPacking.requiredLongs(128, 1));
        assertEquals(1, BitPacking.requiredLongs(8, 8));
        assertEquals(2, BitPacking.requiredLongs(9, 8));
    }

    public void testRoundTripAcrossBitWidthsAndBlockSizes() {
        for (int bitsPerValue : new int[] { 1, 2, 3, 5, 7, 8, 11, 12, 16, 24, 31, 32, 47, 63 }) {
            for (int blockSize : new int[] { 1, 7, 16, 64, 127, 128, 256, 1024 }) {
                assertRoundTrip(bitsPerValue, blockSize, randomSeed());
            }
        }
    }

    public void testZeroValuesAreEncodedFaithfully() {
        final long[] values = new long[128];
        assertRoundTripExplicit(values, 12);
    }

    public void testMaxValuesAreEncodedFaithfully() {
        for (int bitsPerValue : new int[] { 1, 4, 7, 8, 16, 31, 63 }) {
            final long[] values = new long[128];
            Arrays.fill(values, (1L << bitsPerValue) - 1L);
            assertRoundTripExplicit(values, bitsPerValue);
        }
    }

    public void testRejectsOutOfRangeBitsPerValue() {
        expectThrows(IllegalArgumentException.class, () -> BitPacking.requiredLongs(1, 0));
        expectThrows(IllegalArgumentException.class, () -> BitPacking.requiredLongs(1, 64));
        expectThrows(IllegalArgumentException.class, () -> BitPacking.pack(new long[1], 0, 1, 0, new long[1]));
        expectThrows(IllegalArgumentException.class, () -> BitPacking.unpack(new long[1], 0, 1, 64, new long[1], 0));
    }

    private void assertRoundTrip(int bitsPerValue, int blockSize, long seed) {
        final java.util.Random rnd = new java.util.Random(seed);
        final long max = bitsPerValue == 63 ? Long.MAX_VALUE : (1L << bitsPerValue) - 1L;
        final long[] values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = (rnd.nextLong() & Long.MAX_VALUE) % (max + 1);
        }
        assertRoundTripExplicit(values, bitsPerValue);
    }

    private static void assertRoundTripExplicit(long[] values, int bitsPerValue) {
        final int requiredLongs = BitPacking.requiredLongs(values.length, bitsPerValue);
        final long[] packed = new long[requiredLongs];
        final int written = BitPacking.pack(values, 0, values.length, bitsPerValue, packed);
        assertEquals(requiredLongs, written);

        final long[] decoded = new long[values.length];
        BitPacking.unpack(packed, 0, values.length, bitsPerValue, decoded, 0);
        assertArrayEquals(values, decoded);
    }

    private long randomSeed() {
        return randomLong();
    }
}
