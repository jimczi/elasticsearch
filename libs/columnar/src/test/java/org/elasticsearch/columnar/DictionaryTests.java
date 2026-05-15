/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.primitive.Dictionary;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class DictionaryTests extends ESTestCase {

    public void testEmpty() {
        final int[] indices = new int[0];
        final long[] dict = new long[0];
        final int dictSize = Dictionary.encode(new long[0], 0, 0, indices, 0, dict, 0);
        assertEquals(0, dictSize);
    }

    public void testAllSame() {
        final long[] in = new long[16];
        Arrays.fill(in, 42L);
        final int[] indices = new int[in.length];
        final long[] dict = new long[in.length];
        final int dictSize = Dictionary.encode(in, 0, in.length, indices, 0, dict, 0);
        assertEquals(1, dictSize);
        assertEquals(42L, dict[0]);
        for (int idx : indices) {
            assertEquals(0, idx);
        }
    }

    public void testSortedDictionary() {
        final long[] in = { 7, 3, 9, 3, 7, 1, 9 };
        final int[] indices = new int[in.length];
        final long[] dict = new long[in.length];
        final int dictSize = Dictionary.encode(in, 0, in.length, indices, 0, dict, 0);
        assertEquals(4, dictSize);
        // sorted ascending
        for (int i = 1; i < dictSize; i++) {
            assertTrue("dictionary not sorted at " + i, dict[i - 1] < dict[i]);
        }
        // indices in range
        for (int idx : indices) {
            assertTrue(idx >= 0 && idx < dictSize);
        }

        final long[] decoded = new long[in.length];
        Dictionary.decode(indices, 0, in.length, dict, 0, decoded, 0);
        assertArrayEquals(in, decoded);
    }

    public void testAllDistinct() {
        final long[] in = { 5, 4, 3, 2, 1 };
        final int[] indices = new int[in.length];
        final long[] dict = new long[in.length];
        final int dictSize = Dictionary.encode(in, 0, in.length, indices, 0, dict, 0);
        assertEquals(5, dictSize);
        assertArrayEquals(new long[] { 1, 2, 3, 4, 5 }, Arrays.copyOf(dict, dictSize));
        assertArrayEquals(new int[] { 4, 3, 2, 1, 0 }, indices);

        final long[] decoded = new long[in.length];
        Dictionary.decode(indices, 0, in.length, dict, 0, decoded, 0);
        assertArrayEquals(in, decoded);
    }

    public void testRandomRoundTrip() {
        for (int blockSize : new int[] { 1, 16, 128, 1024 }) {
            for (int cardinality : new int[] { 1, 4, 16, 100, 1024 }) {
                if (cardinality > blockSize) {
                    continue;
                }
                final long[] in = randomBlockWithCardinality(blockSize, cardinality);
                assertRoundTrip(in);
            }
        }
    }

    public void testNegativeValues() {
        final long[] in = { -10, 5, -10, 0, 5, -10, Long.MIN_VALUE, Long.MAX_VALUE };
        assertRoundTrip(in);
    }

    private void assertRoundTrip(long[] in) {
        final int[] indices = new int[in.length];
        final long[] dict = new long[in.length];
        final int dictSize = Dictionary.encode(in, 0, in.length, indices, 0, dict, 0);
        final long[] decoded = new long[in.length];
        Dictionary.decode(indices, 0, in.length, dict, 0, decoded, 0);
        assertArrayEquals(in, decoded);
        for (int i = 1; i < dictSize; i++) {
            assertTrue("dictionary not sorted at " + i, dict[i - 1] < dict[i]);
        }
    }

    private long[] randomBlockWithCardinality(int blockSize, int cardinality) {
        final long[] palette = new long[cardinality];
        for (int i = 0; i < cardinality; i++) {
            palette[i] = randomLong();
        }
        final long[] out = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            out[i] = palette[randomIntBetween(0, cardinality - 1)];
        }
        return out;
    }
}
