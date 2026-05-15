/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.columnar.encoder.BitPackBlockEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class BitPackBlockEncoderTests extends ESTestCase {

    public void testAllZeros() throws IOException {
        final long[] values = new long[128];
        assertRoundTrip(values);
        // bitsPerValue=0 means header only (9 bytes).
        final byte[] buf = new byte[BitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int len = BitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        assertEquals(9, len);
    }

    public void testAllSame() throws IOException {
        final long[] values = new long[128];
        Arrays.fill(values, 42L);
        assertRoundTrip(values);
    }

    public void testSmallRange() throws IOException {
        final long[] values = { 100, 103, 99, 105, 100, 101 };
        assertRoundTrip(values);
    }

    public void testNegativeMin() throws IOException {
        final long[] values = { -10, -5, 0, 5, 10, 20 };
        assertRoundTrip(values);
    }

    public void testWideRangeWithin63Bits() throws IOException {
        final long[] values = { 0L, (1L << 50), (1L << 60), -1L };
        // -1 makes min=-1; max = 1L<<60; range = (1L<<60) - (-1) = (1L<<60) + 1 which is positive, ~60 bits.
        assertRoundTrip(values);
    }

    public void testFullRangeForcesRawFallback() throws IOException {
        final long[] values = { Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L, 1L };
        // Signed range overflows ⇒ raw mode (bitsPerValue=64).
        assertRoundTrip(values);
    }

    public void testRandomRoundTrip() throws IOException {
        for (int blockSize : new int[] { 1, 16, 128, 256, 1024 }) {
            assertRoundTrip(randomLongs(blockSize, -1_000_000L, 1_000_000L));
            assertRoundTrip(randomLongs(blockSize, 0L, 100L));
            assertRoundTrip(randomLongs(blockSize, 0L, 1L));
        }
    }

    public void testEmptyEncode() throws IOException {
        final byte[] buf = new byte[16];
        final int len = BitPackBlockEncoder.INSTANCE.encode(new long[0], 0, 0, buf, 0);
        assertEquals(0, len);
    }

    public void testEncodedSizeIsSmallerThanRawForLowEntropy() throws IOException {
        // Workload where every value is in [0, 100): bitsPerValue should be 7; expected size much smaller than 8 bytes per value.
        final long[] values = randomLongs(1024, 0L, 100L);
        final byte[] buf = new byte[BitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int len = BitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        // 1024 * 7 bits = 7168 bits = 896 bytes; plus header (9), plus rounding up to whole longs.
        // Strict bound: encoded is much smaller than the raw 1024 * 8 = 8192 bytes.
        assertTrue("expected encoded size < raw size for low-entropy block: " + len, len < values.length * Long.BYTES);
    }

    private void assertRoundTrip(long[] values) throws IOException {
        final byte[] buf = new byte[BitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int len = BitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        final long[] decoded = new long[values.length];
        final long[] scratch = new long[BitPackBlockEncoder.INSTANCE.scratchLongs(values.length)];
        BitPackBlockEncoder.INSTANCE.decode(
            ColumNARDocValuesFormat.VERSION_CURRENT,
            new ByteArrayDataInput(buf, 0, len),
            decoded,
            0,
            values.length,
            scratch
        );
        assertArrayEquals(values, decoded);
    }

    private long[] randomLongs(int len, long minInclusive, long maxInclusive) {
        final long[] out = new long[len];
        for (int i = 0; i < len; i++) {
            out[i] = randomLongBetween(minInclusive, maxInclusive);
        }
        return out;
    }
}
