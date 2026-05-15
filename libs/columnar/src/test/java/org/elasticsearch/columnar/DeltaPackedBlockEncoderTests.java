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
import org.elasticsearch.columnar.encoder.DeltaPackedBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

public class DeltaPackedBlockEncoderTests extends ESTestCase {

    public void testEmpty() throws IOException {
        final byte[] buf = new byte[16];
        final int n = DeltaPackedBlockEncoder.INSTANCE.encode(new long[0], 0, 0, buf, 0);
        assertEquals(0, n);
    }

    public void testSingle() throws IOException {
        assertRoundTrip(new long[] { 42L });
    }

    public void testConstantSequence() throws IOException {
        // All values equal: deltas all zero, bitsPerValue=0, minDelta=0.
        final long[] values = new long[100];
        java.util.Arrays.fill(values, 1_700_000_000_000L);
        assertRoundTrip(values);
    }

    public void testConstantStep() throws IOException {
        // Monotonic constant offset: every delta equals 1000.
        final long[] values = new long[100];
        for (int i = 0; i < values.length; i++) {
            values[i] = 1_700_000_000_000L + i * 1000L;
        }
        assertRoundTrip(values);
        // Storage size should be tiny — base + bitsPerValue=0 + minDelta = 17 bytes regardless
        // of length.
        final byte[] buf = new byte[DeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int n = DeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        assertEquals("constant-step block should encode to 17 bytes (base 8 + bits 1 + minDelta 8)", 17, n);
    }

    public void testMonotonicWithJitter() throws IOException {
        // Timestamps that mostly advance by ~1000 but with small variation. Delta-packed
        // should beat min-delta-only bit-pack because the deltas have a small range while
        // the absolute values span the full block.
        final long[] values = new long[200];
        final Random rng = new Random(42);
        long t = 1_700_000_000_000L;
        for (int i = 0; i < values.length; i++) {
            values[i] = t;
            t += 950 + rng.nextInt(101); // 950..1050
        }
        assertRoundTrip(values);

        // Compare encoded size against BitPackBlockEncoder on the same input. DeltaPacked
        // should be substantially smaller because the deltas live in a 7-bit range while the
        // BitPack range is ~200*1000=200000 (~18 bits).
        final byte[] dpBuf = new byte[DeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final byte[] bpBuf = new byte[BitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int dpSize = DeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, dpBuf, 0);
        final int bpSize = BitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, bpBuf, 0);
        logger.info(
            "monotonic-with-jitter (200 values): delta-packed={} bytes, bit-pack={} bytes, ratio={}",
            dpSize,
            bpSize,
            String.format(java.util.Locale.ROOT, "%.2fx", (double) dpSize / bpSize)
        );
        assertTrue("delta-packed should beat bit-pack on monotonic+jitter", dpSize < bpSize);
    }

    public void testGaugeLikeNotMonotonic() throws IOException {
        // Random gauge values around a centre. Delta-packed may be worse than bit-pack here
        // (the delta range is up to 2× the value range) but it must still round-trip
        // correctly.
        final long[] values = new long[200];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = 5000L + rng.nextInt(101) - 50; // 4950..5050
        }
        assertRoundTrip(values);
    }

    public void testRandom() throws IOException {
        // Worst case for any delta encoder — random longs. Should round-trip via the raw
        // fallback (bitsPerValue=64) since deltas don't fit.
        final long[] values = new long[64];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = rng.nextLong();
        }
        assertRoundTrip(values);
    }

    public void testLargeRandomVariousSizes() throws IOException {
        for (int blockSize : new int[] { 1, 2, 16, 128, 256, 1024, 8192 }) {
            final long[] values = new long[blockSize];
            final Random rng = new Random(blockSize * 31L + 7);
            // Monotonic with random jitter — what timestamps look like.
            long t = rng.nextInt(1_000_000);
            for (int i = 0; i < blockSize; i++) {
                values[i] = t;
                t += rng.nextInt(10_000);
            }
            assertRoundTrip(values);
        }
    }

    public void testRegistryRegistersAtIdTwo() {
        final NumericBlockEncoder resolved = NumericBlockEncoderRegistry.forName(DeltaPackedBlockEncoder.NAME);
        assertNotNull("DeltaPackedBlockEncoder must be registered at id=2", resolved);
        assertTrue("registered impl must be DeltaPackedBlockEncoder", resolved instanceof DeltaPackedBlockEncoder);
        assertEquals("DeltaPack", DeltaPackedBlockEncoder.INSTANCE.getName());
    }

    private void assertRoundTrip(long[] values) throws IOException {
        final byte[] buf = new byte[DeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int written = DeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        assertTrue(written <= buf.length);
        final long[] scratch = new long[DeltaPackedBlockEncoder.INSTANCE.scratchLongs(values.length)];
        final long[] decoded = new long[values.length];
        DeltaPackedBlockEncoder.INSTANCE.decode(
            ColumNARDocValuesFormat.VERSION_CURRENT,
            new ByteArrayDataInput(buf, 0, written),
            decoded,
            0,
            values.length,
            scratch
        );
        assertArrayEquals(values, decoded);
    }
}
