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
import org.elasticsearch.columnar.encoder.GcdBitPackBlockEncoder;
import org.elasticsearch.columnar.encoder.GcdDeltaPackedBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

public class GcdBitPackBlockEncoderTests extends ESTestCase {

    public void testEmpty() throws IOException {
        final byte[] buf = new byte[32];
        assertEquals(0, GcdBitPackBlockEncoder.INSTANCE.encode(new long[0], 0, 0, buf, 0));
    }

    public void testSingle() throws IOException {
        assertRoundTrip(new long[] { 86_400_000L });
    }

    public void testConstantSequence() throws IOException {
        final long[] values = new long[100];
        java.util.Arrays.fill(values, 1_700_000_000_000L);
        assertRoundTrip(values);
    }

    public void testRandomDayGranularityBeatsDelta() throws IOException {
        // Document publication dates at day granularity, randomly distributed across a year.
        // The values divide cleanly by 86,400,000, but are NOT monotonic — so the delta
        // variant inflates the bit width by 1 bit (zig-zag of ±range). This encoder
        // (min-subtract instead of delta on the divided values) is strictly tighter here.
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] values = new long[500];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = base + rng.nextInt(365) * day;
        }
        assertRoundTrip(values);

        final byte[] gcdBitPackBuf = new byte[GcdBitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final byte[] gcdDeltaBuf = new byte[GcdDeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final byte[] bitPackBuf = new byte[BitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int gcdBitPackSize = GcdBitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, gcdBitPackBuf, 0);
        final int gcdDeltaSize = GcdDeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, gcdDeltaBuf, 0);
        final int bitPackSize = BitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, bitPackBuf, 0);
        logger.info(
            "random-day-dates 500 values: gcd-bitpack={} bytes, gcd-delta={} bytes, bit-pack={} bytes",
            gcdBitPackSize,
            gcdDeltaSize,
            bitPackSize
        );
        assertTrue("gcd-bitpack should beat gcd-delta on non-monotonic GCD data", gcdBitPackSize < gcdDeltaSize);
        assertTrue("gcd-bitpack should beat plain bit-pack on day-granularity dates", gcdBitPackSize < bitPackSize);
    }

    public void testRandomMultiplesOf100() throws IOException {
        // Gauge values quantised to multiples of 100, randomly distributed. Min-subtract on
        // divided values shrinks the bit width substantially without paying delta inflation.
        final long[] values = new long[1000];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = 100L * rng.nextInt(1000);
        }
        assertRoundTrip(values);
    }

    public void testAutoPickRoutesToGcdBitPackOnRandomDayGranularity() throws IOException {
        // Non-monotonic + GCD > 1 should route to GcdBitPackBlockEncoder. The delta variant
        // would lose a bit per value here.
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] sample = new long[256];
        final Random rng = new Random(42);
        for (int i = 0; i < sample.length; i++) {
            // Truly random day-granularity timestamps — no overall monotonic trend.
            sample[i] = base + rng.nextInt(365) * day;
        }
        final NumericBlockEncoder picked = BitPackBlockEncoder.INSTANCE.specializeForSegment(
            LongValuesSupplier.fromArray(sample, 0, sample.length)
        );
        assertTrue(
            "random day-granularity should auto-pick GCD-bitpack, got " + picked.getClass().getSimpleName(),
            picked instanceof GcdBitPackBlockEncoder
        );
    }

    public void testRegistryRegistersAtIdFour() {
        final NumericBlockEncoder resolved = NumericBlockEncoderRegistry.forName(GcdBitPackBlockEncoder.NAME);
        assertNotNull(resolved);
        assertTrue(resolved instanceof GcdBitPackBlockEncoder);
        assertEquals("GcdBitPack", GcdBitPackBlockEncoder.INSTANCE.getName());
    }

    public void testRandomRoundTrip() throws IOException {
        for (int blockSize : new int[] { 2, 16, 128, 256, 1024 }) {
            final long[] values = new long[blockSize];
            final Random rng = new Random(blockSize * 7L);
            for (int i = 0; i < blockSize; i++) {
                values[i] = 10_000L * (1000L + rng.nextInt(1000));
            }
            assertRoundTrip(values);
        }
    }

    private void assertRoundTrip(long[] values) throws IOException {
        final byte[] buf = new byte[GcdBitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int written = GcdBitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        assertTrue(written <= buf.length);
        final long[] scratch = new long[GcdBitPackBlockEncoder.INSTANCE.scratchLongs(values.length)];
        final long[] decoded = new long[values.length];
        GcdBitPackBlockEncoder.INSTANCE.decode(
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
