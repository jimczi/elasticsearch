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
import org.elasticsearch.columnar.encoder.GcdDeltaPackedBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

public class GcdDeltaPackedBlockEncoderTests extends ESTestCase {

    public void testEmpty() throws IOException {
        final byte[] buf = new byte[32];
        assertEquals(0, GcdDeltaPackedBlockEncoder.INSTANCE.encode(new long[0], 0, 0, buf, 0));
    }

    public void testSingle() throws IOException {
        assertRoundTrip(new long[] { 86_400_000L });
    }

    public void testConstantSequence() throws IOException {
        final long[] values = new long[100];
        java.util.Arrays.fill(values, 1_700_000_000_000L);
        assertRoundTrip(values);
    }

    public void testDayGranularityRandomDates() throws IOException {
        // Document publication dates at day granularity, randomly distributed across a year.
        // Raw values are huge (millis since epoch), but every value is a multiple of one
        // day = 86,400,000 — exactly the case the GCD-aware encoder is designed for. Delta-
        // only encoding doesn't help here because the deltas between random dates can span
        // the whole year in raw units.
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] values = new long[200];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = base + rng.nextInt(365) * day;
        }
        assertRoundTrip(values);

        final byte[] gcdBuf = new byte[GcdDeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final byte[] dpBuf = new byte[DeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final byte[] bpBuf = new byte[BitPackBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int gcdSize = GcdDeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, gcdBuf, 0);
        final int dpSize = DeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, dpBuf, 0);
        final int bpSize = BitPackBlockEncoder.INSTANCE.encode(values, 0, values.length, bpBuf, 0);
        logger.info("random-day-dates 200 values: gcd-delta={} bytes, delta={} bytes, bit-pack={} bytes", gcdSize, dpSize, bpSize);
        assertTrue("GCD-aware should beat delta-only on random-day-granularity dates", gcdSize < dpSize);
        assertTrue("GCD-aware should beat plain bit-pack on random-day-granularity dates", gcdSize < bpSize);
    }

    public void testConsecutiveDayTimestamps() throws IOException {
        // Pure monotonic-step (every value = prev + day). DeltaPackedBlockEncoder already
        // crushes this (all deltas equal, bitsPerValue=0); GCD encoder pays 8 extra header
        // bytes for the divisor and ties or slightly loses. Both must round-trip correctly.
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] values = new long[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = base + i * day;
        }
        assertRoundTrip(values);
    }

    public void testRandomMultiplesOf100() throws IOException {
        // Gauge values that are all multiples of 100. GCD ≥ 100 reduces value range
        // proportionally before bit-pack.
        final long[] values = new long[500];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = 100L * rng.nextInt(1000);
        }
        assertRoundTrip(values);
    }

    public void testRandomRoundTrip() throws IOException {
        for (int blockSize : new int[] { 2, 16, 128, 256, 1024 }) {
            final long[] values = new long[blockSize];
            final Random rng = new Random(blockSize * 7L);
            // Sample with GCD=10000 — should exercise the GCD path.
            for (int i = 0; i < blockSize; i++) {
                values[i] = 10_000L * (1000L + rng.nextInt(1000));
            }
            assertRoundTrip(values);
        }
    }

    public void testAutoPickRoutesToGcdOnRandomDayGranularity() throws IOException {
        // The auto-pick should choose GCD-aware on randomly-distributed day-granularity
        // timestamps where the divided range is much smaller than the raw range.
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] sample = new long[256];
        final Random rng = new Random(42);
        long t = base;
        for (int i = 0; i < sample.length; i++) {
            // Mostly-sorted day-granularity timestamps with occasional jumps. Stays roughly
            // monotonic so the GCD+delta combination applies.
            t += (1 + rng.nextInt(3)) * day;
            sample[i] = t;
        }
        final NumericBlockEncoder picked = BitPackBlockEncoder.INSTANCE.specializeForSegment(
            LongValuesSupplier.fromArray(sample, 0, sample.length)
        );
        assertTrue(
            "monotonic random-day-granularity should auto-pick GCD-aware encoder, got " + picked.getClass().getSimpleName(),
            picked instanceof GcdDeltaPackedBlockEncoder
        );
    }

    public void testRegistryRegistersAtIdThree() {
        final NumericBlockEncoder resolved = NumericBlockEncoderRegistry.forName(GcdDeltaPackedBlockEncoder.NAME);
        assertNotNull(resolved);
        assertTrue(resolved instanceof GcdDeltaPackedBlockEncoder);
        assertEquals("GcdDeltaPack", GcdDeltaPackedBlockEncoder.INSTANCE.getName());
    }

    private void assertRoundTrip(long[] values) throws IOException {
        final byte[] buf = new byte[GcdDeltaPackedBlockEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int written = GcdDeltaPackedBlockEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        assertTrue(written <= buf.length);
        final long[] scratch = new long[GcdDeltaPackedBlockEncoder.INSTANCE.scratchLongs(values.length)];
        final long[] decoded = new long[values.length];
        GcdDeltaPackedBlockEncoder.INSTANCE.decode(
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
