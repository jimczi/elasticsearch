/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numericpipeline;

import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

public class NumericPipelineEncoderTests extends ESTestCase {

    public void testEmpty() throws IOException {
        final byte[] buf = new byte[NumericPipelineEncoder.INSTANCE.maxEncodedSize(0)];
        assertEquals(0, NumericPipelineEncoder.INSTANCE.encode(new long[0], 0, 0, buf, 0));
    }

    public void testSingle() throws IOException {
        assertRoundTrip(new long[] { 86_400_000L });
    }

    public void testConstantSequence() throws IOException {
        final long[] values = new long[256];
        java.util.Arrays.fill(values, 1_700_000_000_000L);
        assertRoundTrip(values);
    }

    public void testMonotonicJitterTimestamps() throws IOException {
        // Append-only timestamps with mild jitter — exercises delta + offset + bit-pack.
        final long[] values = new long[512];
        long t = 1_700_000_000_000L;
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = t;
            t += 950 + rng.nextInt(101);
        }
        assertRoundTrip(values);
    }

    public void testRandomDayGranularity() throws IOException {
        // Day-granularity dates, randomly distributed — exercises gcd + offset + bit-pack
        // (gcd divides out the 86,400,000-ms factor before bit-pack sees the values).
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] values = new long[300];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = base + rng.nextInt(365) * day;
        }
        assertRoundTrip(values);
    }

    public void testMonotonicDayGranularity() throws IOException {
        // Monotonic day-granularity counters — exercises all three transforms.
        final long day = 86_400_000L;
        final long base = 1_700_000_000_000L;
        final long[] values = new long[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = base + i * day;
        }
        assertRoundTrip(values);
    }

    public void testUniformRandomLongs() throws IOException {
        // Worst case: no structure for any transform. Only bit-pack runs.
        final long[] values = new long[400];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = rng.nextLong();
        }
        assertRoundTrip(values);
    }

    public void testGaugeNarrowRange() throws IOException {
        // Values clustered around 5000 — offset runs (subtracting min), bit-pack runs.
        final long[] values = new long[300];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = 5000L + rng.nextInt(101) - 50;
        }
        assertRoundTrip(values);
    }

    public void testZeroesAndOnes() throws IOException {
        // Boolean-like field: 0 / 1. bit-pack handles this at 1 bpv.
        final long[] values = new long[1024];
        final Random rng = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = rng.nextInt(2);
        }
        assertRoundTrip(values);
    }

    public void testRegistryRegistersAtIdFive() {
        final NumericBlockEncoder resolved = NumericBlockEncoderRegistry.forName(NumericPipelineEncoder.NAME);
        assertNotNull(resolved);
        assertTrue(resolved instanceof NumericPipelineEncoder);
        assertEquals("Pipeline", NumericPipelineEncoder.INSTANCE.getName());
    }

    public void testEncodeIntoOffsetBuffer() throws IOException {
        // Confirm encode honours destOffset and decode reads from the right position.
        final long[] values = { 100L, 200L, 350L, 500L, 700L };
        final byte[] buf = new byte[NumericPipelineEncoder.INSTANCE.maxEncodedSize(values.length) + 16];
        final int prefix = 7;
        for (int i = 0; i < prefix; i++) {
            buf[i] = (byte) 0xAB; // sentinel — must survive encode
        }
        final int written = NumericPipelineEncoder.INSTANCE.encode(values, 0, values.length, buf, prefix);
        for (int i = 0; i < prefix; i++) {
            assertEquals("sentinel before destOffset must be untouched", (byte) 0xAB, buf[i]);
        }
        final long[] decoded = new long[values.length];
        final long[] scratch = new long[NumericPipelineEncoder.INSTANCE.scratchLongs(values.length)];
        NumericPipelineEncoder.INSTANCE.decode(
            ColumNARDocValuesFormat.VERSION_CURRENT,
            new ByteArrayDataInput(buf, prefix, written),
            decoded,
            0,
            values.length,
            scratch
        );
        assertArrayEquals(values, decoded);
    }

    public void testDecodeIntoOffsetDest() throws IOException {
        // Encode at offset 0; decode into a destination slice mid-array.
        final long[] values = { 0L, 1L, 4L, 9L, 16L, 25L };
        final byte[] buf = new byte[NumericPipelineEncoder.INSTANCE.maxEncodedSize(values.length)];
        final int written = NumericPipelineEncoder.INSTANCE.encode(values, 0, values.length, buf, 0);
        final long[] dest = new long[values.length + 4];
        final int destOff = 3;
        dest[0] = -1L;
        dest[1] = -1L;
        dest[2] = -1L;
        dest[dest.length - 1] = -1L;
        final long[] scratch = new long[NumericPipelineEncoder.INSTANCE.scratchLongs(values.length)];
        NumericPipelineEncoder.INSTANCE.decode(
            ColumNARDocValuesFormat.VERSION_CURRENT,
            new ByteArrayDataInput(buf, 0, written),
            dest,
            destOff,
            values.length,
            scratch
        );
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], dest[destOff + i]);
        }
        assertEquals("prefix slot untouched", -1L, dest[0]);
        assertEquals("trailing slot untouched", -1L, dest[dest.length - 1]);
    }

    public void testRandomRoundTripVariousSizes() throws IOException {
        for (int n : new int[] { 1, 2, 16, 128, 1024, 8192 }) {
            final long[] values = new long[n];
            final Random rng = new Random(n * 7L);
            for (int i = 0; i < n; i++) {
                values[i] = rng.nextLong() >>> 32; // small longs that exercise non-trivial bpv
            }
            assertRoundTrip(values);
        }
    }

    private void assertRoundTrip(long[] values) throws IOException {
        final long[] decoded = NumericPipelineEncoder.roundTrip(values);
        assertArrayEquals(values, decoded);
    }
}
