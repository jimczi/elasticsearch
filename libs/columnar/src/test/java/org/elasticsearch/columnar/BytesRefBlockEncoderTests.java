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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.encoder.RawBytesBlockEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class BytesRefBlockEncoderTests extends ESTestCase {

    public void testEmptyBlock() throws IOException {
        assertRoundTrip(new byte[][] {});
    }

    public void testSingleEmptyValue() throws IOException {
        assertRoundTrip(new byte[][] { new byte[0] });
    }

    public void testSingleValue() throws IOException {
        assertRoundTrip(new byte[][] { "hello".getBytes() });
    }

    public void testManySmallValues() throws IOException {
        final byte[][] values = new byte[128][];
        for (int i = 0; i < values.length; i++) {
            values[i] = ("v" + i).getBytes();
        }
        assertRoundTrip(values);
    }

    public void testMixedEmptyAndNonEmpty() throws IOException {
        assertRoundTrip(new byte[][] { "abc".getBytes(), new byte[0], "defg".getBytes(), new byte[0], new byte[0], "h".getBytes() });
    }

    public void testVariableSizes() throws IOException {
        final byte[][] values = new byte[64][];
        for (int i = 0; i < values.length; i++) {
            final byte[] v = new byte[1 + (i * 7 % 100)];
            random().nextBytes(v);
            values[i] = v;
        }
        assertRoundTrip(values);
    }

    public void testLargeValue() throws IOException {
        // A single block carrying values up to several KB — exercises varint width on the length
        // prefix and large in-block byte payloads.
        final byte[][] values = new byte[4][];
        for (int i = 0; i < values.length; i++) {
            values[i] = new byte[2_000 + i * 500];
            random().nextBytes(values[i]);
        }
        assertRoundTrip(values);
    }

    public void testRandomRoundTrip() throws IOException {
        for (int trial = 0; trial < 20; trial++) {
            final int valuesLen = randomIntBetween(0, 1024);
            final byte[][] values = new byte[valuesLen][];
            for (int i = 0; i < valuesLen; i++) {
                final byte[] v = new byte[randomIntBetween(0, 64)];
                random().nextBytes(v);
                values[i] = v;
            }
            assertRoundTrip(values);
        }
    }

    public void testMaxEncodedSizeIsUpperBound() throws IOException {
        // The reported upper bound must be at least the actual encoded length for every input.
        final byte[][] values = new byte[16][];
        int totalBytes = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = ("payload-" + i).getBytes();
            totalBytes += values[i].length;
        }
        final FlatBlock block = flatten(values);
        final int upper = RawBytesBlockEncoder.INSTANCE.maxEncodedSize(values.length, totalBytes);
        final byte[] buf = new byte[upper];
        final int actual = RawBytesBlockEncoder.INSTANCE.encode(block.bytes, block.offsets, values.length, buf, 0);
        assertTrue("maxEncodedSize must be an upper bound: actual=" + actual + " upper=" + upper, actual <= upper);
    }

    private void assertRoundTrip(byte[][] values) throws IOException {
        final FlatBlock block = flatten(values);
        final int totalBytes = block.offsets[values.length];
        final int max = RawBytesBlockEncoder.INSTANCE.maxEncodedSize(values.length, totalBytes);
        final byte[] buf = new byte[Math.max(1, max)];
        final int encodedLen = RawBytesBlockEncoder.INSTANCE.encode(block.bytes, block.offsets, values.length, buf, 0);

        // Decode via DataInput-backed scratch and verify byte-for-byte.
        final byte[] decodedBytes = new byte[Math.max(1, totalBytes)];
        final int[] decodedOffsets = new int[values.length + 1];
        RawBytesBlockEncoder.INSTANCE.decode(
            ColumNARDocValuesFormat.VERSION_CURRENT,
            new ByteArrayDataInput(buf, 0, encodedLen),
            decodedBytes,
            0,
            decodedOffsets,
            0,
            values.length
        );

        final BytesRef ref = new BytesRef();
        for (int i = 0; i < values.length; i++) {
            ref.bytes = decodedBytes;
            ref.offset = decodedOffsets[i];
            ref.length = decodedOffsets[i + 1] - decodedOffsets[i];
            assertArrayEquals("value " + i, values[i], copyOf(ref));
        }
    }

    private static FlatBlock flatten(byte[][] values) {
        int total = 0;
        for (byte[] v : values) {
            total += v.length;
        }
        final FlatBlock block = new FlatBlock();
        block.bytes = new byte[Math.max(1, total)];
        block.offsets = new int[values.length + 1];
        int p = 0;
        block.offsets[0] = 0;
        for (int i = 0; i < values.length; i++) {
            System.arraycopy(values[i], 0, block.bytes, p, values[i].length);
            p += values[i].length;
            block.offsets[i + 1] = p;
        }
        return block;
    }

    private static byte[] copyOf(BytesRef ref) {
        final byte[] out = new byte[ref.length];
        System.arraycopy(ref.bytes, ref.offset, out, 0, ref.length);
        return out;
    }

    private static final class FlatBlock {
        byte[] bytes;
        int[] offsets;
    }
}
