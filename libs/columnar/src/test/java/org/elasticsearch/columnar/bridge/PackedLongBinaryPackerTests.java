/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.bridge;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

public class PackedLongBinaryPackerTests extends ESTestCase {

    public void testSingleValueRoundTrip() throws IOException {
        final byte[] enc = PackedLongBinaryPacker.encodeSingle(0xDEADBEEFCAFEBABEL);
        final BytesRef br = new BytesRef(enc);
        final long[] dest = new long[1];
        final int count = PackedLongBinaryPacker.decode(br, dest);
        assertEquals(1, count);
        assertEquals(0xDEADBEEFCAFEBABEL, dest[0]);
        assertEquals(10, enc.length); // shape(1) + vint(1) + 8 bytes
        assertEquals("shape marker", (byte) 'L', enc[0]);
    }

    public void testZeroValues() throws IOException {
        final byte[] enc = PackedLongBinaryPacker.encode(new long[0], 0);
        final BytesRef br = new BytesRef(enc);
        final long[] dest = new long[4];
        assertEquals(0, PackedLongBinaryPacker.decode(br, dest));
        assertEquals(2, enc.length); // shape(1) + vint(0)
    }

    public void testMultiValueRoundTrip() throws IOException {
        final long[] values = { 1L, 2L, 3L, Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L };
        final byte[] enc = PackedLongBinaryPacker.encode(values, values.length);
        final BytesRef br = new BytesRef(enc);
        final long[] dest = new long[values.length];
        final int count = PackedLongBinaryPacker.decode(br, dest);
        assertEquals(values.length, count);
        assertArrayEquals(values, dest);
    }

    public void testDecodeCountWithoutValues() throws IOException {
        final long[] values = { 100L, 200L, 300L };
        final byte[] enc = PackedLongBinaryPacker.encode(values, values.length);
        assertEquals(3, PackedLongBinaryPacker.decodeCount(new BytesRef(enc)));
    }

    public void testDecodeRejectsRawBinary() {
        // Bytes written directly via BinaryDocValuesField (bypassing the bridge packer)
        // lack the shape marker — decode must fail fast.
        final byte[] rawBytes = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09 };
        final BytesRef br = new BytesRef(rawBytes);
        final IOException e = expectThrows(IOException.class, () -> PackedLongBinaryPacker.decode(br, new long[1]));
        assertTrue("error mentions shape", e.getMessage().contains("shape") || e.getMessage().contains("marker"));
    }

    public void testDecodeRejectsBytesShape() {
        // A payload produced by the bytes packer is also rejected by the long-packer decode.
        final byte[] bytesPayload = PackedBytesBinaryPacker.encodeSingle(new BytesRef("hello"));
        final BytesRef br = new BytesRef(bytesPayload);
        expectThrows(IOException.class, () -> PackedLongBinaryPacker.decode(br, new long[1]));
    }

    public void testRandomRoundTrip() throws IOException {
        final Random rng = random();
        for (int trial = 0; trial < 20; trial++) {
            final int n = randomIntBetween(1, 200);
            final long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = rng.nextLong();
            }
            final byte[] enc = PackedLongBinaryPacker.encode(values, n);
            assertTrue(enc.length <= PackedLongBinaryPacker.maxEncodedSize(n));
            final long[] dest = new long[n];
            assertEquals(n, PackedLongBinaryPacker.decode(new BytesRef(enc), dest));
            assertArrayEquals(values, dest);
        }
    }

    public void testReusableBufferSingleValue() throws IOException {
        // The allocation-free indexing path reuses one BytesRefBuilder + one
        // BinaryDocValuesField across docs. Confirm round-trip + buffer reuse.
        final org.apache.lucene.util.BytesRefBuilder buf = new org.apache.lucene.util.BytesRefBuilder();
        final long[] inputs = { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 42L };
        final long[] dest = new long[1];
        for (long v : inputs) {
            PackedLongBinaryPacker.encodeSingle(v, buf);
            assertEquals("encoded length", 10, buf.length());
            assertEquals("shape marker", (byte) 'L', buf.bytes()[0]);
            final int count = PackedLongBinaryPacker.decode(buf.get(), dest);
            assertEquals(1, count);
            assertEquals(v, dest[0]);
        }
    }

    public void testReusableBufferMultiValued() throws IOException {
        final org.apache.lucene.util.BytesRefBuilder buf = new org.apache.lucene.util.BytesRefBuilder();
        final long[][] inputs = { { 1L }, { 1L, 2L, 3L }, { Long.MAX_VALUE, Long.MIN_VALUE }, {}, { 0L, 0L, 0L, 0L, 0L } };
        for (long[] row : inputs) {
            PackedLongBinaryPacker.encode(row, row.length, buf);
            assertEquals("shape marker", (byte) 'L', buf.bytes()[0]);
            final long[] dest = new long[Math.max(row.length, 1)];
            final int count = PackedLongBinaryPacker.decode(buf.get(), dest);
            assertEquals(row.length, count);
            for (int i = 0; i < row.length; i++) {
                assertEquals("value " + i, row[i], dest[i]);
            }
        }
    }

    public void testEncodeIntoOffsetBuffer() throws IOException {
        // Confirm encode respects destOffset.
        final long[] values = { 42L, 137L };
        final byte[] buf = new byte[64];
        for (int i = 0; i < buf.length; i++) {
            buf[i] = (byte) 0xCC; // sentinel
        }
        final int written = PackedLongBinaryPacker.encode(values, values.length, buf, 16);
        for (int i = 0; i < 16; i++) {
            assertEquals("prefix sentinel preserved at index " + i, (byte) 0xCC, buf[i]);
        }
        final BytesRef br = new BytesRef(buf, 16, written);
        final long[] dest = new long[2];
        assertEquals(2, PackedLongBinaryPacker.decode(br, dest));
        assertArrayEquals(values, dest);
    }
}
