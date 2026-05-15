/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.bridge;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;

/**
 * On-wire packing for a sequence of {@code long} values stored as a binary doc-values
 * payload. The mapper uses this codec to materialise multi-valued numeric data into the
 * binary substrate that this format ingests via {@code addBinaryField}; the bridge uses
 * the inverse direction to expose the values back as a {@link ColumNARLongValues}
 * iterator.
 *
 * <p>Layout per doc:
 * <pre>
 *   [VInt count][little-endian long_0][little-endian long_1]...[little-endian long_{count-1}]
 * </pre>
 *
 * <p>Single-valued docs encode as {@code [vint(1)][8 bytes]} — 9 bytes total — same shape
 * as the multi-valued case with {@code count == 1}. Empty docs ({@code count == 0}) are
 * intentionally permitted so the codec subsumes the sparse case as well.
 *
 * <p>The codec is byte-order explicit (little-endian) and does not depend on any Lucene
 * {@code DocValues}-typed API: the same bytes round-trip identically whether the
 * underlying format stores them in {@code BinaryDocValues}, a custom blob field, or an
 * external object store.
 */
public final class PackedLongBinaryPacker {

    private static final int MAX_VINT_BYTES = 5;
    /**
     * First byte of every payload produced by this packer. Lets readers verify the binary
     * doc-values value was produced by the bridge and not by a raw {@code BinaryDocValuesField}
     * write — see {@link PayloadShape#expect}.
     */
    private static final byte SHAPE = PayloadShape.LONG.marker();

    public PackedLongBinaryPacker() {}

    /**
     * Upper-bound encoded size for a doc with {@code count} values: the 1-byte shape
     * marker, worst-case VInt for the count, plus 8 bytes per value.
     */
    public static int maxEncodedSize(int count) {
        return 1 + MAX_VINT_BYTES + Math.multiplyExact(count, Long.BYTES);
    }

    /**
     * Encode a single-valued doc. Allocates a {@code byte[10]} — 1-byte shape + 1-byte
     * count + 8-byte value.
     */
    public static byte[] encodeSingle(long value) {
        try {
            final byte[] buf = new byte[1 + 1 + Long.BYTES];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buf);
            out.writeByte(SHAPE);
            out.writeVInt(1);
            writeLongLE(out, value);
            return buf;
        } catch (IOException e) {
            throw new AssertionError("ByteArrayDataOutput should not throw on a sized buffer", e);
        }
    }

    /**
     * Encode {@code values[0..count)} into a fresh byte array. For the hot path that wants
     * to avoid the allocation, see {@link #encode(long[], int, byte[], int)}.
     */
    public static byte[] encode(long[] values, int count) {
        final byte[] buf = new byte[maxEncodedSize(count)];
        final int written = encode(values, count, buf, 0);
        if (written == buf.length) {
            return buf;
        }
        final byte[] trimmed = new byte[written];
        System.arraycopy(buf, 0, trimmed, 0, written);
        return trimmed;
    }

    /**
     * Allocation-free single-valued encode. The mapper owns a single
     * {@link BytesRefBuilder} and reuses it across every doc — no per-doc {@code byte[]} or
     * {@code BytesRef} allocation. Mutates {@code dest} in place; the caller's
     * {@link BytesRef} (obtained via {@link BytesRefBuilder#get()}) sees the new bytes
     * immediately and can be re-bound to a reused {@code BinaryDocValuesField} via
     * {@code Field.setBytesValue}.
     */
    public static void encodeSingle(long value, BytesRefBuilder dest) {
        dest.clear();
        dest.grow(1 + 1 + Long.BYTES);
        final byte[] arr = dest.bytes();
        arr[0] = SHAPE;
        arr[1] = (byte) 1; // VInt(1)
        writeLongLE(arr, 2, value);
        dest.setLength(1 + 1 + Long.BYTES);
    }

    /**
     * Allocation-free multi-valued encode. Same contract as
     * {@link #encodeSingle(long, BytesRefBuilder)} — the caller owns {@code dest} and
     * reuses it across every doc to keep the indexing hot path free of {@code byte[]}
     * allocations.
     */
    public static void encode(long[] values, int count, BytesRefBuilder dest) {
        dest.clear();
        dest.grow(maxEncodedSize(count));
        final byte[] arr = dest.bytes();
        int pos = 0;
        arr[pos++] = SHAPE;
        pos = writeVInt(arr, pos, count);
        for (int i = 0; i < count; i++) {
            writeLongLE(arr, pos, values[i]);
            pos += Long.BYTES;
        }
        dest.setLength(pos);
    }

    /** Encode {@code values[0..count)} into {@code dest} starting at {@code destOffset}; returns bytes written. */
    public static int encode(long[] values, int count, byte[] dest, int destOffset) {
        try {
            final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
            out.writeByte(SHAPE);
            out.writeVInt(count);
            for (int i = 0; i < count; i++) {
                writeLongLE(out, values[i]);
            }
            return out.getPosition() - destOffset;
        } catch (IOException e) {
            throw new AssertionError("ByteArrayDataOutput should not throw on a sized buffer", e);
        }
    }

    /**
     * Decode the per-doc payload at {@code bytes.bytes[bytes.offset, bytes.offset+bytes.length)}
     * into {@code dest} (grown as needed by the caller). Returns the value count.
     *
     * <p>Verifies the {@link PayloadShape#LONG} marker first; throws {@link IOException}
     * when the payload wasn't produced by this packer (e.g. a caller wrote a raw
     * {@code BinaryDocValuesField} bypassing the bridge).
     */
    public static int decode(BytesRef bytes, long[] dest) throws IOException {
        PayloadShape.expect(PayloadShape.LONG, bytes);
        int pos = bytes.offset + 1; // skip shape marker
        final byte[] arr = bytes.bytes;
        // VInt decode inline — avoids the ByteArrayDataInput allocation on the hot read path.
        int count = arr[pos++] & 0xFF;
        if ((count & 0x80) != 0) {
            count &= 0x7F;
            int shift = 7;
            byte b;
            do {
                b = arr[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
        }
        if (count > dest.length) {
            throw new IllegalArgumentException("destination buffer too small: need " + count + " values, dest has " + dest.length);
        }
        for (int i = 0; i < count; i++) {
            dest[i] = readLongLE(arr, pos);
            pos += Long.BYTES;
        }
        return count;
    }

    /**
     * Decode the count without materialising values — useful when callers only need to
     * decide whether the doc has any value. Also verifies the shape marker.
     */
    public static int decodeCount(BytesRef bytes) throws IOException {
        PayloadShape.expect(PayloadShape.LONG, bytes);
        int pos = bytes.offset + 1; // skip shape marker
        final byte[] arr = bytes.bytes;
        int count = arr[pos++] & 0xFF;
        if ((count & 0x80) != 0) {
            count &= 0x7F;
            int shift = 7;
            byte b;
            do {
                b = arr[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
        }
        return count;
    }

    private static void writeLongLE(ByteArrayDataOutput out, long v) throws IOException {
        out.writeByte((byte) v);
        out.writeByte((byte) (v >> 8));
        out.writeByte((byte) (v >> 16));
        out.writeByte((byte) (v >> 24));
        out.writeByte((byte) (v >> 32));
        out.writeByte((byte) (v >> 40));
        out.writeByte((byte) (v >> 48));
        out.writeByte((byte) (v >> 56));
    }

    /** Direct {@code byte[]} write used by the allocation-free BytesRefBuilder paths. */
    private static void writeLongLE(byte[] arr, int pos, long v) {
        arr[pos] = (byte) v;
        arr[pos + 1] = (byte) (v >> 8);
        arr[pos + 2] = (byte) (v >> 16);
        arr[pos + 3] = (byte) (v >> 24);
        arr[pos + 4] = (byte) (v >> 32);
        arr[pos + 5] = (byte) (v >> 40);
        arr[pos + 6] = (byte) (v >> 48);
        arr[pos + 7] = (byte) (v >> 56);
    }

    /** Inline VInt write; returns the next position. */
    private static int writeVInt(byte[] arr, int pos, int v) {
        while ((v & ~0x7F) != 0) {
            arr[pos++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        arr[pos++] = (byte) v;
        return pos;
    }

    private static long readLongLE(byte[] arr, int pos) {
        return (arr[pos] & 0xFFL) | ((arr[pos + 1] & 0xFFL) << 8) | ((arr[pos + 2] & 0xFFL) << 16) | ((arr[pos + 3] & 0xFFL) << 24)
            | ((arr[pos + 4] & 0xFFL) << 32) | ((arr[pos + 5] & 0xFFL) << 40) | ((arr[pos + 6] & 0xFFL) << 48) | ((arr[pos + 7] & 0xFFL)
                << 56);
    }
}
