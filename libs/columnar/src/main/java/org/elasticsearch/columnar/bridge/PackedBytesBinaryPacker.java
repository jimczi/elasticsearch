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
 * On-wire packing for a sequence of byte values stored as a binary doc-values payload.
 * Mapper-facing complement to {@link PackedLongBinaryPacker}: turns one or many
 * {@code BytesRef}s into a single binary blob, preserving insertion order.
 *
 * <p>Layout per doc:
 * <pre>
 *   [VInt count][VInt len_0][bytes_0][VInt len_1][bytes_1]...
 * </pre>
 *
 * <p>Single-valued docs encode as {@code [vint(1)][vint(len)][bytes]}. Empty docs
 * ({@code count == 0}) are permitted.
 */
public final class PackedBytesBinaryPacker {

    private static final int MAX_VINT_BYTES = 5;
    private static final byte SHAPE = PayloadShape.BYTES.marker();

    public PackedBytesBinaryPacker() {}

    /** Upper-bound encoded size for {@code count} values with {@code totalValueBytes} payload bytes. */
    public static int maxEncodedSize(int count, int totalValueBytes) {
        // 1-byte shape + count vint + per-value len vint + payload
        return 1 + MAX_VINT_BYTES + Math.multiplyExact(count, MAX_VINT_BYTES) + totalValueBytes;
    }

    /** Encode a single-valued doc into a fresh byte array. */
    public static byte[] encodeSingle(BytesRef value) {
        try {
            final byte[] buf = new byte[1 + 1 + MAX_VINT_BYTES + value.length];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buf);
            out.writeByte(SHAPE);
            out.writeVInt(1);
            out.writeVInt(value.length);
            out.writeBytes(value.bytes, value.offset, value.length);
            final int written = out.getPosition();
            if (written == buf.length) {
                return buf;
            }
            final byte[] trimmed = new byte[written];
            System.arraycopy(buf, 0, trimmed, 0, written);
            return trimmed;
        } catch (IOException e) {
            throw new AssertionError("ByteArrayDataOutput should not throw on a sized buffer", e);
        }
    }

    /** Encode a multi-valued doc into a fresh byte array. */
    public static byte[] encode(BytesRef[] values) {
        int totalBytes = 0;
        for (BytesRef v : values) {
            totalBytes += v.length;
        }
        final byte[] buf = new byte[maxEncodedSize(values.length, totalBytes)];
        try {
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buf);
            out.writeByte(SHAPE);
            out.writeVInt(values.length);
            for (BytesRef v : values) {
                out.writeVInt(v.length);
                out.writeBytes(v.bytes, v.offset, v.length);
            }
            final int written = out.getPosition();
            if (written == buf.length) {
                return buf;
            }
            final byte[] trimmed = new byte[written];
            System.arraycopy(buf, 0, trimmed, 0, written);
            return trimmed;
        } catch (IOException e) {
            throw new AssertionError("ByteArrayDataOutput should not throw on a sized buffer", e);
        }
    }

    /**
     * Allocation-free single-valued encode into a reusable {@link BytesRefBuilder}. Same
     * shape as {@link PackedLongBinaryPacker#encodeSingle(long, BytesRefBuilder)} — the
     * mapper owns a single builder and reuses it across every doc on the indexing hot
     * path.
     */
    public static void encodeSingle(BytesRef value, BytesRefBuilder dest) {
        dest.clear();
        dest.grow(1 + MAX_VINT_BYTES + value.length);
        final byte[] arr = dest.bytes();
        int pos = 0;
        arr[pos++] = SHAPE;
        arr[pos++] = (byte) 1; // VInt(1)
        pos = writeVInt(arr, pos, value.length);
        System.arraycopy(value.bytes, value.offset, arr, pos, value.length);
        pos += value.length;
        dest.setLength(pos);
    }

    /** Allocation-free multi-valued encode into a reusable {@link BytesRefBuilder}. */
    public static void encode(BytesRef[] values, BytesRefBuilder dest) {
        int totalBytes = 0;
        for (BytesRef v : values) {
            totalBytes += v.length;
        }
        dest.clear();
        dest.grow(maxEncodedSize(values.length, totalBytes));
        final byte[] arr = dest.bytes();
        int pos = 0;
        arr[pos++] = SHAPE;
        pos = writeVInt(arr, pos, values.length);
        for (BytesRef v : values) {
            pos = writeVInt(arr, pos, v.length);
            System.arraycopy(v.bytes, v.offset, arr, pos, v.length);
            pos += v.length;
        }
        dest.setLength(pos);
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

    /**
     * Decode the count from the per-doc payload without materialising the values. Also
     * verifies the {@link PayloadShape#BYTES} marker; throws when the payload bypassed
     * the bridge.
     */
    public static int decodeCount(BytesRef bytes) throws IOException {
        PayloadShape.expect(PayloadShape.BYTES, bytes);
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
}
