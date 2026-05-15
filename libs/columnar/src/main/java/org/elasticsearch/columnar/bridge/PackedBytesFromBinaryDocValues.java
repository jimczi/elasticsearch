/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.bridge;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * {@link ColumNARBytesValues} backed by a Lucene {@link BinaryDocValues}, decoding the
 * per-doc payload written by {@link PackedBytesBinaryPacker}. Single- and multi-valued
 * docs share the same shape.
 *
 * <p>The iterator owns a reusable {@code byte[]} that holds the concatenated value bytes
 * for the current doc, plus an {@code int[] offsets} that record each value's start and
 * end positions in that buffer. Per-value {@link BytesRef}s share the underlying buffer
 * (the returned {@code BytesRef.bytes} is the iterator's internal array — valid only
 * until the next iteration step, like Lucene's standard {@code BinaryDocValues}).
 */
public final class PackedBytesFromBinaryDocValues extends ColumNARBytesValues {

    private final BinaryDocValues source;
    private byte[] valueBytes;
    private int[] valueOffsets;
    private int valueCount;
    private final BytesRef scratch = new BytesRef();

    public PackedBytesFromBinaryDocValues(BinaryDocValues source) {
        this.source = source;
        this.valueBytes = new byte[64];
        this.valueOffsets = new int[2];
        this.valueCount = 0;
    }

    @Override
    public int docID() {
        return source.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        final int doc = source.nextDoc();
        if (doc != NO_MORE_DOCS) {
            decodeCurrent();
        } else {
            valueCount = 0;
        }
        return doc;
    }

    @Override
    public int advance(int target) throws IOException {
        final int doc = source.advance(target);
        if (doc != NO_MORE_DOCS) {
            decodeCurrent();
        } else {
            valueCount = 0;
        }
        return doc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (source.advanceExact(target) == false) {
            valueCount = 0;
            return false;
        }
        decodeCurrent();
        return true;
    }

    @Override
    public long cost() {
        return source.cost();
    }

    @Override
    public int valueCount() {
        return valueCount;
    }

    @Override
    public BytesRef bytesAt(int i) {
        scratch.bytes = valueBytes;
        scratch.offset = valueOffsets[i];
        scratch.length = valueOffsets[i + 1] - valueOffsets[i];
        return scratch;
    }

    @Override
    public int readValues(byte[] destBytes, int bytesOff, int[] destOffsets, int offsetsOff) {
        // valueBytes is already a flat slab with offsets at [0..valueCount]; one memcpy
        // gives the caller every value at this doc, the offsets table copies in bulk too.
        final int total = valueOffsets[valueCount];
        System.arraycopy(valueBytes, 0, destBytes, bytesOff, total);
        System.arraycopy(valueOffsets, 0, destOffsets, offsetsOff, valueCount + 1);
        return total;
    }

    private void decodeCurrent() throws IOException {
        final BytesRef payload = source.binaryValue();
        // Verify the BYTES shape marker — fails fast if the field's BinaryDocValuesField
        // was written without going through the bridge packer.
        PayloadShape.expect(PayloadShape.BYTES, payload);
        // Decode inline: [shape byte] [vint count] [vint len_i] [bytes_i] ...
        int pos = payload.offset + 1;
        final byte[] arr = payload.bytes;
        int count = readVInt(arr, pos);
        pos += vIntSize(count);
        if (count + 1 > valueOffsets.length) {
            valueOffsets = new int[ArrayUtil.oversize(count + 1, Integer.BYTES)];
        }
        // First sweep to determine total bytes and per-value offsets in the original arr.
        // Then copy into our buffer so offsets are stable for the bytesAt() accessor.
        int writeOffset = 0;
        for (int i = 0; i < count; i++) {
            final int len = readVInt(arr, pos);
            pos += vIntSize(len);
            valueOffsets[i] = writeOffset;
            valueBytes = ArrayUtil.grow(valueBytes, writeOffset + len);
            System.arraycopy(arr, pos, valueBytes, writeOffset, len);
            pos += len;
            writeOffset += len;
        }
        valueOffsets[count] = writeOffset;
        valueCount = count;
    }

    private static int readVInt(byte[] arr, int pos) {
        int v = arr[pos++] & 0xFF;
        if ((v & 0x80) == 0) {
            return v;
        }
        v &= 0x7F;
        int shift = 7;
        byte b;
        do {
            b = arr[pos++];
            v |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return v;
    }

    private static int vIntSize(int v) {
        int n = 1;
        int u = v;
        while ((u & ~0x7F) != 0) {
            u >>>= 7;
            n++;
        }
        return n;
    }
}
