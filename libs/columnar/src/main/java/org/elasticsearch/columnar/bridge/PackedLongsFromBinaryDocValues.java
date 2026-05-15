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
 * {@link ColumNARLongValues} backed by a Lucene {@link BinaryDocValues}. The bytes follow
 * the layout written by {@link PackedLongBinaryPacker}: a per-doc
 * {@code [vint count][count × little-endian long]} payload that subsumes single- and
 * multi-valued docs in one shape.
 *
 * <p>This is the format's "no-typed-DV" read bridge: callers open a
 * {@link BinaryDocValues} via the standard Lucene
 * {@link org.apache.lucene.index.LeafReader#getBinaryDocValues} entry point, wrap it in
 * this class, and call {@link #longAt(int)} (or {@link #intAt(int)} / {@link #floatAt(int)}
 * / {@link #doubleAt(int)} for typed views) — they never touch {@code NumericDocValues}
 * or {@code SortedNumericDocValues}.
 */
public final class PackedLongsFromBinaryDocValues extends ColumNARLongValues {

    private final BinaryDocValues source;
    private long[] buffer;
    private int valueCount;

    public PackedLongsFromBinaryDocValues(BinaryDocValues source) {
        this.source = source;
        this.buffer = new long[1];
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
    public long longAt(int i) {
        return buffer[i];
    }

    @Override
    public int readValues(long[] dest, int destOffset) {
        System.arraycopy(buffer, 0, dest, destOffset, valueCount);
        return valueCount;
    }

    private void decodeCurrent() throws IOException {
        final BytesRef bytes = source.binaryValue();
        final int count = PackedLongBinaryPacker.decodeCount(bytes);
        if (count > buffer.length) {
            buffer = new long[ArrayUtil.oversize(count, Long.BYTES)];
        }
        valueCount = PackedLongBinaryPacker.decode(bytes, buffer);
    }
}
