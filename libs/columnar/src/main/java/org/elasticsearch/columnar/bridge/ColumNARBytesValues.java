/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.bridge;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Forward iterator over a column of variable-length byte values — the bridge for keyword,
 * text, IP, and binary fields. Extends {@link DocIdSetIterator} so consumers use the
 * standard Lucene iteration protocol.
 *
 * <p>Multi-valued docs expose {@link #valueCount()} values at the current position. As
 * with the numeric bridge, the order is implementation-defined (insertion order preserved
 * by the mapper); the bridge never sorts, dedups, or surfaces ordinals.
 *
 * <p>For keyword aggregations that historically depended on {@code SortedSetDocValues}
 * (sorted + deduped + ord-indexed), a sibling adapter applies the sort + dedup
 * <em>dynamically on read</em> over the bytes the bridge exposes — keeping the codec's
 * binary-only contract intact while still supporting the legacy aggregation contract on
 * the bridge layer.
 */
public abstract class ColumNARBytesValues extends DocIdSetIterator {

    /**
     * Number of values at the current doc. Always {@code >= 1} after a successful
     * {@link #nextDoc()} / {@link #advance(int)} / {@link #advanceExact(int)}.
     */
    public abstract int valueCount();

    /**
     * The bytes for value {@code i} at the current doc. The returned {@link BytesRef} is
     * owned by the iterator and valid only until the next iteration step.
     */
    public abstract BytesRef bytesAt(int i);

    /** Convenience: decode the value at {@code i} as a UTF-8 string. */
    public String stringAt(int i) {
        return bytesAt(i).utf8ToString();
    }

    /**
     * Bulk-copy every value at the current doc into the caller-owned flat layout:
     * concatenated bytes into {@code valueBytes[bytesOff, …)} and per-value boundaries into
     * {@code offsets[offsetsOff, offsetsOff + valueCount()]} (one extra entry for the end
     * sentinel — value {@code i} occupies
     * {@code [offsets[offsetsOff + i], offsets[offsetsOff + i + 1])} relative to
     * {@code bytesOff}). Returns the total bytes written, so the caller can advance its own
     * write cursor.
     *
     * <p>Mirrors the long-typed {@link ColumNARLongValues#readValues}: a single bulk-fill
     * seam for ES|QL's column-at-a-time builders, with the default implementation correct
     * for every subclass via per-value {@link BytesRef} copies; impls backed by a
     * contiguous buffer ({@link PackedBytesFromBinaryDocValues}) override with one
     * {@code System.arraycopy}.
     */
    public int readValues(byte[] valueBytes, int bytesOff, int[] offsets, int offsetsOff) {
        final int n = valueCount();
        int writeOffset = 0;
        for (int i = 0; i < n; i++) {
            final BytesRef ref = bytesAt(i);
            offsets[offsetsOff + i] = writeOffset;
            System.arraycopy(ref.bytes, ref.offset, valueBytes, bytesOff + writeOffset, ref.length);
            writeOffset += ref.length;
        }
        offsets[offsetsOff + n] = writeOffset;
        return writeOffset;
    }

    /**
     * Returns {@code true} when the requested doc has at least one value, advancing the
     * iterator to that doc.
     */
    public boolean advanceExact(int target) throws java.io.IOException {
        final int next = advance(target);
        return next == target;
    }
}
