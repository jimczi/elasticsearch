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

/**
 * Forward iterator over a column of fixed-width numeric values, exposed as longs at the
 * storage layer and as typed values ({@code long}, {@code int}, {@code float},
 * {@code double}) at the consumer-facing API. The column is backed by the binary
 * doc-values substrate; this class is the read-side bridge that hides that detail and
 * gives the consumer of an int field {@code int}s, of a float field {@code float}s, etc.
 *
 * <p>Extends {@link DocIdSetIterator} so consumers get the standard Lucene iteration
 * protocol ({@link #nextDoc()}, {@link #advance(int)}, {@link #docID()}, {@link #cost()}).
 * Multi-valued docs expose {@link #valueCount()} values at the current position; the order
 * is implementation-defined — insertion order preserved by the mapper, never sorted, never
 * deduped, no ordinals exposed.
 *
 * <p>Typed accessors:
 * <ul>
 *   <li>{@link #longAt(int)} — native; the canonical accessor.</li>
 *   <li>{@link #intAt(int)} — narrowing cast for int fields.</li>
 *   <li>{@link #floatAt(int)} — bit reinterpretation for float fields
 *       ({@link Float#intBitsToFloat}).</li>
 *   <li>{@link #doubleAt(int)} — bit reinterpretation for double fields
 *       ({@link Double#longBitsToDouble}).</li>
 * </ul>
 * Consumers call the accessor that matches the field type they declared in their mapping;
 * the storage is identical underneath.
 */
public abstract class ColumNARLongValues extends DocIdSetIterator {

    /**
     * Number of values at the current doc. Always {@code >= 1} after a successful
     * {@link #nextDoc()} / {@link #advance(int)} / {@link #advanceExact(int)}.
     */
    public abstract int valueCount();

    /** Native long accessor. Implementations override this; the typed views derive from it. */
    public abstract long longAt(int i);

    /** Narrowing accessor for {@code int}-typed fields. */
    public int intAt(int i) {
        return (int) longAt(i);
    }

    /** Bit-reinterpret accessor for {@code float}-typed fields. */
    public float floatAt(int i) {
        return Float.intBitsToFloat((int) longAt(i));
    }

    /** Bit-reinterpret accessor for {@code double}-typed fields. */
    public double doubleAt(int i) {
        return Double.longBitsToDouble(longAt(i));
    }

    /**
     * Bulk-copy every value at the current doc into
     * {@code dest[destOffset, destOffset + valueCount())} and return the number of values
     * written. Equivalent to a loop over {@link #longAt(int)} but lets implementations
     * backed by a contiguous buffer (e.g. {@link PackedLongsFromBinaryDocValues}) collapse
     * the copy to a single {@code System.arraycopy}, which is the shape ES|QL's
     * {@code BlockLoader.SingletonLongBuilder.appendLongs(long[], int, int)} consumes.
     *
     * <p>Callers ensure {@code dest} has room for at least {@link #valueCount()} entries
     * starting at {@code destOffset}; the default loop-based implementation is correct for
     * every subclass, so adding the bulk seam costs nothing for impls that don't override.
     */
    public int readValues(long[] dest, int destOffset) {
        final int n = valueCount();
        for (int i = 0; i < n; i++) {
            dest[destOffset + i] = longAt(i);
        }
        return n;
    }

    /**
     * Returns {@code true} when the requested doc has at least one value, advancing the
     * iterator to that doc. Equivalent to the standard Lucene "advance to exact" pattern.
     */
    public boolean advanceExact(int target) throws java.io.IOException {
        final int next = advance(target);
        return next == target;
    }
}
