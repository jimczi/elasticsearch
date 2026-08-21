/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;
import java.util.function.IntSupplier;

/**
 * A string column at the {@code BINARY} surface: {@link #binaryValue} hands back a document's value as the
 * bytes it was given, which is what a keyword field writes for a lone value. Which {@link StringColumnLayout}
 * the segment used is invisible here — a layout resolves its own encoding inside the reader, so nothing
 * layout-specific reaches this surface.
 */
public final class ColumnarStringBinaryDocValues extends BinaryDocValues {

    private final StringColumnReader reader;
    private final ColumnIterator iterator;

    // Holds the several values of one document in the shape the mapper writes them; a lone value needs none.
    private final BytesRefBuilder payload = new BytesRefBuilder();

    public ColumnarStringBinaryDocValues(StringColumnReader reader, ColumnIterator iterator) {
        this.reader = reader;
        this.iterator = iterator;
    }

    /**
     * The document's values, in the shape the mapper writes them: a lone value as its raw bytes, and several
     * as {@code [length + 1][bytes]} each.
     *
     * <p>A slot the mapper left empty was kept on the way in as a value of no bytes, so it comes back out as
     * one: this column holds values and does not distinguish the two. What survives the round trip is the
     * document's value count, which is what the count recorded beside the field has to agree with.
     */
    @Override
    public BytesRef binaryValue() throws IOException {
        final int rank = iterator.rank();
        final long first = reader.firstValueAddress(rank);
        final long count = reader.valueCount(rank);
        if (count == 1) {
            return reader.valueAt(first);
        }
        payload.clear();
        for (long i = 0; i < count; i++) {
            // Copied rather than pointed at: the reader hands back one buffer it reuses, so the values have
            // to be taken out of it as they are read.
            final BytesRef value = reader.valueAt(first + i);
            writeVInt(payload, value.length + 1);
            payload.append(value);
        }
        return payload.get();
    }

    /** A vint into the builder, matching what the mapper's multi-valued encoding uses for a length. */
    private static void writeVInt(BytesRefBuilder out, int value) {
        while ((value & ~0x7F) != 0) {
            out.append((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.append((byte) value);
    }

    /** The column behind this surface, so a merge can read what it recorded rather than its values. */
    public StringColumnReader reader() {
        return reader;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return iterator.advanceExact(target);
    }

    @Override
    public int docID() {
        return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return iterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return iterator.advance(target);
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
        iterator.intoBitSet(upTo, bitSet, offset);
    }

    @Override
    public long cost() {
        return iterator.cost();
    }

    /**
     * A streaming cursor that reads this column's values directly off the data input — block-decoded, without
     * a payload round-trip. Used on merge to feed one segment's values into the writer.
     */
    public StringColumnValues directValues() {
        return directValues(null);
    }

    /**
     * As {@link #directValues()}, but reporting each value's ordinal translated through {@code ordinalMap}
     * so a merge can carry it over instead of resolving the value's bytes and looking them up again. A null
     * map, or a value that escaped this column's dictionary, falls back to the bytes.
     */
    public StringColumnValues directValues(int[] ordinalMap) {
        return new StringColumnValues() {
            private long first;
            private long count;
            private int upto;

            @Override
            public int valueCount() {
                return (int) count;
            }

            @Override
            public int nextOrdinal() throws IOException {
                if (ordinalMap == null) {
                    return -1;
                }
                final int ordinal = reader.ordinalAt(first + upto);
                if (ordinal >= ordinalMap.length) {
                    // Escaped this column's dictionary, so only its bytes say what it is.
                    return -1;
                }
                upto++;
                return ordinalMap[ordinal];
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return reader.valueAt(first + upto++);
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return position(iterator.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return position(iterator.advance(target));
            }

            @Override
            public long cost() {
                return iterator.cost();
            }

            private int position(int doc) throws IOException {
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    int rank = iterator.rank();
                    first = reader.firstValueAddress(rank);
                    count = reader.valueCount(rank);
                    upto = 0;
                }
                return doc;
            }
        };
    }

    /**
     * Wraps a foreign {@link BinaryDocValues} as a write-path cursor, reading the shape the mapper writes:
     * a lone value as its raw bytes, several as {@code [length + 1][bytes]} each, where a length of zero is
     * a slot the mapper left empty. This is the ingest path and the merge fallback for a segment written by
     * some other implementation of this surface.
     *
     * <p>A slot the mapper left empty is kept, as a value of no bytes. The column does not know what an
     * empty slot meant and does not need to: keeping it is what makes the count this column reports agree
     * with the count the mapper recorded beside it, which is what every other reader of this field trusts.
     */
    public static StringColumnValues decodeValues(BinaryDocValues binary, IntSupplier valueCount) {
        return new StringColumnValues() {
            private final BytesRef value = new BytesRef();
            private final ByteArrayDataInput in = new ByteArrayDataInput();
            private BytesRef blob;
            private int count;

            @Override
            public int valueCount() {
                return count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                if (count == 1) {
                    // A lone value is stored as itself, with nothing to say how long it is.
                    return binary.binaryValue();
                }
                final int length = in.readVInt() - 1;
                value.bytes = blob.bytes;
                value.offset = in.getPosition();
                value.length = Math.max(length, 0);
                in.skipBytes(value.length);
                return value;
            }

            @Override
            public int docID() {
                return binary.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return position(binary.nextDoc());
            }

            @Override
            public int advance(int target) throws IOException {
                return position(binary.advance(target));
            }

            @Override
            public long cost() {
                return binary.cost();
            }

            /**
             * How many values this document holds comes from beside the blob, not from within it: a lone
             * value is stored as its own bytes with no length, which is indistinguishable from the first
             * value of several.
             */
            private int position(int doc) throws IOException {
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    count = valueCount.getAsInt();
                    if (count > 1) {
                        blob = binary.binaryValue();
                        in.reset(blob.bytes, blob.offset, blob.length);
                    }
                }
                return doc;
            }
        };
    }
}
