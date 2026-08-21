/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Writes a string column, choosing between the plain and dictionary shapes from what the values look like.
 *
 * <p>The dictionary shape is taken when the most frequent values account for enough of the rows to make an
 * ordinal worth reading — that is a property of how values repeat, not of how many distinct ones there are,
 * because a value that misses the dictionary still costs only what it would have cost plain.
 */
public final class StringColumnWriter {

    /** Documents behind one value-address entry, for a multi-valued column. */
    public static final int DOC_BLOCK = 128;

    private StringColumnWriter() {}

    /**
     * Writes the values in the order they arrive, in one indexed byte stream, and records whether they
     * arrived in term order.
     *
     * @param valuesPerBlock values behind one offset in a byte stream
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        ChunkCodec codec,
        int targetChunkBytes,
        int valuesPerBlock,
        Directory dir,
        IOContext ctx,
        IndexOutput data
    ) throws IOException {

        final ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.plain(iterator, 0, 0, 0, ValueStream.Metadata.empty());
        }
        final boolean[] valuesSorted = { true };
        final long[] columnBytes = { 0 };
        final ValueStream.Metadata values = writeValues(
            cursors.get(),
            valuesSorted,
            columnBytes,
            codec,
            targetChunkBytes,
            valuesPerBlock,
            numValues,
            dir,
            ctx,
            data
        );
        final StringColumnMetadata written = StringColumnMetadata.plain(
            iterator,
            numDocsWithField,
            numValues,
            columnBytes[0],
            values,
            valuesSorted[0]
        );
        return withAddresses(written, cursors, numDocsWithField, numValues, dir, ctx, data);
    }

    private static StringColumnMetadata withAddresses(
        StringColumnMetadata metadata,
        IOSupplier<StringColumnValues> cursors,
        int numDocsWithField,
        long numValues,
        Directory dir,
        IOContext ctx,
        IndexOutput data
    ) throws IOException {
        if (metadata.multiValued() == false) {
            return metadata;
        }
        final NumericColumnMetadata counts = NumericColumnWriter.write(
            numDocsWithField,
            numDocsWithField,
            numDocsWithField,
            () -> countCursor(cursors.get()),
            NumericPipeline.defaultPipeline(128),
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            null,
            dir,
            ctx,
            data
        );
        final MonotonicWriter.Table bases;
        try (MonotonicWriter writer = new MonotonicWriter(dir, ctx, data.getName(), (numDocsWithField + DOC_BLOCK - 1) / DOC_BLOCK + 1L)) {
            final StringColumnValues values = cursors.get();
            long first = 0;
            int rank = 0;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                if (rank % DOC_BLOCK == 0) {
                    writer.add(first);
                }
                first += values.valueCount();
                rank++;
            }
            writer.add(first);
            bases = writer.finish(data);
        }
        return new StringColumnMetadata(
            metadata.iterator(),
            metadata.numDocsWithField(),
            metadata.numValues(),
            metadata.valueBytes(),
            metadata.dictionarySize(),
            metadata.values(),
            metadata.dictionary(),
            metadata.exceptions(),
            metadata.ordinals(),
            metadata.escapeRankBlockSize(),
            metadata.escapeRanks(),
            counts,
            DOC_BLOCK,
            bases,
            metadata.valuesSorted(),
            metadata.summaryTerms(),
            metadata.summaryCountsOffset(),
            metadata.summaryCountsLength(),
            metadata.summaryValues()
        );
    }

    /** Yields one value per document: how many values that document holds. */
    private static NumericColumnValues countCursor(StringColumnValues values) {
        return new NumericColumnValues() {
            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public long nextValue() {
                return values.valueCount();
            }

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return values.cost();
            }
        };
    }

    private static ValueStream.Metadata writeValues(
        StringColumnValues values,
        boolean[] valuesSorted,
        long[] columnBytes,
        ChunkCodec codec,
        int targetChunkBytes,
        int valuesPerBlock,
        long numValues,
        Directory dir,
        IOContext ctx,
        IndexOutput data
    ) throws IOException {
        try (
            ValueStream.Writer writer = new ValueStream.Writer(
                codec,
                targetChunkBytes,
                valuesPerBlock,
                numValues,
                dir,
                ctx,
                data.getName(),
                data
            )
        ) {
            final BytesRefBuilder previous = new BytesRefBuilder();
            boolean sorted = true;
            boolean first = true;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                for (int i = 0, count = values.valueCount(); i < count; i++) {
                    final BytesRef value = values.nextValue();
                    columnBytes[0] += value.length;
                    // Values that arrive in order let a term be found by bisection rather than by comparing
                    // every one of them, which is what an index sorted by this field produces.
                    sorted &= first || previous.get().compareTo(value) <= 0;
                    previous.copyBytes(value);
                    first = false;
                    writer.add(value);
                }
            }
            valuesSorted[0] = sorted;
            return writer.finish();
        }
    }

}
