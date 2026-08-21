/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.MonotonicWriter;

import java.io.IOException;

/**
 * Describes a string column in one of two shapes.
 *
 * <p><b>Plain</b> ({@code dictionarySize == 0}): the values in written order, in one {@link ValueStream}.
 *
 * <p><b>Dictionary</b>: the most frequent values in a sorted dictionary, an ordinal per document, and the
 * values that missed the dictionary in an exception stream. An ordinal below {@code dictionarySize} names a
 * dictionary entry; the escape marker {@code dictionarySize} says the value is in the exception stream, and
 * which one is found by counting escapes before that document — the {@code escapeRanks} table holds that
 * count per block of documents so the count never restarts from zero.
 *
 * <p>The escape marker is a constant rather than the exception's index because a block of ordinals is packed
 * to the width of its largest value: an index would scale with the row count, the marker only with the
 * dictionary.
 */
public record StringColumnMetadata(
    ColumnIteratorMetadata iterator,
    int numDocsWithField,
    long numValues,
    long valueBytes,
    int dictionarySize,
    ValueStream.Metadata values,
    ValueStream.Metadata dictionary,
    ValueStream.Metadata exceptions,
    NumericColumnMetadata ordinals,
    int escapeRankBlockSize,
    MonotonicWriter.Table escapeRanks,
    NumericColumnMetadata valueCounts,
    int docBlockSize,
    MonotonicWriter.Table docBlockBases,
    boolean valuesSorted,
    ValueStream.Metadata summaryTerms,
    long summaryCountsOffset,
    long summaryCountsLength,
    long summaryValues
) {

    /** True when at least one document holds more than one value. */
    public boolean multiValued() {
        return numValues > numDocsWithField;
    }

    public boolean hasDictionary() {
        return dictionarySize > 0;
    }

    public static StringColumnMetadata plain(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long valueBytes,
        ValueStream.Metadata values
    ) {
        return plain(iterator, numDocsWithField, numValues, valueBytes, values, false);
    }

    /** A plain column, recording whether its values arrived in term order. */
    public static StringColumnMetadata plain(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        long valueBytes,
        ValueStream.Metadata values,
        boolean valuesSorted
    ) {
        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            valueBytes,
            0,
            values,
            ValueStream.Metadata.empty(),
            ValueStream.Metadata.empty(),
            null,
            0,
            MonotonicWriter.Table.NONE,
            null,
            0,
            MonotonicWriter.Table.NONE,
            valuesSorted,
            null,
            0,
            0,
            0
        );
    }

    public void writeTo(DataOutput out) throws IOException {
        iterator.writeTo(out);
        out.writeVInt(numDocsWithField);
        if (numDocsWithField == 0) {
            return;
        }
        out.writeVLong(numValues);
        out.writeVLong(valueBytes);
        out.writeVInt(dictionarySize);
        if (dictionarySize == 0) {
            values.writeTo(out);
            out.writeByte((byte) (valuesSorted ? 1 : 0));
            writeSummary(out);
            writeAddresses(out);
            return;
        }
        dictionary.writeTo(out);
        exceptions.writeTo(out);
        ordinals.writeTo(out);
        out.writeByte((byte) (valuesSorted ? 1 : 0));
        writeSummary(out);
        out.writeVInt(escapeRankBlockSize);
        out.writeVLong(escapeRanks.dataOffset());
        out.writeVLong(escapeRanks.dataLength());
        out.writeVInt(escapeRanks.meta().length);
        out.writeBytes(escapeRanks.meta(), 0, escapeRanks.meta().length);
        writeAddresses(out);
    }

    /**
     * The most frequent terms of a column that has no dictionary, and how often each was seen. It is what a
     * merge reads instead of surveying the values again: the summaries of the segments being merged combine
     * into one, so the merged column's vocabulary is chosen without a pass to discover it.
     */
    private void writeSummary(DataOutput out) throws IOException {
        out.writeByte((byte) (summaryTerms != null ? 1 : 0));
        if (summaryTerms == null) {
            return;
        }
        summaryTerms.writeTo(out);
        out.writeVLong(summaryCountsOffset);
        out.writeVLong(summaryCountsLength);
        out.writeVLong(summaryValues);
    }

    private static StringColumnMetadata readSummary(DataInput in, StringColumnMetadata partial) throws IOException {
        if (in.readByte() == 0) {
            return partial;
        }
        final ValueStream.Metadata terms = ValueStream.Metadata.readFrom(in);
        final long offset = in.readVLong();
        final long length = in.readVLong();
        final long values = in.readVLong();
        return new StringColumnMetadata(
            partial.iterator(),
            partial.numDocsWithField(),
            partial.numValues(),
            partial.valueBytes(),
            partial.dictionarySize(),
            partial.values(),
            partial.dictionary(),
            partial.exceptions(),
            partial.ordinals(),
            partial.escapeRankBlockSize(),
            partial.escapeRanks(),
            partial.valueCounts(),
            partial.docBlockSize(),
            partial.docBlockBases(),
            partial.valuesSorted(),
            terms,
            offset,
            length,
            values
        );
    }

    private void writeAddresses(DataOutput out) throws IOException {
        out.writeByte((byte) (multiValued() ? 1 : 0));
        if (multiValued() == false) {
            return;
        }
        valueCounts.writeTo(out);
        out.writeVInt(docBlockSize);
        out.writeVLong(docBlockBases.dataOffset());
        out.writeVLong(docBlockBases.dataLength());
        out.writeVInt(docBlockBases.meta().length);
        out.writeBytes(docBlockBases.meta(), 0, docBlockBases.meta().length);
    }

    public static StringColumnMetadata readFrom(DataInput in, int maxDoc, FormatVersion version) throws IOException {
        final ColumnIteratorMetadata iterator = ColumnIteratorMetadata.readFrom(in, maxDoc, version);
        final int numDocsWithField = in.readVInt();
        if (numDocsWithField == 0) {
            return plain(iterator, 0, 0, 0, ValueStream.Metadata.empty());
        }
        final long numValues = in.readVLong();
        final long valueBytes = in.readVLong();
        final int dictionarySize = in.readVInt();
        if (dictionarySize == 0) {
            final ValueStream.Metadata plainValues = ValueStream.Metadata.readFrom(in);
            final boolean plainSorted = in.readByte() == 1;
            final StringColumnMetadata plain = readSummary(
                in,
                plain(iterator, numDocsWithField, numValues, valueBytes, plainValues, plainSorted)
            );
            return readAddresses(in, maxDoc, version, plain);
        }
        final ValueStream.Metadata dictionary = ValueStream.Metadata.readFrom(in);
        final ValueStream.Metadata exceptions = ValueStream.Metadata.readFrom(in);
        // The ordinals are a column of their own, one value per value of this one, so their presence layer
        // spans that many documents and not this column's. Handing down the wrong extent leaves their
        // iterator willing to walk past the end of them.
        final NumericColumnMetadata ordinals = NumericColumnMetadata.readFrom(in, Math.toIntExact(numValues), version);
        final boolean sorted = in.readByte() == 1;
        final boolean hasSummary = in.readByte() == 1;
        final ValueStream.Metadata summary = hasSummary ? ValueStream.Metadata.readFrom(in) : null;
        final long summaryOffset = hasSummary ? in.readVLong() : 0;
        final long summaryLength = hasSummary ? in.readVLong() : 0;
        final long summaryValues = hasSummary ? in.readVLong() : 0;
        final int rankBlockSize = in.readVInt();
        final long rankOffset = in.readVLong();
        final long rankLength = in.readVLong();
        final byte[] rankMeta = new byte[in.readVInt()];
        in.readBytes(rankMeta, 0, rankMeta.length);
        final StringColumnMetadata partial = new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            valueBytes,
            dictionarySize,
            ValueStream.Metadata.empty(),
            dictionary,
            exceptions,
            ordinals,
            rankBlockSize,
            new MonotonicWriter.Table(rankOffset, rankLength, rankMeta),
            null,
            0,
            MonotonicWriter.Table.NONE,
            sorted,
            summary,
            summaryOffset,
            summaryLength,
            summaryValues
        );
        return readAddresses(in, maxDoc, version, partial);
    }

    private static StringColumnMetadata readAddresses(DataInput in, int maxDoc, FormatVersion version, StringColumnMetadata partial)
        throws IOException {
        if (in.readByte() == 0) {
            return partial;
        }
        // One count per document that has a value, so the counts span the documents with values.
        final NumericColumnMetadata counts = NumericColumnMetadata.readFrom(in, partial.numDocsWithField(), version);
        final int docBlockSize = in.readVInt();
        final long offset = in.readVLong();
        final long length = in.readVLong();
        final byte[] meta = new byte[in.readVInt()];
        in.readBytes(meta, 0, meta.length);
        return new StringColumnMetadata(
            partial.iterator(),
            partial.numDocsWithField(),
            partial.numValues(),
            partial.valueBytes(),
            partial.dictionarySize(),
            partial.values(),
            partial.dictionary(),
            partial.exceptions(),
            partial.ordinals(),
            partial.escapeRankBlockSize(),
            partial.escapeRanks(),
            counts,
            docBlockSize,
            new MonotonicWriter.Table(offset, length, meta),
            partial.valuesSorted(),
            partial.summaryTerms(),
            partial.summaryCountsOffset(),
            partial.summaryCountsLength(),
            partial.summaryValues()
        );
    }
}
