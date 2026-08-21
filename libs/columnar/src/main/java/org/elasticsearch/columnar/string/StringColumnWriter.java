/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntroSorter;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntBinaryOperator;

/**
 * Writes a string column, choosing between the plain and dictionary shapes from what the values look like.
 *
 * <p>The dictionary shape is taken when the most frequent values account for enough of the rows to make an
 * ordinal worth reading — that is a property of how values repeat, not of how many distinct ones there are,
 * because a value that misses the dictionary still costs only what it would have cost plain.
 */
public final class StringColumnWriter {

    /** Documents behind one escape-rank entry: the walk to resolve an escape never exceeds this. */
    public static final int ESCAPE_RANK_BLOCK = 128;

    /** Documents behind one value-address entry, for a multi-valued column. */
    public static final int DOC_BLOCK = 128;

    private StringColumnWriter() {}

    /**
     * @param valuesPerBlock values behind one offset in a byte stream
     * @param policy bounds the dictionary and decides whether it is worth keeping
     * @param known  a vocabulary already known to cover the values, or null to survey them
     */
    public static StringColumnMetadata write(
        int maxDoc,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        ChunkCodec codec,
        int targetChunkBytes,
        int valuesPerBlock,
        DictionaryPolicy policy,
        Vocabulary known,
        Directory dir,
        IOContext ctx,
        IndexOutput data
    ) throws IOException {

        final ColumnIteratorMetadata iterator = ColumnIteratorWriter.write(cursors.get(), numDocsWithField, maxDoc, data);
        if (numDocsWithField == 0) {
            return StringColumnMetadata.plain(iterator, 0, 0, 0, ValueStream.Metadata.empty());
        }

        final Vocabulary vocabulary = known != null ? known : (policy.enabled() ? survey(cursors.get(), policy, numValues) : null);
        if (vocabulary == null || policy.worthKeeping(vocabulary.coverage, vocabulary.dictionaryBytes, vocabulary.columnBytes) == false) {
            final boolean[] plainSorted = { true };
            final long[] plainBytes = { 0 };
            final ValueStream.Metadata plainValues = writeValues(
                cursors.get(),
                plainSorted,
                plainBytes,
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
                plainBytes[0],
                plainValues,
                plainSorted[0]
            );
            // The terms this column holds most are kept even though it stays plain, so a merge of plain
            // segments combines their summaries instead of surveying their values a second time.
            final StringColumnMetadata plain = withSummary(written, vocabulary, codec, targetChunkBytes, valuesPerBlock, dir, ctx, data);
            return withAddresses(plain, cursors, numDocsWithField, numValues, dir, ctx, data);
        }
        final StringColumnMetadata written = writeDictionary(
            iterator,
            numDocsWithField,
            numValues,
            cursors,
            vocabulary,
            codec,
            targetChunkBytes,
            valuesPerBlock,
            dir,
            ctx,
            data
        );
        // A dictionary that let values escape does not describe the column on its own, so a merge of such
        // columns would have to survey their values again. The counts behind it are recorded for the same
        // reason a plain column records them, and cost only the counts: the terms are the dictionary.
        final StringColumnMetadata dictionary = withSummary(written, vocabulary, codec, targetChunkBytes, valuesPerBlock, dir, ctx, data);
        return withAddresses(dictionary, cursors, numDocsWithField, numValues, dir, ctx, data);
    }

    /**
     * Stores the surveyed terms and their counts on a column that stayed plain. The counts are lower bounds
     * — a term is charged an occurrence whenever room has to be made — so a coverage worked out from them
     * is an under-estimate, and a merge that reads them can only be too cautious about building a
     * dictionary, never too eager.
     */
    private static StringColumnMetadata withSummary(
        StringColumnMetadata metadata,
        Vocabulary vocabulary,
        ChunkCodec codec,
        int targetChunkBytes,
        int valuesPerBlock,
        Directory dir,
        IOContext ctx,
        IndexOutput data
    ) throws IOException {
        // Without counts there is nothing worth recording: a summary that claimed each term was seen once
        // would read back as near-zero coverage and talk every later merge out of a dictionary.
        if (vocabulary == null || vocabulary.counted() == false || vocabulary.sortedIds().length == 0) {
            return metadata;
        }
        final int size = vocabulary.sortedIds().length;
        final ValueStream.Metadata terms;
        if (metadata.hasDictionary()) {
            // The dictionary already holds these terms, in this order. Pointing at it costs nothing.
            assert metadata.dictionarySize() == size : metadata.dictionarySize() + " != " + size;
            terms = metadata.dictionary();
        } else {
            final BytesRef term = new BytesRef();
            try (
                ValueStream.Writer writer = new ValueStream.Writer(
                    codec,
                    targetChunkBytes,
                    valuesPerBlock,
                    size,
                    dir,
                    ctx,
                    data.getName(),
                    data
                )
            ) {
                for (int ordinal = 0; ordinal < size; ordinal++) {
                    vocabulary.terms().get(vocabulary.sortedIds()[ordinal], term);
                    writer.add(term);
                }
                terms = writer.finish();
            }
        }
        final long countsOffset = data.getFilePointer();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            data.writeVLong(vocabulary.countOf(ordinal));
        }
        final long countsLength = data.getFilePointer() - countsOffset;
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
            metadata.valueCounts(),
            metadata.docBlockSize(),
            metadata.docBlockBases(),
            metadata.valuesSorted(),
            terms,
            countsOffset,
            countsLength,
            metadata.numValues()
        );
    }

    /**
     * Writes what a multi-valued column needs to find a document's values: one base per {@link #DOC_BLOCK}
     * documents, and the per-document counts as a numeric column. A column of mostly ones packs to almost
     * nothing there, and a single-valued column writes neither — its rank is its value index.
     */
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

    /** The terms a dictionary will hold, in term order, and what share of the column they account for. */
    public record Vocabulary(
        BytesRefHash terms,
        int[] sortedIds,
        int[] ordinalOfId,
        double coverage,
        long dictionaryBytes,
        long columnBytes,
        int[] counts
    ) {
        /** Whether this vocabulary knows how often it saw each of its terms. */
        boolean counted() {
            return counts != null;
        }

        /** How often the term at {@code ordinal} was seen, as a lower bound. */
        int countOf(int ordinal) {
            return counts[sortedIds[ordinal]];
        }
    }

    /**
     * A vocabulary a merge worked out from what its inputs recorded rather than from their values: the
     * union of their dictionaries, or the sum of their summaries. Either way the values need not be
     * surveyed a second time to discover what they contain.
     *
     * @param sortedTerms the vocabulary, in term order
     * @param coverage    the share of the merged column's values these terms hold — one for a union of
     *                    dictionaries that let nothing escape, and otherwise an under-estimate, since the
     *                    counts a summary carries are themselves lower bounds
     */
    public static Vocabulary knownVocabulary(List<BytesRef> sortedTerms, long columnBytes, double coverage, long[] countsPerTerm) {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        final int[] sortedIds = new int[sortedTerms.size()];
        final int[] ordinalOfId = new int[sortedTerms.size()];
        final int[] counts = countsPerTerm == null ? null : new int[sortedTerms.size()];
        long dictionaryBytes = 0;
        for (int ordinal = 0; ordinal < sortedTerms.size(); ordinal++) {
            int id = terms.add(sortedTerms.get(ordinal));
            if (id < 0) {
                id = -1 - id;
            }
            sortedIds[ordinal] = id;
            ordinalOfId[id] = ordinal;
            dictionaryBytes += sortedTerms.get(ordinal).length;
            if (counts != null) {
                counts[id] = (int) Math.min(Integer.MAX_VALUE, countsPerTerm[ordinal]);
            }
        }
        return new Vocabulary(terms, sortedIds, ordinalOfId, coverage, dictionaryBytes, columnBytes, counts);
    }

    private static Vocabulary survey(StringColumnValues values, DictionaryPolicy policy, long numValues) throws IOException {
        final BytesRefHash terms = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(Counter.newCounter())));
        int[] counts = new int[64];
        long dictionaryBytes = 0;
        long columnBytes = 0;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            for (int i = 0, count = values.valueCount(); i < count; i++) {
                final BytesRef value = values.nextValue();
                columnBytes += value.length;
                int id = terms.find(value);
                if (id < 0) {
                    if (dictionaryBytes + value.length > policy.maxBytes()) {
                        // The dictionary is full, so room is made by charging every term one occurrence and
                        // dropping those that reach zero. A term is only displaced by terms that between them
                        // occur more often than it does, so the values most of the column holds survive
                        // however late they are first seen; admitting whatever arrived first would keep the
                        // leading values instead of the common ones.
                        final long[] freed = { 0 };
                        counts = evictLeastFrequent(terms, counts, freed);
                        dictionaryBytes -= freed[0];
                        if (dictionaryBytes + value.length > policy.maxBytes()) {
                            // Nothing could be displaced: every term held occurs at least as often as this.
                            continue;
                        }
                    }
                    id = terms.add(value);
                    if (id < 0) {
                        id = -1 - id;
                    }
                    counts = ArrayUtil.grow(counts, id + 1);
                    dictionaryBytes += value.length;
                }
                counts[id]++;
            }
        }
        if (terms.size() == 0) {
            return null;
        }
        // The survey admits every term that fits, which on a column with a long tail means the budget goes
        // to terms seen once. Keeping only the most frequent that fit the column's budget leaves a
        // dictionary that costs a fraction of what it describes, and the terms dropped here escape.
        final int[] kept = keepMostFrequent(terms, counts, policy.budgetFor(columnBytes));
        final int size = kept.length;
        if (size == 0) {
            return null;
        }
        // The kept ids are in term order, so an ordinal comparison is a term comparison and a range of
        // terms resolves to a range of ordinals. The order is built over the ids rather than through
        // BytesRefHash#sort, which compacts the hash and leaves it unable to answer find() for the pass
        // that assigns the ordinals.
        final int[] sortedIds = kept;
        // Indexed by id, so a term the survey saw but did not keep has to be told apart from ordinal zero.
        final int[] ordinalOfId = new int[terms.size()];
        Arrays.fill(ordinalOfId, DROPPED);
        long covered = 0;
        long keptBytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            final int id = sortedIds[ordinal];
            ordinalOfId[id] = ordinal;
            covered += counts[id];
            terms.get(id, scratch);
            keptBytes += scratch.length;
        }
        return new Vocabulary(terms, sortedIds, ordinalOfId, (double) covered / numValues, keptBytes, columnBytes, counts);
    }

    /** An id the survey saw but left out of the dictionary; its values escape like any unknown term. */
    private static final int DROPPED = -1;

    /**
     * The ids worth a dictionary entry, in term order: the most frequent terms whose bytes fit
     * {@code budget}. Terms seen equally often are ordered by term, so the same column always yields the
     * same dictionary.
     */
    private static int[] keepMostFrequent(BytesRefHash terms, int[] counts, long budget) {
        final int size = terms.size();
        final int[] ids = new int[size];
        for (int id = 0; id < size; id++) {
            ids[id] = id;
        }
        sort(ids, 0, size, terms, (a, b) -> Integer.compare(counts[b], counts[a]));
        int keptCount = 0;
        long bytes = 0;
        final BytesRef scratch = new BytesRef();
        for (int i = 0; i < size; i++) {
            // A term seen once is worth one value's coverage and costs its own bytes plus, once there are
            // enough of them, a wider ordinal on every value in the column. It is cheaper to let it escape.
            // The counts are lower bounds, so a term dropped here was seen at most twice.
            if (counts[ids[i]] <= 1) {
                break;
            }
            terms.get(ids[i], scratch);
            if (bytes + scratch.length > budget) {
                break;
            }
            bytes += scratch.length;
            keptCount++;
        }
        final int[] kept = ArrayUtil.copyOfSubArray(ids, 0, keptCount);
        // Back into term order, which is the order the dictionary is written and searched in.
        sort(kept, 0, keptCount, terms, null);
        return kept;
    }

    /**
     * Orders {@code ids} by {@code first}, and by their terms where it does not separate them. Comparing by
     * term last leaves the order total, so the same column always yields the same dictionary.
     */
    private static void sort(int[] ids, int from, int to, BytesRefHash terms, IntBinaryOperator first) {
        new IntroSorter() {
            private final BytesRef left = new BytesRef();
            private final BytesRef right = new BytesRef();
            private int pivotId;

            @Override
            protected void swap(int i, int j) {
                final int tmp = ids[i];
                ids[i] = ids[j];
                ids[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                return compareIds(ids[i], ids[j]);
            }

            @Override
            protected void setPivot(int i) {
                pivotId = ids[i];
            }

            @Override
            protected int comparePivot(int j) {
                return compareIds(pivotId, ids[j]);
            }

            private int compareIds(int a, int b) {
                if (first != null) {
                    final int cmp = first.applyAsInt(a, b);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                terms.get(a, left);
                terms.get(b, right);
                return left.compareTo(right);
            }
        }.sort(from, to);
    }

    /** Writes the staged escaped values, now that how many of them there are is known. */
    private static ValueStream.Metadata replayExceptions(
        Directory dir,
        IOContext ctx,
        String name,
        long count,
        ChunkCodec codec,
        int targetChunkBytes,
        int valuesPerBlock,
        IndexOutput data
    ) throws IOException {
        if (count == 0) {
            return ValueStream.Metadata.empty();
        }
        try (
            IndexInput staged = dir.openInput(name, ctx);
            ValueStream.Writer writer = new ValueStream.Writer(
                codec,
                targetChunkBytes,
                valuesPerBlock,
                count,
                dir,
                ctx,
                data.getName(),
                data
            )
        ) {
            final BytesRef value = new BytesRef();
            for (long i = 0; i < count; i++) {
                final int length = staged.readVInt();
                if (value.bytes.length < length) {
                    value.bytes = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
                }
                staged.readBytes(value.bytes, 0, length);
                value.offset = 0;
                value.length = length;
                writer.add(value);
            }
            return writer.finish();
        }
    }

    /**
     * Charges every tracked term one occurrence and drops those that fall to zero, reporting the bytes the
     * dropped terms held. Survivors keep their remaining counts, so a term seen many times is not displaced
     * by one seen once.
     */
    private static int[] evictLeastFrequent(BytesRefHash terms, int[] counts, long[] freed) {
        final int size = terms.size();
        // Every count falls by the same amount, and the terms that reach zero leave. Taking that amount to
        // be the median rather than one frees half the table at a stroke, so a column of mostly distinct
        // values makes room a few times rather than once per value it cannot fit. The bound is unchanged:
        // a round of decrements absorbs as many occurrences as there are terms held, so across the column
        // they can absorb at most one term's worth of n/k, which is the error a count already carries.
        final int[] sorted = ArrayUtil.copyOfSubArray(counts, 0, size);
        Arrays.sort(sorted);
        final int decrement = Math.max(1, sorted[size / 2]);

        final BytesRef scratch = new BytesRef();
        final List<BytesRef> survivors = new ArrayList<>();
        final IntArrayList survivorCounts = new IntArrayList();
        for (int id = 0; id < size; id++) {
            terms.get(id, scratch);
            if (counts[id] > decrement) {
                survivors.add(BytesRef.deepCopyOf(scratch));
                survivorCounts.add(counts[id] - decrement);
            } else {
                freed[0] += scratch.length;
            }
        }
        if (survivors.size() == size) {
            // Nothing fell to zero, so the counts are simply reduced where they are.
            for (int id = 0; id < size; id++) {
                counts[id] -= decrement;
            }
            return counts;
        }
        terms.clear();
        terms.reinit();
        // The ids are handed out afresh, so a count left over from the old numbering would be read as a
        // new term's.
        final int[] rebuilt = new int[Math.max(counts.length, survivors.size() + 1)];
        for (int i = 0; i < survivors.size(); i++) {
            int id = terms.add(survivors.get(i));
            if (id < 0) {
                id = -1 - id;
            }
            rebuilt[id] = survivorCounts.get(i);
        }
        return rebuilt;
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

    private static StringColumnMetadata writeDictionary(
        ColumnIteratorMetadata iterator,
        int numDocsWithField,
        long numValues,
        IOSupplier<StringColumnValues> cursors,
        Vocabulary vocabulary,
        ChunkCodec codec,
        int targetChunkBytes,
        int valuesPerBlock,
        Directory dir,
        IOContext ctx,
        IndexOutput data
    ) throws IOException {

        final int dictionarySize = vocabulary.sortedIds.length;
        final BytesRef scratch = new BytesRef();

        final ValueStream.Metadata dictionary;
        try (
            // The dictionary is read by ordinal, so consecutive values land anywhere in it. Compressing it
            // would mean decompressing a chunk for nearly every value read, to save a few tens of kilobytes
            // — the dictionary is bounded by the policy however large the column is.
            ValueStream.Writer writer = new ValueStream.Writer(
                ChunkCodec.IDENTITY,
                targetChunkBytes,
                valuesPerBlock,
                dictionarySize,
                dir,
                ctx,
                data.getName(),
                data
            )
        ) {
            for (int ordinal = 0; ordinal < dictionarySize; ordinal++) {
                vocabulary.terms.get(vocabulary.sortedIds[ordinal], scratch);
                writer.add(scratch);
            }
            dictionary = writer.finish();
        }

        // One pass emitting the ordinal per value and the escaped values, with the escape counts per block.
        // The ordinals are staged in a temporary file rather than an array: there is one per value, so an
        // array would be the one structure in this writer that grows with the column.
        final IndexOutput ordinalTemp = dir.createTempOutput(data.getName(), "columnar-ordinals", ctx);
        final String ordinalTempName = ordinalTemp.getName();
        final boolean[] sorted = { true };
        final int[] previous = { 0 };
        // What these values would cost stored plainly, which is what the dictionary describing them is
        // weighed against. It cannot be read back from a dictionary column, where a term stands for as
        // many values as use it, so it is counted here, on the pass that visits every value anyway.
        final long[] columnBytes = { 0 };
        long escapes = 0;
        final ValueStream.Metadata exceptions;
        MonotonicWriter ranks = null;
        MonotonicWriter.Table rankTable;
        try {
            ranks = new MonotonicWriter(dir, ctx, data.getName(), (numValues + ESCAPE_RANK_BLOCK - 1) / ESCAPE_RANK_BLOCK + 1L);
            // The escaped values are staged and written afterwards: how many escape is not known until the
            // pass is over, because the survey's counts are lower bounds, and the stream has to be told its
            // length before it starts.
            final IndexOutput exceptionTemp = dir.createTempOutput(data.getName(), "columnar-exceptions", ctx);
            final String exceptionTempName = exceptionTemp.getName();
            try {
                final StringColumnValues values = cursors.get();
                int index = 0;
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    for (int i = 0, count = values.valueCount(); i < count; i++) {
                        if (index % ESCAPE_RANK_BLOCK == 0) {
                            ranks.add(escapes);
                        }
                        // A cursor that already knows which ordinal this value takes - a merge whose inputs
                        // are dictionaries the vocabulary was built from - saves resolving the value's bytes
                        // only to look them up again, which is most of what merging such a column costs.
                        final int mapped = values.nextOrdinal();
                        if (mapped >= 0) {
                            vocabulary.terms.get(vocabulary.sortedIds[mapped], scratch);
                            columnBytes[0] += scratch.length;
                            sorted[0] &= mapped >= previous[0];
                            previous[0] = mapped;
                            ordinalTemp.writeVInt(mapped);
                            index++;
                            continue;
                        }
                        final BytesRef value = values.nextValue();
                        columnBytes[0] += value.length;
                        final int id = vocabulary.terms.find(value);
                        final int ordinal = id >= 0 ? vocabulary.ordinalOfId[id] : DROPPED;
                        if (ordinal != DROPPED) {
                            // A column whose values arrive in term order — an index sorted by this field —
                            // has ordinals that never decrease, which lets a term be found by bisection
                            // instead of by comparing every one of them.
                            sorted[0] &= ordinal >= previous[0];
                            previous[0] = ordinal;
                            ordinalTemp.writeVInt(ordinal);
                        } else {
                            ordinalTemp.writeVInt(dictionarySize);
                            sorted[0] &= dictionarySize >= previous[0];
                            previous[0] = dictionarySize;
                            exceptionTemp.writeVInt(value.length);
                            exceptionTemp.writeBytes(value.bytes, value.offset, value.length);
                            escapes++;
                        }
                        index++;
                    }
                }
                ranks.add(escapes);
                exceptionTemp.close();
                exceptions = replayExceptions(dir, ctx, exceptionTempName, escapes, codec, targetChunkBytes, valuesPerBlock, data);
            } finally {
                IOUtils.deleteFilesIgnoringExceptions(dir, exceptionTempName);
            }
            rankTable = ranks.finish(data);
        } finally {
            IOUtils.close(ranks);
        }
        ordinalTemp.close();

        final int ordinalCount = Math.toIntExact(numValues);
        final NumericColumnMetadata ordinalMeta;
        // The writer opens a cursor more than once and need not exhaust every one, so the replays are
        // tracked and closed here rather than closing themselves when they happen to run out.
        final List<IndexInput> replays = new ArrayList<>();
        try {
            ordinalMeta = NumericColumnWriter.write(
                ordinalCount,
                ordinalCount,
                ordinalCount,
                () -> stagedOrdinals(dir, ordinalTempName, ctx, ordinalCount, replays),
                NumericPipeline.defaultPipeline(128),
                BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                // No skip index over the ordinals yet: it would let a term query pass over the blocks whose
                // ordinals cannot hold the term, which is most of a clustered column, but nothing has
                // measured that against what it costs to write and store.
                null,
                dir,
                ctx,
                data
            );
        } finally {
            IOUtils.closeWhileHandlingException(replays);
            IOUtils.deleteFilesIgnoringExceptions(dir, ordinalTempName);
        }

        return new StringColumnMetadata(
            iterator,
            numDocsWithField,
            numValues,
            columnBytes[0],
            dictionarySize,
            ValueStream.Metadata.empty(),
            dictionary,
            exceptions,
            ordinalMeta,
            ESCAPE_RANK_BLOCK,
            rankTable,
            null,
            0,
            MonotonicWriter.Table.NONE,
            sorted[0],
            null,
            0,
            0,
            0
        );
    }

    /** Replays the staged ordinals, so nothing the size of the column is held while they are encoded. */
    private static NumericColumnValues stagedOrdinals(Directory dir, String name, IOContext ctx, int count, List<IndexInput> replays)
        throws IOException {
        final IndexInput input = dir.openInput(name, ctx);
        replays.add(input);
        return new NumericColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public long nextValue() throws IOException {
                return input.readVInt();
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc = (++doc < count ? doc : DocIdSetIterator.NO_MORE_DOCS);
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return count;
            }
        };
    }
}
