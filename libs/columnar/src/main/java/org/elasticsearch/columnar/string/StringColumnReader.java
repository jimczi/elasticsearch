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
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * Reads a string column in either shape, and serves a block of documents in whichever form costs the
 * consumer less: ordinals into a dictionary built for that block, or the values themselves.
 */
public final class StringColumnReader {

    /**
     * A block is served as ordinals only when its distinct values are this much fewer than its documents.
     * A consumer that groups pays one hash per dictionary entry plus one array lookup per document, against
     * one hash per document for values; below that ratio the dictionary costs more to build than it saves.
     */
    static final int MIN_BLOCK_REPEAT = 2;

    private final StringColumnMetadata meta;
    private final IndexInput data;
    private final ColumnIteratorReader iteratorReader;
    private final ValueStream.Reader values;
    private final ValueStream.Reader dictionary;
    private final ValueStream.Reader exceptions;
    private final NumericColumnReader ordinals;
    private final LongValues escapeRanks;
    private final NumericColumnReader valueCounts;
    private final LongValues docBlockBases;

    private final BytesRef scratch = new BytesRef();
    private int[] blockOrdinals = new int[0];
    private int[] blockRanks = new int[0];
    private int[] slotOf = new int[0];
    private BytesRef[] blockDictionary = new BytesRef[0];
    private BytesRef[] blockValues = new BytesRef[0];
    // The values of a block are copied into one buffer and pointed at, rather than each being given its own
    // array: a block decodes into a buffer that is reused as the block advances, so a reference into it does
    // not survive, and a copy per value would allocate twice per document.
    private byte[] blockBytes = new byte[0];
    private int[] blockStarts = new int[0];
    private int[] blockLengths = new int[0];
    private int blockBytesLength;
    // Where the escape walk left off, so a page's escapes are counted once between them rather than each
    // being counted from its block's head.
    private long cursorIndex = -1;
    private long cursorEscapes;

    public StringColumnReader(StringColumnMetadata meta, IndexInput data) throws IOException {
        this.meta = meta;
        this.data = data;
        this.iteratorReader = new ColumnIteratorReader(meta.iterator(), data);
        if (meta.multiValued()) {
            this.valueCounts = new NumericColumnReader(meta.valueCounts(), data);
            final long entries = (meta.numDocsWithField() + meta.docBlockSize() - 1) / meta.docBlockSize() + 1L;
            this.docBlockBases = MonotonicReader.open(
                data,
                meta.docBlockBases().meta(),
                entries,
                meta.docBlockBases().dataOffset(),
                meta.docBlockBases().dataLength()
            );
        } else {
            this.valueCounts = null;
            this.docBlockBases = null;
        }
        if (meta.hasDictionary()) {
            this.values = null;
            this.dictionary = meta.dictionary().open(data);
            this.exceptions = meta.exceptions().open(data);
            this.ordinals = new NumericColumnReader(meta.ordinals(), data);
            final long rankEntries = (meta.numValues() + meta.escapeRankBlockSize() - 1) / meta.escapeRankBlockSize() + 1L;
            this.escapeRanks = MonotonicReader.open(
                data,
                meta.escapeRanks().meta(),
                rankEntries,
                meta.escapeRanks().dataOffset(),
                meta.escapeRanks().dataLength()
            );
        } else {
            this.values = meta.values().open(data);
            this.dictionary = null;
            this.exceptions = null;
            this.ordinals = null;
            this.escapeRanks = null;
        }
    }

    /** This column at the binary surface, in a shape a ColumNAR query can recognise. */
    public static ColumnarStringBinaryDocValues binaryDocValues(StringColumnReader reader) throws IOException {
        return new ColumnarStringBinaryDocValues(reader, reader.iterator());
    }

    public ColumnIterator iterator() throws IOException {
        return iteratorReader.iterator();
    }

    public boolean multiValued() {
        return meta.multiValued();
    }

    /**
     * The index of a document's first value. A single-valued column has none of this: the rank is the index.
     * A multi-valued one holds one base per block of documents and the counts inside it, so the walk is
     * bounded by the block and runs over counts the numeric reader has already decoded together.
     */
    public long firstValue(int rank) throws IOException {
        if (meta.multiValued() == false) {
            return rank;
        }
        final int blockSize = meta.docBlockSize();
        final int block = rank / blockSize;
        long index = docBlockBases.get(block);
        for (int r = block * blockSize; r < rank; r++) {
            index += valueCounts.valueForOrdinal(r);
        }
        return index;
    }

    /** How many values a document holds. */
    public int valueCount(int rank) throws IOException {
        return meta.multiValued() ? (int) valueCounts.valueForOrdinal(rank) : 1;
    }

    /** The value at {@code index} in written order; the bytes are valid until the next call on this reader. */
    public BytesRef valueAt(long index, BytesRef dst) throws IOException {
        values.get(index, dst);
        return dst;
    }

    /**
     * Documents whose value is {@code term}.
     *
     * <p>There is nothing to match but the values, so they are compared as they are read: a window at a
     * time through {@code intoBitSet}, and by length before bytes. A column whose values arrived in term
     * order is bisected instead.
     */
    public DocIdSetIterator matchTerm(BytesRef term) throws IOException {
        return matching(term, term);
    }

    /**
     * Documents whose value starts with {@code prefix}. The dictionary is in term order, so a prefix is a
     * run of ordinals and matching it against the dictionary is one range.
     */
    public DocIdSetIterator matchPrefix(BytesRef prefix) throws IOException {
        return matching(prefix, null);
    }

    /**
     * The ranks whose value equals {@code exact}, or starts with {@code prefix} when {@code exact} is null.
     * Ranks rather than documents because that is the space the ordinals and the values are indexed in.
     */
    private DocIdSetIterator matching(BytesRef prefix, BytesRef exact) throws IOException {
        if (meta.numDocsWithField() == 0) {
            return DocIdSetIterator.empty();
        }
        // Already named in documents, since the values are walked alongside the presence layer.
        return scanValues(prefix, exact);
    }

    /**
     * Compares the values, for a column with no ordinals to match instead.
     *
     * <p>A two-phase iterator, so a scorer fills a window at a time through {@link #scanIntoBitSet} rather
     * than asking about one document at a time. Within a window the values are read where they lie and
     * their lengths are compared before their bytes: a value of the wrong length cannot be the term, and
     * its length is known from reading its block.
     */
    private DocIdSetIterator scanValues(BytesRef prefix, BytesRef exact) throws IOException {
        if (meta.valuesSorted() && meta.multiValued() == false) {
            return documents(sortedValueRange(prefix, exact));
        }
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                return matchesRank(presence.index(), value, prefix, exact);
            }

            @Override
            public float matchCost() {
                return 10f;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                scanIntoBitSet(presence, value, prefix, exact, upTo, bitSet, offset);
            }
        });
    }

    /** Fills a window without the callback a document at a time costs. */
    private void scanIntoBitSet(
        ColumnIterator presence,
        BytesRef value,
        BytesRef prefix,
        BytesRef exact,
        int upTo,
        FixedBitSet bitSet,
        int offset
    ) throws IOException {
        int doc = presence.docID();
        while (doc < upTo && doc != DocIdSetIterator.NO_MORE_DOCS) {
            if (matchesRank(presence.index(), value, prefix, exact)) {
                bitSet.set(doc - offset);
            }
            doc = presence.nextDoc();
        }
    }

    /**
     * The ranks holding a term, or a prefix, in a column whose values arrive in order.
     *
     * <p>Ordered values put every match in one run, so its ends are found by bisection over the values
     * themselves — no ordinals are needed for this, only the order. A term costs a couple of dozen block
     * reads instead of a comparison per document, which is the difference between a filter costing the
     * column and costing its logarithm.
     */
    private DocIdSetIterator sortedValueRange(BytesRef prefix, BytesRef exact) throws IOException {
        final int count = meta.numDocsWithField();
        final BytesRef target = exact != null ? exact : prefix;
        final int first = firstValueAtLeast(target, 0, count);
        if (first == count) {
            return DocIdSetIterator.empty();
        }
        final BytesRef probe = new BytesRef();
        values.get(first, probe);
        if (exact != null ? probe.bytesEquals(exact) == false : startsWith(probe, prefix) == false) {
            return DocIdSetIterator.empty();
        }
        // The run ends where the values stop carrying the term, which is again a boundary in value order.
        int low = first;
        int high = count;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            values.get(mid, probe);
            if (exact != null ? probe.bytesEquals(exact) : startsWith(probe, prefix)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return DocIdSetIterator.range(first, low);
    }

    /** The first rank whose value sorts at or after {@code target}, by bisection over ordered values. */
    private int firstValueAtLeast(BytesRef target, int from, int to) throws IOException {
        final BytesRef probe = new BytesRef();
        int low = from;
        int high = to;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            values.get(mid, probe);
            if (probe.compareTo(target) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /** A value of the wrong length cannot be the term, and cannot be shorter than the prefix. */
    private static boolean matchesLength(int length, BytesRef prefix, BytesRef exact) {
        return exact != null ? length == exact.length : length >= prefix.length;
    }

    /** Whether any of a document's values matches; a document with several matches when one of them does. */
    private boolean matchesRank(int rank, BytesRef value, BytesRef prefix, BytesRef exact) throws IOException {
        final long first = firstValue(rank);
        for (int i = 0, count = valueCount(rank); i < count; i++) {
            valueAt(first + i, value);
            if (matchesLength(value.length, prefix, exact) == false) {
                continue;
            }
            if (exact != null ? value.bytesEquals(exact) : startsWith(value, prefix)) {
                return true;
            }
        }
        return false;
    }

    private static void collect(DocIdSetIterator iterator, FixedBitSet into) throws IOException {
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            into.set(doc);
        }
    }

    /**
     * Turns matching ranks into the documents that hold them. A dense column needs nothing — its rank is
     * its document — and a sparse one is walked alongside its presence layer, which both sides ascend
     * together so the walk costs one pass however selective the match.
     */
    private DocIdSetIterator documents(DocIdSetIterator ranks) throws IOException {
        if (meta.iterator().isDense()) {
            return ranks;
        }
        final ColumnIterator presence = iterator();
        return new DocIdSetIterator() {
            private int doc = -1;

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() throws IOException {
                final int rank = ranks.nextDoc();
                if (rank == NO_MORE_DOCS) {
                    return doc = NO_MORE_DOCS;
                }
                while (presence.index() < rank || presence.docID() < 0) {
                    if (presence.nextDoc() == NO_MORE_DOCS) {
                        return doc = NO_MORE_DOCS;
                    }
                }
                return doc = presence.docID();
            }

            @Override
            public int advance(int target) throws IOException {
                int next = nextDoc();
                while (next < target && next != NO_MORE_DOCS) {
                    next = nextDoc();
                }
                return next;
            }

            @Override
            public long cost() {
                return ranks.cost();
            }
        };
    }

    /** Values the column holds, across every document. */
    public long numValues() {
        return meta.numValues();
    }

    /** Bytes the column's values occupy, uncompressed, which is what a dictionary is weighed against. */
    public long valueBytes() {
        return meta.valueBytes();
    }

    /** Whether the values arrived in term order, which lets a term be found by bisection. */
    public boolean valuesSorted() {
        return meta.valuesSorted();
    }

    private static int insertionPoint(int lookup) {
        return lookup >= 0 ? lookup : -1 - lookup;
    }

    private static boolean startsWith(BytesRef term, BytesRef prefix) {
        if (term.length < prefix.length) {
            return false;
        }
        return Arrays.equals(
            term.bytes,
            term.offset,
            term.offset + prefix.length,
            prefix.bytes,
            prefix.offset,
            prefix.offset + prefix.length
        );
    }

    /**
     * Serves the values of {@code count} documents to {@code sink}. The column decides the form: ordinals
     * into a dictionary built for this block when the block repeats enough to make that cheaper, otherwise
     * the values. Ordinals are meaningful only within the call.
     */
    public boolean readBlock(int[] docs, int offset, int count, StringBlockSink sink) throws IOException {
        if (meta.multiValued()) {
            // A document with several values needs a form that can carry them, which the sink has no shape
            // for yet; the caller falls back to reading the document's values one at a time.
            return false;
        }
        if (blockRanks.length < count) {
            blockRanks = new int[count];
        }
        iterator().ranks(docs, offset, count, blockRanks);
        final int[] indexes = blockRanks;
        offset = 0;
        if (blockOrdinals.length < count) {
            blockOrdinals = new int[count];
            blockStarts = new int[count];
            blockLengths = new int[count];
            blockValues = new BytesRef[count];
            blockDictionary = new BytesRef[count];
            for (int i = 0; i < count; i++) {
                blockValues[i] = new BytesRef();
                blockDictionary[i] = new BytesRef();
            }
        }
        if (consecutive(indexes, offset, count)) {
            blockBytes = values.sequential(indexes[offset], count, blockBytes, blockStarts, blockLengths);
            point(blockValues, count);
            sink.appendValues(blockValues, count);
            return true;
        }
        blockBytesLength = 0;
        for (int i = 0; i < count; i++) {
            values.get(indexes[offset + i], scratch);
            append(i, scratch);
        }
        point(blockValues, count);
        sink.appendValues(blockValues, count);
        return true;
    }

    /** Copies one value into the block buffer, recording where it landed. */
    private void append(int slot, BytesRef value) {
        if (blockBytes.length < blockBytesLength + value.length) {
            blockBytes = ArrayUtil.grow(blockBytes, blockBytesLength + value.length);
        }
        System.arraycopy(value.bytes, value.offset, blockBytes, blockBytesLength, value.length);
        blockStarts[slot] = blockBytesLength;
        blockLengths[slot] = value.length;
        blockBytesLength += value.length;
    }

    /** Points the block's references at the buffer, which only settles once it has stopped growing. */
    private void point(BytesRef[] refs, int count) {
        for (int i = 0; i < count; i++) {
            refs[i].bytes = blockBytes;
            refs[i].offset = blockStarts[i];
            refs[i].length = blockLengths[i];
        }
    }

    /** Whether the requested values form a run, which is what a scan asks for. */
    private static boolean consecutive(int[] indexes, int offset, int count) {
        for (int i = 1; i < count; i++) {
            if (indexes[offset + i] != indexes[offset + i - 1] + 1) {
                return false;
            }
        }
        return true;
    }

    private void resetSlots(int dictionarySize) {
        Arrays.fill(slotOf, 0, dictionarySize, -1);
    }

}
