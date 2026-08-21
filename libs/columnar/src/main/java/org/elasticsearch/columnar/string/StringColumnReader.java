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
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
    // The distinct ordinals of the page being read, and how to find the slot each one landed in. Both are
    // proportional to the page: a dictionary larger than one is never walked, and a smaller one is mapped
    // directly and stamped per page rather than cleared between them.
    private int[] touched = new int[0];
    private int[] slotByOrdinal = new int[0];
    private int[] stampByOrdinal = new int[0];
    private int generation;
    private boolean directSlots;
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

    public boolean hasDictionary() {
        return meta.hasDictionary();
    }

    public int dictionarySize() {
        return meta.dictionarySize();
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

    /** The ordinal at {@code index}, on a column that has a dictionary; the escape marker for an escape. */
    public int ordinalAt(long index) throws IOException {
        return (int) ordinals.valueForOrdinal(index);
    }

    /** The value at {@code index} in written order; the bytes are valid until the next call on this reader. */
    public BytesRef valueAt(long index, BytesRef dst) throws IOException {
        if (meta.hasDictionary() == false) {
            values.get(index, dst);
            return dst;
        }
        final int ordinal = (int) ordinals.valueForOrdinal(index);
        if (ordinal < meta.dictionarySize()) {
            dictionary.get(ordinal, dst);
        } else {
            exceptions.get(escapeIndex(index), dst);
        }
        return dst;
    }

    /**
     * Counts the escapes before {@code index}: the table gives the count at the head of its block, and the
     * remainder is walked over that block's ordinals.
     *
     * <p>A page resolves its escapes in ascending order, so the walk carries forward from the previous one
     * rather than restarting at the block head. Restarting makes the cost of an escape proportional to the
     * block, which on a column that escapes often is most of the read.
     */
    private long escapeIndex(long index) throws IOException {
        final int blockSize = meta.escapeRankBlockSize();
        long from;
        long escapes;
        if (cursorIndex >= 0 && cursorIndex <= index && index - cursorIndex < blockSize) {
            from = cursorIndex;
            escapes = cursorEscapes;
        } else {
            final long block = index / blockSize;
            from = block * blockSize;
            escapes = escapeRanks.get(block);
        }
        for (long i = from; i < index; i++) {
            if (ordinals.valueForOrdinal(i) >= meta.dictionarySize()) {
                escapes++;
            }
        }
        cursorIndex = index;
        cursorEscapes = escapes;
        return escapes;
    }

    /**
     * The ordinal of {@code target}, or {@code -(insertionPoint) - 1} when the dictionary does not hold it.
     *
     * <p>The dictionary is stored in term order, so this is a binary search and needs nothing resident: the
     * probes read through the mapped file, and a dictionary bounded to a single chunk is decompressed once
     * and walked thereafter. Ordering the dictionary by term rather than by frequency is what buys this —
     * a frequency-ordered dictionary would need a hash, and a hash would have to be materialised.
     *
     * @return the ordinal, or a negative encoding of where the term would sort
     */
    public int lookupTerm(BytesRef target) throws IOException {
        if (meta.hasDictionary() == false) {
            return -1;
        }
        int low = 0;
        int high = meta.dictionarySize() - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            dictionary.get(mid, scratch);
            final int cmp = scratch.compareTo(target);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low) - 1;
    }

    /**
     * Documents whose value is {@code term}.
     *
     * <p>With a dictionary, a term it holds is matched over the ordinals alone and never against a value: a
     * value in the dictionary is always stored as its ordinal, so it cannot also be in the exception stream.
     * A term it does not hold is the reverse — the ordinals cannot hold it, so only the escaped documents
     * are examined, which is the small part of a column worth a dictionary. Matching an ordinal is a range
     * of width one, so it runs on the numeric column's own path: the vectorized mask, {@code intoBitSet},
     * {@code docIDRunEnd}, and the skip index that passes over blocks whose ordinals cannot hold the term.
     *
     * <p>Without one there is nothing to match but the values, so they are compared as they are read.
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
        if (meta.hasDictionary() == false || meta.multiValued()) {
            // Already named in documents, since the values are walked alongside the presence layer.
            return scanValues(prefix, exact);
        }
        return documents(matchingRanks(prefix, exact));
    }

    /** The ranks the ordinals match, for a column that has them. */
    private DocIdSetIterator matchingRanks(BytesRef prefix, BytesRef exact) throws IOException {
        if (exact != null) {
            final int ordinal = lookupTerm(exact);
            if (ordinal >= 0) {
                return ordinalRange(ordinal, ordinal);
            }
            final DocIdSetIterator escaped = escapedMatching(prefix, exact);
            return escaped == null ? DocIdSetIterator.empty() : escaped;
        }
        final int[] range = lookupPrefix(prefix);
        final DocIdSetIterator inDictionary = range[0] < range[1] ? ordinalRange(range[0], range[1] - 1) : DocIdSetIterator.empty();
        // A term outside the dictionary can carry the prefix too, and only the exception stream holds one.
        final DocIdSetIterator escaped = escapedMatching(prefix, null);
        if (escaped == null) {
            return inDictionary;
        }
        final FixedBitSet matches = new FixedBitSet(meta.numDocsWithField());
        collect(inDictionary, matches);
        collect(escaped, matches);
        return new BitSetIterator(matches, matches.approximateCardinality());
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

    /**
     * The escaped ranks whose value equals {@code exact}, or starts with {@code prefix} when {@code exact}
     * is null. Returns null when nothing escaped, so a caller can skip the pass entirely.
     */
    private DocIdSetIterator escapedMatching(BytesRef prefix, BytesRef exact) throws IOException {
        if (exceptions.numValues() == 0) {
            return null;
        }
        final FixedBitSet matches = new FixedBitSet(meta.numDocsWithField());
        final BytesRef value = new BytesRef();
        final int dictionarySize = meta.dictionarySize();
        long escapes = 0;
        for (int rank = 0; rank < meta.numDocsWithField(); rank++) {
            if (ordinals.valueForOrdinal(rank) < dictionarySize) {
                continue;
            }
            exceptions.get(escapes++, value);
            if (exact != null ? value.bytesEquals(exact) : startsWith(value, prefix)) {
                matches.set(rank);
            }
        }
        return new BitSetIterator(matches, matches.approximateCardinality());
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

    /**
     * The ranks whose ordinal falls in {@code [low, high]}.
     *
     * <p>When the ordinals never decrease — a column whose values arrive in term order, which an index
     * sorted by this field produces — the ranks that hold a range are one run, and its ends are found by
     * bisection. That reads a few blocks rather than all of them, which is the difference between a filter
     * costing the column and costing its logarithm.
     *
     * <p>Otherwise every ordinal is compared, on the numeric column's own path: the vectorized mask,
     * {@code intoBitSet} and {@code docIDRunEnd}.
     */
    private DocIdSetIterator ordinalRange(int low, int high) throws IOException {
        if (meta.valuesSorted() == false) {
            return ordinalValues().rangeIterator(low, high);
        }
        final int count = meta.numDocsWithField();
        final int first = firstRankAtLeast(low, 0, count);
        if (first == count || ordinals.valueForOrdinal(first) > high) {
            return DocIdSetIterator.empty();
        }
        final int end = firstRankAtLeast(high + 1, first, count);
        // range is half-open, and end is already the first rank past the run.
        return DocIdSetIterator.range(first, end);
    }

    /** The first rank whose ordinal is at least {@code target}, by bisection over a sorted column. */
    private int firstRankAtLeast(long target, int from, int to) throws IOException {
        int low = from;
        int high = to;
        while (low < high) {
            final int mid = (low + high) >>> 1;
            if (ordinals.valueForOrdinal(mid) < target) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /**
     * A view of the ordinals for one match. It decodes through a reader of its own rather than the one this
     * class serves values from: the iterator it hands back is lazy, and a reader holds one decoded block at
     * a time, so sharing it with a walk that runs before the iterator is drained has the two decoding over
     * each other.
     */
    private ColumnarNumericBinaryDocValues ordinalValues() throws IOException {
        final NumericColumnReader reader = new NumericColumnReader(meta.ordinals(), data);
        // The skip index is read by seeking, so it gets an input of its own rather than the one this class
        // hands to the value, dictionary and exception streams.
        return new ColumnarNumericBinaryDocValues(
            reader,
            reader.iterator(),
            meta.numDocsWithField(),
            meta.ordinals().skipper(),
            data.clone()
        );
    }

    /** Whether this column kept a summary of the terms it holds most, which a plain column does. */
    public boolean hasSummary() {
        return meta.summaryTerms() != null;
    }

    /**
     * The summarised terms and their counts, in term order. The counts are lower bounds: a term is charged
     * an occurrence whenever room has to be made for another, so a coverage worked out from them errs
     * towards saying a column is less covered than it is.
     */
    public void readSummary(List<BytesRef> terms, List<Long> counts) throws IOException {
        if (hasSummary() == false) {
            return;
        }
        final ValueStream.Reader reader = meta.summaryTerms().open(data);
        final BytesRef term = new BytesRef();
        for (long i = 0; i < reader.numValues(); i++) {
            reader.get(i, term);
            terms.add(BytesRef.deepCopyOf(term));
        }
        final IndexInput counted = data.clone();
        counted.seek(meta.summaryCountsOffset());
        for (long i = 0; i < reader.numValues(); i++) {
            counts.add(counted.readVLong());
        }
    }

    /** Values the column was summarised over, which is what its counts are a share of. */
    public long summaryValues() {
        return meta.summaryValues();
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

    /** How many values escaped the dictionary; zero means it holds every value the column has. */
    public long exceptionCount() {
        return meta.hasDictionary() ? exceptions.numValues() : 0;
    }

    /** The dictionary term at {@code ordinal}; the bytes are valid until the next call on this reader. */
    public BytesRef termAt(int ordinal, BytesRef dst) throws IOException {
        dictionary.get(ordinal, dst);
        return dst;
    }

    /**
     * The ordinals whose terms start with {@code prefix}, as the half-open range {@code [start, end)}.
     * Because the dictionary is sorted, a prefix is a contiguous run of ordinals and needs no term scan.
     *
     * @return {@code {start, end}}, empty when no term carries the prefix
     */
    public int[] lookupPrefix(BytesRef prefix) throws IOException {
        if (meta.hasDictionary() == false) {
            return new int[] { 0, 0 };
        }
        final int start = insertionPoint(lookupTerm(prefix));
        int low = start;
        int high = meta.dictionarySize();
        // The end is the first term that does not carry the prefix, which is again a boundary in term order.
        while (low < high) {
            final int mid = (low + high) >>> 1;
            dictionary.get(mid, scratch);
            if (startsWith(scratch, prefix)) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return new int[] { start, low };
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
     * Fills {@code ordinals} with the column ordinal of each requested value, without resolving any bytes.
     * An ordinal below {@link #dictionarySize()} names a dictionary entry and is stable for the whole
     * column, which lets a consumer accumulate into a dense array rather than a hash; the escape marker
     * says the value is in the exception stream and has to be resolved with {@link #resolveEscape}.
     *
     * @return false when the column has no dictionary, so there are no ordinals to serve
     */
    public boolean readOrdinals(int[] indexes, int offset, int count, int[] ordinals) throws IOException {
        if (meta.hasDictionary() == false) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            ordinals[i] = (int) this.ordinals.valueForOrdinal(indexes[offset + i]);
        }
        return true;
    }

    /** The value of an escaped document, for a consumer that took {@link #readOrdinals}. */
    public BytesRef resolveEscape(long index, BytesRef dst) throws IOException {
        exceptions.get(escapeIndex(index), dst);
        return dst;
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
        if (meta.hasDictionary() == false) {
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

        final int dictionarySize = meta.dictionarySize();
        int escapes = 0;
        for (int i = 0; i < count; i++) {
            final int ordinal = (int) ordinals.valueForOrdinal(indexes[offset + i]);
            blockOrdinals[i] = ordinal;
            if (ordinal >= dictionarySize) {
                escapes++;
            }
        }

        // The ordinals this page holds, each once and in order. Ordered because the dictionary is read
        // forward: resolving values in whatever order the documents happen to be in would re-enter its
        // blocks, and a block is decoded every time it is re-entered.
        final int distinct = distinctOrdinals(count, dictionarySize);

        blockBytesLength = 0;
        int slot = 0;
        for (; slot < distinct; slot++) {
            dictionary.get(touched[slot], scratch);
            append(slot, scratch);
        }
        for (int i = 0; i < count; i++) {
            final int ordinal = blockOrdinals[i];
            if (ordinal < dictionarySize) {
                blockOrdinals[i] = slotOf(ordinal, distinct);
            } else {
                exceptions.get(escapeIndex(indexes[offset + i]), scratch);
                append(slot, scratch);
                blockOrdinals[i] = slot++;
            }
        }
        point(blockDictionary, slot);

        // Every escaped document is its own entry, so a block that mostly escapes has as many entries as
        // documents and an ordinal form saves its consumer nothing.
        if ((long) (distinct + escapes) * MIN_BLOCK_REPEAT > count) {
            for (int i = 0; i < count; i++) {
                blockValues[i] = blockDictionary[blockOrdinals[i]];
            }
            sink.appendValues(blockValues, count);
            return true;
        }
        sink.appendOrdinals(blockOrdinals, count, blockDictionary, slot);
        return true;
    }

    /**
     * The distinct dictionary ordinals the page holds, ascending, left in {@link #touched}, and returns how
     * many there are. A dictionary no larger than the page is mapped directly, stamped with the page it was
     * written for so it never has to be cleared; a larger one is not indexed at all, and the page's own
     * ordinals are sorted instead. Either way nothing here grows with the dictionary beyond the page.
     */
    private int distinctOrdinals(int count, int dictionarySize) {
        if (touched.length < count) {
            touched = new int[count];
        }
        int distinct = 0;
        if (dictionarySize <= count) {
            if (slotByOrdinal.length < dictionarySize) {
                slotByOrdinal = new int[dictionarySize];
                stampByOrdinal = new int[dictionarySize];
                generation = 0;
            }
            if (++generation == Integer.MAX_VALUE) {
                Arrays.fill(stampByOrdinal, 0);
                generation = 1;
            }
            for (int i = 0; i < count; i++) {
                final int ordinal = blockOrdinals[i];
                if (ordinal < dictionarySize && stampByOrdinal[ordinal] != generation) {
                    stampByOrdinal[ordinal] = generation;
                    touched[distinct++] = ordinal;
                }
            }
            Arrays.sort(touched, 0, distinct);
            for (int i = 0; i < distinct; i++) {
                slotByOrdinal[touched[i]] = i;
            }
            directSlots = true;
            return distinct;
        }
        for (int i = 0; i < count; i++) {
            final int ordinal = blockOrdinals[i];
            if (ordinal < dictionarySize) {
                touched[distinct++] = ordinal;
            }
        }
        Arrays.sort(touched, 0, distinct);
        int unique = 0;
        for (int i = 0; i < distinct; i++) {
            if (i == 0 || touched[i] != touched[i - 1]) {
                touched[unique++] = touched[i];
            }
        }
        directSlots = false;
        return unique;
    }

    /** Where the value for {@code ordinal} was put, for an ordinal {@link #distinctOrdinals} accounted for. */
    private int slotOf(int ordinal, int distinct) {
        return directSlots ? slotByOrdinal[ordinal] : Arrays.binarySearch(touched, 0, distinct, ordinal);
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

}
