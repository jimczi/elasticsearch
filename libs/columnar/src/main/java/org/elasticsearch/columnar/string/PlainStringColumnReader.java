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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * A column that stores its values. Nothing names a value but its own bytes, so every filter the column's
 * order cannot answer compares them, and a page hands them over as they are.
 *
 * <p>A null takes an address like any other slot and stores no bytes. Its stored length is what tells it
 * from an empty string, so every read and every filter that can meet one asks the lengths.
 */
public final class PlainStringColumnReader extends StringColumnReader {

    /** Whether the column's values repeat often enough that naming a page's values pays, as the writer found. */
    private final boolean valuesWorthNaming;

    private final PlainValues.Reader values;

    private final boolean hasNullSlots;

    PlainStringColumnReader(StringColumnMetadata.Plain column, ColumnInputs inputs) throws IOException {
        super(column, inputs, column.values() == null ? StringColumnOptions.DEFAULT_VALUES_PER_BLOCK : column.values().valuesPerBlock());
        this.valuesWorthNaming = column.valuesWorthNaming();
        this.values = column.numDocsWithField() == 0 ? null : column.values().open(inputs);
        this.hasNullSlots = column.hasNullSlots();
    }

    @Override
    public int byteLengthAt(long valueAddress) throws IOException {
        return values.length(valueAddress);
    }

    /** Whether the slot at {@code valueAddress} is null, which its stored length says. */
    @Override
    public boolean isNullSlot(long valueAddress) throws IOException {
        return hasNullSlots && values.isNull(valueAddress);
    }

    /**
     * What one match decided about the last value it saw. A value read from the same stored bytes as the one
     * before it matches exactly as it did. Held per match rather than on the reader, since what it remembers is
     * the answer to one term.
     */
    private static final class LastSeen {
        private final BytesRef value = new BytesRef();
        private long identity = -1;
        private int length = -1;
        private boolean matched;
    }

    @Override
    public BytesRef valueAt(long valueAddress) throws IOException {
        if (isNullSlot(valueAddress)) {
            return null;
        }
        values.get(valueAddress, value);
        return value;
    }

    @Override
    protected DocIdSetIterator valueMatches(Predicate<BytesRef> matcher) throws IOException {
        final ColumnIterator presence = iterator();
        final BytesRef value = new BytesRef();
        final LastSeen lastSeen = new LastSeen();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                final int rank = presence.rank();
                final long first = firstValueAddress(rank);
                final long count = valueCount(rank);
                if (count == 1) {
                    if (isNullSlot(first)) {
                        return false;
                    }
                    // A value repeating the one before it answers as it answered.
                    final long identity = values.read(first, value);
                    if (identity == lastSeen.identity && value.length == lastSeen.length) {
                        return lastSeen.matched;
                    }
                    final boolean matched = matcher.test(value);
                    lastSeen.identity = identity;
                    lastSeen.length = value.length;
                    lastSeen.matched = matched;
                    return matched;
                }
                for (long i = 0; i < count; i++) {
                    // A null is stored as no bytes, so without this it would be offered as an empty string.
                    if (isNullSlot(first + i)) {
                        continue;
                    }
                    values.get(first + i, value);
                    if (matcher.test(value)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return 10f;
            }
        });
    }

    /**
     * Documents whose value equals {@code exact}, or starts with {@code prefix} when {@code exact} is null, in
     * two phases. The approximation is the documents holding a slot whose length could match, found a block of
     * stored lengths at a time; the confirmation compares the bytes. An empty term or prefix is settled by the
     * length, so its confirmation is free.
     */
    @Override
    protected DocIdSetIterator unorderedMatches(BytesRef prefix, BytesRef exact) throws IOException {
        final BytesRef target = exact != null ? exact : prefix;
        final SlotWindow window = lengthWindow(target.length, exact != null ? target.length : Integer.MAX_VALUE);
        final Slots candidates = slotsHeld(window);
        final boolean settled = target.length == 0;
        final LastSeen lastSeen = new LastSeen();
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(candidates) {
            @Override
            public boolean matches() throws IOException {
                if (settled) {
                    return true;
                }
                final long first = candidates.firstSlot();
                final long count = candidates.slotCount();
                for (long i = 0; i < count; i++) {
                    final long slot = first + i;
                    if (window.holds(slot) && matchesSlot(slot, prefix, exact, lastSeen)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return settled ? 0f : target.length;
            }

            @Override
            public int docIDRunEnd() throws IOException {
                // Settled by the window, so every document of a run it holds matches.
                return settled ? candidates.docIDRunEnd() : super.docIDRunEnd();
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (settled) {
                    candidates.intoBitSet(upTo, bitSet, offset);
                } else {
                    super.intoBitSet(upTo, bitSet, offset);
                }
            }
        });
    }

    /** Whether the value at {@code slot}, which is not null, matches; a value read from the same bytes as the last answers as it did. */
    private boolean matchesSlot(long slot, BytesRef prefix, BytesRef exact, LastSeen lastSeen) throws IOException {
        final long identity = values.read(slot, lastSeen.value);
        if (identity == lastSeen.identity && lastSeen.value.length == lastSeen.length) {
            return lastSeen.matched;
        }
        final boolean matched = matches(lastSeen.value, prefix, exact);
        lastSeen.identity = identity;
        lastSeen.length = lastSeen.value.length;
        lastSeen.matched = matched;
        return matched;
    }

    /**
     * The slots whose value is {@code [min, max]} bytes long, compared on the stored codes. A null's code is
     * below every length's, so no range holds one; a repeat's code says nothing of its length, so it takes the
     * answer of the slot before it.
     */
    private SlotWindow lengthWindow(long min, long max) {
        return codeWindow(PlainValues.code(min), PlainValues.code(max));
    }

    /** The slots whose stored code is in any of the inclusive {@code ranges}, a repeat taking the answer of the slot before it. */
    private SlotWindow codeWindow(long... ranges) {
        return new SlotWindow(values.codes(), ranges) {
            @Override
            protected void adjust(long first, long[] block, int count, long[] bits) {
                // A block never starts with a repeat.
                for (int i = 1; i < count; i++) {
                    if (block[i] == PlainValues.REPEAT) {
                        final int before = i - 1;
                        if ((bits[before >>> 6] & (1L << before)) != 0) {
                            bits[i >>> 6] |= 1L << i;
                        } else {
                            bits[i >>> 6] &= ~(1L << i);
                        }
                    }
                }
            }
        };
    }

    @Override
    protected SlotTest slotHolding(BytesRef term) throws IOException {
        final long code = PlainValues.code(term.length);
        final SlotWindow window = codeWindow(code, code);
        if (term.length == 0) {
            // The length settles an empty term.
            return window::holds;
        }
        final LastSeen lastSeen = new LastSeen();
        return slot -> window.holds(slot) && matchesSlot(slot, null, term, lastSeen);
    }

    @Override
    protected SlotWindow slotsHoldingWindow(BytesRef term) throws IOException {
        // The empty length settles an empty term; any other takes the values.
        return term.length == 0 ? codeWindow(PlainValues.code(0), PlainValues.code(0)) : null;
    }

    @Override
    protected SlotWindow slotsNotHoldingWindow(BytesRef term) throws IOException {
        final long code = PlainValues.code(term.length);
        // Every code but the term's length settles it, a null's included; a repeat takes the answer before it.
        if (term.length == 0) {
            return codeWindow(PlainValues.NULL, code - 1, code + 1, Long.MAX_VALUE);
        }
        if (minLength() == term.length && maxLength() == term.length) {
            // Every slot is the term's length, so the codes settle nothing and reading them all would cost more
            // than answering a document at a time.
            return null;
        }
        // A slot of the term's length is the only one whose bytes decide it, so the window settles the rest for
        // free and reads only those. A repeat is corrected after, so it takes the answer of the slot before it.
        return new SlotWindow(values.codes(), PlainValues.NULL, code - 1, code + 1, Long.MAX_VALUE) {
            private final LastSeen lastSeen = new LastSeen();

            @Override
            protected void adjust(long first, long[] block, int count, long[] bits) throws IOException {
                for (int i = 0; i < count; i++) {
                    if (block[i] == code && matchesSlot(first + i, null, term, lastSeen)) {
                        bits[i >>> 6] &= ~(1L << i);
                    } else if (block[i] == code) {
                        bits[i >>> 6] |= 1L << i;
                    }
                }
                // A block never starts with a repeat.
                for (int i = 1; i < count; i++) {
                    if (block[i] == PlainValues.REPEAT) {
                        final int before = i - 1;
                        if ((bits[before >>> 6] & (1L << before)) != 0) {
                            bits[i >>> 6] |= 1L << i;
                        } else {
                            bits[i >>> 6] &= ~(1L << i);
                        }
                    }
                }
            }
        };
    }

    /**
     * Documents holding a value with {@code term} inside it, in two phases. A document asked about on its own
     * has its value searched; a window of documents asked about at once has the bytes of its values searched
     * in one vectorized pass a value block at a time, so the search covers what was asked and nothing more.
     */
    @Override
    protected DocIdSetIterator containsMatches(BytesRef term) throws IOException {
        final ColumnIterator presence = iterator();
        final ContainsSearch search = new ContainsSearch(term);
        final SlotFold fold = new SlotFold();
        final float cost = Math.max(1f, (float) valueBytes() / Math.max(1L, numValues()));
        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(presence) {
            @Override
            public boolean matches() throws IOException {
                final int rank = presence.rank();
                final long first = firstValueAddress(rank);
                final long count = valueCount(rank);
                for (long i = 0; i < count; i++) {
                    if (search.holds(first + i)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public float matchCost() {
                return cost;
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                // A run of present documents holds a contiguous stretch of slots, searched a value block at a time.
                if (presence.docID() < upTo) {
                    fold.collect(presence, search::into, upTo, bitSet, offset);
                }
            }
        });
    }

    /** Which slots hold {@code term}: one at a time, or a run of them searched a value block at a time. */
    private final class ContainsSearch {
        private final BytesRef term;
        private final int shift;
        private final int mask;

        ContainsSearch(BytesRef term) {
            this.term = term;
            final int blockSize = values.valuesPerBlock();
            this.shift = Integer.numberOfTrailingZeros(blockSize);
            this.mask = blockSize - 1;
        }

        boolean holds(long slot) throws IOException {
            if (term.length == 0) {
                // Every value holds the empty term; a null is no value.
                return isNullSlot(slot) == false;
            }
            values.decode(slot >>> shift);
            final int i = (int) (slot & mask);
            final int length = values.valueLength(i);
            return length >= term.length
                && ESVectorUtil.contains(values.blockBytes(), values.valueStart(i), length, term.bytes, term.offset, term.length);
        }

        /** Sets the bit {@code slot - offset} in {@code dest} of every slot in {@code [from, to)} that holds the term. */
        void into(long from, long to, FixedBitSet dest, long offset) throws IOException {
            while (from < to) {
                final long block = from >>> shift;
                final long blockStart = block << shift;
                final int count = values.decode(block);
                final int lo = (int) (from - blockStart);
                final int hi = (int) Math.min(count, to - blockStart);
                if (term.length == 0) {
                    for (int i = lo; i < hi; i++) {
                        if (isNullSlot(blockStart + i) == false) {
                            dest.set((int) (blockStart + i - offset));
                        }
                    }
                } else {
                    search(lo, hi, blockStart - offset, dest);
                }
                from = blockStart + hi;
            }
        }

        /**
         * Searches values {@code [lo, hi)} of the decoded block in one pass. Their starts never decrease, a repeat
         * sharing the start of the value before it, so each value is answered by the first occurrence at or after
         * its start, and a value whose bytes end before that occurrence holds none.
         */
        private void search(int lo, int hi, long base, FixedBitSet dest) {
            final byte[] bytes = values.blockBytes();
            final int end = values.valueStart(hi - 1) + values.valueLength(hi - 1);
            final int n = term.length;
            // The first occurrence at or after the last start searched from, and so at or after every start up to it.
            int hit = -1;
            for (int i = lo; i < hi; i++) {
                final int length = values.valueLength(i);
                if (length < n) {
                    continue;
                }
                final int start = values.valueStart(i);
                if (hit < start) {
                    final int found = ESVectorUtil.indexOf(bytes, start, end - start, term.bytes, term.offset, n);
                    if (found < 0) {
                        return;
                    }
                    hit = start + found;
                }
                if (hit + n <= start + length) {
                    dest.set((int) (base + i));
                }
            }
        }
    }

    /**
     * Consecutive equal values take one entry in the page, so a column made of runs takes one a run rather than
     * one a document.
     */
    @Override
    protected boolean appendPage(int docCount, StringBlockSink sink) throws IOException {
        if (pageOfOneApiece()) {
            // One value a document and every one of them present: the page is the documents, with none of the
            // accounting below.
            return appendSingleValuedPage(docCount, null, docCount, sink);
        }
        if (pageable()) {
            // One value a document, some documents holding none: the present ones are read as above, and the counts
            // say which documents they belong to.
            return appendSingleValuedPage(compactPresentRanks(docCount), pageValueCounts, docCount, sink);
        }
        final int values = countPageValues(docCount);
        growPageValues(Math.max(values, 1));
        pageBytesLength = 0;
        startPageSlots(values);
        int slots = 0;
        int at = 0;
        for (int i = 0; i < docCount; i++) {
            final int rank = pageRanks[i];
            if (rank == ColumnIterator.NO_RANK) {
                continue;
            }
            final long first = firstValueAddress(rank);
            final long held = valueCount(rank);
            for (long s = 0; s < held; s++) {
                final long address = first + s;
                if (isNullSlot(address)) {
                    continue;
                }
                this.values.get(address, scratch);
                final int slot = pageSlotFor(scratch, slots);
                if (slot == slots) {
                    slots++;
                }
                pageOrdinals[at++] = slot;
            }
        }
        assert at == values : "wrote " + at + " values, counted " + values;
        point(pageDictionary, slots);
        if ((long) slots * MIN_PAGE_REPEAT > values) {
            for (int i = 0; i < values; i++) {
                pageValues[i] = pageDictionary[pageOrdinals[i]];
            }
            sink.appendValues(pageValues, values, pageValueCounts, docCount);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, values, pageValueCounts, docCount, pageDictionary, slots);
        return true;
    }

    /** A page of a column holding one value a document, which is the shape a run-encoded column pays off on. */
    private boolean appendSingleValuedPage(int count, int[] counts, int docCount, StringBlockSink sink) throws IOException {
        if (valuesWorthNaming == false) {
            return appendSingleValuedPageAsValues(count, counts, docCount, sink);
        }
        growPageValues(count);
        pageBytesLength = 0;
        startPageSlots(count);
        int slots = 0;
        long previous = -1;
        int previousLength = -1;
        int previousSlot = -1;
        for (int i = 0; i < count; i++) {
            final long identity = values.read(pageRanks[i], scratch);
            // A value read from the same stored bytes as the one before it is a repeat without looking at them.
            if (previousSlot < 0 || identity != previous || scratch.length != previousLength) {
                // The slot before is the only one a column in term order can be repeating, so it is compared
                // before anything is hashed, and a column in term order then hashes once a run rather than once
                // a value. A value the page held earlier is found by its bytes, or it would take two slots.
                final int slot = previousSlot >= 0 && pageSlotHolds(previousSlot, scratch) ? previousSlot : pageSlotFor(scratch, slots);
                if (slot == slots) {
                    slots++;
                }
                previous = identity;
                previousLength = scratch.length;
                previousSlot = slot;
            }
            pageOrdinals[i] = previousSlot;
        }
        point(pageDictionary, slots);
        // As many entries as documents is no shorter as ordinals than as values.
        if ((long) slots * MIN_PAGE_REPEAT > count) {
            for (int i = 0; i < count; i++) {
                pageValues[i] = pageDictionary[pageOrdinals[i]];
            }
            sink.appendValues(pageValues, count, counts, docCount);
            return true;
        }
        sink.appendOrdinals(pageOrdinals, count, counts, docCount, pageDictionary, slots);
        return true;
    }

    /**
     * The same page, without a dictionary being built for it. A page handed over as values never reads the one
     * the method above builds, and building it hashes every value and probes a table for it. So a column whose
     * values do not repeat is read this way instead: runs are still collapsed, which costs no bytes to find,
     * but nothing is hashed.
     *
     * <p>Only the way the values are found changes. What the sink is given is what it would have been given.
     */
    private boolean appendSingleValuedPageAsValues(int count, int[] counts, int docCount, StringBlockSink sink) throws IOException {
        growPageValues(count);
        pageBytesLength = 0;
        int runs = 0;
        long previous = -1;
        int previousLength = -1;
        int previousRun = -1;
        for (int i = 0; i < count; i++) {
            final long identity = values.read(pageRanks[i], scratch);
            if (previousRun < 0 || identity != previous || scratch.length != previousLength) {
                // The run before is compared by its bytes before a new one is started.
                if (previousRun < 0 || pageSlotHolds(previousRun, scratch) == false) {
                    appendToPage(runs, scratch);
                    previousRun = runs++;
                }
                previous = identity;
                previousLength = scratch.length;
            }
            pageOrdinals[i] = previousRun;
        }
        point(pageDictionary, runs);
        for (int i = 0; i < count; i++) {
            pageValues[i] = pageDictionary[pageOrdinals[i]];
        }
        sink.appendValues(pageValues, count, counts, docCount);
        return true;
    }
}
