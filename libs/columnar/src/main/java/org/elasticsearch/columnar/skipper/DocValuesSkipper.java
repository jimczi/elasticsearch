/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.skipper;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * Per-column <strong>skip list</strong> over doc-id ranges. A skipper is the format's
 * primary acceleration structure for both <em>filter pushdown</em> (skip blocks where a
 * predicate is provably absent) and <em>aggregation pushdown</em> (answer a SUM / COUNT /
 * AVG over a whole skip interval without iterating per-doc).
 *
 * <p>The skipper is built as a hierarchy: {@link SkipperConfig#levels()} levels stacked,
 * each upper level aggregating {@link SkipperConfig#levelFanOut()} intervals from the
 * level below. Forward iteration with large skips is what a bulk scorer wants — see
 * {@link Reader#advance(int)} below.
 *
 * <p><b>Decoupled from encoders.</b> The skipper sees raw pre-encoding values and the
 * eventual <em>block offsets</em> the encoder produces; it never inspects encoded payload
 * bytes. The skipper API is therefore generic over every encoder: a numeric skipper works
 * with bit-pack, delta, gcd, or pipeline encoders without modification.
 *
 * <p><b>Two-phase write.</b> The writer is fed in two parallel streams:
 * <ol>
 *   <li>{@link Writer#addNumeric}/{@link Writer#addBytes} stream the raw values as the
 *       consumer reads them from the source.</li>
 *   <li>{@link Writer#blockBoundary} fires when the encoder closes a block, telling the
 *       skipper which doc-id (and which block index) marks the start of the next block.
 *       The skipper records the mapping so the reader can name the block containing any
 *       given doc.</li>
 * </ol>
 * The streams are interleaved: the value stream is hot, the block-boundary stream is
 * sparse. The skipper never opens the encoded payload.
 *
 * <p><b>Configuration.</b> {@link SkipperConfig} controls every aspect: granularity per
 * level, number of levels, fan-out, which stats are tracked, and whether the skipper is
 * enabled at all. The configuration is recorded on disk so segments written with one
 * policy stay readable when the resolver's defaults change.
 *
 * <p><b>Stats.</b> The set of stats tracked per interval is itself a configuration knob.
 * Filter pushdown needs {@link StatType#MIN_MAX}; aggregation pushdown adds
 * {@link StatType#SUM}, {@link StatType#NULL_COUNT}. Future stats (bloom filter, hyperloglog,
 * histogram) plug in as new {@link StatType} entries with new bit positions.
 *
 * <p><b>BWC.</b> Each implementation declares a stable {@link #getName()} persisted in
 * per-column metadata; once shipped the wire format is frozen. New behavior arrives as a
 * new implementation under a new id.
 */
public interface DocValuesSkipper extends org.apache.lucene.util.NamedSPILoader.NamedSPI {

    /** Value-type family this skipper applies to. */
    enum Kind {
        /** Numeric (long-typed) columns. */
        NUMERIC,
        /** Variable-length bytes columns. */
        BYTES
    }

    // String getName() inherited from NamedSPI; the name is the stable wire-format key.

    /** Which value-type family this implementation supports. */
    Kind kind();

    /** Construct a fresh {@link Writer} bound to one column. */
    Writer newWriter(SkipperConfig config);

    /** Construct a {@link Reader} backed by the on-disk metadata section. */
    Reader newReader(int formatVersion, IndexInput metaIn, long metaOffset) throws IOException;

    /**
     * Write side. Fed value-by-value during the consumer's per-column write pass; the
     * encoder calls {@link #blockBoundary} when it closes a block so the skipper can map
     * each interval to a concrete block index. The two streams are interleaved in doc
     * order. A writer for a disabled skipper is a no-op that emits zero bytes on
     * {@link #finish}.
     */
    interface Writer extends AutoCloseable {

        /**
         * Stream a numeric value at {@code docId}. Multi-valued docs may call this multiple
         * times with the same {@code docId}. Valid only when the parent
         * {@link DocValuesSkipper#kind()} is {@link Kind#NUMERIC}.
         */
        void addNumeric(int docId, long value) throws IOException;

        /**
         * Stream a byte-sequence value at {@code docId} (slice {@code bytes[off, off+len)}).
         * Valid only when the parent {@link DocValuesSkipper#kind()} is {@link Kind#BYTES}.
         */
        void addBytes(int docId, byte[] bytes, int off, int len) throws IOException;

        /**
         * Inform the skipper that the encoder has just closed a block. {@code blockIndex}
         * is the 0-based index of the block that ended; {@code firstDocOfNextBlock} is the
         * doc id where the next block begins (or the doc id one past the end of the column
         * for the trailing block). The skipper uses this to remember which block each
         * skip interval starts in, so the reader can name the block containing any doc.
         */
        void blockBoundary(int blockIndex, int firstDocOfNextBlock) throws IOException;

        /**
         * Flush all pending intervals and write the skipper's on-disk section to
         * {@code out}. Returns the number of bytes written.
         */
        long finish(DataOutput out) throws IOException;

        @Override
        void close() throws IOException;
    }

    /**
     * Read side — forward-iterator shaped, optimised for bulk-scorer-style large skips.
     *
     * <p>The reader maintains an internal cursor across every level. {@link #advance(int)}
     * moves the cursor to the first interval whose doc-id range can intersect
     * {@code targetDoc} or later. Callers then read level-0 stats for filter / aggregation
     * decisions; if a higher level's stats prove the predicate absent for the whole
     * upper-level range, the caller can {@code advance(maxDocID(level) + 1)} to leap
     * across many level-0 intervals in one step.
     *
     * <p>The API mirrors Lucene's {@link org.apache.lucene.index.DocValuesSkipper} so the
     * format's producer can adapt it directly for legacy {@code DocValuesSkipper}-aware
     * code paths.
     */
    interface Reader {

        /** Doc id one past the last doc of any interval — same constant as Lucene. */
        int NO_MORE_DOCS = DocIdSetIterator.NO_MORE_DOCS;

        /** Number of skip-list levels in this reader. Level 0 is the most granular. */
        int numLevels();

        /**
         * Advance the cursor so the current level-0 interval covers some doc {@code >= target}.
         * Returns the first doc of the new current interval, or {@link #NO_MORE_DOCS} when
         * exhausted.
         */
        int advance(int target) throws IOException;

        /** Returns the first doc id (inclusive) of the current interval at {@code level}. */
        int minDocID(int level);

        /** Returns the last doc id (inclusive) of the current interval at {@code level}. */
        int maxDocID(int level);

        /** Number of docs in the current interval at {@code level}. */
        int docCount(int level);

        /**
         * Block index of the block that contains {@link #minDocID(int) minDocID(level)} —
         * the producer uses this to seek directly to the right block in the value stream
         * after the skipper has narrowed the doc range.
         */
        int blockIndex(int level);

        /**
         * Minimum value in the current interval at {@code level}, or {@link Long#MAX_VALUE}
         * when {@link StatType#MIN_MAX} was not tracked.
         */
        long minValue(int level);

        /**
         * Maximum value in the current interval at {@code level}, or {@link Long#MIN_VALUE}
         * when {@link StatType#MIN_MAX} was not tracked.
         */
        long maxValue(int level);

        /**
         * Sum of values in the current interval at {@code level}, or {@code 0} when
         * {@link StatType#SUM} was not tracked.
         */
        long sumValue(int level);

        /**
         * Number of null / missing entries in the current interval at {@code level}, or
         * {@code 0} when {@link StatType#NULL_COUNT} was not tracked.
         */
        int nullCount(int level);

        /** Global min across every interval. O(1). */
        long globalMin();

        /** Global max across every interval. O(1). */
        long globalMax();

        /** Global sum across every interval. O(1). */
        long globalSum();

        /** Global doc count. O(1). */
        int globalDocCount();
    }
}
