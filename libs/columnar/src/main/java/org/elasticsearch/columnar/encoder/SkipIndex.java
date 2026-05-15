/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.NamedSPILoader;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;

import java.io.IOException;

/**
 * Extension-point interface for column-level <strong>skip indexes</strong> —
 * registry-discovered, type-pluggable, and <strong>completely decoupled from
 * {@link NumericBlockEncoder} / {@link BlockEncoding}</strong>. The skip index is what lets a query
 * engine skip <em>blocks of doc ids</em> when a filter is provably absent: per-interval
 * min/max for numerics today; bloom filters, sets, ngram filters, lexicographic min/max for
 * bytes-typed columns in later iterations.
 *
 * <p><b>One concept, many types.</b> A {@code SkipIndex} declares a stable {@link #getName()}
 * persisted in per-column metadata; the {@link SkipIndexRegistry} resolves the id to its
 * implementation at read time. Each type owns its own on-disk layout, written to a
 * <strong>separate section</strong> of the format's metadata file by the type's
 * {@link Writer} and read back lazily by its {@link Reader}. Nothing in the encoder /
 * encoding chain references the skip index, and the skip index has no opinion about how the
 * column's values were encoded — they're orthogonal axes.
 *
 * <p><b>Type families.</b> Each implementation declares its {@link Kind}: which value type
 * the skip index is built for. The consumer validates at write time that a numeric column
 * receives a {@link Kind#NUMERIC} skip index, a bytes column receives a {@link Kind#BYTES}
 * skip index, etc. Unknown / mismatched combinations are a configuration error.
 *
 * <p><b>Two thresholds close a skip interval.</b> The {@link Writer} accumulates values until
 * <em>either</em> {@link SkipIndexParams#intervalDocs()} docs <em>or</em>
 * {@link SkipIndexParams#intervalMaxBytes()} source bytes are reached, then closes the
 * interval and emits its on-disk entry. This decouples skip-index granularity from the
 * byte-bounded block size and matches Lucene's own multi-level skip API where each level
 * carries its own interval definition.
 *
 * <p><b>BWC contract.</b> Once an id ships in a release the bytes the type writes are frozen
 * forever. Significant evolution ships a new type with a new id; small non-additive tweaks
 * may stay under the same id by branching on the {@code formatVersion} passed to
 * {@link Reader} on construction.
 *
 * <p>Reserved built-in ids (Iter 5p):
 * <ul>
 *   <li>{@code 0} — {@code NoSkipIndex}, sentinel for "no skip index attached".</li>
 *   <li>{@code 1} — {@code NumericMinMaxSkipIndex}, per-interval long min/max; surfaces as
 *       Lucene's {@code DocValuesSkipper} on the producer's
 *       {@link org.apache.lucene.codecs.DocValuesProducer#getSkipper}.</li>
 *   <li>{@code 2} — {@code BytesRefMinMaxSkipIndex}, per-interval lexicographic
 *       {@link org.apache.lucene.util.BytesRef} min/max; exposed via a sibling
 *       {@code BytesRefSkipReader} since Lucene's {@code DocValuesSkipper} is long-typed.</li>
 *   <li>{@code 3+} — reserved for {@code NumericBloomFilterSkipIndex},
 *       {@code BytesRefBloomFilterSkipIndex}, {@code SetSkipIndex},
 *       {@code NgramBloomFilterSkipIndex} as use cases land.</li>
 * </ul>
 *
 * <p>This interface is the design commitment; the production implementations land in
 * Iter 5p. The current per-block min/max stored in the numeric block table is the existing
 * implementation of the same shape under a different layout; it gets lifted into
 * {@code NumericMinMaxSkipIndex} as part of that iteration.
 */
public interface SkipIndex extends NamedSPILoader.NamedSPI {

    /** Value-type family this skip index applies to. */
    enum Kind {
        /** Numeric (long-typed) columns. */
        NUMERIC,
        /** Variable-length bytes columns (keyword / text / ip / binary). */
        BYTES,
        /** Boolean / bitmap-typed columns. */
        BOOLEAN
    }

    /**
     * Stable identifier persisted in the per-column metadata. Resolved on read via
     * {@link SkipIndexRegistry#forName(String)}. See the interface Javadoc for the BWC contract.
     */
    // String getName() inherited from NamedSPILoader.NamedSPI.
    /** Which value-type family this implementation supports. */
    Kind kind();

    /**
     * Construct a fresh {@link Writer} bound to one consumer's worth of state. Called once
     * per column by {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer}.
     */
    Writer newWriter(SkipIndexParams params);

    /**
     * Construct a {@link Reader} backed by the skip-index section of the on-disk metadata
     * file. The {@code metaIn} input is positioned at {@code metaOffset}; the implementation
     * may read header bytes (interval count, fixed per-entry record size, summary stats) and
     * then cache only a small fixed amount of state — per-interval records stay mmap'd and
     * are read on demand by the reader's advance / probe calls.
     *
     * @param formatVersion the format version recorded in the segment header (see
     *                      {@link ColumNARDocValuesFormat#VERSION_CURRENT})
     */
    Reader newReader(int formatVersion, IndexInput metaIn, long metaOffset, IndexInput dataIn) throws IOException;

    /**
     * Write side of a {@code SkipIndex} for one column. Fed value-by-value during the
     * consumer's per-block buffer fill. Implementations track running stats per skip
     * interval and close the interval (emitting its on-disk record) when the doc or byte
     * threshold fires. Held by exactly one {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer}; per-Writer
     * scratch is safe to keep as instance state.
     */
    interface Writer extends AutoCloseable {

        /**
         * Add the next numeric value at {@code docId}. Only valid when {@link #kind()} on
         * the parent {@code SkipIndex} is {@link Kind#NUMERIC}. The implementation tracks
         * its running stats and decides whether this value closes the current interval.
         */
        void addNumeric(int docId, long value) throws IOException;

        /**
         * Add the next byte-sequence value at {@code docId}, occupying
         * {@code bytes[off, off + len)}. Only valid for {@link Kind#BYTES}.
         */
        void addBytes(int docId, byte[] bytes, int off, int len) throws IOException;

        /**
         * Finish the column: close any pending interval and write trailing summary state.
         * Returns the number of bytes the skip index occupies on disk.
         */
        long finish(DataOutput out) throws IOException;

        @Override
        void close() throws IOException;
    }

    /**
     * Read side of a {@code SkipIndex}. The primary contract is "advance to the interval
     * containing {@code targetDoc}, then expose enough state for a query engine to decide
     * whether the filter can be skipped." For numeric min/max readers this is the existing
     * Lucene {@code DocValuesSkipper} shape; bytes / bloom variants ship sibling APIs.
     */
    interface Reader {

        /** Number of skip intervals stored. */
        int intervalCount();

        /** First doc id of interval {@code i} (inclusive). */
        int intervalFirstDoc(int i) throws IOException;

        /** Last doc id of interval {@code i} (inclusive). */
        int intervalLastDoc(int i) throws IOException;

        /**
         * Number of values recorded in interval {@code i}. Always > 0 for stored intervals
         * (empty intervals are elided entirely from the on-disk layout).
         */
        int intervalDocCount(int i) throws IOException;
    }
}
