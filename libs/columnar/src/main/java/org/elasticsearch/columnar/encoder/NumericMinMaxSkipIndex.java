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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Per-interval long min/max {@link SkipIndex} for numeric columns. The production-default
 * numeric skip index — surfaces as Lucene's {@code DocValuesSkipper} on the producer side so
 * existing range-query / ES|QL filter-pushdown code paths pick it up unchanged.
 *
 * <p><b>What it tracks.</b> One on-disk entry per closed skip interval, each holding
 * {@code firstDoc} / {@code lastDoc} / {@code docCount} / {@code min} / {@code max}. A query
 * engine that asks "can my range filter {@code [lo, hi]} be skipped for doc range
 * {@code [firstDoc, lastDoc]}?" reads the matching entry and answers
 * {@code hi < min || lo > max}.
 *
 * <p><b>When intervals close.</b> The {@link Writer} closes the current interval when
 * <em>either</em> {@link SkipIndexParams#intervalDocs()} docs <em>or</em>
 * {@link SkipIndexParams#intervalMaxBytes()} accumulated source bytes (8 per long) have been
 * fed in since the interval opened. Either threshold fires independently — bit-packed numeric
 * columns typically close on the doc threshold; wide multi-valued numeric columns can close
 * on the byte threshold. Both behaviours fall out of the same code path.
 *
 * <p><b>On-disk layout.</b> One contiguous section at the offset captured by the consumer
 * when it invoked {@link Writer#finish}:
 * <pre>
 *   header (24 bytes):
 *     [Int intervalCount]
 *     [Int totalDocCount]
 *     [Long globalMin]
 *     [Long globalMax]
 *   per-interval table (intervalCount * {@value #INTERVAL_RECORD_SIZE} bytes):
 *     [Int firstDoc] [Int lastDoc] [Int docCount] [Long min] [Long max]
 * </pre>
 * Fixed-width records so the {@link Reader} can seek to interval {@code i}'s entry by
 * arithmetic without an on-heap table.
 */
public final class NumericMinMaxSkipIndex implements SkipIndex {

    public static final String NAME = "NumericMinMax";
    public static final NumericMinMaxSkipIndex INSTANCE = new NumericMinMaxSkipIndex();

    /** Fixed bytes per interval record on disk: firstDoc(4) + lastDoc(4) + docCount(4) + min(8) + max(8). */
    public static final int INTERVAL_RECORD_SIZE = 4 + 4 + 4 + 8 + 8;

    /** Fixed header size: intervalCount(4) + totalDocCount(4) + globalMin(8) + globalMax(8). */
    public static final int HEADER_SIZE = 4 + 4 + 8 + 8;

    private static final int BYTES_PER_NUMERIC = 8;

    public NumericMinMaxSkipIndex() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Kind kind() {
        return Kind.NUMERIC;
    }

    @Override
    public Writer newWriter(SkipIndexParams params) {
        return new NumericMinMaxWriter(params);
    }

    @Override
    public Reader newReader(int formatVersion, IndexInput metaIn, long metaOffset, IndexInput dataIn) throws IOException {
        return new NumericMinMaxReader(metaIn, metaOffset);
    }

    /**
     * Lucene-{@code DocValuesSkipper}-friendly accessors on top of {@link Reader}. Numeric
     * skip-index readers expose this so the producer can bridge to Lucene's existing API
     * without per-call casts.
     */
    public interface NumericReader extends Reader {
        /** Minimum long value recorded in interval {@code i}. */
        long intervalMin(int i) throws IOException;

        /** Maximum long value recorded in interval {@code i}. */
        long intervalMax(int i) throws IOException;

        /** Global minimum across every recorded interval. O(1). */
        long globalMin();

        /** Global maximum across every recorded interval. O(1). */
        long globalMax();

        /** Total doc count across every recorded interval. O(1). */
        int totalDocCount();
    }

    private static final class NumericMinMaxWriter implements Writer {
        private final SkipIndexParams params;
        // Held in heap during the column write; flushed to disk in finish().
        private final List<IntervalRecord> intervals = new ArrayList<>();

        // Current open interval state.
        private int curFirstDoc = -1;
        private int curLastDoc = -1;
        private int curCount = 0;
        private int curBytes = 0;
        private long curMin = Long.MAX_VALUE;
        private long curMax = Long.MIN_VALUE;

        private boolean closed = false;

        NumericMinMaxWriter(SkipIndexParams params) {
            this.params = params;
        }

        @Override
        public void addNumeric(int docId, long value) {
            if (curCount == 0) {
                curFirstDoc = docId;
                curMin = value;
                curMax = value;
            } else {
                if (value < curMin) curMin = value;
                if (value > curMax) curMax = value;
            }
            curLastDoc = docId;
            curCount++;
            curBytes += BYTES_PER_NUMERIC;
            // Either threshold closes the interval. The byte threshold uses source bytes
            // (8 per long); for multi-valued columns the same writer can be reused without
            // change — the threshold simply fires after fewer "docs" if many values per doc.
            if (curCount >= params.intervalDocs() || curBytes >= params.intervalMaxBytes()) {
                closeInterval();
            }
        }

        @Override
        public void addBytes(int docId, byte[] bytes, int off, int len) {
            throw new UnsupportedOperationException("NumericMinMaxSkipIndex is for numeric columns only");
        }

        private void closeInterval() {
            intervals.add(new IntervalRecord(curFirstDoc, curLastDoc, curCount, curMin, curMax));
            curFirstDoc = -1;
            curLastDoc = -1;
            curCount = 0;
            curBytes = 0;
            curMin = Long.MAX_VALUE;
            curMax = Long.MIN_VALUE;
        }

        @Override
        public long finish(DataOutput out) throws IOException {
            if (closed) {
                throw new IllegalStateException("Writer already finished");
            }
            closed = true;
            // Flush any pending interval — short tail is allowed.
            if (curCount > 0) {
                closeInterval();
            }
            // Compute globals across closed intervals.
            long globalMin = Long.MAX_VALUE;
            long globalMax = Long.MIN_VALUE;
            int totalDocCount = 0;
            for (IntervalRecord r : intervals) {
                if (r.min < globalMin) globalMin = r.min;
                if (r.max > globalMax) globalMax = r.max;
                totalDocCount += r.docCount;
            }
            // Empty column edge case: keep header consistent.
            if (intervals.isEmpty()) {
                globalMin = 0L;
                globalMax = 0L;
            }

            out.writeInt(intervals.size());
            out.writeInt(totalDocCount);
            out.writeLong(globalMin);
            out.writeLong(globalMax);
            for (IntervalRecord r : intervals) {
                out.writeInt(r.firstDoc);
                out.writeInt(r.lastDoc);
                out.writeInt(r.docCount);
                out.writeLong(r.min);
                out.writeLong(r.max);
            }
            return (long) HEADER_SIZE + (long) intervals.size() * INTERVAL_RECORD_SIZE;
        }

        @Override
        public void close() {
            // No native resources; finish() handles state.
        }
    }

    /** Heap record held only between addNumeric calls until {@link Writer#finish} flushes them. */
    private record IntervalRecord(int firstDoc, int lastDoc, int docCount, long min, long max) {}

    private static final class NumericMinMaxReader implements NumericReader {
        private final IndexInput metaIn;
        private final int intervalCount;
        private final int totalDocCount;
        private final long globalMin;
        private final long globalMax;
        private final long intervalTableOffset;

        NumericMinMaxReader(IndexInput metaIn, long metaOffset) throws IOException {
            this.metaIn = metaIn;
            metaIn.seek(metaOffset);
            this.intervalCount = metaIn.readInt();
            this.totalDocCount = metaIn.readInt();
            this.globalMin = metaIn.readLong();
            this.globalMax = metaIn.readLong();
            this.intervalTableOffset = metaOffset + HEADER_SIZE;
        }

        @Override
        public int intervalCount() {
            return intervalCount;
        }

        @Override
        public int intervalFirstDoc(int i) throws IOException {
            metaIn.seek(intervalTableOffset + (long) i * INTERVAL_RECORD_SIZE);
            return metaIn.readInt();
        }

        @Override
        public int intervalLastDoc(int i) throws IOException {
            metaIn.seek(intervalTableOffset + (long) i * INTERVAL_RECORD_SIZE + 4);
            return metaIn.readInt();
        }

        @Override
        public int intervalDocCount(int i) throws IOException {
            metaIn.seek(intervalTableOffset + (long) i * INTERVAL_RECORD_SIZE + 8);
            return metaIn.readInt();
        }

        @Override
        public long intervalMin(int i) throws IOException {
            metaIn.seek(intervalTableOffset + (long) i * INTERVAL_RECORD_SIZE + 12);
            return metaIn.readLong();
        }

        @Override
        public long intervalMax(int i) throws IOException {
            metaIn.seek(intervalTableOffset + (long) i * INTERVAL_RECORD_SIZE + 20);
            return metaIn.readLong();
        }

        @Override
        public long globalMin() {
            return globalMin;
        }

        @Override
        public long globalMax() {
            return globalMax;
        }

        @Override
        public int totalDocCount() {
            return totalDocCount;
        }
    }
}
