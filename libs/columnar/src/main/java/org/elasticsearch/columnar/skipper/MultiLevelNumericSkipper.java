/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.skipper;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Multi-level skip list for numeric columns. Tracks per-interval {@link StatType#MIN_MAX
 * min/max}, {@link StatType#COUNT count}, optionally {@link StatType#SUM sum} and
 * {@link StatType#NULL_COUNT null count}. Configurable levels, granularity, fan-out and
 * stats — see {@link SkipperConfig}.
 *
 * <p><b>On-disk layout.</b>
 * <pre>
 *   header (variable):
 *     [Byte version]               // local layout version
 *     [VInt numLevels]
 *     [VInt level0Granularity]
 *     [VInt levelFanOut]
 *     [VInt statBitmap]            // bit set in {@link StatType#bit()} order
 *     [VInt globalDocCount]
 *     [stats globals]              // present only when the corresponding stat bit is set
 *
 *   per level, from level-0 up:
 *     [VInt intervalCount]
 *     [intervalCount × INTERVAL_RECORD_SIZE bytes]
 *
 *   per interval:
 *     [Int firstDoc][Int lastDoc][Int docCount][Int blockIndex]  (16 bytes fixed)
 *     [stats payload, in StatType.bit() order]                  (variable, presence-gated)
 * </pre>
 * Fixed prefix per interval is 16 bytes; stats add 16 bytes for min+max, 8 for sum, 4 for
 * null count. The per-level table is contiguous and fixed-width so the reader can seek
 * directly to interval {@code i} at level {@code L} with O(1) arithmetic.
 *
 * <p><b>Forward-iteration semantics.</b> {@link Reader#advance(int)} maintains a cursor
 * per level; on each call it advances the top level until {@code maxDocID(top) >= target},
 * then descends one level at a time, advancing the next level's cursor to cover the
 * narrowed range. This is the same shape Lucene's {@code DocValuesSkipper} uses, except
 * the levels are populated denser (one entry per fan-out × granularity docs).
 */
public final class MultiLevelNumericSkipper implements DocValuesSkipper {

    public static final String NAME = "MultiLevelNumericSkipper";
    public static final MultiLevelNumericSkipper INSTANCE = new MultiLevelNumericSkipper();

    /** Per-interval fixed record size: firstDoc(4) + lastDoc(4) + docCount(4) + blockIndex(4). */
    static final int INTERVAL_FIXED_BYTES = 4 + 4 + 4 + 4;

    /** Layout version — bumped only on non-additive changes. */
    private static final byte LAYOUT_VERSION = 0;

    public MultiLevelNumericSkipper() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Kind kind() {
        return Kind.NUMERIC;
    }

    @Override
    public Writer newWriter(SkipperConfig config) {
        if (config.enabled() == false) {
            return new NoopWriter();
        }
        return new MultiLevelNumericWriter(config);
    }

    @Override
    public Reader newReader(int formatVersion, IndexInput metaIn, long metaOffset) throws IOException {
        return new MultiLevelNumericReader(metaIn, metaOffset);
    }

    // -------------------------------------------------------------------------------------
    // Stats bitmap helpers
    // -------------------------------------------------------------------------------------

    private static int statBitmap(SkipperConfig config) {
        int b = 0;
        for (StatType s : StatType.values()) {
            if (config.tracks(s)) {
                b |= (1 << s.bit());
            }
        }
        return b;
    }

    private static boolean tracks(int bitmap, StatType stat) {
        return (bitmap & (1 << stat.bit())) != 0;
    }

    /** Per-interval stats record. Sentinel values mark "not tracked". */
    private static int intervalStatsBytes(int bitmap) {
        int n = 0;
        if (tracks(bitmap, StatType.MIN_MAX)) n += 16;     // min(8) + max(8)
        if (tracks(bitmap, StatType.SUM)) n += 8;          // sum(8)
        if (tracks(bitmap, StatType.NULL_COUNT)) n += 4;   // nullCount(4)
        return n;
    }

    private static int intervalRecordSize(int bitmap) {
        return INTERVAL_FIXED_BYTES + intervalStatsBytes(bitmap);
    }

    // -------------------------------------------------------------------------------------
    // Writer
    // -------------------------------------------------------------------------------------

    /** A skipper writer that records nothing — produced when the skipper is disabled. */
    private static final class NoopWriter implements Writer {
        @Override
        public void addNumeric(int docId, long value) {}

        @Override
        public void addBytes(int docId, byte[] bytes, int off, int len) {
            throw new UnsupportedOperationException("NoopWriter is configured for numeric Kind");
        }

        @Override
        public void blockBoundary(int blockIndex, int firstDocOfNextBlock) {}

        @Override
        public long finish(DataOutput out) throws IOException {
            // Emit the disabled-marker header: version + numLevels=0. Producers see
            // numLevels==0 and treat the skipper as absent.
            out.writeByte(LAYOUT_VERSION);
            out.writeVInt(0);
            return 2L;
        }

        @Override
        public void close() {}
    }

    private static final class MultiLevelNumericWriter implements Writer {

        private final SkipperConfig config;
        private final int statBitmap;
        private final List<IntervalRecord> level0Intervals = new ArrayList<>();

        // Open level-0 interval state.
        private int curFirstDoc = -1;
        private int curLastDoc = -1;
        private int curCount = 0;
        private int curNullCount = 0;
        private long curMin = Long.MAX_VALUE;
        private long curMax = Long.MIN_VALUE;
        private long curSum = 0L;
        /** Block index whose values started this interval. */
        private int curBlockIndex = 0;
        /** Counter of completed blocks — incremented on {@link #blockBoundary}. */
        private int completedBlocks = 0;

        // Globals across the column.
        private int globalDocCount = 0;
        private long globalMin = Long.MAX_VALUE;
        private long globalMax = Long.MIN_VALUE;
        private long globalSum = 0L;
        private int globalNullCount = 0;

        private boolean closed = false;

        MultiLevelNumericWriter(SkipperConfig config) {
            this.config = config;
            this.statBitmap = statBitmap(config);
        }

        @Override
        public void addNumeric(int docId, long value) {
            if (curCount == 0) {
                curFirstDoc = docId;
            }
            curLastDoc = docId;
            curCount++;
            if (config.tracks(StatType.MIN_MAX)) {
                if (value < curMin) curMin = value;
                if (value > curMax) curMax = value;
                if (value < globalMin) globalMin = value;
                if (value > globalMax) globalMax = value;
            }
            if (config.tracks(StatType.SUM)) {
                curSum += value;
                globalSum += value;
            }
            globalDocCount++;
            if (curCount >= config.level0Granularity()) {
                closeLevel0Interval();
            }
        }

        @Override
        public void addBytes(int docId, byte[] bytes, int off, int len) {
            throw new UnsupportedOperationException("MultiLevelNumericSkipper is for numeric columns only");
        }

        @Override
        public void blockBoundary(int blockIndex, int firstDocOfNextBlock) {
            // Track which block "owns" the start of the next interval — the consumer's call
            // to blockBoundary is the only way the skipper learns about block geometry.
            // We use blockIndex+1 because the boundary fires after blockIndex closed.
            completedBlocks = blockIndex + 1;
            // The current open interval may straddle the boundary; we don't relabel it
            // mid-flight. The next interval that opens picks up the new block index.
        }

        private void closeLevel0Interval() {
            // Per-interval block index is the block the FIRST doc of the interval lives in.
            // We approximate as the most recent completed-block index — for the dense
            // single-valued case this is exact (one value per doc, blocks close on row
            // counts) and good enough for the multi-valued case (off by at most one block).
            level0Intervals.add(
                new IntervalRecord(
                    curFirstDoc,
                    curLastDoc,
                    curCount,
                    curBlockIndex,
                    config.tracks(StatType.MIN_MAX) ? curMin : Long.MAX_VALUE,
                    config.tracks(StatType.MIN_MAX) ? curMax : Long.MIN_VALUE,
                    config.tracks(StatType.SUM) ? curSum : 0L,
                    config.tracks(StatType.NULL_COUNT) ? curNullCount : 0
                )
            );
            // Reset open interval; the next addNumeric assigns the current block index.
            curBlockIndex = completedBlocks;
            curFirstDoc = -1;
            curLastDoc = -1;
            curCount = 0;
            curNullCount = 0;
            curMin = Long.MAX_VALUE;
            curMax = Long.MIN_VALUE;
            curSum = 0L;
        }

        @Override
        public long finish(DataOutput out) throws IOException {
            if (closed) {
                throw new IllegalStateException("Writer already finished");
            }
            closed = true;
            if (curCount > 0) {
                closeLevel0Interval();
            }
            // Build upper levels by aggregating fan-out level-(k-1) intervals.
            final List<List<IntervalRecord>> levels = new ArrayList<>(config.levels());
            levels.add(level0Intervals);
            final int fanOut = config.levelFanOut();
            for (int k = 1; k < config.levels(); k++) {
                final List<IntervalRecord> prev = levels.get(k - 1);
                final List<IntervalRecord> agg = new ArrayList<>((prev.size() + fanOut - 1) / fanOut);
                for (int i = 0; i < prev.size(); i += fanOut) {
                    final int end = Math.min(i + fanOut, prev.size());
                    final IntervalRecord first = prev.get(i);
                    int lastDoc = first.lastDoc;
                    int docCount = first.docCount;
                    long mn = first.min;
                    long mx = first.max;
                    long sm = first.sum;
                    int nullCt = first.nullCount;
                    for (int j = i + 1; j < end; j++) {
                        final IntervalRecord r = prev.get(j);
                        lastDoc = r.lastDoc;
                        docCount += r.docCount;
                        if (r.min < mn) mn = r.min;
                        if (r.max > mx) mx = r.max;
                        sm += r.sum;
                        nullCt += r.nullCount;
                    }
                    agg.add(new IntervalRecord(first.firstDoc, lastDoc, docCount, first.blockIndex, mn, mx, sm, nullCt));
                }
                if (agg.isEmpty()) {
                    // No higher-level entries — stop adding empty levels.
                    break;
                }
                levels.add(agg);
            }
            final int writtenLevels = levels.size();

            // Header.
            out.writeByte(LAYOUT_VERSION);
            out.writeVInt(writtenLevels);
            out.writeVInt(config.level0Granularity());
            out.writeVInt(config.levelFanOut());
            out.writeVInt(statBitmap);
            out.writeVInt(globalDocCount);
            if (tracks(statBitmap, StatType.MIN_MAX)) {
                out.writeLong(globalDocCount == 0 ? 0L : globalMin);
                out.writeLong(globalDocCount == 0 ? 0L : globalMax);
            }
            if (tracks(statBitmap, StatType.SUM)) {
                out.writeLong(globalSum);
            }
            if (tracks(statBitmap, StatType.NULL_COUNT)) {
                out.writeVInt(globalNullCount);
            }

            // Per-level interval tables.
            long bytesWritten = 2L + vIntSize(writtenLevels) + vIntSize(config.level0Granularity()) + vIntSize(config.levelFanOut())
                + vIntSize(statBitmap) + vIntSize(globalDocCount);
            if (tracks(statBitmap, StatType.MIN_MAX)) bytesWritten += 16;
            if (tracks(statBitmap, StatType.SUM)) bytesWritten += 8;
            if (tracks(statBitmap, StatType.NULL_COUNT)) bytesWritten += vIntSize(globalNullCount);

            final int recordSize = intervalRecordSize(statBitmap);
            for (int k = 0; k < writtenLevels; k++) {
                final List<IntervalRecord> lvl = levels.get(k);
                out.writeVInt(lvl.size());
                bytesWritten += vIntSize(lvl.size());
                for (IntervalRecord r : lvl) {
                    out.writeInt(r.firstDoc);
                    out.writeInt(r.lastDoc);
                    out.writeInt(r.docCount);
                    out.writeInt(r.blockIndex);
                    if (tracks(statBitmap, StatType.MIN_MAX)) {
                        out.writeLong(r.min);
                        out.writeLong(r.max);
                    }
                    if (tracks(statBitmap, StatType.SUM)) {
                        out.writeLong(r.sum);
                    }
                    if (tracks(statBitmap, StatType.NULL_COUNT)) {
                        out.writeInt(r.nullCount);
                    }
                }
                bytesWritten += (long) lvl.size() * recordSize;
            }
            return bytesWritten;
        }

        @Override
        public void close() {}

        private static int vIntSize(int v) {
            int n = 1;
            int u = v;
            while ((u & ~0x7F) != 0) {
                u >>>= 7;
                n++;
            }
            return n;
        }
    }

    /** Heap record held by the writer between adds. */
    private record IntervalRecord(int firstDoc, int lastDoc, int docCount, int blockIndex, long min, long max, long sum, int nullCount) {}

    // -------------------------------------------------------------------------------------
    // Reader
    // -------------------------------------------------------------------------------------

    private static final class MultiLevelNumericReader implements Reader {

        private final IndexInput metaIn;
        private final int numLevels;
        private final int statBitmap;
        private final int recordSize;
        private final int globalDocCount;
        private final long globalMin;
        private final long globalMax;
        private final long globalSum;
        /** Per-level interval count + first-record absolute offset in {@code metaIn}. */
        private final int[] levelIntervalCount;
        private final long[] levelTableOffset;

        /** Cursor: index of the current interval at each level. */
        private final int[] cursor;
        /** Cached current-interval fields per level. */
        private final int[] curFirstDoc;
        private final int[] curLastDoc;
        private final int[] curDocCount;
        private final int[] curBlockIndex;
        private final long[] curMin;
        private final long[] curMax;
        private final long[] curSum;
        private final int[] curNullCount;

        MultiLevelNumericReader(IndexInput metaIn, long metaOffset) throws IOException {
            this.metaIn = metaIn;
            metaIn.seek(metaOffset);
            final byte version = metaIn.readByte();
            if (version != LAYOUT_VERSION) {
                throw new IOException("unknown MultiLevelNumericSkipper layout version " + version);
            }
            this.numLevels = metaIn.readVInt();
            if (numLevels == 0) {
                // Skipper was disabled at write time — initialise an empty reader.
                this.statBitmap = 0;
                this.recordSize = 0;
                this.globalDocCount = 0;
                this.globalMin = 0L;
                this.globalMax = 0L;
                this.globalSum = 0L;
                this.levelIntervalCount = new int[0];
                this.levelTableOffset = new long[0];
                this.cursor = new int[0];
                this.curFirstDoc = new int[0];
                this.curLastDoc = new int[0];
                this.curDocCount = new int[0];
                this.curBlockIndex = new int[0];
                this.curMin = new long[0];
                this.curMax = new long[0];
                this.curSum = new long[0];
                this.curNullCount = new int[0];
                return;
            }
            // Granularity / fan-out are recorded for inspection — not strictly needed by
            // the reader's cursor logic.
            metaIn.readVInt(); // level0Granularity
            metaIn.readVInt(); // levelFanOut
            this.statBitmap = metaIn.readVInt();
            this.globalDocCount = metaIn.readVInt();
            this.globalMin = tracks(statBitmap, StatType.MIN_MAX) ? metaIn.readLong() : Long.MAX_VALUE;
            this.globalMax = tracks(statBitmap, StatType.MIN_MAX) ? metaIn.readLong() : Long.MIN_VALUE;
            this.globalSum = tracks(statBitmap, StatType.SUM) ? metaIn.readLong() : 0L;
            if (tracks(statBitmap, StatType.NULL_COUNT)) {
                metaIn.readVInt();
            }
            this.recordSize = intervalRecordSize(statBitmap);

            // Read each level's count + capture its table offset.
            this.levelIntervalCount = new int[numLevels];
            this.levelTableOffset = new long[numLevels];
            for (int k = 0; k < numLevels; k++) {
                final int n = metaIn.readVInt();
                levelIntervalCount[k] = n;
                levelTableOffset[k] = metaIn.getFilePointer();
                metaIn.skipBytes((long) n * recordSize);
            }

            this.cursor = new int[numLevels];
            this.curFirstDoc = new int[numLevels];
            this.curLastDoc = new int[numLevels];
            this.curDocCount = new int[numLevels];
            this.curBlockIndex = new int[numLevels];
            this.curMin = new long[numLevels];
            this.curMax = new long[numLevels];
            this.curSum = new long[numLevels];
            this.curNullCount = new int[numLevels];
            for (int k = 0; k < numLevels; k++) {
                cursor[k] = -1;
                curFirstDoc[k] = -1;
                curLastDoc[k] = -1;
            }
        }

        @Override
        public int numLevels() {
            return numLevels;
        }

        @Override
        public int advance(int target) throws IOException {
            if (numLevels == 0 || levelIntervalCount[0] == 0) {
                return NO_MORE_DOCS;
            }
            // Top-down advance: at each level, skip intervals until lastDoc >= target.
            // Top-level lets us leap many level-0 intervals in one step.
            for (int k = numLevels - 1; k >= 0; k--) {
                int idx = cursor[k];
                if (idx < 0) {
                    idx = 0;
                    loadInterval(k, idx);
                }
                while (idx < levelIntervalCount[k] && curLastDoc[k] < target) {
                    idx++;
                    if (idx >= levelIntervalCount[k]) break;
                    loadInterval(k, idx);
                }
                cursor[k] = idx;
                if (idx >= levelIntervalCount[k]) {
                    // Exhausted at this level — descend to refine, but the level-0 walk
                    // below will likely also exhaust.
                    continue;
                }
                // Constrain the next-lower level's search to start near this level's window.
            }
            if (cursor[0] >= levelIntervalCount[0]) {
                return NO_MORE_DOCS;
            }
            return curFirstDoc[0];
        }

        private void loadInterval(int level, int idx) throws IOException {
            metaIn.seek(levelTableOffset[level] + (long) idx * recordSize);
            curFirstDoc[level] = metaIn.readInt();
            curLastDoc[level] = metaIn.readInt();
            curDocCount[level] = metaIn.readInt();
            curBlockIndex[level] = metaIn.readInt();
            if (tracks(statBitmap, StatType.MIN_MAX)) {
                curMin[level] = metaIn.readLong();
                curMax[level] = metaIn.readLong();
            } else {
                curMin[level] = Long.MAX_VALUE;
                curMax[level] = Long.MIN_VALUE;
            }
            curSum[level] = tracks(statBitmap, StatType.SUM) ? metaIn.readLong() : 0L;
            curNullCount[level] = tracks(statBitmap, StatType.NULL_COUNT) ? metaIn.readInt() : 0;
        }

        @Override
        public int minDocID(int level) {
            return curFirstDoc[level];
        }

        @Override
        public int maxDocID(int level) {
            return curLastDoc[level];
        }

        @Override
        public int docCount(int level) {
            return curDocCount[level];
        }

        @Override
        public int blockIndex(int level) {
            return curBlockIndex[level];
        }

        @Override
        public long minValue(int level) {
            return curMin[level];
        }

        @Override
        public long maxValue(int level) {
            return curMax[level];
        }

        @Override
        public long sumValue(int level) {
            return curSum[level];
        }

        @Override
        public int nullCount(int level) {
            return curNullCount[level];
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
        public long globalSum() {
            return globalSum;
        }

        @Override
        public int globalDocCount() {
            return globalDocCount;
        }
    }
}
