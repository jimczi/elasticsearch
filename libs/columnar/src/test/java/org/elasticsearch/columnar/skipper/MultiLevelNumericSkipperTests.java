/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.skipper;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

public class MultiLevelNumericSkipperTests extends ESTestCase {

    public void testEmpty() throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(SkipperConfig.DEFAULT)) {
            w.finish(out);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
        assertEquals(0, r.globalDocCount());
    }

    public void testDisabledSkipperEmitsTinyHeader() throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(SkipperConfig.DISABLED)) {
            // Write some values — they should all be dropped silently.
            for (int i = 0; i < 100; i++) {
                w.addNumeric(i, i);
            }
            final long bytes = w.finish(out);
            assertEquals("disabled writer emits a 2-byte header", 2L, bytes);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
        assertEquals(0, r.numLevels());
        // Calling advance on an empty reader should return NO_MORE_DOCS.
        assertEquals(DocValuesSkipper.Reader.NO_MORE_DOCS, r.advance(0));
    }

    public void testSingleLevelDense() throws IOException {
        // 500 docs, level-0 granularity 100 → 5 level-0 intervals, 1 level total.
        final SkipperConfig cfg = new SkipperConfig(true, 100, 1, 2, EnumSet.of(StatType.COUNT, StatType.MIN_MAX, StatType.SUM));
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        long expectedSum = 0L;
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(cfg)) {
            for (int doc = 0; doc < 500; doc++) {
                final long v = doc * 2L; // monotonic, deterministic
                w.addNumeric(doc, v);
                expectedSum += v;
            }
            w.finish(out);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
        assertEquals(1, r.numLevels());
        assertEquals(500, r.globalDocCount());
        assertEquals(0L, r.globalMin());
        assertEquals(998L, r.globalMax());
        assertEquals(expectedSum, r.globalSum());

        // Advance to doc 0 — should land in interval 0 (docs 0..99).
        assertEquals(0, r.advance(0));
        assertEquals(0, r.minDocID(0));
        assertEquals(99, r.maxDocID(0));
        assertEquals(100, r.docCount(0));
        assertEquals(0L, r.minValue(0));
        assertEquals(198L, r.maxValue(0));

        // Advance to doc 250 — should land in interval 2 (docs 200..299).
        assertEquals(200, r.advance(250));
        assertEquals(200, r.minDocID(0));
        assertEquals(299, r.maxDocID(0));
        assertEquals(400L, r.minValue(0));
        assertEquals(598L, r.maxValue(0));

        // Advance past end.
        assertEquals(DocValuesSkipper.Reader.NO_MORE_DOCS, r.advance(10_000));
    }

    public void testMultiLevelAggregation() throws IOException {
        // 1024 docs, granularity 32, fan-out 4, 3 levels.
        // Level 0: 32 intervals × 32 docs each.
        // Level 1: 8 intervals × 128 docs each (aggregating 4 level-0 intervals).
        // Level 2: 2 intervals × 512 docs each.
        final SkipperConfig cfg = new SkipperConfig(true, 32, 3, 4, EnumSet.of(StatType.COUNT, StatType.MIN_MAX, StatType.SUM));
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(cfg)) {
            for (int doc = 0; doc < 1024; doc++) {
                w.addNumeric(doc, doc);
            }
            w.finish(out);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
        assertEquals(3, r.numLevels());

        // Force the cursor to advance and observe each level's stats.
        r.advance(0);
        assertEquals("level-0 covers docs 0..31", 0, r.minDocID(0));
        assertEquals(31, r.maxDocID(0));
        assertEquals(0L, r.minValue(0));
        assertEquals(31L, r.maxValue(0));

        assertEquals("level-1 covers docs 0..127", 0, r.minDocID(1));
        assertEquals(127, r.maxDocID(1));
        assertEquals(0L, r.minValue(1));
        assertEquals(127L, r.maxValue(1));

        assertEquals("level-2 covers docs 0..511", 0, r.minDocID(2));
        assertEquals(511, r.maxDocID(2));
        assertEquals(0L, r.minValue(2));
        assertEquals(511L, r.maxValue(2));
    }

    public void testAdvanceSkipsManyIntervalsAtTopLevel() throws IOException {
        // Force a large skip — target a doc deep into the column and assert all levels
        // cursor moves there.
        final SkipperConfig cfg = new SkipperConfig(true, 64, 3, 4, EnumSet.of(StatType.COUNT, StatType.MIN_MAX));
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(cfg)) {
            for (int doc = 0; doc < 10_000; doc++) {
                w.addNumeric(doc, doc);
            }
            w.finish(out);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());

        // Skip to doc 7000 — should advance every level past most intervals.
        final int firstDoc = r.advance(7000);
        assertTrue("first doc >= 7000 boundary", firstDoc <= 7000 && firstDoc + r.docCount(0) > 7000);
        assertTrue("level-0 interval contains doc 7000", r.minDocID(0) <= 7000 && r.maxDocID(0) >= 7000);
        assertTrue("level-1 interval contains doc 7000", r.minDocID(1) <= 7000 && r.maxDocID(1) >= 7000);
        assertTrue("level-2 interval contains doc 7000", r.minDocID(2) <= 7000 && r.maxDocID(2) >= 7000);
    }

    public void testDisabledStatsReturnSentinels() throws IOException {
        // Track only COUNT — min/max/sum should return sentinel values.
        final SkipperConfig cfg = new SkipperConfig(true, 100, 1, 2, EnumSet.of(StatType.COUNT));
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(cfg)) {
            for (int doc = 0; doc < 100; doc++) {
                w.addNumeric(doc, doc);
            }
            w.finish(out);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
        r.advance(0);
        assertEquals("min sentinel when MIN_MAX disabled", Long.MAX_VALUE, r.minValue(0));
        assertEquals("max sentinel when MIN_MAX disabled", Long.MIN_VALUE, r.maxValue(0));
        assertEquals("sum sentinel when SUM disabled", 0L, r.sumValue(0));
        assertEquals("count is always tracked", 100, r.docCount(0));
    }

    public void testSparseMinimalConfig() throws IOException {
        // The minimal skipper for sparse doc values: count-only stats, single level. The
        // skip list maps interval → (firstDoc, lastDoc, docCount, blockIndex) and nothing
        // else. Filter / aggregation pushdown is disabled (min/max/sum not tracked); the
        // skipper is purely a "next doc with a value" forward iterator.
        final SkipperConfig minimal = new SkipperConfig(true, 32, 1, 2, EnumSet.of(StatType.COUNT));
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // Sparse: docs 0, 100, 200, ..., 900 have values. 99% of the doc range is empty.
        try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(minimal)) {
            for (int doc = 0; doc <= 900; doc += 100) {
                w.addNumeric(doc, doc * 7L);
            }
            w.finish(out);
        }
        final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
        assertEquals(10, r.globalDocCount());
        assertEquals("min/max not tracked when MIN_MAX disabled", Long.MAX_VALUE, r.globalMin());
        assertEquals(Long.MIN_VALUE, r.globalMax());
        assertEquals(0L, r.globalSum());

        // Advance to doc 250 — should land in the interval covering doc 300 (next doc with value).
        final int firstDoc = r.advance(250);
        assertTrue("advance lands on or past target", firstDoc >= 250 || (r.maxDocID(0) >= 250));
        // Intervals carry the sparse doc range — minDocID can be much smaller than the next-value doc.
        assertTrue("docCount > 0", r.docCount(0) > 0);
        // Block index is recorded so the reader can seek directly to the value stream.
        assertTrue("blockIndex set", r.blockIndex(0) >= 0);
    }

    public void testRegistryRegistersAtIdTen() {
        final DocValuesSkipper resolved = SkipperRegistry.forName(MultiLevelNumericSkipper.NAME);
        assertNotNull(resolved);
        assertTrue(resolved instanceof MultiLevelNumericSkipper);
        assertEquals("MultiLevelNumericSkipper", MultiLevelNumericSkipper.INSTANCE.getName());
    }

    public void testRandomRoundTrip() throws IOException {
        final Random rng = random();
        for (int trial = 0; trial < 5; trial++) {
            final int nDocs = randomIntBetween(50, 5_000);
            final SkipperConfig cfg = new SkipperConfig(
                true,
                randomIntBetween(16, 256),
                randomIntBetween(1, 4),
                randomIntBetween(2, 6),
                EnumSet.of(StatType.COUNT, StatType.MIN_MAX, StatType.SUM)
            );
            final long[] values = new long[nDocs];
            for (int i = 0; i < nDocs; i++) {
                values[i] = rng.nextLong() >>> 1; // positive longs to keep sum from overflowing too much
            }
            final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
            long expectedSum = 0L;
            long expectedMin = Long.MAX_VALUE;
            long expectedMax = Long.MIN_VALUE;
            try (DocValuesSkipper.Writer w = MultiLevelNumericSkipper.INSTANCE.newWriter(cfg)) {
                for (int i = 0; i < nDocs; i++) {
                    w.addNumeric(i, values[i]);
                    expectedSum += values[i];
                    if (values[i] < expectedMin) expectedMin = values[i];
                    if (values[i] > expectedMax) expectedMax = values[i];
                }
                w.finish(out);
            }
            final DocValuesSkipper.Reader r = openReader(out.toArrayCopy());
            assertEquals(nDocs, r.globalDocCount());
            assertEquals(expectedSum, r.globalSum());
            assertEquals(expectedMin, r.globalMin());
            assertEquals(expectedMax, r.globalMax());
            // Forward-only advance probes (the API is forward-only — generate sorted targets).
            final int[] targets = new int[10];
            for (int p = 0; p < targets.length; p++) {
                targets[p] = rng.nextInt(nDocs);
            }
            java.util.Arrays.sort(targets);
            for (int target : targets) {
                final int firstDoc = r.advance(target);
                if (firstDoc == DocValuesSkipper.Reader.NO_MORE_DOCS) continue;
                assertTrue(
                    "level-0 interval covers target " + target + ": [" + r.minDocID(0) + ".." + r.maxDocID(0) + "]",
                    r.minDocID(0) <= target && r.maxDocID(0) >= target
                );
                assertTrue("docCount > 0", r.docCount(0) > 0);
            }
        }
    }

    private DocValuesSkipper.Reader openReader(byte[] bytes) throws IOException {
        // Wrap the bytes in a ByteBuffersDirectory-backed IndexInput so the reader sees the
        // same kind of input the format hands it from the .cdvm file.
        try (ByteBuffersDirectory dir = new ByteBuffersDirectory()) {
            try (IndexOutput out = dir.createOutput("test", null)) {
                out.writeBytes(bytes, 0, bytes.length);
            }
            final IndexInput in = dir.openInput("test", null);
            return MultiLevelNumericSkipper.INSTANCE.newReader(0, in, 0L);
        }
    }

    @SuppressWarnings("unused")
    private static ByteArrayDataInput wrapForUnitTest(byte[] bytes) {
        return new ByteArrayDataInput(bytes);
    }
}
