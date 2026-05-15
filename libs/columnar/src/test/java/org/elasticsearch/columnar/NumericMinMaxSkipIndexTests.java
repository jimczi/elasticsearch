/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.encoder.NumericMinMaxSkipIndex;
import org.elasticsearch.columnar.encoder.SkipIndex;
import org.elasticsearch.columnar.encoder.SkipIndexParams;
import org.elasticsearch.columnar.encoder.SkipIndexRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class NumericMinMaxSkipIndexTests extends ESTestCase {

    public void testRegistryRegistersAtIdOne() {
        final SkipIndex resolved = SkipIndexRegistry.forName(NumericMinMaxSkipIndex.NAME);
        assertNotNull(resolved);
        assertTrue(resolved instanceof NumericMinMaxSkipIndex);
        assertEquals(SkipIndex.Kind.NUMERIC, resolved.kind());
    }

    public void testRejectsBytesAdd() {
        final SkipIndex.Writer w = NumericMinMaxSkipIndex.INSTANCE.newWriter(SkipIndexParams.DEFAULTS);
        expectThrows(UnsupportedOperationException.class, () -> w.addBytes(0, new byte[1], 0, 1));
    }

    public void testDocThresholdClosesInterval() throws IOException {
        // intervalDocs=4 → every 4 docs forms one interval.
        final SkipIndexParams params = new SkipIndexParams(4, 1024 * 1024);
        final byte[] disk = encode(params, new long[] { 10, 20, 30, 40, 50, 60, 70, 80, 90 });
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAndReopen(dir, disk);
            try (IndexInput in = dir.openInput("skip", IOContext.DEFAULT)) {
                final NumericMinMaxSkipIndex.NumericReader r = openReader(in, 0);
                // 9 values → 4 + 4 + 1 = 3 intervals.
                assertEquals(3, r.intervalCount());
                // Interval 0: docs 0..3, values 10..40.
                assertEquals(0, r.intervalFirstDoc(0));
                assertEquals(3, r.intervalLastDoc(0));
                assertEquals(4, r.intervalDocCount(0));
                assertEquals(10L, r.intervalMin(0));
                assertEquals(40L, r.intervalMax(0));
                // Interval 1: docs 4..7, values 50..80.
                assertEquals(50L, r.intervalMin(1));
                assertEquals(80L, r.intervalMax(1));
                // Interval 2: docs 8..8, single value 90.
                assertEquals(8, r.intervalFirstDoc(2));
                assertEquals(8, r.intervalLastDoc(2));
                assertEquals(1, r.intervalDocCount(2));
                assertEquals(90L, r.intervalMin(2));
                assertEquals(90L, r.intervalMax(2));
                // Globals.
                assertEquals(10L, r.globalMin());
                assertEquals(90L, r.globalMax());
                assertEquals(9, r.totalDocCount());
            }
        }
    }

    public void testByteThresholdClosesInterval() throws IOException {
        // intervalMaxBytes=24 → after 3 longs (24 bytes) the interval closes regardless
        // of how many "more docs" the doc threshold would permit.
        final SkipIndexParams params = new SkipIndexParams(1_000_000, 24);
        final byte[] disk = encode(params, new long[] { 1, 2, 3, 4, 5, 6, 7 });
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAndReopen(dir, disk);
            try (IndexInput in = dir.openInput("skip", IOContext.DEFAULT)) {
                final NumericMinMaxSkipIndex.NumericReader r = openReader(in, 0);
                // 7 values, 8 bytes each = 56 bytes / 24 bytes per interval = 3 intervals
                // (3 + 3 + 1).
                assertEquals(3, r.intervalCount());
                assertEquals(3, r.intervalDocCount(0));
                assertEquals(3, r.intervalDocCount(1));
                assertEquals(1, r.intervalDocCount(2));
            }
        }
    }

    public void testEmptyColumn() throws IOException {
        final SkipIndexParams params = SkipIndexParams.DEFAULTS;
        final byte[] disk = encode(params, new long[0]);
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAndReopen(dir, disk);
            try (IndexInput in = dir.openInput("skip", IOContext.DEFAULT)) {
                final NumericMinMaxSkipIndex.NumericReader r = openReader(in, 0);
                assertEquals(0, r.intervalCount());
                assertEquals(0, r.totalDocCount());
                assertEquals(0L, r.globalMin());
                assertEquals(0L, r.globalMax());
            }
        }
    }

    public void testRandomRoundTrip() throws IOException {
        final int nValues = randomIntBetween(1, 5000);
        final long[] values = new long[nValues];
        for (int i = 0; i < nValues; i++) {
            values[i] = randomLong();
        }
        final int intervalDocs = randomIntBetween(8, 256);
        final SkipIndexParams params = new SkipIndexParams(intervalDocs, 1_000_000);
        final byte[] disk = encode(params, values);
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAndReopen(dir, disk);
            try (IndexInput in = dir.openInput("skip", IOContext.DEFAULT)) {
                final NumericMinMaxSkipIndex.NumericReader r = openReader(in, 0);
                final int expectedIntervals = (nValues + intervalDocs - 1) / intervalDocs;
                assertEquals(expectedIntervals, r.intervalCount());

                // Verify each interval's min/max matches the slice of values it covers.
                int docPos = 0;
                long globalMin = Long.MAX_VALUE;
                long globalMax = Long.MIN_VALUE;
                for (int i = 0; i < r.intervalCount(); i++) {
                    final int count = r.intervalDocCount(i);
                    long expectedMin = Long.MAX_VALUE;
                    long expectedMax = Long.MIN_VALUE;
                    for (int j = 0; j < count; j++) {
                        if (values[docPos + j] < expectedMin) expectedMin = values[docPos + j];
                        if (values[docPos + j] > expectedMax) expectedMax = values[docPos + j];
                    }
                    assertEquals("min at interval " + i, expectedMin, r.intervalMin(i));
                    assertEquals("max at interval " + i, expectedMax, r.intervalMax(i));
                    assertEquals("firstDoc at interval " + i, docPos, r.intervalFirstDoc(i));
                    assertEquals("lastDoc at interval " + i, docPos + count - 1, r.intervalLastDoc(i));
                    if (expectedMin < globalMin) globalMin = expectedMin;
                    if (expectedMax > globalMax) globalMax = expectedMax;
                    docPos += count;
                }
                assertEquals(nValues, r.totalDocCount());
                assertEquals(globalMin, r.globalMin());
                assertEquals(globalMax, r.globalMax());
            }
        }
    }

    public void testReaderIsRandomAccess() throws IOException {
        // Read intervals in shuffled order — proves the reader doesn't depend on sequential
        // calls and that the fixed-size record arithmetic is correct.
        final SkipIndexParams params = new SkipIndexParams(2, 1_000_000);
        final long[] values = { 100, 200, 300, 400, 500, 600 };
        final byte[] disk = encode(params, values);
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAndReopen(dir, disk);
            try (IndexInput in = dir.openInput("skip", IOContext.DEFAULT)) {
                final NumericMinMaxSkipIndex.NumericReader r = openReader(in, 0);
                assertEquals(3, r.intervalCount());
                // Probe in reverse.
                assertEquals(500L, r.intervalMin(2));
                assertEquals(100L, r.intervalMin(0));
                assertEquals(300L, r.intervalMin(1));
                // And again in a different order.
                assertEquals(600L, r.intervalMax(2));
                assertEquals(400L, r.intervalMax(1));
                assertEquals(200L, r.intervalMax(0));
            }
        }
    }

    private static byte[] encode(SkipIndexParams params, long[] values) throws IOException {
        try (
            Directory dir = new ByteBuffersDirectory();
            IndexOutput out = dir.createOutput("scratch", IOContext.DEFAULT);
            SkipIndex.Writer writer = NumericMinMaxSkipIndex.INSTANCE.newWriter(params)
        ) {
            for (int i = 0; i < values.length; i++) {
                writer.addNumeric(i, values[i]);
            }
            final long size = writer.finish(out);
            out.close();
            try (IndexInput in = dir.openInput("scratch", IOContext.DEFAULT)) {
                assertEquals("finish() return matches actual bytes written", size, in.length());
                final byte[] bytes = new byte[(int) size];
                in.readBytes(bytes, 0, bytes.length);
                return bytes;
            }
        }
    }

    private static void writeAndReopen(Directory dir, byte[] bytes) throws IOException {
        try (IndexOutput out = dir.createOutput("skip", IOContext.DEFAULT)) {
            out.writeBytes(bytes, 0, bytes.length);
        }
    }

    private static NumericMinMaxSkipIndex.NumericReader openReader(IndexInput in, long offset) throws IOException {
        return (NumericMinMaxSkipIndex.NumericReader) NumericMinMaxSkipIndex.INSTANCE.newReader(
            ColumNARDocValuesFormat.VERSION_CURRENT,
            in,
            offset,
            in
        );
    }
}
