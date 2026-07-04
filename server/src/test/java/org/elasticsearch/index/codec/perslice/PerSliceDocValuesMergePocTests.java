/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perslice;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * M3 — proves the <b>(segment &times; slice) merge</b> mechanic: merging two segments consolidates each
 * slice independently into the merged segment's own per-slice files, using an <b>unmodified stock
 * {@link Lucene90DocValuesFormat}</b>. Tenants are never mixed, the merged segment stays slice-contiguous
 * (so it can be merged again), and read isolation survives the merge.
 */
public class PerSliceDocValuesMergePocTests extends ESTestCase {

    private static final int NUM_SLICES = 3;

    public void testMergePreservesPerSliceIsolationWithStockFormat() throws IOException {
        final Lucene90DocValuesFormat stockFormat = new Lucene90DocValuesFormat();
        final RecordingDirectory dir = new RecordingDirectory(new ByteBuffersDirectory());
        final FieldInfo field = numericDocValuesField("val", 0);
        final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { field });

        // Source A (index-sorted by slice): s0=[100,101] s1=[110,111] s2=[120]
        final SegmentInfo srcA = newSegmentInfo(dir, "A", 5);
        writeSource(stockFormat, dir, srcA, fieldInfos, field, new int[] { 0, 0, 1, 1, 2 }, new long[] { 100, 101, 110, 111, 120 });

        // Source B (index-sorted by slice): s0=[200] s1=[210,211] s2=[220,221]
        final SegmentInfo srcB = newSegmentInfo(dir, "B", 5);
        writeSource(stockFormat, dir, srcB, fieldInfos, field, new int[] { 0, 1, 1, 2, 2 }, new long[] { 200, 210, 211, 220, 221 });

        // Merge A + B -> C. Every doc belongs to exactly one slice, so targetMaxDoc = sum of source maxDocs.
        final SegmentInfo tgt = newSegmentInfo(dir, "C", srcA.maxDoc() + srcB.maxDoc());
        final SegmentWriteState tgtState = new SegmentWriteState(InfoStream.getDefault(), dir, tgt, fieldInfos, null, IOContext.DEFAULT);
        PerSliceDocValues.mergeNumeric(stockFormat, dir, List.of(srcA, srcB), fieldInfos, field, tgtState, NUM_SLICES, IOContext.DEFAULT);

        // The merged segment has one independent data file per slice — tenants stayed physically separate.
        int mergedDvd = 0;
        for (String name : dir.listAll()) {
            if (name.startsWith("C") && name.endsWith(".dvd")) {
                mergedDvd++;
                assertTrue("merged data file not slice-scoped: " + name, name.contains("slice"));
            }
        }
        assertEquals("merged segment must keep one data file per slice", NUM_SLICES, mergedDvd);

        // Each slice = consolidation of that slice across A then B, in order. No cross-tenant values.
        final long[][] expected = {
            { 100, 101, 200 },      // slice 0: A(2) + B(1)
            { 110, 111, 210, 211 }, // slice 1: A(2) + B(2)
            { 120, 220, 221 },      // slice 2: A(1) + B(2)
        };
        for (int s = 0; s < NUM_SLICES; s++) {
            try (DocValuesProducer producer = PerSliceDocValues.openSlice(stockFormat, dir, tgt, fieldInfos, IOContext.DEFAULT, s)) {
                final NumericDocValues ndv = producer.getNumeric(field);
                final List<Long> got = new ArrayList<>();
                for (int d = ndv.nextDoc(); d != NumericDocValues.NO_MORE_DOCS; d = ndv.nextDoc()) {
                    got.add(ndv.longValue());
                }
                assertEquals("slice " + s + " consolidation", asList(expected[s]), got);
            }
        }

        // Read isolation survives the merge: reading slice 1 of C opens only C's slice-1 files.
        dir.opened.clear();
        try (DocValuesProducer producer = PerSliceDocValues.openSlice(stockFormat, dir, tgt, fieldInfos, IOContext.DEFAULT, 1)) {
            final NumericDocValues ndv = producer.getNumeric(field);
            while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                ndv.longValue();
            }
        }
        assertFalse(dir.opened.isEmpty());
        for (String opened : dir.opened) {
            assertTrue("merge read leaked into another tenant's file: " + opened, opened.contains(PerSliceDocValues.sliceSuffix(1)));
        }
    }

    private static void writeSource(
        Lucene90DocValuesFormat format,
        Directory dir,
        SegmentInfo si,
        FieldInfos fieldInfos,
        FieldInfo field,
        int[] docToSlice,
        long[] values
    ) throws IOException {
        final SegmentWriteState state = new SegmentWriteState(InfoStream.getDefault(), dir, si, fieldInfos, null, IOContext.DEFAULT);
        PerSliceDocValues.writeNumeric(format, state, field, docToSlice, values, NUM_SLICES);
    }

    private static List<Long> asList(long[] a) {
        final List<Long> l = new ArrayList<>(a.length);
        for (long v : a) {
            l.add(v);
        }
        return l;
    }

    private static FieldInfo numericDocValuesField(String name, int number) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.NONE,
            DocValuesType.NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    private static SegmentInfo newSegmentInfo(Directory dir, String name, int maxDoc) {
        return new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            name,
            maxDoc,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null
        );
    }

    private static final class RecordingDirectory extends FilterDirectory {
        final List<String> opened = new ArrayList<>();

        RecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            opened.add(name);
            return super.openInput(name, context);
        }
    }
}
