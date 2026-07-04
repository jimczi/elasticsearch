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
 * Proof of concept for <b>true per-tenant physical isolation of Lucene structures</b>: every slice
 * gets its own doc-values files, produced by an <b>unmodified stock {@link Lucene90DocValuesFormat}</b>
 * driven once per slice by {@link PerSliceDocValues}. This is the "reuse stock formats per slice,
 * don't rewrite formats to be multi-tenant" thesis, demonstrated end to end.
 *
 * <p>The test asserts the three properties that matter for the serverless multi-tenant story:
 * <ol>
 *   <li><b>Physical isolation</b> — each slice lands in its own {@code *_sliceN.dvd/.dvm} files.</li>
 *   <li><b>Correctness</b> — reading slice {@code s} returns exactly slice {@code s}'s docs/values.</li>
 *   <li><b>Read isolation / laziness</b> — opening one slice touches only that slice's files, so an
 *       inactive tenant's bytes are never loaded (and could carry its own encryption key).</li>
 * </ol>
 */
public class PerSliceDocValuesPocTests extends ESTestCase {

    private static final int NUM_SLICES = 3;
    private static final int DOCS_PER_SLICE = 4;
    private static final int MAX_DOC = NUM_SLICES * DOCS_PER_SLICE; // 12

    public void testEachSliceIsPhysicallyIsolatedUsingStockFormat() throws IOException {
        // Index sorted by slice => each slice is a contiguous doc range: [0,4) [4,8) [8,12).
        final int[] docToSlice = new int[MAX_DOC];
        final long[] values = new long[MAX_DOC];
        for (int d = 0; d < MAX_DOC; d++) {
            docToSlice[d] = d / DOCS_PER_SLICE;
            values[d] = d * 10L;
        }

        // A stock, unmodified Lucene doc-values format. Nothing about it knows what a "slice" is.
        final Lucene90DocValuesFormat stockFormat = new Lucene90DocValuesFormat();

        final RecordingDirectory dir = new RecordingDirectory(new ByteBuffersDirectory());
        final FieldInfo field = numericDocValuesField("val", 0);
        final FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { field });
        final SegmentInfo si = newSegmentInfo(dir);
        final SegmentWriteState writeState = new SegmentWriteState(InfoStream.getDefault(), dir, si, fieldInfos, null, IOContext.DEFAULT);

        // Drive the stock format once per slice.
        PerSliceDocValues.writeNumeric(stockFormat, writeState, field, docToSlice, values, NUM_SLICES);

        // (1) Physical isolation: one independent .dvd file per slice, each with a distinct slice suffix.
        int dvdFiles = 0;
        for (String name : dir.listAll()) {
            if (name.endsWith(".dvd")) {
                dvdFiles++;
                assertTrue("data file not slice-scoped: " + name, name.contains("slice"));
            }
        }
        assertEquals("expected one doc-values data file per slice", NUM_SLICES, dvdFiles);

        // (2) Correctness: each slice reads back exactly its own docs and values.
        for (int s = 0; s < NUM_SLICES; s++) {
            try (DocValuesProducer producer = PerSliceDocValues.openSlice(stockFormat, dir, si, fieldInfos, IOContext.DEFAULT, s)) {
                final NumericDocValues ndv = producer.getNumeric(field);
                for (int d = s * DOCS_PER_SLICE; d < (s + 1) * DOCS_PER_SLICE; d++) {
                    assertEquals("slice " + s + " should contain doc " + d, d, ndv.nextDoc());
                    assertEquals(values[d], ndv.longValue());
                }
                assertEquals("slice " + s + " must not expose any other tenant's docs", NumericDocValues.NO_MORE_DOCS, ndv.nextDoc());
            }
        }

        // (3) Read isolation / laziness: opening slice 1 must touch ONLY slice 1's files.
        dir.opened.clear();
        try (DocValuesProducer producer = PerSliceDocValues.openSlice(stockFormat, dir, si, fieldInfos, IOContext.DEFAULT, 1)) {
            final NumericDocValues ndv = producer.getNumeric(field);
            while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                ndv.longValue();
            }
        }
        assertFalse("reading a slice should open some files", dir.opened.isEmpty());
        for (String opened : dir.opened) {
            assertTrue("reading slice 1 leaked into another tenant's file: " + opened, opened.contains(PerSliceDocValues.sliceSuffix(1)));
        }
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

    private static SegmentInfo newSegmentInfo(Directory dir) {
        return new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "0",
            MAX_DOC,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null
        );
    }

    /** Records every {@link #openInput} so we can prove reading one slice never touches another's files. */
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
