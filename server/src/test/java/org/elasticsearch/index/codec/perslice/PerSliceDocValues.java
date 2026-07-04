/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perslice;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * Proof-of-concept, format-agnostic driver that gives every slice (tenant) its own <em>physically
 * isolated</em> doc-values structures, by invoking an <b>unmodified stock {@link DocValuesFormat}</b>
 * once per slice.
 * <p>
 * The idea mirrors Lucene's {@code PerFieldDocValuesFormat} (and ES's {@code XPerFieldDocValuesFormat}),
 * which hands each <em>field</em> its own files by writing through a distinct {@link SegmentWriteState}
 * {@code segmentSuffix}. Here we add a second demux axis — the <em>slice</em>: each slice is written
 * through {@code fieldsConsumer(new SegmentWriteState(base, sliceSuffix(s)))}, so slice {@code s} lands
 * in its own {@code *_sliceS.dvd/.dvm} files. The delegate format never learns about slices — it just
 * does its normal single-tenant job over the docs it is handed.
 * <p>
 * Contrast with the DiskBBQ / {@code PartitionedDocValues} approach, which bakes slice-awareness
 * <em>into</em> the format. That does not generalize — every format would need rewriting. This driver
 * needs zero format changes, so every format (postings, doc values, points, KNN, stored) becomes
 * per-slice for free.
 * <p>
 * This POC keeps global doc ids (each per-slice file is sparse over the full {@code maxDoc}); a
 * production version would remap to slice-local doc ids for compactness. Read isolation — the property
 * that makes a per-slice encryption key possible and lets an inactive tenant stay in object storage —
 * holds either way: reading slice {@code s} opens only slice {@code s}'s files.
 */
public final class PerSliceDocValues {

    private PerSliceDocValues() {}

    /** The {@link SegmentWriteState#segmentSuffix} that isolates slice {@code s} into its own files. */
    public static String sliceSuffix(int slice) {
        return "slice" + slice;
    }

    /**
     * Writes {@code field}'s numeric values partitioned by slice: slice {@code s}'s docs go through their
     * own {@code delegate} consumer and therefore their own files. {@code docToSlice[d]} is the slice of
     * doc {@code d}; {@code values[d]} is its value.
     */
    public static void writeNumeric(
        DocValuesFormat delegate,
        SegmentWriteState base,
        FieldInfo field,
        int[] docToSlice,
        long[] values,
        int numSlices
    ) throws IOException {
        final int maxDoc = base.segmentInfo.maxDoc();
        for (int s = 0; s < numSlices; s++) {
            final SegmentWriteState sub = new SegmentWriteState(base, sliceSuffix(s));
            try (DocValuesConsumer consumer = delegate.fieldsConsumer(sub)) {
                consumer.addNumericField(field, sparseNumeric(docToSlice, values, s, maxDoc));
            }
        }
    }

    /**
     * Merges {@code sources} into {@code target} at <b>(segment &times; slice)</b> granularity: slice {@code s}
     * in the target is the consolidation of slice {@code s} across every source, and is written to the
     * target's slice-{@code s} files. Tenants are never mixed — slice {@code s}'s bytes only ever meet other
     * slice-{@code s} bytes. The target is index-sorted by slice (all slice 0 docs, then slice 1, ...), so
     * the merged segment is itself slice-contiguous and can be merged again the same way.
     * <p>
     * This is the core cost property behind a slice-aware merge policy: each slice is consolidated
     * independently, so a slice's write amplification is governed by its <em>own</em> size history, not the
     * segment's. The stock format is invoked exactly as on the write path (via {@link #writeNumeric}); it
     * never learns about slices or merging.
     */
    public static void mergeNumeric(
        DocValuesFormat delegate,
        Directory dir,
        java.util.List<SegmentInfo> sources,
        FieldInfos fieldInfos,
        FieldInfo field,
        SegmentWriteState target,
        int numSlices,
        IOContext context
    ) throws IOException {
        final int targetMaxDoc = target.segmentInfo.maxDoc();
        final int[] targetDocToSlice = new int[targetMaxDoc];
        final long[] targetValues = new long[targetMaxDoc];

        int targetDoc = 0;
        for (int s = 0; s < numSlices; s++) {
            // Consolidate slice s across every source, in source order, into a contiguous target range.
            for (SegmentInfo source : sources) {
                try (DocValuesProducer producer = openSlice(delegate, dir, source, fieldInfos, context, s)) {
                    final NumericDocValues values = producer.getNumeric(field);
                    for (int d = values.nextDoc(); d != NumericDocValues.NO_MORE_DOCS; d = values.nextDoc()) {
                        targetDocToSlice[targetDoc] = s;
                        targetValues[targetDoc] = values.longValue();
                        targetDoc++;
                    }
                }
            }
        }
        assert targetDoc == targetMaxDoc : targetDoc + " != " + targetMaxDoc;

        // Re-emit through the ordinary per-slice write path: each slice back into its own files.
        writeNumeric(delegate, target, field, targetDocToSlice, targetValues, numSlices);
    }

    /**
     * Opens <b>only</b> slice {@code s}'s doc-values producer. Other slices' files are never touched —
     * this is the read-side isolation that keeps inactive tenants out of local memory/cache.
     */
    public static DocValuesProducer openSlice(
        DocValuesFormat delegate,
        Directory dir,
        SegmentInfo si,
        FieldInfos fieldInfos,
        IOContext context,
        int slice
    ) throws IOException {
        return delegate.fieldsProducer(new SegmentReadState(dir, si, fieldInfos, context, sliceSuffix(slice)));
    }

    /**
     * A producer exposing only slice {@code s}'s docs, at their global doc ids (sparse over {@code maxDoc}).
     * Returns a fresh iterator on every {@link DocValuesProducer#getNumeric} call, since the stock consumer
     * iterates the values twice (statistics pass + write pass).
     */
    private static DocValuesProducer sparseNumeric(int[] docToSlice, long[] values, int slice, int maxDoc) {
        return new DocValuesProducer() {
            @Override
            public NumericDocValues getNumeric(FieldInfo f) {
                return new NumericDocValues() {
                    private int doc = -1;

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() {
                        return advance(doc + 1);
                    }

                    @Override
                    public int advance(int target) {
                        for (int d = target; d < maxDoc; d++) {
                            if (docToSlice[d] == slice) {
                                return doc = d;
                            }
                        }
                        return doc = NO_MORE_DOCS;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        doc = target;
                        return docToSlice[target] == slice;
                    }

                    @Override
                    public long longValue() {
                        return values[doc];
                    }

                    @Override
                    public long cost() {
                        int c = 0;
                        for (int d = 0; d < maxDoc; d++) {
                            if (docToSlice[d] == slice) {
                                c++;
                            }
                        }
                        return c;
                    }
                };
            }

            @Override
            public BinaryDocValues getBinary(FieldInfo f) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortedDocValues getSorted(FieldInfo f) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo f) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SortedSetDocValues getSortedSet(FieldInfo f) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DocValuesSkipper getSkipper(FieldInfo f) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void checkIntegrity() {}

            @Override
            public void close() {}
        };
    }
}
