/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A write/merge stress harness (not a strict JMH benchmark) for the slice-per-segment architecture, using
 * DiskBBQ vectors + text (postings) + numeric doc values, to answer: how many slices can index in parallel,
 * whether merges keep up, and whether deletes/updates isolate per slice. Timings are logged, not asserted;
 * correctness invariants (one-segment-per-slice, delete behavior) are asserted.
 */
public class SlicePartitionedStressBenchmarkTests extends ESTestCase {

    private static final int DIM = 16;
    private static final String TEXT_TERMS = "alpha beta gamma delta epsilon zeta eta theta";

    /** Codec that writes the vector field with DiskBBQ; reuses the default codec name so reads resolve. */
    private static Codec diskbbqCodec() {
        final Codec base = Codec.getDefault();
        final KnnVectorsFormat perField = new PerFieldKnnVectorsFormat() {
            private final KnnVectorsFormat diskbbq = new ES920DiskBBQVectorsFormat();

            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return diskbbq;
            }
        };
        return new FilterCodec(base.getName(), base) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return perField;
            }
        };
    }

    private static IndexWriterConfig slicePartitionedConfig(int maxActiveSlices) {
        final IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setCodec(diskbbqCodec());
        iwc.setDocumentPartitioner(doc -> {
            for (IndexableField f : doc) {
                if (f.name().equals("slice")) {
                    return f.stringValue();
                }
            }
            return null;
        });
        iwc.setMergePolicy(new SlicePartitionedMergePolicy(new TieredMergePolicy()));
        iwc.setMergeScheduler(new ConcurrentMergeScheduler());
        if (maxActiveSlices > 0) {
            iwc.setMaxActivePartitions(maxActiveSlices);
        }
        return iwc;
    }

    private static Document makeDoc(Random random, String slice, String id) {
        final Document d = new Document();
        d.add(new StringField("slice", slice, Field.Store.YES));
        d.add(new StringField("id", id, Field.Store.NO));
        final StringBuilder body = new StringBuilder();
        final String[] terms = TEXT_TERMS.split(" ");
        for (int i = 0; i < 4; i++) {
            body.append(terms[random.nextInt(terms.length)]).append(' ');
        }
        d.add(new TextField("body", body.toString(), Field.Store.NO));
        d.add(new NumericDocValuesField("val", random.nextInt(1_000_000)));
        final float[] vec = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            vec[i] = random.nextFloat();
        }
        d.add(new KnnFloatVectorField("vec", vec, VectorSimilarityFunction.EUCLIDEAN));
        return d;
    }

    public void testParallelSliceIndexingThroughputAndOneSegmentPerSlice() throws Exception {
        final int numSlices = 96;
        final int docsPerSlice = 100;
        final int threads = 8;
        final int maxActiveSlices = 16; // bound the working set -> forces flush-eviction under load
        final int totalDocs = numSlices * docsPerSlice;

        try (Directory dir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(dir, slicePartitionedConfig(maxActiveSlices))) {
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            final CountDownLatch done = new CountDownLatch(threads);
            final AtomicLong indexed = new AtomicLong();
            final long start = System.nanoTime();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                pool.execute(() -> {
                    try {
                        final Random random = new Random(threadId * 1000L + 7);
                        for (int s = threadId; s < numSlices; s += threads) {
                            final String slice = "tenant" + s;
                            for (int i = 0; i < docsPerSlice; i++) {
                                w.addDocument(makeDoc(random, slice, slice + "-" + i));
                                indexed.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue("indexing timed out", done.await(120, TimeUnit.SECONDS));
            pool.shutdown();
            w.flush();
            final long indexNanos = System.nanoTime() - start;

            final int segmentsAfterIndex = segmentCount(w);
            final long mergeStart = System.nanoTime();
            w.forceMerge(1);
            final long mergeNanos = System.nanoTime() - mergeStart;
            final int segmentsAfterMerge = segmentCount(w);

            logger.info(
                "SLICE STRESS: {} slices x {} docs = {} docs, {} threads, maxActiveSlices={}, DiskBBQ+text+DV",
                numSlices,
                docsPerSlice,
                totalDocs,
                threads,
                maxActiveSlices
            );
            logger.info(
                "  index: {} ms ({} docs/s) | segments after index: {} | forceMerge(1): {} ms | segments after merge: {}",
                indexNanos / 1_000_000,
                (long) (indexed.get() / (indexNanos / 1e9)),
                segmentsAfterIndex,
                mergeNanos / 1_000_000,
                segmentsAfterMerge
            );

            assertEquals("all docs indexed", totalDocs, indexed.get());
            assertEquals("forceMerge(1) -> one segment per slice", numSlices, segmentsAfterMerge);

            w.commit();
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final var bySlice = SliceCommitFiles.groupBySlice(reader.getIndexCommit());
                assertEquals(numSlices + 1, bySlice.size()); // +1 for the shared/null (segments_N) group
            }
        }
    }

    public void testDeleteAndUpdateIsolationPerSlice() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(dir, slicePartitionedConfig(0))) {
            final Random random = new Random(42);
            // Scenario 1: two tenants share the SAME plain _id "p" (each in its own slice/segment).
            w.addDocument(makeDoc(random, "tenantA", "p"));
            w.addDocument(makeDoc(random, "tenantB", "p"));
            // Scenario 2: distinct plain ids, but a slice-scoped identity term (mimics composite id+slice of #151686).
            addWithScopedId(w, random, "tenantA", "sa", "tenantA shared");
            addWithScopedId(w, random, "tenantB", "sb", "tenantB shared");
            w.flush();

            // (1) Plain global _id term: deleting "p" removes it in BOTH tenants — even though they are in
            // separate slices/segments with separate _id dictionaries — because the delete queue is global.
            w.deleteDocuments(new Term("id", "p"));
            try (DirectoryReader r = DirectoryReader.open(w)) {
                final IndexSearcher s = newSearcher(r);
                assertEquals("plain-id delete leaks across tenants (global delete queue)", 0, s.count(new TermQuery(new Term("id", "p"))));
            }

            // (2) Slice-scoped identity term: deleting only tenantA's scoped id leaves tenantB's intact.
            w.deleteDocuments(new Term("scoped_id", "tenantA shared"));
            try (DirectoryReader r = DirectoryReader.open(w)) {
                final IndexSearcher s = newSearcher(r);
                assertEquals("tenantA's scoped doc deleted", 0, s.count(new TermQuery(new Term("scoped_id", "tenantA shared"))));
                assertEquals(
                    "tenantB's scoped doc survives -> per-slice identity isolation needs a slice-scoped term",
                    1,
                    s.count(new TermQuery(new Term("scoped_id", "tenantB shared")))
                );
            }
            logger.info(
                "DELETE ISOLATION: plain _id -> GLOBAL (leaks across tenants despite separate per-slice segments/dicts, "
                    + "because the delete queue is global); slice-scoped id term -> ISOLATED. Confirms per-slice _id "
                    + "uniqueness needs the composite id+slice term (#151686), not just per-slice segments."
            );
        }
    }

    private static int segmentCount(IndexWriter w) throws IOException {
        try (DirectoryReader r = DirectoryReader.open(w)) {
            return r.leaves().size();
        }
    }

    private static void addWithScopedId(IndexWriter w, Random random, String slice, String plainId, String scopedId) throws Exception {
        final Document d = makeDoc(random, slice, plainId);
        d.add(new StringField("scoped_id", scopedId, Field.Store.NO));
        w.addDocument(d);
    }
}
