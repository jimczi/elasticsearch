/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/** Dumps every file a vectors format opens or writes, with the context it used. */
public class VectorFileAccessAuditTests extends ESTestCase {

    private static final int DIM = 8;

    public void testAuditPlainHnsw() throws Exception {
        audit("hnsw", new ES93HnswVectorsFormat());
    }

    public void testAuditQuantizedHnsw() throws Exception {
        audit("int8_hnsw", new ES93HnswScalarQuantizedVectorsFormat());
    }

    public void testAuditDiskBBQ() throws Exception {
        audit("bbq_disk", new ES950DiskBBQVectorsFormat());
    }

    public void testAuditFlat() throws Exception {
        audit("flat", new ES93FlatVectorFormat());
    }

    public void testAuditDiskBBQBFloat16() throws Exception {
        audit(
            "bbq_disk bfloat16",
            new ES950DiskBBQVectorsFormat(
                QuantEncoding.ONE_BIT_4BIT_QUERY,
                ES950DiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER,
                ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                DenseVectorFieldMapper.ElementType.BFLOAT16,
                null,
                1,
                false,
                ES950DiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION
            )
        );
    }

    private void audit(String label, KnnVectorsFormat format) throws Exception {
        List<String> log = new ArrayList<>();
        AtomicBoolean merging = new AtomicBoolean();
        try (Directory dir = new AuditingDirectory(newDirectory(), log, merging)) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(new OneFormatCodec(format));
            iwc.setUseCompoundFile(false);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int segment = 0; segment < 2; segment++) {
                    for (int i = 0; i < 64; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField("vector", randomVector(), VectorSimilarityFunction.DOT_PRODUCT));
                        w.addDocument(doc);
                    }
                    w.commit();
                }
                // a searcher holds the segments, so the merge meets readers mapped for search
                try (DirectoryReader reader = DirectoryReader.open(w)) {
                    assertEquals(2, reader.leaves().size());
                    log.clear();
                    merging.set(true);
                    w.forceMerge(1);
                    merging.set(false);
                }
            }
        }
        System.out.println("=================== " + label + " : during forceMerge ===================");
        log.forEach(l -> System.out.println("   " + l));
    }

    private static float[] randomVector() {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = randomFloat() + 0.01f;
        }
        return v;
    }

    private static String describe(String op, String name, IOContext context) {
        return String.format(Locale.ROOT, "%-7s %-46s ctx=%-7s %s", op, name, context.context(), context.hints());
    }

    private static class AuditingDirectory extends FilterDirectory {
        private final List<String> log;
        private final AtomicBoolean merging;

        AuditingDirectory(Directory in, List<String> log, AtomicBoolean merging) {
            super(in);
            this.log = log;
            this.merging = merging;
        }

        private void record(String s) {
            if (merging.get()) {
                synchronized (log) {
                    log.add(s);
                }
            }
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            record(describe("OPEN", name, context));
            IndexInput in = super.openInput(name, context);
            return new FilterIndexInput("audit(" + name + ")", in) {
                @Override
                public void close() throws IOException {
                    record(describe("CLOSE", name, context));
                    super.close();
                }

                @Override
                public void updateIOContext(IOContext updated) throws IOException {
                    record(describe("READVISE", name, updated));
                    super.updateIOContext(updated);
                }
            };
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            record(describe("WRITE", name, context));
            return super.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            IndexOutput out = super.createTempOutput(prefix, suffix, context);
            record(describe("WRITETMP", out.getName(), context));
            return out;
        }
    }

    /** Uses one vectors format for every field, keeping a registered codec name. */
    private static class OneFormatCodec extends FilterCodec {
        private final KnnVectorsFormat format;

        OneFormatCodec(KnnVectorsFormat format) {
            super(Codec.getDefault().getName(), Codec.getDefault());
            this.format = format;
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new PerFieldKnnVectorsFormat() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return format;
                }
            };
        }
    }
}
