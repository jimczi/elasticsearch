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
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Constants;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94HnswScalarQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es94.ES94ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * How a vectors file is opened for search is decided by the structure reading it, not by the format
 * holding the bytes. A graph walks the vectors it searches, so it asks for random access; a format
 * that keeps the raw vectors only to rescore says they are read at random and not read again, which
 * is what lets a directory act on {@code on_disk_rescore}.
 */
public class VectorReadHintsTests extends ESTestCase {

    private static final int DIM = 64;
    /**
     * bfloat16, so the raw vectors go through the format Elasticsearch owns. The float32 path goes
     * through Lucene's flat reader, which replaces its caller's hints rather than adding to them, so
     * it drops what the wrapper says until apache/lucene#16682 is released.
     */
    private static final DenseVectorFieldMapper.ElementType RAW = DenseVectorFieldMapper.ElementType.BFLOAT16;

    /**
     * What each {@code index_options} type builds, whether something walks the vectors it opens, and
     * whether it keeps the raw vectors only to rescore with them.
     */
    private record Case(String type, KnnVectorsFormat format, boolean walked, boolean rescoresFromRaw) {}

    private static List<Case> cases() {
        return List.of(
            new Case("hnsw", new ES93HnswVectorsFormat(16, 100, RAW, 1, null, -1), true, false),
            new Case("int8_hnsw", new ES94HnswScalarQuantizedVectorsFormat(16, 100, RAW, 7, 1, null, -1), true, true),
            new Case("int4_hnsw", new ES94HnswScalarQuantizedVectorsFormat(16, 100, RAW, 4, 1, null, -1), true, true),
            new Case("bbq_hnsw", new ES93HnswBinaryQuantizedVectorsFormat(16, 100, RAW, 1, null, -1), true, true),
            new Case("int8_flat", new ES94ScalarQuantizedVectorsFormat(RAW, 7), false, true),
            new Case("int4_flat", new ES94ScalarQuantizedVectorsFormat(RAW, 4), false, true),
            new Case("bbq_flat", new ES93BinaryQuantizedVectorsFormat(RAW), false, true),
            new Case("flat", new ES93FlatVectorFormat(RAW), false, false),
            new Case(
                "bbq_disk",
                new ES950DiskBBQVectorsFormat(
                    QuantEncoding.ONE_BIT_4BIT_QUERY,
                    ES950DiskBBQVectorsFormat.DEFAULT_VECTORS_PER_CLUSTER,
                    ES950DiskBBQVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
                    DenseVectorFieldMapper.ElementType.BFLOAT16,
                    null,
                    1,
                    false,
                    ES950DiskBBQVectorsFormat.DEFAULT_PRECONDITIONING_BLOCK_DIMENSION
                ),
                false,
                true
            )
        );
    }

    /**
     * The raw vectors are reached through something that knows how it reads them, so they never open
     * without an access pattern unless nothing sits above them. Where that something only rescores
     * with them, they also say they are not read again, which is what the directory acts on.
     */
    public void testEveryIndexTypeSaysHowItReadsTheRawVectors() throws Exception {
        for (Case each : cases()) {
            Set<IOContext.FileOpenHint> hints = openHintsForVectorData(each.format());

            if (each.walked() || each.rescoresFromRaw()) {
                assertTrue(
                    each.type() + " opened its vectors without saying how they are read: " + hints,
                    hints.contains(DataAccessHint.RANDOM)
                );
            } else {
                assertFalse(each.type() + " assumed how its vectors are read: " + hints, hints.contains(DataAccessHint.RANDOM));
                assertFalse(each.type() + " assumed how its vectors are read: " + hints, hints.contains(DataAccessHint.SEQUENTIAL));
            }
            assertEquals(
                each.type() + " disagreed about whether the raw vectors are read again: " + hints,
                each.rescoresFromRaw(),
                hints.contains(NoReuseHint.INSTANCE)
            );
        }
    }

    /** Advice that costs a mapping its recency is only for files that say they are not read again. */
    public void testOnlyNoReuseFilesGetAdvice() {
        var advice = FsDirectoryFactory.getReadAdviceFunc();

        assertEquals(
            "vectors a graph walks must stay eligible for reclaim by age",
            Optional.of(Constants.DEFAULT_READADVICE),
            advice.apply("_0.vec", IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM))
        );
        assertEquals(
            "a file that is not read again can give up its recency",
            Optional.of(ReadAdvice.RANDOM),
            advice.apply("_0.vec", IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM, NoReuseHint.INSTANCE))
        );
        assertEquals(
            "sequential advice costs recency too, so it needs saying as well",
            Optional.of(Constants.DEFAULT_READADVICE),
            advice.apply("_0.vec", IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.SEQUENTIAL))
        );
        assertEquals(
            "a merge that says it reads front to back and does not come back",
            Optional.of(ReadAdvice.SEQUENTIAL),
            advice.apply("_0.vec", IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.SEQUENTIAL, NoReuseHint.INSTANCE))
        );
    }

    /** A merge is not on its own a reason to advise anything. */
    public void testMergeContextAloneAdvisesNothing() {
        var advice = FsDirectoryFactory.getReadAdviceFunc();
        IOContext merge = IOContext.merge(new MergeInfo(1, 1L, false, 1)).withHints(FileTypeHint.DATA);

        assertEquals(Optional.of(Constants.DEFAULT_READADVICE), advice.apply("_0.vec", merge));
        assertEquals(
            "a merge building a graph reads at random and says so",
            Optional.of(Constants.DEFAULT_READADVICE),
            advice.apply("_0.vec", merge.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM))
        );
    }

    /** The hints a searcher's open of the raw vectors file carries. */
    private Set<IOContext.FileOpenHint> openHintsForVectorData(KnnVectorsFormat format) throws Exception {
        Map<String, IOContext> opens = new HashMap<>();
        try (Directory dir = new RecordingDirectory(newDirectory(), opens)) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(new OneFormatCodec(format));
            iwc.setUseCompoundFile(false);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 64; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("vector", randomVector(), VectorSimilarityFunction.DOT_PRODUCT));
                    w.addDocument(doc);
                }
                w.commit();
                opens.clear();
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals(1, reader.leaves().size());
                }
            }
        }
        return opens.entrySet()
            .stream()
            .filter(e -> e.getKey().endsWith(".vec"))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no raw vectors file was opened, saw " + opens.keySet()))
            .hints();
    }

    private static float[] randomVector() {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = randomFloat() + 0.01f;
        }
        return v;
    }

    private static class RecordingDirectory extends FilterDirectory {
        private final Map<String, IOContext> opens;

        RecordingDirectory(Directory in, Map<String, IOContext> opens) {
            super(in);
            this.opens = opens;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            synchronized (opens) {
                opens.putIfAbsent(name, context);
            }
            return super.openInput(name, context);
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
