/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.bridge.PackedLongBinaryPacker;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Ingestion-speed bench across doc-values formats. Each format runs through <em>its</em>
 * native write API — no cross-format compromise:
 *
 * <ul>
 *   <li>{@code lucene90} / {@code es87tsdb} / {@code es819v3tsdb} / {@code es95tsdb} write
 *       single-valued longs through {@link NumericDocValuesField} and multi-valued longs
 *       through {@link SortedNumericDocValuesField} — the typed Lucene shape every TSDB
 *       format is tuned for. Anything else would be a bridge tax that misrepresents these
 *       formats.</li>
 *   <li>{@code columnar} writes through the bridge's typed wrapper
 *       ({@link org.elasticsearch.columnar.bridge.ColumNARLongField}) — itself a thin
 *       {@link BinaryDocValuesField} subclass that packs the long(s) via
 *       {@link PackedLongBinaryPacker}. The hot path uses the allocation-free
 *       {@link BytesRefBuilder}-based packer overload so the {@link Field} instance and
 *       its backing buffer are reused across every doc.</li>
 * </ul>
 *
 * <p>The result is an apples-to-best comparison: each format pays only its native
 * indexing cost. The columnar format's overhead is two things, both visible in the
 * numbers: (1) one extra byte per payload (the {@code PayloadShape} marker) and (2) the
 * per-doc {@code byte[]} pack — minimised by the reusable {@link BytesRefBuilder}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumNARDocValuesIndexingBenchmark {

    private static final Logger logger = LogManager.getLogger(ColumNARDocValuesIndexingBenchmark.class);

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = "value";

    @Param("250000")
    private int nDocs;

    @Param("42")
    private int seed;

    @Param({ "lucene90", "es87tsdb", "es819v3tsdb", "es95tsdb", "columnar" })
    private String format;

    /** Workload shape — single-valued (one long per doc) or multi-valued (1..4 longs per doc). */
    @Param({ "single", "multi" })
    private String shape;

    /** Maximum values per doc for the multi-valued shape. Min is always 1. */
    @Param("4")
    private int maxValuesPerDoc;

    /** Pre-generated values so the bench measures only the encode + index path. */
    private long[] singleValues;
    private long[][] multiValues;
    private DocValuesFormat dvFormat;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(ColumNARDocValuesIndexingBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        final Random rng = new Random(seed);
        if (shape.equals("single")) {
            singleValues = new long[nDocs];
            for (int i = 0; i < nDocs; i++) {
                singleValues[i] = rng.nextLong();
            }
        } else {
            multiValues = new long[nDocs][];
            for (int i = 0; i < nDocs; i++) {
                final int n = 1 + rng.nextInt(maxValuesPerDoc);
                multiValues[i] = new long[n];
                for (int j = 0; j < n; j++) {
                    multiValues[i][j] = rng.nextLong();
                }
            }
        }
        dvFormat = newDocValuesFormat(format);
    }

    /**
     * Bench body: build a fresh index from scratch each invocation. We use a temp directory
     * per iteration to keep each measurement independent.
     */
    @Benchmark
    public long indexAll() throws IOException {
        final Path directoryPath = Files.createTempDirectory("columnar-indexing-bench-");
        long bytes;
        try {
            try (Directory directory = FSDirectory.open(directoryPath)) {
                final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Lucene104Codec() {
                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                        return dvFormat;
                    }
                });
                try (IndexWriter writer = new IndexWriter(directory, config)) {
                    if (format.equals("columnar")) {
                        indexColumnar(writer);
                    } else {
                        indexNative(writer);
                    }
                }
                bytes = directorySize(directoryPath);
            }
        } finally {
            deleteRecursively(directoryPath);
        }
        return bytes;
    }

    /**
     * Columnar indexing path: reuses one {@link BinaryDocValuesField} + one
     * {@link BytesRefBuilder} across all docs, packing the values in place via the
     * allocation-free packer overload. The only per-doc allocations are the {@link Document}
     * itself (unavoidable for Lucene's add-doc contract).
     */
    private void indexColumnar(IndexWriter writer) throws IOException {
        // Reused state — no per-doc allocations beyond the Document itself.
        final BytesRefBuilder buf = new BytesRefBuilder();
        final BinaryDocValuesField field = new BinaryDocValuesField(FIELD, new BytesRef());
        if (shape.equals("single")) {
            for (int i = 0; i < nDocs; i++) {
                PackedLongBinaryPacker.encodeSingle(singleValues[i], buf);
                field.setBytesValue(buf.get());
                final Document doc = new Document();
                doc.add(field);
                writer.addDocument(doc);
            }
        } else {
            for (int i = 0; i < nDocs; i++) {
                final long[] row = multiValues[i];
                PackedLongBinaryPacker.encode(row, row.length, buf);
                field.setBytesValue(buf.get());
                final Document doc = new Document();
                doc.add(field);
                writer.addDocument(doc);
            }
        }
    }

    /**
     * Native indexing path for the other formats: single-valued via {@link NumericDocValuesField},
     * multi-valued via {@link SortedNumericDocValuesField} — the contract each TSDB format
     * is tuned for.
     */
    private void indexNative(IndexWriter writer) throws IOException {
        if (shape.equals("single")) {
            for (int i = 0; i < nDocs; i++) {
                final Document doc = new Document();
                doc.add(new NumericDocValuesField(FIELD, singleValues[i]));
                writer.addDocument(doc);
            }
        } else {
            for (int i = 0; i < nDocs; i++) {
                final Document doc = new Document();
                for (long v : multiValues[i]) {
                    doc.add(new SortedNumericDocValuesField(FIELD, v));
                }
                writer.addDocument(doc);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Each @Benchmark invocation cleans up its own directory; nothing to do here.
        logger.info("teardown: format={} shape={} nDocs={}", format, shape, nDocs);
    }

    private static DocValuesFormat newDocValuesFormat(String name) {
        return switch (name) {
            case "lucene90" -> new Lucene90DocValuesFormat();
            case "es87tsdb" -> new ES87TSDBDocValuesFormat();
            case "es819v3tsdb" -> new ES819Version3TSDBDocValuesFormat();
            case "es95tsdb" -> new ES95TSDBDocValuesFormat();
            case "columnar" -> new ColumNARDocValuesFormat();
            default -> throw new IllegalArgumentException("unknown format: " + name);
        };
    }

    private static long directorySize(Path root) throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || Files.exists(root) == false) {
            return;
        }
        try (Stream<Path> files = Files.walk(root)) {
            files.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            });
        }
    }
}
