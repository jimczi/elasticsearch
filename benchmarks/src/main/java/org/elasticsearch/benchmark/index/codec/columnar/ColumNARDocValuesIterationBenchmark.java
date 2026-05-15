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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.bridge.ColumNARLongValues;
import org.elasticsearch.columnar.bridge.PackedLongBinaryPacker;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.infra.Blackhole;
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
 * Iteration-throughput bench for the columnar long read path. Every read goes through the
 * format's <strong>binary substrate bridge</strong> ({@link ColumNARLongValues}); the
 * benchmark NEVER touches {@code NumericDocValues}, {@code SortedNumericDocValues},
 * {@code SortedDocValues}, or {@code SortedSetDocValues}. That is by design — the new
 * format's lineage is binary doc values, and these typed Lucene abstractions are
 * incompatible with the "preserve insertion order, no exposed ordinals" contract.
 *
 * <p>Indexing emits one {@link BinaryDocValuesField} per doc, with single- or multi-valued
 * data packed via {@link PackedLongBinaryPacker}. Reads route through a custom
 * {@link ColumNARLongFieldType} — a minimal {@link org.elasticsearch.index.mapper.MappedFieldType}
 * subclass that exposes {@link ColumNARLongFieldType#longValues(org.apache.lucene.index.LeafReader)
 * longValues}. The bench is the concrete proof that the integration story works at the
 * MappedFieldType level: agg / fielddata / block-loader callers see {@code long}s through a
 * shape-agnostic iterator that is identical for single- and multi-valued data.
 *
 * <p>Two operations are measured separately:
 * <ul>
 *   <li>{@link #sequentialScan} — the aggregation-style scan over every doc, iterating
 *       all values per doc (the multi-value case).</li>
 *   <li>{@link #randomAdvance} — sparse access via {@link ColumNARLongValues#advanceExact}.</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumNARDocValuesIterationBenchmark {

    private static final Logger logger = LogManager.getLogger(ColumNARDocValuesIterationBenchmark.class);

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Doc count per trial. */
    @Param("1000000")
    private int nDocs;

    /** Random seed for reproducibility. */
    @Param("42")
    private int seed;

    /** Doc-values format under test — the bench treats all formats as opaque binary stores. */
    @Param({ "lucene90", "es87tsdb", "es819v3tsdb", "es95tsdb", "columnar" })
    private String format;

    /** Workload shape. {@code multi_*} variants emit between 1 and {@code maxValuesPerDoc} longs per doc. */
    @Param({ "monotonic", "gauge_like", "counter", "multi_gauge", "multi_counter" })
    private String workload;

    /** Maximum values per doc for the multi-valued variants. Min is always 1 — every doc has at least one value. */
    @Param("4")
    private int maxValuesPerDoc;

    /** Number of random-advance probes per invocation. */
    @Param("4096")
    private int probeCount;

    private Path directoryPath;
    private Directory directory;
    private DirectoryReader reader;
    private ColumNARLongFieldType fieldType;
    private long onDiskBytes;
    private int[] probeTargets;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(ColumNARDocValuesIterationBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directoryPath = Files.createTempDirectory("columnar-iter-bench-");
        directory = FSDirectory.open(directoryPath);
        fieldType = new ColumNARLongFieldType();
        final DocValuesFormat dvFormat = newDocValuesFormat(format);
        // Production-style IndexWriterConfig: Lucene104 codec with the doc-values format
        // overridden for our field. No index sort or merge-policy tweaks — this bench
        // targets the iteration read path.
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return dvFormat;
            }
        });
        final Random random = new Random(seed);
        final boolean multiValued = workload.startsWith("multi_");
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            long counter = 0;
            long baseline = 5000;
            final long[] valueBuf = new long[Math.max(1, maxValuesPerDoc)];
            for (int i = 0; i < nDocs; i++) {
                final int valueCount;
                if (multiValued) {
                    // Random 1..maxValuesPerDoc — every doc has at least one value.
                    valueCount = 1 + random.nextInt(maxValuesPerDoc);
                } else {
                    valueCount = 1;
                }
                for (int j = 0; j < valueCount; j++) {
                    final long v = nextValue(workload, i, random, counter, baseline);
                    valueBuf[j] = v;
                    if (workload.endsWith("counter")) {
                        counter = v;
                    }
                }
                final byte[] packed = PackedLongBinaryPacker.encode(valueBuf, valueCount);
                final Document doc = new Document();
                // The mapper packs every doc's values — single OR multi — into one binary
                // payload. Order inside the payload is preserved exactly as we wrote it.
                doc.add(new BinaryDocValuesField(ColumNARLongFieldType.FIELD, new BytesRef(packed)));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        onDiskBytes = directorySize(directoryPath);

        // Pre-generate random targets so the random-advance bench doesn't pay for RNG cost.
        final Random rng = new Random(seed ^ 0x9E3779B97F4A7C15L);
        probeTargets = new int[probeCount];
        for (int i = 0; i < probeCount; i++) {
            probeTargets[i] = rng.nextInt(nDocs);
        }
        java.util.Arrays.sort(probeTargets); // forward-only advance requires sorted targets

        logger.info("setup: format={} workload={} multi={} nDocs={} onDiskBytes={}", format, workload, multiValued, nDocs, onDiskBytes);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
        deleteRecursively(directoryPath);
    }

    /**
     * Aggregation-style sequential scan. Iterates every doc and consumes every value via
     * the bridge — exactly the access pattern an ES|QL block aggregation or a doc-values
     * fielddata sum would issue against a multi-valued long field.
     */
    @Benchmark
    public void sequentialScan(Blackhole bh, StorageCounters storage) throws IOException {
        storage.onDiskBytes = onDiskBytes;
        long checksum = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            final ColumNARLongValues values = fieldType.longValues(ctx.reader());
            if (values == null) {
                continue;
            }
            for (int doc = values.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                final int count = values.valueCount();
                for (int v = 0; v < count; v++) {
                    checksum ^= values.longAt(v);
                }
            }
        }
        bh.consume(checksum);
    }

    /**
     * Sparse / random access: {@code advanceExact} on pre-generated targets, consuming all
     * values for each hit. Mirrors the point-lookup pattern of a join / lookup workload
     * combined with a multi-valued aggregation reducer.
     */
    @Benchmark
    public void randomAdvance(Blackhole bh, StorageCounters storage) throws IOException {
        storage.onDiskBytes = onDiskBytes;
        long checksum = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            final ColumNARLongValues values = fieldType.longValues(ctx.reader());
            if (values == null) {
                continue;
            }
            for (int target : probeTargets) {
                if (values.advanceExact(target)) {
                    final int count = values.valueCount();
                    for (int v = 0; v < count; v++) {
                        checksum ^= values.longAt(v);
                    }
                }
            }
        }
        bh.consume(checksum);
    }

    /**
     * Per-(format, workload) on-disk size reported as a JMH auxiliary counter. Each bench
     * invocation re-asserts the trial-level constant so it shows up next to the timing in
     * JMH output; treat it as a non-regression measurement, not a per-invocation cost.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class StorageCounters {
        public long onDiskBytes;
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

    private static long nextValue(String workload, int i, Random random, long counter, long baseline) {
        return switch (workload) {
            case "monotonic" -> 1_700_000_000_000L + i * 1000L;
            case "gauge_like", "multi_gauge" -> baseline + random.nextInt(101) - 50;
            case "counter", "multi_counter" -> counter + 1 + random.nextInt(100);
            default -> throw new IllegalArgumentException("unknown workload: " + workload);
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
