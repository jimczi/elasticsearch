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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
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
 * Block-loading bench measuring the access pattern ES|QL uses: load a contiguous range of doc IDs
 * worth of values into a single buffer, advancing through the index in non-descending order. This
 * mirrors what {@code LongsBlockLoader#singletonReader} does internally for
 * {@code NumericDocValues}: {@code advanceExact} + {@code longValue} per doc, with the format
 * left to amortize the block decode internally.
 *
 * <p>The bench loads {@code blockLoaderPageSize} contiguous docs per invocation, sweeping forward
 * through the segment in successive calls. Page size sweeps both the format's internal block size
 * (typically 128 / 256) and the ES|QL page target (~256) so callers can see the alignment
 * effects.
 *
 * <p>We intentionally do not depend on the ES|QL {@code BlockFactory} / {@code Block} classes
 * here — those allocate breaker-tracked memory and would make this bench measure mostly
 * allocator behavior. The bench writes into a caller-owned {@code long[]} sized to the page so
 * the format's amortized decode cost dominates the loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumNARDocValuesBlockLoadingBenchmark {

    private static final Logger logger = LogManager.getLogger(ColumNARDocValuesBlockLoadingBenchmark.class);

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = "value";

    /** Doc count per trial. */
    @Param("1000000")
    private int nDocs;

    /** Random seed for reproducibility. */
    @Param("42")
    private int seed;

    /** Doc-values format under test. */
    @Param({ "lucene90", "es87tsdb", "es819v3tsdb", "es95tsdb", "columnar" })
    private String format;

    /** Workload shape — affects encode size, not block-load CPU. */
    @Param({ "monotonic", "gauge_like" })
    private String workload;

    /** ES|QL page size analog: how many contiguous docs we load per invocation. */
    @Param({ "128", "256", "1024" })
    private int pageSize;

    private Path directoryPath;
    private Directory directory;
    private DirectoryReader reader;
    private long onDiskBytes;
    private long[] page;
    private int cursor;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(ColumNARDocValuesBlockLoadingBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directoryPath = Files.createTempDirectory("columnar-bload-bench-");
        directory = FSDirectory.open(directoryPath);
        final DocValuesFormat dvFormat = newDocValuesFormat(format);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return dvFormat;
            }
        });
        final Random random = new Random(seed);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < nDocs; i++) {
                final long v = nextValue(workload, i, random);
                final Document doc = new Document();
                doc.add(NumericDocValuesField.indexedField(FIELD, v));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        onDiskBytes = directorySize(directoryPath);
        page = new long[pageSize];
        cursor = 0;
        logger.info("setup: format={} workload={} nDocs={} pageSize={} onDiskBytes={}", format, workload, nDocs, pageSize, onDiskBytes);
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
     * Load one page of contiguous values, sweeping forward through the segment in successive
     * invocations. After reaching the end, restart from doc 0 with a fresh DV iterator. This
     * mirrors what {@code LongsBlockLoader} does to fill an ES|QL {@code LongBlock} for a
     * {@code Docs} range that the source operator handed it.
     */
    @Benchmark
    public void loadPage(Blackhole bh, StorageCounters storage) throws IOException {
        storage.onDiskBytes = onDiskBytes;
        final LeafReaderContext ctx = reader.leaves().get(0);
        final NumericDocValues dv = ctx.reader().getNumericDocValues(FIELD);
        if (dv == null) {
            return;
        }
        if (cursor + pageSize > nDocs) {
            cursor = 0;
        }
        final int endExclusive = cursor + pageSize;
        for (int i = 0; i < pageSize; i++) {
            final int doc = cursor + i;
            if (dv.advanceExact(doc)) {
                page[i] = dv.longValue();
            } else {
                page[i] = 0L;
            }
        }
        cursor = endExclusive;
        bh.consume(page[0]);
        bh.consume(page[pageSize - 1]);
    }

    /**
     * Sequential variant that uses {@code nextDoc} instead of {@code advanceExact}. Generally
     * faster than {@link #loadPage} for dense fields because each call avoids the per-doc seek
     * logic, but it can only advance forward — useful for measuring the upper bound the
     * format's internal block cache can deliver.
     */
    @Benchmark
    public void loadPageSequential(Blackhole bh, StorageCounters storage) throws IOException {
        storage.onDiskBytes = onDiskBytes;
        final LeafReaderContext ctx = reader.leaves().get(0);
        final NumericDocValues dv = ctx.reader().getNumericDocValues(FIELD);
        if (dv == null) {
            return;
        }
        // Restart from the beginning every invocation — getNumericDocValues hands back a fresh
        // iterator anyway, so this just reflects how a downstream block loader paginates.
        int read = 0;
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS && read < pageSize; doc = dv.nextDoc()) {
            page[read++] = dv.longValue();
        }
        bh.consume(read);
        bh.consume(page[0]);
        if (read > 0) {
            bh.consume(page[read - 1]);
        }
    }

    /**
     * Per-(format, workload, pageSize) on-disk size reported as a JMH auxiliary counter so it
     * appears alongside throughput in the bench output. Use as a non-regression check on
     * encoder size.
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

    private static long nextValue(String workload, int i, Random random) {
        return switch (workload) {
            case "monotonic" -> 1_700_000_000_000L + i * 1000L;
            case "gauge_like" -> 5000L + random.nextInt(101) - 50;
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
