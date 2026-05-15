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
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
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
 * Scan-throughput baseline for single-valued keyword doc values across the formats
 * Elasticsearch currently ships. Sweeps the cardinality dimension (low ~100,
 * mid ~10k, high ~1M) since dictionary/ordinal encoding behavior is the most relevant
 * differentiator for keyword fields.
 *
 * <p>Two benchmark methods are exposed:
 * <ul>
 *   <li>{@link #scanOrds} — iterates ords without dereferencing them (measures the
 *       doc-values traversal alone).</li>
 *   <li>{@link #scanWithLookup} — iterates ords and resolves each one to its
 *       {@link BytesRef} via {@link SortedSetDocValues#lookupOrd}; closer to the
 *       end-to-end cost paid by {@code BytesRefsFromOrdsBlockLoader} in ES|QL.</li>
 * </ul>
 *
 * <p>On-disk size for each trial is logged once at setup time so a results table can
 * be assembled from a single benchmark run.
 *
 * <p>Part of the Iteration 1 baseline suite for the next-generation columnar doc
 * values effort. No new format code is involved.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumNARDocValuesKeywordScanBenchmark {

    private static final Logger logger = LogManager.getLogger(ColumNARDocValuesKeywordScanBenchmark.class);

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = "keyword";

    /** Doc count per trial. */
    @Param("1000000")
    private int nDocs;

    /** Random seed for reproducibility. */
    @Param("42")
    private int seed;

    /** Doc values format under test. */
    @Param({ "lucene90", "es87tsdb", "es819v3tsdb", "es95tsdb" })
    private String format;

    /** Distinct-value cardinality. Drives the dictionary encoding behavior. */
    @Param({ "100", "10000", "1000000" })
    private int cardinality;

    private Path directoryPath;
    private Directory directory;
    private DirectoryReader reader;
    private long onDiskBytes;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(ColumNARDocValuesKeywordScanBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directoryPath = Files.createTempDirectory("columnar-bench-kw-");
        directory = FSDirectory.open(directoryPath);
        final DocValuesFormat dvFormat = newDocValuesFormat(format);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return dvFormat;
            }
        });
        final Random random = new Random(seed);
        final byte[][] dictionary = generateDictionary(cardinality, random);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < nDocs; i++) {
                final Document doc = new Document();
                final BytesRef value = new BytesRef(dictionary[random.nextInt(dictionary.length)]);
                doc.add(new SortedSetDocValuesField(FIELD, value));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        onDiskBytes = directorySize(directoryPath);
        logger.info("setup: format={} cardinality={} nDocs={} onDiskBytes={}", format, cardinality, nDocs, onDiskBytes);
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

    @Benchmark
    public void scanOrds(Blackhole bh) throws IOException {
        long checksum = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            final SortedSetDocValues dv = ctx.reader().getSortedSetDocValues(FIELD);
            if (dv == null) {
                continue;
            }
            for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                final int count = dv.docValueCount();
                for (int i = 0; i < count; i++) {
                    checksum ^= dv.nextOrd();
                }
            }
        }
        bh.consume(checksum);
    }

    @Benchmark
    public void scanWithLookup(Blackhole bh) throws IOException {
        long checksum = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            final SortedSetDocValues dv = ctx.reader().getSortedSetDocValues(FIELD);
            if (dv == null) {
                continue;
            }
            for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                final int count = dv.docValueCount();
                for (int i = 0; i < count; i++) {
                    final BytesRef bytes = dv.lookupOrd(dv.nextOrd());
                    checksum ^= bytes.length;
                    if (bytes.length > 0) {
                        checksum ^= bytes.bytes[bytes.offset];
                    }
                }
            }
        }
        bh.consume(checksum);
    }

    private static DocValuesFormat newDocValuesFormat(String name) {
        return switch (name) {
            case "lucene90" -> new Lucene90DocValuesFormat();
            case "es87tsdb" -> new ES87TSDBDocValuesFormat();
            case "es819v3tsdb" -> new ES819Version3TSDBDocValuesFormat();
            case "es95tsdb" -> new ES95TSDBDocValuesFormat();
            default -> throw new IllegalArgumentException("unknown format: " + name);
        };
    }

    private static byte[][] generateDictionary(int cardinality, Random random) {
        final byte[][] terms = new byte[cardinality][];
        for (int i = 0; i < cardinality; i++) {
            final int len = 4 + random.nextInt(20);
            final byte[] b = new byte[len];
            random.nextBytes(b);
            terms[i] = b;
        }
        return terms;
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
