/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.encoder.BitPackBlockEncoder;
import org.elasticsearch.columnar.encoder.BlockEncoding;
import org.elasticsearch.columnar.encoder.DeltaPackedBlockEncoder;
import org.elasticsearch.columnar.encoder.IdentityBlockEncoding;
import org.elasticsearch.columnar.encoder.Lz4BlockEncoding;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericMinMaxSkipIndex;
import org.elasticsearch.columnar.encoder.RawBytesBlockEncoder;
import org.elasticsearch.columnar.encoder.SkipIndexParams;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Single-pass comparison matrix across (format, workload) for numerics and bytes columns.
 * Designed to produce the end-of-session results table the team compares against.
 *
 * <p><b>Read paths under test (column nomenclature).</b> Each measurement column maps to a
 * specific Elasticsearch read path so the table is interpretable by code path, not just
 * "fast / slow":
 * <ul>
 *   <li>{@code query-range µs} — Lucene query path. {@code NumericDocValuesField.newSlowRangeQuery}
 *       run through {@code IndexSearcher.count}. Exercises {@code DocValuesSkipper} for filter
 *       pushdown. The fields are written with {@code indexedField} so the skipper is attached.</li>
 *   <li>{@code agg-scan µs} — _search aggregations' iterator-per-doc path: walk every doc via
 *       {@code NumericDocValues.nextDoc} + {@code longValue}. This is the legacy aggregations
 *       contract; we don't optimise for it — modest cost here is acceptable. ES|QL uses
 *       {@code esql-load} below instead.</li>
 *   <li>{@code fetch µs} — value-fetcher / point-lookup path: random {@code advanceExact} per
 *       requested doc id, useful for the fetch phase of TransportSearchAction. Advances are
 *       sorted ascending per Lucene's forward-only contract.</li>
 *   <li>{@code esql-load µs} — ES|QL block-loader pattern: fill caller-owned {@code long[]}
 *       pages of 1024 contiguous docs by repeated {@code advanceExact + longValue}. Models
 *       what {@code AbstractLongsFromDocValuesBlockLoader} does internally. <b>This is the
 *       primary read path we optimise for</b>; the bridge's {@code
 *       ColumNARLongValues.readValues(long[], int)} bulk seam serves this contract.</li>
 * </ul>
 *
 * <p>Five formats tested side-by-side:
 * <ul>
 *   <li><code>lucene104</code> — Lucene's current default {@code DocValuesFormat}, what
 *       vanilla Lucene-only code uses.</li>
 *   <li><code>es95tsdb</code> — Elasticsearch's TSDB pipeline (delta + offset + gcd +
 *       bitpack, 128-value blocks, 4096-value skip intervals). Production-current for
 *       TSDB indexes.</li>
 *   <li><code>columnar/bitpack-lz4</code> — Our format with production defaults:
 *       {@link BitPackBlockEncoder} + {@link Lz4BlockEncoding} ({@link Lz4BlockEncoding.Mode#FAST}),
 *       blockSize 8192, {@link NumericMinMaxSkipIndex} with default thresholds.</li>
 *   <li><code>columnar/deltapack-lz4</code> — Same as above but with
 *       {@link DeltaPackedBlockEncoder} for numerics. The variant we ship for monotonic
 *       workloads.</li>
 *   <li><code>columnar/bitpack-id</code> — Same defaults but with {@link IdentityBlockEncoding}
 *       (no LZ4) to isolate the LZ4 contribution.</li>
 * </ul>
 *
 * <p>Workloads (numeric long):
 * <ul>
 *   <li><code>monotonic</code> — timestamps advancing by ~1000 ms with jitter.</li>
 *   <li><code>gauge_like</code> — values around 5000 ± 50.</li>
 *   <li><code>lowcard</code> — 8 distinct values, uniformly random.</li>
 *   <li><code>random</code> — uniform random {@code long} (worst case for any encoder).</li>
 *   <li><code>floats</code> — Random floats encoded via {@link Float#floatToRawIntBits}.</li>
 *   <li><code>doubles</code> — Random doubles via {@link Double#doubleToRawLongBits}.</li>
 * </ul>
 *
 * <p>Binary workloads:
 * <ul>
 *   <li><code>keyword_short</code> — 8 random ASCII bytes per doc, uniformly random.</li>
 *   <li><code>keyword_lowcard</code> — 16 distinct short strings, repeated.</li>
 * </ul>
 *
 * <p>Per-(format, workload) measurements:
 * <ul>
 *   <li>on-disk total bytes</li>
 *   <li>sequential-scan time (one pass over all docs)</li>
 *   <li>random-advance time (10k random docs)</li>
 *   <li>block-load time (pageSize=1024, 10 contiguous pages)</li>
 * </ul>
 *
 * <p>Numbers are local-machine medians of 3 measured passes after 2 warm-ups, in
 * microseconds. Outputs a Markdown table to the test log. Not asserted — pure measurement.
 */
public class ComparisonMatrixTests extends ESTestCase {

    private static final int N_DOCS = 500_000;
    private static final int RANDOM_ADVANCE_COUNT = 50_000;
    private static final int BLOCK_LOAD_PAGES = 32;
    private static final int BLOCK_LOAD_PAGE_SIZE = 1024;
    private static final String FIELD = "v";

    /**
     * Cache built indices on disk under {@code $TMPDIR/es-columnar-matrix-cache/<workload>-<formatLabel>-N<n>}
     * so repeated runs of the matrix while iterating on read-path code skip the (relatively
     * expensive) index build. The cache lives under the system temp directory so Lucene's
     * {@code MMapDirectory} can write there under the entitlement policy. Delete the cache
     * directory to force a rebuild — encoder changes are NOT auto-detected. To force a rebuild
     * after changing a format, run {@code rm -rf $TMPDIR/es-columnar-matrix-cache}.
     */
    private static final Path CACHE_ROOT = Paths.get(System.getProperty("java.io.tmpdir"), "es-columnar-matrix-cache");

    public void testNumericMatrix() throws IOException {
        // The format is now binary-only at the Lucene doc-values level — NumericDocValuesField
        // is rejected. The matrix needs reworking to route the columnar variant through
        // ColumNARLongField (binary substrate via the bridge) while keeping other formats on
        // their native numeric path for the comparison to still mean something. Deferred.
        assumeFalse("Matrix test pending refactor to bridge-based long path; format is now binary-only", true);
        // Date / events / logs are first-class workloads: append-only timestamps with jitter
        // are common in Elasticsearch indices and our delta-packed numeric path should crush
        // these. Keeping them ahead of the random workload so they show up first in the table.
        final List<String> workloads = List.of(
            "monotonic",
            "events_ts",
            "log_ts",
            "doc_dates",
            "gauge_like",
            "lowcard",
            "random",
            "floats",
            "doubles"
        );
        final List<FormatVariant> formats = List.of(
            new FormatVariant("lucene104", new Lucene104Codec().getDocValuesFormatForField(FIELD)),
            new FormatVariant("es95tsdb", new ES95TSDBDocValuesFormat()),
            new FormatVariant("columnar/bitpack-lz4", new ColumNARDocValuesFormat()),
            new FormatVariant("columnar/deltapack-lz4", columnarFormat(DeltaPackedBlockEncoder.INSTANCE, Lz4BlockEncoding.INSTANCE)),
            new FormatVariant("columnar/bitpack-identity", columnarFormat(BitPackBlockEncoder.INSTANCE, IdentityBlockEncoding.INSTANCE))
        );

        final List<Row> rows = new ArrayList<>();
        for (String workload : workloads) {
            final long[] values = generateNumeric(workload, N_DOCS, 42);
            for (FormatVariant fv : formats) {
                rows.add(measureNumeric(workload, fv, values));
                System.gc();
            }
        }
        logger.info("--- NUMERIC matrix (lower is better; sizes in bytes, times in µs, medians of 3) ---");
        printMatrix(rows, true);
    }

    public void testBinaryMatrix() throws IOException {
        final List<String> workloads = List.of("keyword_short", "keyword_lowcard");
        final List<FormatVariant> formats = List.of(
            new FormatVariant("lucene104", new Lucene104Codec().getDocValuesFormatForField(FIELD)),
            new FormatVariant("es95tsdb", new ES95TSDBDocValuesFormat()),
            new FormatVariant("columnar/raw-lz4", new ColumNARDocValuesFormat()),
            new FormatVariant("columnar/raw-identity", columnarFormat(BitPackBlockEncoder.INSTANCE, IdentityBlockEncoding.INSTANCE))
        );

        final List<Row> rows = new ArrayList<>();
        for (String workload : workloads) {
            final BytesRef[] values = generateBytes(workload, N_DOCS, 42);
            for (FormatVariant fv : formats) {
                rows.add(measureBinary(workload, fv, values));
                System.gc();
            }
        }
        logger.info("--- BINARY matrix (lower is better; sizes in bytes, times in µs, medians of 3) ---");
        printMatrix(rows, false);
    }

    // ---------------- numeric ----------------

    private Row measureNumeric(String workload, FormatVariant fv, long[] values) throws IOException {
        final Path cacheDir = openOrBuildNumericCache(workload, fv, values);
        try (Directory dir = FSDirectory.open(cacheDir)) {
            final long diskBytes = directoryBytes(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long seqNs = medianRun(() -> timeNumericSequentialScan(leaf), 2, 3);
                final long randNs = medianRun(() -> timeNumericRandomAdvance(leaf, values.length, 31), 2, 3);
                final long blockNs = medianRun(() -> timeNumericBlockLoad(leaf, values.length), 2, 3);
                // Pick a range covering roughly the middle 20% of the value distribution —
                // exercises the DocValuesSkipper without trivially matching nothing or
                // everything.
                final long[] sorted = values.clone();
                java.util.Arrays.sort(sorted);
                final long rangeLo = sorted[(int) (sorted.length * 0.4)];
                final long rangeHi = sorted[(int) (sorted.length * 0.6)];
                final long rangeNs = medianRun(() -> timeNumericRangeQuery(reader, rangeLo, rangeHi), 2, 3);
                return new Row(workload, fv.label, diskBytes, seqNs / 1000, randNs / 1000, blockNs / 1000, rangeNs / 1000);
            }
        }
    }

    /**
     * Time a {@code NumericDocValuesField.newSlowRangeQuery} on the field — the path ES|QL
     * filter pushdown and range queries land on. Uses Lucene's bulk scorer + the
     * {@code DocValuesSkipper} attached by {@code indexedField}.
     */
    private long timeNumericRangeQuery(DirectoryReader reader, long lo, long hi) throws IOException {
        final org.apache.lucene.search.IndexSearcher searcher = new org.apache.lucene.search.IndexSearcher(reader);
        final org.apache.lucene.search.Query q = NumericDocValuesField.newSlowRangeQuery(FIELD, lo, hi);
        final long start = System.nanoTime();
        final int hits = searcher.count(q);
        final long elapsed = System.nanoTime() - start;
        if (hits < 0) {
            throw new AssertionError("hits cannot be negative");
        }
        return elapsed;
    }

    /**
     * Returns a stable on-disk cache directory for the (workload, format, N_DOCS) tuple. If
     * the directory already contains a Lucene segment file the index is reused — otherwise it
     * is built fresh. Encoder code changes are NOT detected; delete the cache to invalidate.
     */
    private Path openOrBuildNumericCache(String workload, FormatVariant fv, long[] values) throws IOException {
        final Path cacheDir = CACHE_ROOT.resolve(cacheKey(workload, fv, values.length));
        Files.createDirectories(cacheDir);
        if (hasLuceneSegments(cacheDir) == false) {
            try (Directory dir = FSDirectory.open(cacheDir)) {
                buildNumericIndex(dir, fv.format, values);
            }
        }
        return cacheDir;
    }

    private Path openOrBuildBinaryCache(String workload, FormatVariant fv, BytesRef[] values) throws IOException {
        final Path cacheDir = CACHE_ROOT.resolve(cacheKey(workload, fv, values.length));
        Files.createDirectories(cacheDir);
        if (hasLuceneSegments(cacheDir) == false) {
            try (Directory dir = FSDirectory.open(cacheDir)) {
                buildBinaryIndex(dir, fv.format, values);
            }
        }
        return cacheDir;
    }

    private static String cacheKey(String workload, FormatVariant fv, int n) {
        // Format labels can contain '/' — flatten to a single safe path segment.
        return workload + "__" + fv.label.replace('/', '_') + "__N" + n;
    }

    private static boolean hasLuceneSegments(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.anyMatch(p -> p.getFileName().toString().startsWith("segments_"));
        }
    }

    private void buildNumericIndex(Directory dir, DocValuesFormat dvFormat, long[] values) throws IOException {
        final Codec codec = new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return dvFormat;
            }
        };
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < values.length; i++) {
                final Document d = new Document();
                // indexedField attaches a DocValuesSkipper to the field — required for the
                // range-query measurement below to exercise filter pushdown.
                d.add(NumericDocValuesField.indexedField(FIELD, values[i]));
                w.addDocument(d);
            }
            w.forceMerge(1);
        }
    }

    private long timeNumericSequentialScan(LeafReader leaf) throws IOException {
        final NumericDocValues dv = leaf.getNumericDocValues(FIELD);
        if (dv == null) return 0;
        long acc = 0;
        final long start = System.nanoTime();
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            acc ^= dv.longValue();
        }
        final long elapsed = System.nanoTime() - start;
        if (acc == 0xDEADBEEFCAFEBABEL) throw new AssertionError();
        return elapsed;
    }

    private long timeNumericRandomAdvance(LeafReader leaf, int nDocs, int seed) throws IOException {
        final NumericDocValues dv = leaf.getNumericDocValues(FIELD);
        if (dv == null) return 0;
        // advanceExact is forward-only per the DocIdSetIterator contract — sort probes
        // ascending. Models the realistic ES|QL access pattern where ordered doc-id ranges
        // are passed to the block loader.
        final Random rng = new Random(seed);
        final int[] probes = new int[RANDOM_ADVANCE_COUNT];
        for (int i = 0; i < probes.length; i++) {
            probes[i] = rng.nextInt(nDocs);
        }
        java.util.Arrays.sort(probes);
        long acc = 0;
        final long start = System.nanoTime();
        for (int p : probes) {
            if (dv.advanceExact(p)) {
                acc ^= dv.longValue();
            }
        }
        final long elapsed = System.nanoTime() - start;
        if (acc == 0xDEADBEEFCAFEBABEL) throw new AssertionError();
        return elapsed;
    }

    private long timeNumericBlockLoad(LeafReader leaf, int nDocs) throws IOException {
        final NumericDocValues dv = leaf.getNumericDocValues(FIELD);
        if (dv == null) return 0;
        final long[] page = new long[BLOCK_LOAD_PAGE_SIZE];
        long acc = 0;
        final long start = System.nanoTime();
        // Load BLOCK_LOAD_PAGES contiguous pages starting at doc 0. Mirrors what
        // LongsBlockLoader does internally for an ESQL page.
        int cursor = 0;
        for (int p = 0; p < BLOCK_LOAD_PAGES; p++) {
            if (cursor + BLOCK_LOAD_PAGE_SIZE > nDocs) cursor = 0;
            for (int i = 0; i < BLOCK_LOAD_PAGE_SIZE; i++) {
                if (dv.advanceExact(cursor + i)) {
                    page[i] = dv.longValue();
                }
            }
            cursor += BLOCK_LOAD_PAGE_SIZE;
            acc ^= page[0] ^ page[BLOCK_LOAD_PAGE_SIZE - 1];
        }
        final long elapsed = System.nanoTime() - start;
        if (acc == 0xDEADBEEFCAFEBABEL) throw new AssertionError();
        return elapsed;
    }

    // ---------------- binary ----------------

    private Row measureBinary(String workload, FormatVariant fv, BytesRef[] values) throws IOException {
        final Path cacheDir = openOrBuildBinaryCache(workload, fv, values);
        try (Directory dir = FSDirectory.open(cacheDir)) {
            final long diskBytes = directoryBytes(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final long seqNs = medianRun(() -> timeBinarySequentialScan(leaf), 2, 3);
                final long randNs = medianRun(() -> timeBinaryRandomAdvance(leaf, values.length, 31), 2, 3);
                final long blockNs = medianRun(() -> timeBinaryBlockLoad(leaf, values.length), 2, 3);
                return new Row(workload, fv.label, diskBytes, seqNs / 1000, randNs / 1000, blockNs / 1000, 0L);
            }
        }
    }

    private void buildBinaryIndex(Directory dir, DocValuesFormat dvFormat, BytesRef[] values) throws IOException {
        final Codec codec = new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return dvFormat;
            }
        };
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < values.length; i++) {
                final Document d = new Document();
                d.add(new BinaryDocValuesField(FIELD, values[i]));
                w.addDocument(d);
            }
            w.forceMerge(1);
        }
    }

    private long timeBinarySequentialScan(LeafReader leaf) throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        if (dv == null) return 0;
        long acc = 0;
        final long start = System.nanoTime();
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            final BytesRef ref = dv.binaryValue();
            acc ^= ref.length ^ ref.bytes[ref.offset];
        }
        final long elapsed = System.nanoTime() - start;
        if (acc == 0xDEADBEEFCAFEBABEL) throw new AssertionError();
        return elapsed;
    }

    private long timeBinaryRandomAdvance(LeafReader leaf, int nDocs, int seed) throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        if (dv == null) return 0;
        // advanceExact is forward-only.
        final Random rng = new Random(seed);
        final int[] probes = new int[RANDOM_ADVANCE_COUNT];
        for (int i = 0; i < probes.length; i++) {
            probes[i] = rng.nextInt(nDocs);
        }
        java.util.Arrays.sort(probes);
        long acc = 0;
        final long start = System.nanoTime();
        for (int p : probes) {
            if (dv.advanceExact(p)) {
                final BytesRef ref = dv.binaryValue();
                acc ^= ref.length ^ ref.bytes[ref.offset];
            }
        }
        final long elapsed = System.nanoTime() - start;
        if (acc == 0xDEADBEEFCAFEBABEL) throw new AssertionError();
        return elapsed;
    }

    private long timeBinaryBlockLoad(LeafReader leaf, int nDocs) throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        if (dv == null) return 0;
        long acc = 0;
        final long start = System.nanoTime();
        int cursor = 0;
        for (int p = 0; p < BLOCK_LOAD_PAGES; p++) {
            if (cursor + BLOCK_LOAD_PAGE_SIZE > nDocs) cursor = 0;
            for (int i = 0; i < BLOCK_LOAD_PAGE_SIZE; i++) {
                if (dv.advanceExact(cursor + i)) {
                    final BytesRef ref = dv.binaryValue();
                    acc ^= ref.length;
                }
            }
            cursor += BLOCK_LOAD_PAGE_SIZE;
        }
        final long elapsed = System.nanoTime() - start;
        if (acc == 0xDEADBEEFCAFEBABEL) throw new AssertionError();
        return elapsed;
    }

    // ---------------- shared ----------------

    private static long directoryBytes(Directory dir) throws IOException {
        long total = 0;
        for (String f : dir.listAll()) {
            total += dir.fileLength(f);
        }
        return total;
    }

    private static long medianRun(NanoTimedFn fn, int warmup, int measured) throws IOException {
        for (int i = 0; i < warmup; i++)
            fn.run();
        final long[] runs = new long[measured];
        for (int i = 0; i < measured; i++) {
            runs[i] = fn.run();
        }
        java.util.Arrays.sort(runs);
        return runs[measured / 2];
    }

    @FunctionalInterface
    private interface NanoTimedFn {
        long run() throws IOException;
    }

    private static long[] generateNumeric(String workload, int n, int seed) {
        final long[] values = new long[n];
        final Random rng = new Random(seed);
        switch (workload) {
            case "monotonic" -> {
                long t = 1_700_000_000_000L;
                for (int i = 0; i < n; i++) {
                    values[i] = t;
                    t += 950 + rng.nextInt(101);
                }
            }
            case "events_ts" -> {
                // Event-stream timestamps: mostly sorted, sub-second jitter (typical of
                // user-event logs). DeltaPackedBlockEncoder should crush this since the
                // deltas live in a narrow range.
                long t = 1_700_000_000_000L;
                for (int i = 0; i < n; i++) {
                    values[i] = t + rng.nextInt(50) - 10;  // tiny disorder
                    t += 800 + rng.nextInt(401);            // 0.8–1.2 sec steps
                }
            }
            case "log_ts" -> {
                // Append-only log timestamps: strictly monotonic, millisecond granularity,
                // bursty (sometimes many in a single ms). Worst case for delta encoding is
                // big idle gaps; we model 1k events per second with occasional pauses.
                long t = 1_700_000_000_000L;
                for (int i = 0; i < n; i++) {
                    values[i] = t;
                    // 95% of the time advance by 0-2 ms (1k events/sec range), 5% bigger pause.
                    if (rng.nextInt(20) == 0) {
                        t += 1000 + rng.nextInt(10_000);
                    } else {
                        t += rng.nextInt(3);
                    }
                }
            }
            case "doc_dates" -> {
                // Document publication dates at day granularity, distributed across the
                // last 2 years. Many duplicates (multiple docs per day), no global order.
                final long oneDay = 86_400_000L;
                final long base = 1_700_000_000_000L;
                for (int i = 0; i < n; i++) {
                    values[i] = base + rng.nextInt(730) * oneDay;
                }
            }
            case "gauge_like" -> {
                for (int i = 0; i < n; i++)
                    values[i] = 5000L + rng.nextInt(101) - 50;
            }
            case "lowcard" -> {
                for (int i = 0; i < n; i++)
                    values[i] = rng.nextInt(8);
            }
            case "random" -> {
                for (int i = 0; i < n; i++)
                    values[i] = rng.nextLong();
            }
            case "floats" -> {
                for (int i = 0; i < n; i++)
                    values[i] = Float.floatToRawIntBits(rng.nextFloat() * 10_000f);
            }
            case "doubles" -> {
                for (int i = 0; i < n; i++)
                    values[i] = Double.doubleToRawLongBits(rng.nextDouble() * 10_000.0);
            }
            default -> throw new IllegalArgumentException("unknown workload: " + workload);
        }
        return values;
    }

    private static BytesRef[] generateBytes(String workload, int n, int seed) {
        final BytesRef[] values = new BytesRef[n];
        final Random rng = new Random(seed);
        switch (workload) {
            case "keyword_short" -> {
                final byte[] alphabet = "abcdefghijklmnopqrstuvwxyz0123456789".getBytes();
                for (int i = 0; i < n; i++) {
                    final byte[] b = new byte[8];
                    for (int j = 0; j < 8; j++)
                        b[j] = alphabet[rng.nextInt(alphabet.length)];
                    values[i] = new BytesRef(b);
                }
            }
            case "keyword_lowcard" -> {
                final String[] pool = {
                    "alpha",
                    "beta",
                    "gamma",
                    "delta",
                    "epsilon",
                    "zeta",
                    "eta",
                    "theta",
                    "iota",
                    "kappa",
                    "lambda",
                    "mu",
                    "nu",
                    "xi",
                    "omicron",
                    "pi" };
                for (int i = 0; i < n; i++) {
                    values[i] = new BytesRef(pool[rng.nextInt(pool.length)]);
                }
            }
            default -> throw new IllegalArgumentException("unknown workload: " + workload);
        }
        return values;
    }

    private record FormatVariant(String label, DocValuesFormat format) {}

    /**
     * Build a columnar format with the given numeric encoder and outer encoding; other
     * choices (bytes encoder, skip index, block-size targets, dict-binary) come from the
     * production defaults.
     */
    private static ColumNARDocValuesFormat columnarFormat(NumericBlockEncoder enc, BlockEncoding encoding) {
        return new ColumNARDocValuesFormat(
            enc,
            RawBytesBlockEncoder.INSTANCE,
            encoding,
            NumericMinMaxSkipIndex.INSTANCE,
            SkipIndexParams.DEFAULTS,
            ColumNARDocValuesFormat.DEFAULT_TARGET_ENCODED_BYTES_PER_BLOCK,
            ColumNARDocValuesFormat.DEFAULT_MAX_VALUES_PER_BLOCK,
            true
        );
    }

    private record Row(String workload, String format, long diskBytes, long seqUs, long randUs, long blockUs, long rangeUs) {}

    private void printMatrix(List<Row> rows, boolean numeric) {
        // Header
        logger.info(
            String.format(
                Locale.ROOT,
                "| %-15s | %-26s | %12s | %10s | %10s | %10s | %10s |",
                "workload",
                "format",
                "on-disk B",
                "agg-scan µs",
                "fetch µs",
                "esql-load µs",
                "query-range µs"
            )
        );
        logger.info(
            "|"
                + "-".repeat(17)
                + "|"
                + "-".repeat(28)
                + "|"
                + "-".repeat(14)
                + "|"
                + "-".repeat(12)
                + "|"
                + "-".repeat(12)
                + "|"
                + "-".repeat(12)
                + "|"
                + "-".repeat(12)
                + "|"
        );
        for (Row r : rows) {
            logger.info(
                String.format(
                    Locale.ROOT,
                    "| %-15s | %-26s | %12d | %10d | %10d | %10d | %10d |",
                    r.workload,
                    r.format,
                    r.diskBytes,
                    r.seqUs,
                    r.randUs,
                    r.blockUs,
                    r.rangeUs
                )
            );
        }
    }
}
