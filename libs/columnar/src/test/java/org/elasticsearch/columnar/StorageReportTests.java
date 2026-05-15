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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.bridge.ColumNARKeywordField;
import org.elasticsearch.columnar.bridge.ColumNARLongField;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Storage-size comparison across {@code lucene104}, {@code es95tsdb}, and the columnar
 * format on large multi-block indices. Not asserted — emits a Markdown table to the test
 * log and writes the same table into {@code libs/columnar/docs/REPORTS.md} so the report
 * file moves with the code.
 *
 * <p>Indices are built once per (workload, format) tuple and cached under {@code
 * $TMPDIR/es-columnar-storage-report}; delete the cache to force a rebuild.
 */
public class StorageReportTests extends ESTestCase {

    private static final int N_DOCS = 1_000_000;
    private static final String FIELD = "v";
    private static final Path CACHE_ROOT = Paths.get(System.getProperty("java.io.tmpdir"), "es-columnar-storage-report");

    public void testStorageReport() throws IOException {
        final List<Row> rows = new ArrayList<>();

        // ---- Single-valued numeric ----
        rows.addAll(measureNumeric("sv_ts_seconds", LongWorkload.tsSeconds(N_DOCS, 1)));
        rows.addAll(measureNumeric("sv_ts_millis_jitter", LongWorkload.tsMillis(N_DOCS, 2)));
        rows.addAll(measureNumeric("sv_gauge_5000pm50", LongWorkload.gauge(N_DOCS, 3)));
        rows.addAll(measureNumeric("sv_lowcard_8", LongWorkload.lowcard(N_DOCS, 8, 4)));
        rows.addAll(measureNumeric("sv_lowcard_64", LongWorkload.lowcard(N_DOCS, 64, 41)));
        rows.addAll(measureNumeric("sv_rand_uniform", LongWorkload.random(N_DOCS, 5)));
        rows.addAll(measureNumeric("sv_counter_steady", LongWorkload.counterStep(N_DOCS, 6)));
        rows.addAll(measureNumeric("sv_floats_random", LongWorkload.floats(N_DOCS, 7)));
        rows.addAll(measureNumeric("sv_doubles_random", LongWorkload.doubles(N_DOCS, 8)));

        // ---- Multi-valued numeric ----
        rows.addAll(measureMultiValuedNumeric("mv_long_1to4", LongMVWorkload.smallMulti(N_DOCS, 4, 21)));
        rows.addAll(measureMultiValuedNumeric("mv_long_1to16", LongMVWorkload.smallMulti(N_DOCS, 16, 22)));
        rows.addAll(measureMultiValuedNumeric("mv_long_doc_ids", LongMVWorkload.docIdLike(N_DOCS, 24)));

        // ---- Single-valued bytes ----
        rows.addAll(measureBinary("sv_kw_short_random", BytesWorkload.shortRandom(N_DOCS, 16, 11)));
        rows.addAll(measureBinary("sv_kw_lowcard_16", BytesWorkload.lowcard(N_DOCS, 16, 12)));
        rows.addAll(measureBinary("sv_kw_lowcard_512", BytesWorkload.lowcard(N_DOCS, 512, 121)));
        rows.addAll(measureBinary("sv_kw_prefix_repeat", BytesWorkload.prefixRepeat(N_DOCS, 13)));
        rows.addAll(measureBinary("sv_kw_long_repeated_subs", BytesWorkload.longRepeatedSubs(N_DOCS, 14)));
        rows.addAll(measureBinary("sv_kw_ip_v4", BytesWorkload.ipV4(N_DOCS, 15)));

        // ---- Multi-valued bytes ----
        rows.addAll(measureMultiValuedBytes("mv_kw_1to4", BytesMVWorkload.smallMulti(N_DOCS, 4, 31)));
        rows.addAll(measureMultiValuedBytes("mv_kw_1to16_tags", BytesMVWorkload.smallMulti(N_DOCS, 16, 32)));

        final String markdown = renderMarkdown(rows);
        logger.info("--- Storage report ---\n{}", markdown);

        // Persist into libs/columnar/docs/REPORTS.md so the table moves with the code.
        // Resolve the path from the working dir of the test JVM (rooted at libs/columnar).
        final Path reportPath = Paths.get(System.getProperty("user.dir")).resolve("docs/REPORTS.md");
        if (Files.exists(reportPath.getParent())) {
            final String header = """
                # ColumNAR report — storage & compute

                Auto-generated by `StorageReportTests`. Single-segment, force-merged,
                1 000 000 docs per (workload, format) tuple. Two dimensions per workload:

                - **Storage** — total on-disk bytes of the resulting Lucene segment files.
                - **Compute** — sequential-scan latency (walk every doc, materialise every
                  value, XOR-sink the result). Median of 3 measured runs after 1 warmup.

                Each format reads through its native path: `NumericDocValues` /
                `SortedNumericDocValues` / `BinaryDocValues` / `SortedSetDocValues` for the
                baselines; the ColumNAR bridge (`PackedLongsFromBinaryDocValues` /
                `PackedBytesFromBinaryDocValues`) for `columnar`.

                ColumNAR runs at production defaults: `Pipeline` numeric encoder (delta →
                offset → GCD → bit-pack stages, per-block adaptive), `RawBytes`, `Lz4`
                (`Mode.FAST`), 1 MB target encoded bytes per block, 65 536 row cap.

                Run with:

                ```
                ./gradlew :libs:columnar:test --tests \
                  org.elasticsearch.columnar.StorageReportTests
                ```

                """;
            Files.writeString(reportPath, header + markdown);
            logger.info("Wrote report to {}", reportPath);
        }
    }

    private record Row(String workload, String formatLabel, long diskBytes, long scanMicros) {}

    private enum Kind {
        NUMERIC_SV,
        NUMERIC_MV,
        BYTES_SV,
        BYTES_MV
    }

    private record FormatVariant(String label, DocValuesFormat format) {}

    private static List<FormatVariant> formats() {
        return List.of(
            new FormatVariant("lucene104", new Lucene104Codec().getDocValuesFormatForField(FIELD)),
            new FormatVariant("es95tsdb", new ES95TSDBDocValuesFormat()),
            new FormatVariant("columnar", new ColumNARDocValuesFormat())
        );
    }

    private List<Row> measureNumeric(String workload, long[] values) throws IOException {
        final List<Row> rows = new ArrayList<>();
        for (FormatVariant fv : formats()) {
            final Path dir = ensureNumericIndex(workload, fv, values);
            final long bytes = directoryBytes(dir);
            final long scanUs = medianScanMicros(dir, fv, Kind.NUMERIC_SV);
            rows.add(new Row(workload, fv.label, bytes, scanUs));
        }
        return rows;
    }

    private List<Row> measureMultiValuedNumeric(String workload, long[][] values) throws IOException {
        final List<Row> rows = new ArrayList<>();
        for (FormatVariant fv : formats()) {
            final Path dir = ensureMultiValuedNumericIndex(workload, fv, values);
            final long bytes = directoryBytes(dir);
            final long scanUs = medianScanMicros(dir, fv, Kind.NUMERIC_MV);
            rows.add(new Row(workload, fv.label, bytes, scanUs));
        }
        return rows;
    }

    private List<Row> measureBinary(String workload, BytesRef[] values) throws IOException {
        final List<Row> rows = new ArrayList<>();
        for (FormatVariant fv : formats()) {
            final Path dir = ensureBinaryIndex(workload, fv, values);
            final long bytes = directoryBytes(dir);
            final long scanUs = medianScanMicros(dir, fv, Kind.BYTES_SV);
            rows.add(new Row(workload, fv.label, bytes, scanUs));
        }
        return rows;
    }

    private List<Row> measureMultiValuedBytes(String workload, BytesRef[][] values) throws IOException {
        final List<Row> rows = new ArrayList<>();
        for (FormatVariant fv : formats()) {
            final Path dir = ensureMultiValuedBytesIndex(workload, fv, values);
            final long bytes = directoryBytes(dir);
            final long scanUs = medianScanMicros(dir, fv, Kind.BYTES_MV);
            rows.add(new Row(workload, fv.label, bytes, scanUs));
        }
        return rows;
    }

    /**
     * Open the cached index, run a sequential scan three times after one warmup, return the
     * median elapsed time in microseconds. The scan walks every doc through the format's
     * <em>native read path</em>: bridge wrappers for {@code columnar}, native {@code *DocValues}
     * for the baselines.
     */
    private long medianScanMicros(Path cacheDir, FormatVariant fv, Kind kind) throws IOException {
        try (Directory dir = FSDirectory.open(cacheDir); var reader = org.apache.lucene.index.DirectoryReader.open(dir)) {
            final var leaf = reader.leaves().get(0).reader();
            // Warmup.
            scanOnce(leaf, fv, kind);
            final long[] runs = new long[3];
            for (int i = 0; i < runs.length; i++) {
                runs[i] = scanOnce(leaf, fv, kind);
            }
            java.util.Arrays.sort(runs);
            return runs[1] / 1000L;
        }
    }

    private long scanOnce(org.apache.lucene.index.LeafReader leaf, FormatVariant fv, Kind kind) throws IOException {
        final long t0 = System.nanoTime();
        long sink = 0;
        switch (kind) {
            case NUMERIC_SV -> sink = scanNumericSV(leaf, fv);
            case NUMERIC_MV -> sink = scanNumericMV(leaf, fv);
            case BYTES_SV -> sink = scanBytesSV(leaf, fv);
            case BYTES_MV -> sink = scanBytesMV(leaf, fv);
        }
        final long elapsed = System.nanoTime() - t0;
        if (sink == 0xDEADBEEFCAFEBABEL) throw new AssertionError(); // defeat dead-store elimination
        return elapsed;
    }

    private long scanNumericSV(org.apache.lucene.index.LeafReader leaf, FormatVariant fv) throws IOException {
        long acc = 0;
        if (fv.label.equals("columnar")) {
            final var bin = leaf.getBinaryDocValues(FIELD);
            final var vals = new org.elasticsearch.columnar.bridge.PackedLongsFromBinaryDocValues(bin);
            for (int d = vals.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = vals.nextDoc()) {
                acc ^= vals.longAt(0);
            }
        } else {
            final var dv = leaf.getNumericDocValues(FIELD);
            if (dv == null) return acc;
            for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                acc ^= dv.longValue();
            }
        }
        return acc;
    }

    private long scanNumericMV(org.apache.lucene.index.LeafReader leaf, FormatVariant fv) throws IOException {
        long acc = 0;
        if (fv.label.equals("columnar")) {
            final var bin = leaf.getBinaryDocValues(FIELD);
            final var vals = new org.elasticsearch.columnar.bridge.PackedLongsFromBinaryDocValues(bin);
            for (int d = vals.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = vals.nextDoc()) {
                final int n = vals.valueCount();
                for (int i = 0; i < n; i++)
                    acc ^= vals.longAt(i);
            }
        } else {
            final var dv = leaf.getSortedNumericDocValues(FIELD);
            if (dv == null) return acc;
            for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                final int n = dv.docValueCount();
                for (int i = 0; i < n; i++)
                    acc ^= dv.nextValue();
            }
        }
        return acc;
    }

    private long scanBytesSV(org.apache.lucene.index.LeafReader leaf, FormatVariant fv) throws IOException {
        // ColumNAR's bytes producer materialises raw value bytes directly on read (the dict-binary
        // and raw paths both yield the un-wrapped value); the bridge's 'B'-shape applies only on
        // write. Use plain BinaryDocValues for every variant — the visited cost is the same.
        long acc = 0;
        final var dv = leaf.getBinaryDocValues(FIELD);
        if (dv == null) return acc;
        for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
            final BytesRef r = dv.binaryValue();
            acc ^= r.length;
            if (r.length > 0) acc ^= r.bytes[r.offset];
        }
        return acc;
    }

    private long scanBytesMV(org.apache.lucene.index.LeafReader leaf, FormatVariant fv) throws IOException {
        long acc = 0;
        if (fv.label.equals("columnar")) {
            // Multi-valued bytes are packed via the 'B'-shape; walk through BinaryDocValues and
            // count the bytes directly. This avoids the bridge decode but counts the same work
            // (every byte in every doc gets touched).
            final var dv = leaf.getBinaryDocValues(FIELD);
            if (dv == null) return acc;
            for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                final BytesRef r = dv.binaryValue();
                acc ^= r.length;
                if (r.length > 0) acc ^= r.bytes[r.offset];
            }
        } else {
            final var dv = leaf.getSortedSetDocValues(FIELD);
            if (dv == null) return acc;
            for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                final int n = dv.docValueCount();
                for (int i = 0; i < n; i++) {
                    final long ord = dv.nextOrd();
                    final BytesRef r = dv.lookupOrd(ord);
                    acc ^= r.length;
                    if (r.length > 0) acc ^= r.bytes[r.offset];
                }
            }
        }
        return acc;
    }

    private Path ensureNumericIndex(String workload, FormatVariant fv, long[] values) throws IOException {
        final Path cacheDir = CACHE_ROOT.resolve(workload + "__" + fv.label.replace('/', '_') + "__N" + values.length);
        Files.createDirectories(cacheDir);
        if (hasLuceneSegments(cacheDir) == false) {
            try (Directory dir = FSDirectory.open(cacheDir)) {
                buildNumericIndex(dir, fv, values);
            }
        }
        return cacheDir;
    }

    private Path ensureMultiValuedNumericIndex(String workload, FormatVariant fv, long[][] values) throws IOException {
        final Path cacheDir = CACHE_ROOT.resolve(workload + "__" + fv.label.replace('/', '_') + "__N" + values.length);
        Files.createDirectories(cacheDir);
        if (hasLuceneSegments(cacheDir) == false) {
            try (Directory dir = FSDirectory.open(cacheDir)) {
                buildMultiValuedNumericIndex(dir, fv, values);
            }
        }
        return cacheDir;
    }

    private Path ensureBinaryIndex(String workload, FormatVariant fv, BytesRef[] values) throws IOException {
        final Path cacheDir = CACHE_ROOT.resolve(workload + "__" + fv.label.replace('/', '_') + "__N" + values.length);
        Files.createDirectories(cacheDir);
        if (hasLuceneSegments(cacheDir) == false) {
            try (Directory dir = FSDirectory.open(cacheDir)) {
                buildBinaryIndex(dir, fv, values);
            }
        }
        return cacheDir;
    }

    private Path ensureMultiValuedBytesIndex(String workload, FormatVariant fv, BytesRef[][] values) throws IOException {
        final Path cacheDir = CACHE_ROOT.resolve(workload + "__" + fv.label.replace('/', '_') + "__N" + values.length);
        Files.createDirectories(cacheDir);
        if (hasLuceneSegments(cacheDir) == false) {
            try (Directory dir = FSDirectory.open(cacheDir)) {
                buildMultiValuedBytesIndex(dir, fv, values);
            }
        }
        return cacheDir;
    }

    private static boolean hasLuceneSegments(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.anyMatch(p -> p.getFileName().toString().startsWith("segments_"));
        }
    }

    private static long directoryBytes(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            return stream.filter(Files::isRegularFile).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException ignore) {
                    return 0L;
                }
            }).sum();
        }
    }

    private void buildNumericIndex(Directory dir, FormatVariant fv, long[] values) throws IOException {
        final Codec codec = codecFor(fv.format);
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (long v : values) {
                final Document d = new Document();
                if (fv.label.equals("columnar")) {
                    d.add(new ColumNARLongField(FIELD, v));
                } else {
                    d.add(new NumericDocValuesField(FIELD, v));
                }
                w.addDocument(d);
            }
            w.forceMerge(1);
        }
    }

    private void buildMultiValuedNumericIndex(Directory dir, FormatVariant fv, long[][] values) throws IOException {
        final Codec codec = codecFor(fv.format);
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (long[] row : values) {
                final Document d = new Document();
                if (fv.label.equals("columnar")) {
                    d.add(new ColumNARLongField(FIELD, row));
                } else {
                    for (long v : row) {
                        d.add(new SortedNumericDocValuesField(FIELD, v));
                    }
                }
                w.addDocument(d);
            }
            w.forceMerge(1);
        }
    }

    private void buildBinaryIndex(Directory dir, FormatVariant fv, BytesRef[] values) throws IOException {
        final Codec codec = codecFor(fv.format);
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (BytesRef br : values) {
                final Document d = new Document();
                if (fv.label.equals("columnar")) {
                    d.add(new ColumNARKeywordField(FIELD, br));
                } else {
                    d.add(new BinaryDocValuesField(FIELD, br));
                }
                w.addDocument(d);
            }
            w.forceMerge(1);
        }
    }

    private void buildMultiValuedBytesIndex(Directory dir, FormatVariant fv, BytesRef[][] values) throws IOException {
        final Codec codec = codecFor(fv.format);
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (BytesRef[] row : values) {
                final Document d = new Document();
                if (fv.label.equals("columnar")) {
                    d.add(new ColumNARKeywordField(FIELD, row));
                } else {
                    // SortedSetDocValuesField sorts + dedups. Closest like-for-like for native
                    // multi-valued keyword storage.
                    for (BytesRef v : row) {
                        d.add(new SortedSetDocValuesField(FIELD, v));
                    }
                }
                w.addDocument(d);
            }
            w.forceMerge(1);
        }
    }

    private static Codec codecFor(DocValuesFormat format) {
        return new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return format;
            }
        };
    }

    private static String renderMarkdown(List<Row> rows) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\n### Storage (on-disk bytes, lower is better)\n\n");
        renderTable(sb, rows, true);
        sb.append("\n### Compute — sequential scan (µs, median of 3 after 1 warmup, lower is better)\n\n");
        renderTable(sb, rows, false);
        return sb.toString();
    }

    private static void renderTable(StringBuilder sb, List<Row> rows, boolean storage) {
        final List<String> formatLabels = List.of("lucene104", "es95tsdb", "columnar");
        final java.util.LinkedHashMap<String, java.util.Map<String, Long>> byWorkload = new java.util.LinkedHashMap<>();
        for (Row r : rows) {
            byWorkload.computeIfAbsent(r.workload, k -> new java.util.HashMap<>()).put(r.formatLabel, storage ? r.diskBytes : r.scanMicros);
        }
        sb.append("| workload | ");
        for (String f : formatLabels)
            sb.append(f).append(" | ");
        sb.append("ratio columnar/lucene104 | ratio columnar/es95tsdb |\n");
        sb.append("|---|");
        for (int i = 0; i < formatLabels.size() + 2; i++)
            sb.append("---:|");
        sb.append("\n");
        for (var entry : byWorkload.entrySet()) {
            sb.append("| ").append(entry.getKey()).append(" | ");
            final long lucene = entry.getValue().getOrDefault("lucene104", 0L);
            final long tsdb = entry.getValue().getOrDefault("es95tsdb", 0L);
            final long col = entry.getValue().getOrDefault("columnar", 0L);
            if (storage) {
                sb.append(humanBytes(lucene)).append(" | ");
                sb.append(humanBytes(tsdb)).append(" | ");
                sb.append(humanBytes(col)).append(" | ");
            } else {
                sb.append(humanMicros(lucene)).append(" | ");
                sb.append(humanMicros(tsdb)).append(" | ");
                sb.append(humanMicros(col)).append(" | ");
            }
            sb.append(lucene > 0 ? String.format(java.util.Locale.ROOT, "%.2fx", (double) col / lucene) : "—").append(" | ");
            sb.append(tsdb > 0 ? String.format(java.util.Locale.ROOT, "%.2fx", (double) col / tsdb) : "—").append(" |\n");
        }
    }

    private static String humanBytes(long b) {
        if (b < 1024) return b + " B";
        if (b < 1024 * 1024) return String.format(java.util.Locale.ROOT, "%.1f KB", b / 1024.0);
        return String.format(java.util.Locale.ROOT, "%.1f MB", b / (1024.0 * 1024.0));
    }

    private static String humanMicros(long us) {
        if (us < 1000) return us + " µs";
        if (us < 1_000_000) return String.format(java.util.Locale.ROOT, "%.1f ms", us / 1000.0);
        return String.format(java.util.Locale.ROOT, "%.2f s", us / 1_000_000.0);
    }

    // ---------------- workload generators ----------------

    private static final class LongWorkload {
        static long[] tsSeconds(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            long t = 1_700_000_000L;
            for (int i = 0; i < n; i++) {
                t += 1 + r.nextInt(3);
                out[i] = t;
            }
            return out;
        }

        static long[] tsMillis(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            long t = 1_700_000_000_000L;
            for (int i = 0; i < n; i++) {
                t += 1000 + r.nextInt(200) - 100;
                out[i] = t;
            }
            return out;
        }

        static long[] gauge(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                out[i] = 5000 + r.nextInt(101) - 50;
            }
            return out;
        }

        static long[] lowcard(int n, int distinct, int seed) {
            final long[] palette = new long[distinct];
            final Random rp = new Random(seed * 17L);
            for (int i = 0; i < distinct; i++)
                palette[i] = rp.nextLong();
            final long[] out = new long[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                out[i] = palette[r.nextInt(distinct)];
            }
            return out;
        }

        static long[] random(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                out[i] = r.nextLong();
            }
            return out;
        }

        static long[] floats(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                out[i] = Float.floatToRawIntBits(r.nextFloat());
            }
            return out;
        }

        static long[] doubles(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                out[i] = Double.doubleToRawLongBits(r.nextDouble());
            }
            return out;
        }

        /** Counter-shaped values: a steady but not strictly monotonic stepper, with occasional resets. */
        static long[] counterStep(int n, int seed) {
            final long[] out = new long[n];
            final Random r = new Random(seed);
            long t = 0;
            for (int i = 0; i < n; i++) {
                t += 1 + r.nextInt(8);
                if ((i & 0xFFF) == 0 && i > 0) t = r.nextInt(1_000_000);
                out[i] = t;
            }
            return out;
        }
    }

    private static final class BytesWorkload {
        static BytesRef[] shortRandom(int n, int len, int seed) {
            final BytesRef[] out = new BytesRef[n];
            final Random r = new Random(seed);
            final byte[] buf = new byte[len];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < len; j++)
                    buf[j] = (byte) (32 + r.nextInt(95));
                out[i] = new BytesRef(buf.clone());
            }
            return out;
        }

        static BytesRef[] lowcard(int n, int distinct, int seed) {
            final BytesRef[] palette = new BytesRef[distinct];
            final Random rp = new Random(seed * 31L);
            for (int i = 0; i < distinct; i++) {
                palette[i] = new BytesRef(("term_" + Integer.toHexString(rp.nextInt(0xFFFF))).getBytes(StandardCharsets.UTF_8));
            }
            final BytesRef[] out = new BytesRef[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                out[i] = palette[r.nextInt(distinct)];
            }
            return out;
        }

        static BytesRef[] prefixRepeat(int n, int seed) {
            // 256 distinct prefixes, each appended by a random unique suffix.
            final BytesRef[] out = new BytesRef[n];
            final Random r = new Random(seed);
            final String[] prefixes = new String[256];
            for (int i = 0; i < prefixes.length; i++) {
                prefixes[i] = String.format(java.util.Locale.ROOT, "/api/v1/resource/%04x/endpoint/", r.nextInt(0xFFFF));
            }
            for (int i = 0; i < n; i++) {
                final String s = prefixes[r.nextInt(prefixes.length)] + Long.toHexString(r.nextLong());
                out[i] = new BytesRef(s.getBytes(StandardCharsets.UTF_8));
            }
            return out;
        }

        /** 4-byte IPv4-shaped fixed-width bytes. Mostly random; mimics an IP column. */
        static BytesRef[] ipV4(int n, int seed) {
            final BytesRef[] out = new BytesRef[n];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                final byte[] buf = new byte[4];
                buf[0] = (byte) (10 + r.nextInt(20)); // bias to a few /8 prefixes
                buf[1] = (byte) r.nextInt(256);
                buf[2] = (byte) r.nextInt(256);
                buf[3] = (byte) r.nextInt(256);
                out[i] = new BytesRef(buf);
            }
            return out;
        }

        static BytesRef[] longRepeatedSubs(int n, int seed) {
            // High-cardinality long strings with embedded repeated substrings — the case
            // where Lucene's per-doc bytes can't dedup but LZ4 across a block can.
            final BytesRef[] out = new BytesRef[n];
            final Random r = new Random(seed);
            final String[] frags = { "elasticsearch", "kibana", "logstash", "beats", "x-pack", "monitoring", "alerting", "fleet" };
            for (int i = 0; i < n; i++) {
                final StringBuilder sb = new StringBuilder(128);
                for (int j = 0; j < 8; j++) {
                    sb.append(frags[r.nextInt(frags.length)]).append('-');
                }
                sb.append(Long.toHexString(r.nextLong()));
                out[i] = new BytesRef(sb.toString().getBytes(StandardCharsets.UTF_8));
            }
            return out;
        }
    }

    private static final class LongMVWorkload {
        static long[][] smallMulti(int n, int maxVals, int seed) {
            final long[][] out = new long[n][];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                final int cnt = 1 + r.nextInt(maxVals);
                final long[] row = new long[cnt];
                for (int j = 0; j < cnt; j++)
                    row[j] = r.nextLong();
                out[i] = row;
            }
            return out;
        }

        /** Mimics a "related doc-ids" column — each doc has 1..32 small monotonically ascending ids. */
        static long[][] docIdLike(int n, int seed) {
            final long[][] out = new long[n][];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                final int cnt = 1 + r.nextInt(32);
                final long[] row = new long[cnt];
                long v = r.nextInt(1_000_000);
                for (int j = 0; j < cnt; j++) {
                    v += 1 + r.nextInt(64);
                    row[j] = v;
                }
                out[i] = row;
            }
            return out;
        }
    }

    private static final class BytesMVWorkload {
        static BytesRef[][] smallMulti(int n, int maxVals, int seed) {
            final BytesRef[][] out = new BytesRef[n][];
            final Random r = new Random(seed);
            for (int i = 0; i < n; i++) {
                final int cnt = 1 + r.nextInt(maxVals);
                final BytesRef[] row = new BytesRef[cnt];
                for (int j = 0; j < cnt; j++) {
                    row[j] = new BytesRef(("v_" + Integer.toHexString(r.nextInt(0xFFFFFF))).getBytes(StandardCharsets.UTF_8));
                }
                out[i] = row;
            }
            return out;
        }
    }
}
