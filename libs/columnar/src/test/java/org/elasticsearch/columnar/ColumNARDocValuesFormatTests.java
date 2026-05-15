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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.bridge.ColumNARBytesValues;
import org.elasticsearch.columnar.bridge.ColumNARKeywordField;
import org.elasticsearch.columnar.bridge.ColumNARLongField;
import org.elasticsearch.columnar.bridge.ColumNARLongValues;
import org.elasticsearch.columnar.bridge.PackedBytesFromBinaryDocValues;
import org.elasticsearch.columnar.bridge.PackedLongsFromBinaryDocValues;
import org.elasticsearch.columnar.encoder.IdentityBlockEncoding;
import org.elasticsearch.columnar.encoder.NumericMinMaxSkipIndex;
import org.elasticsearch.columnar.encoder.RawBlockEncoder;
import org.elasticsearch.columnar.encoder.RawBytesBlockEncoder;
import org.elasticsearch.columnar.encoder.SkipIndexParams;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Round-trip tests for {@link ColumNARDocValuesFormat}. The format is binary-only at the
 * Lucene doc-values level: every test routes through {@link BinaryDocValuesField}
 * (single-valued binary, or longs / strings packed via the bridge wrappers). Numerics
 * exposed through Lucene's {@code NumericDocValuesField} are rejected — the bridge is the
 * only sanctioned path to longs.
 */
public class ColumNARDocValuesFormatTests extends ESTestCase {

    public void testFormatRejectsNumericDocValuesField() throws IOException {
        // Lucene's NumericDocValuesField writes a NUMERIC doc-values type — the format's
        // addNumericField is wired to throw UOE so any write attempt fails fast.
        final Codec codec = codecFor(new ColumNARDocValuesFormat());
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                final Document doc = new Document();
                doc.add(new NumericDocValuesField("v", 42L));
                w.addDocument(doc);
                expectThrows(Exception.class, w::commit);
            }
        }
    }

    public void testRoundTripDenseBinaryDefault() throws IOException {
        assertRoundTripDenseBinary(new ColumNARDocValuesFormat(), 500);
    }

    public void testRoundTripDenseBinaryExplicitBlockSize() throws IOException {
        assertRoundTripDenseBinary(formatWithMaxValues(32), 200);
    }

    public void testRoundTripBinaryMixedSizes() throws IOException {
        final ColumNARDocValuesFormat format = formatWithMaxValues(16);
        final int nDocs = 100;
        final byte[][] expected = new byte[nDocs][];
        for (int i = 0; i < nDocs; i++) {
            // mix empty values, short values, and one long value to exercise the flat-buffer grow path
            final int len = i == 50 ? 4096 : i % 5 == 0 ? 0 : 1 + (i % 32);
            expected[i] = new byte[len];
            random().nextBytes(expected[i]);
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final Codec codec = codecFor(format);
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < nDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new BinaryDocValuesField("b", new BytesRef(expected[i])));
                    w.addDocument(doc);
                }
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues("b");
                assertNotNull(dv);
                for (int i = 0; i < nDocs; i++) {
                    assertEquals("doc id at i=" + i, i, dv.nextDoc());
                    final BytesRef ref = dv.binaryValue();
                    assertEquals("length at doc " + i, expected[i].length, ref.length);
                    for (int b = 0; b < expected[i].length; b++) {
                        assertEquals("byte " + b + " of doc " + i, expected[i][b], ref.bytes[ref.offset + b]);
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    public void testColumNARLongFieldRoundTripViaBridge() throws IOException {
        // Single-valued long fields go through the bridge: write via ColumNARLongField (a
        // typed BinaryDocValuesField wrapper) and read back via the long-typed bridge
        // iterator. The codec sees only bytes; consumers see typed long values.
        final Codec codec = codecFor(new ColumNARDocValuesFormat());
        final int nDocs = 1000;
        final long[] expected = new long[nDocs];
        for (int i = 0; i < nDocs; i++) {
            expected[i] = random().nextLong();
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < nDocs; i++) {
                    final Document d = new Document();
                    d.add(new ColumNARLongField("v", expected[i]));
                    w.addDocument(d);
                }
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues binary = leaf.getBinaryDocValues("v");
                assertNotNull(binary);
                final ColumNARLongValues vals = new PackedLongsFromBinaryDocValues(binary);
                for (int i = 0; i < nDocs; i++) {
                    assertEquals("doc id", i, vals.nextDoc());
                    assertEquals("valueCount", 1, vals.valueCount());
                    assertEquals("longAt(0) at doc " + i, expected[i], vals.longAt(0));
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, vals.nextDoc());
            }
        }
    }

    public void testColumNARLongFieldMultiValuedRoundTrip() throws IOException {
        // Multi-valued long fields: insertion order preserved through the binary substrate.
        final Codec codec = codecFor(new ColumNARDocValuesFormat());
        final long[][] expected = { { 1L, 2L, 3L }, { Long.MAX_VALUE, Long.MIN_VALUE }, { 0L }, { 42L, -7L, 999L, 1L } };
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (long[] row : expected) {
                    final Document d = new Document();
                    d.add(new ColumNARLongField("v", row));
                    w.addDocument(d);
                }
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues binary = leaf.getBinaryDocValues("v");
                assertNotNull(binary);
                final ColumNARLongValues vals = new PackedLongsFromBinaryDocValues(binary);
                for (int i = 0; i < expected.length; i++) {
                    assertEquals("doc id", i, vals.nextDoc());
                    assertEquals("valueCount at doc " + i, expected[i].length, vals.valueCount());
                    for (int v = 0; v < expected[i].length; v++) {
                        assertEquals("longAt(" + v + ") at doc " + i + " — insertion order preserved", expected[i][v], vals.longAt(v));
                    }
                }
            }
        }
    }

    public void testColumNARLongValuesBulkReadValues() throws IOException {
        // Validates the bulk seam ColumNARLongValues.readValues — the path ES|QL block
        // loaders take via BlockLoader.SingletonLongBuilder.appendLongs. Mixed single- and
        // multi-valued docs; concatenated into one caller-owned long[] like an ES|QL page
        // builder would do.
        final Codec codec = codecFor(new ColumNARDocValuesFormat());
        final long[][] expected = {
            { 1L },
            { Long.MAX_VALUE, Long.MIN_VALUE, 0L },
            { 42L },
            { -7L, 999L },
            { 0L, 0L, 0L, 0L, 0L },
            { Long.MIN_VALUE } };
        int totalValues = 0;
        for (long[] row : expected)
            totalValues += row.length;
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (long[] row : expected) {
                    final Document d = new Document();
                    d.add(new ColumNARLongField("v", row));
                    w.addDocument(d);
                }
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues binary = leaf.getBinaryDocValues("v");
                assertNotNull(binary);
                final ColumNARLongValues vals = new PackedLongsFromBinaryDocValues(binary);
                final long[] page = new long[totalValues];
                int write = 0;
                for (int i = 0; i < expected.length; i++) {
                    assertEquals("doc id", i, vals.nextDoc());
                    final int wrote = vals.readValues(page, write);
                    assertEquals("readValues count at doc " + i, expected[i].length, wrote);
                    write += wrote;
                }
                int read = 0;
                for (int i = 0; i < expected.length; i++) {
                    for (int v = 0; v < expected[i].length; v++) {
                        assertEquals("page[" + read + "] (doc " + i + " value " + v + ")", expected[i][v], page[read++]);
                    }
                }
                assertEquals("wrote every value", totalValues, write);
            }
        }
    }

    public void testColumNARBytesValuesBulkReadValues() throws IOException {
        // Bytes-typed sibling of testColumNARLongValuesBulkReadValues. Mixed single- and
        // multi-valued keyword docs; concatenate into one caller-owned byte[] page plus
        // offsets, then verify the values come back in the same order.
        final Codec codec = codecFor(new ColumNARDocValuesFormat());
        final String[][] expected = {
            { "a" },
            { "alpha", "beta", "gamma" },
            { "" },
            { "x", "yy", "zzz" },
            { "single-value" },
            { "p", "q" } };
        int totalValues = 0;
        int totalBytes = 0;
        for (String[] row : expected) {
            totalValues += row.length;
            for (String s : row)
                totalBytes += s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (String[] row : expected) {
                    final Document d = new Document();
                    d.add(ColumNARKeywordField.of("v", row));
                    w.addDocument(d);
                }
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues binary = leaf.getBinaryDocValues("v");
                assertNotNull(binary);
                final ColumNARBytesValues vals = new PackedBytesFromBinaryDocValues(binary);
                final byte[] pageBytes = new byte[totalBytes];
                final int[] pageOffsets = new int[totalValues + expected.length];
                int bytesCursor = 0;
                int offsetsCursor = 0;
                final int[] docBytesWritten = new int[expected.length];
                for (int i = 0; i < expected.length; i++) {
                    assertEquals("doc id", i, vals.nextDoc());
                    final int wrote = vals.readValues(pageBytes, bytesCursor, pageOffsets, offsetsCursor);
                    docBytesWritten[i] = wrote;
                    bytesCursor += wrote;
                    offsetsCursor += expected[i].length + 1;
                }
                assertEquals("total bytes written", totalBytes, bytesCursor);
                // Walk back through offsets/bytes to verify each value.
                int byteBase = 0;
                int offsetBase = 0;
                for (int i = 0; i < expected.length; i++) {
                    for (int v = 0; v < expected[i].length; v++) {
                        final int from = pageOffsets[offsetBase + v];
                        final int to = pageOffsets[offsetBase + v + 1];
                        final String got = new String(pageBytes, byteBase + from, to - from, java.nio.charset.StandardCharsets.UTF_8);
                        assertEquals("doc " + i + " value " + v, expected[i][v], got);
                    }
                    byteBase += docBytesWritten[i];
                    offsetBase += expected[i].length + 1;
                }
            }
        }
    }

    private void assertRoundTripDenseBinary(ColumNARDocValuesFormat format, int nDocs) throws IOException {
        final byte[][] expected = new byte[nDocs][];
        final Codec codec = codecFor(format);
        try (Directory dir = new ByteBuffersDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < nDocs; i++) {
                    expected[i] = new byte[randomIntBetween(0, 32)];
                    random().nextBytes(expected[i]);
                    final Document doc = new Document();
                    doc.add(new BinaryDocValuesField("b", new BytesRef(expected[i])));
                    w.addDocument(doc);
                }
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues("b");
                assertNotNull(dv);
                for (int i = 0; i < nDocs; i++) {
                    assertEquals("doc id at i=" + i, i, dv.nextDoc());
                    final BytesRef ref = dv.binaryValue();
                    assertEquals("length at doc " + i, expected[i].length, ref.length);
                    for (int b = 0; b < expected[i].length; b++) {
                        assertEquals("byte " + b + " of doc " + i, expected[i][b], ref.bytes[ref.offset + b]);
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.nextDoc());
            }
        }
    }

    private static Codec codecFor(DocValuesFormat format) {
        return new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return format;
            }
        };
    }

    /**
     * Build a format with the production defaults except for {@code maxValuesPerBlock}.
     * Tests use this to force small blocks and exercise the block-boundary code paths.
     */
    private static ColumNARDocValuesFormat formatWithMaxValues(int maxValuesPerBlock) {
        return new ColumNARDocValuesFormat(
            RawBlockEncoder.INSTANCE,
            RawBytesBlockEncoder.INSTANCE,
            IdentityBlockEncoding.INSTANCE,
            NumericMinMaxSkipIndex.INSTANCE,
            SkipIndexParams.DEFAULTS,
            ColumNARDocValuesFormat.DEFAULT_TARGET_ENCODED_BYTES_PER_BLOCK,
            maxValuesPerBlock,
            true
        );
    }
}
