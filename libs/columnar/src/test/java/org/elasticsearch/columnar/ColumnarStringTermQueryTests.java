/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Runs {@link ColumnarStringTermQuery} through a real {@link IndexSearcher} over a ColumNAR-coded index and
 * checks its hits against a brute-force scan. Going through the searcher is the point: a scorer collects a
 * match a window at a time, so this exercises the bulk path a filter actually runs on, which asking the
 * iterator document by document would not.
 */
public class ColumnarStringTermQueryTests extends ESTestCase {

    private static final String FIELD = "value";

    /** Few distinct values, so the column carries a dictionary and terms match over ordinals. */
    public void testLowCardinality() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(randomFrom("INFO", "DEBUG", "WARN", "ERROR"));
        }
        assertQueries(values, List.of("INFO", "ERROR", "ABSENT", ""), List.of("IN", "E", "", "Z"));
    }

    /** Every value distinct, so the column is plain and terms match over the values. */
    public void testHighCardinality() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            values.add("id-" + i + "-" + randomAlphaOfLength(6));
        }
        final List<String> terms = List.of(values.get(0), values.get(values.size() - 1), "absent");
        assertQueries(values, terms, List.of("id-1", "id-", "zzz"));
    }

    /** Hot values with a long tail, so a term is answered by the dictionary or by the exceptions. */
    public void testHotValuesWithTail() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 8000; i++) {
            values.add(random().nextDouble() < 0.85 ? randomFrom("success", "timeout") : "err-" + i);
        }
        assertQueries(values, List.of("success", "timeout", "err-7", "err-9999"), List.of("err-", "s", "t"));
    }

    /** Documents without a value, so the matches have to be named in document ids and not in ranks. */
    public void testSparse() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 4000; i++) {
            values.add(random().nextDouble() < 0.3 ? randomFrom("a", "b", "c") : null);
        }
        assertQueries(values, List.of("a", "b", "c", "absent"), List.of("a", ""));
    }

    /** Several segments merged into one: the merged column must answer the same terms as the sources. */
    public void testAcrossMergedSegments() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 6000; i++) {
            values.add(random().nextDouble() < 0.7 ? randomFrom("alpha", "beta", "gamma") : "tail-" + i);
        }
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < values.size(); i++) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(values.get(i)), type));
                    writer.addDocument(doc);
                    if (i % 700 == 699) {
                        // Several segments, so the merge has more than one source to read.
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("one segment after the merge", 1, reader.leaves().size());
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (String term : List.of("alpha", "beta", "gamma", "tail-5", "absent")) {
                    final int expected = (int) values.stream().filter(term::equals).count();
                    assertEquals(term, expected, searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef(term))));
                }
                assertEquals(
                    "prefix",
                    (int) values.stream().filter(v -> v.startsWith("tail-")).count(),
                    searcher.count(ColumnarStringTermQuery.prefix(FIELD, new BytesRef("tail-")))
                );
            }
        }
    }

    /** Values in term order, written as several segments and merged, which is what an index sort gives. */
    public void testSortedAcrossMergedSegments() throws IOException {
        final String[] terms = { "alpha", "beta", "gamma", "delta" };
        java.util.Arrays.sort(terms);
        final List<String> values = new ArrayList<>();
        for (String term : terms) {
            for (int i = 0; i < 3000; i++) {
                values.add(term);
            }
        }
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int i = 0; i < values.size(); i++) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(values.get(i)), type));
                    writer.addDocument(doc);
                    if (i % 2000 == 1999) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (String term : terms) {
                    assertEquals(term, 3000, searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef(term))));
                }
            }
        }
    }

    /**
     * Segments that stayed plain because no one of them was covered enough, merging into a column that is.
     * Their summaries combine into the merged vocabulary, so the values are not surveyed a second time, and
     * the terms the merged column holds most must be the ones it ends up with.
     */
    public void testPlainSegmentsMergeIntoADictionary() throws IOException {
        // Each segment holds a few common values and a long tail of its own, so alone none is covered
        // enough for a dictionary; together the common values account for most of the merged column.
        final List<String> values = new ArrayList<>();
        for (int segment = 0; segment < 6; segment++) {
            for (int i = 0; i < 2000; i++) {
                values.add(i % 2 == 0 ? randomFrom("hot-a", "hot-b", "hot-c") : "tail-" + segment + "-" + i);
            }
        }
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int i = 0; i < values.size(); i++) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(values.get(i)), type));
                    writer.addDocument(doc);
                    if (i % 2000 == 1999) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (String term : List.of("hot-a", "hot-b", "hot-c", "tail-0-1", "tail-5-1999", "absent")) {
                    final int expected = (int) values.stream().filter(term::equals).count();
                    assertEquals("term [" + term + "]", expected, searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef(term))));
                }
            }
        }
    }

    private static StringColumnReader column(DirectoryReader reader) throws IOException {
        assertEquals("one segment", 1, reader.leaves().size());
        final BinaryDocValues binary = reader.leaves().get(0).reader().getBinaryDocValues(FIELD);
        return ((ColumnarStringBinaryDocValues) binary).reader();
    }

    /** Values indexed in term order stay recognisably in term order through the codec and a merge. */
    public void testSortedThroughTheCodec() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int term = 0; term < 5000; term++) {
            final String value = "pod-" + String.format(java.util.Locale.ROOT, "%06d", term);
            for (int i = 0; i < 20; i++) {
                values.add(value);
            }
        }
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            // Adjacent segments only, which is what an index sorted by this field guarantees: the tiered
            // policy merges whichever segments it likes and leaves the documents in an order the values no
            // longer follow.
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec());
            iwc.setMergePolicy(new LogByteSizeMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < values.size(); i++) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(values.get(i)), type));
                    writer.addDocument(doc);
                    if (i % 25_000 == 24_999) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertTrue("a column written in term order is sorted", column(reader).valuesSorted());
            }
        }
    }

    private void assertQueries(List<String> values, List<String> terms, List<String> prefixes) throws IOException {
        try (Directory dir = newDirectory()) {
            index(dir, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                // The searcher caches, and a cached result would not exercise the column a second time.
                searcher.setQueryCache(null);
                for (String term : terms) {
                    final int expected = (int) values.stream().filter(v -> v != null && v.equals(term)).count();
                    assertEquals("term [" + term + "]", expected, searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef(term))));
                }
                for (String prefix : prefixes) {
                    final int expected = (int) values.stream().filter(v -> v != null && v.startsWith(prefix)).count();
                    assertEquals(
                        "prefix [" + prefix + "]",
                        expected,
                        searcher.count(ColumnarStringTermQuery.prefix(FIELD, new BytesRef(prefix)))
                    );
                }
            }
        }
    }

    private void index(Directory dir, List<String> values) throws IOException {
        final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec());
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (String value : values) {
                final Document doc = new Document();
                if (value != null) {
                    doc.add(new Field(FIELD, new BytesRef(value), type));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }
}
