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
import org.apache.lucene.index.LeafReaderContext;
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
     * Several fully-covered segments merged: the merged column must hold every term its sources did, and
     * answer for all of them. Their union is taken instead of the values being surveyed again, so a term
     * that only one segment held has to survive.
     */
    public void testMergedDictionariesKeepEveryTerm() throws IOException {
        final List<String> values = new ArrayList<>();
        final List<String> perSegment = List.of("alpha", "beta", "gamma", "delta", "epsilon", "zeta");
        for (int segment = 0; segment < perSegment.size(); segment++) {
            for (int i = 0; i < 1000; i++) {
                // Each segment holds one term the others do not, plus one they share.
                values.add(i % 3 == 0 ? "shared" : perSegment.get(segment));
            }
        }
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int i = 0; i < values.size(); i++) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, new BytesRef(values.get(i)), type));
                    writer.addDocument(doc);
                    if (i % 1000 == 999) {
                        writer.flush();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("one segment after the merge", 1, reader.leaves().size());
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (String term : perSegment) {
                    final int expected = (int) values.stream().filter(term::equals).count();
                    assertEquals("term [" + term + "]", expected, searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef(term))));
                }
                assertEquals(
                    "shared",
                    (int) values.stream().filter("shared"::equals).count(),
                    searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef("shared")))
                );
                // Seven terms standing for six thousand values: the union of the inputs' dictionaries
                // covers the merged column entirely and costs almost nothing to keep.
                assertTrue("a union that holds every value is worth keeping", column(reader).hasDictionary());
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

    /**
     * A column that stays plain through a merge keeps a summary of what it holds, and the counts in it are
     * the ones its inputs really saw. Without them the merged column would read back as though it held
     * every term once, and the merge after this one would never find a dictionary worth building.
     */
    public void testSummarySurvivesAPlainMerge() throws IOException {
        final List<String> hot = List.of("hot-a", "hot-b", "hot-c");
        final List<String> values = new ArrayList<>();
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                // Too thinly covered for a dictionary: one value in five is one of the hot terms, and the
                // rest are distinct and long enough that no affordable dictionary covers many of them.
                for (int segment = 0; segment < 3; segment++) {
                    for (int i = 0; i < 3000; i++) {
                        final String value = i % 5 == 0 ? randomFrom(hot) : "tail-" + segment + "-" + i + "-" + "x".repeat(60);
                        values.add(value);
                        final Document doc = new Document();
                        doc.add(new Field(FIELD, new BytesRef(value), type));
                        writer.addDocument(doc);
                    }
                    writer.flush();
                }
                writer.forceMerge(1);

                try (DirectoryReader merged = DirectoryReader.open(writer)) {
                    final StringColumnReader column = column(merged);
                    assertFalse("still too thinly covered for a dictionary", column.hasDictionary());
                    assertTrue("a plain column keeps a summary of what it holds", column.hasSummary());

                    final List<BytesRef> terms = new ArrayList<>();
                    final List<Long> counts = new ArrayList<>();
                    column.readSummary(terms, counts);
                    for (String term : hot) {
                        final int at = terms.indexOf(new BytesRef(term));
                        assertTrue("the summary kept [" + term + "]", at >= 0);
                        // The counts are lower bounds — a term is charged an occurrence whenever room has
                        // to be made — so they may fall short of the truth, but not by an order of it.
                        final long actual = values.stream().filter(term::equals).count();
                        assertTrue(
                            "[" + term + "] counted " + counts.get(at) + " of " + actual,
                            counts.get(at) > actual / 2 && counts.get(at) <= actual
                        );
                    }
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (String term : List.of("hot-a", "hot-b", "hot-c", "absent")) {
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

    /**
     * A segment the field never appeared in, merged with segments that hold a dictionary. It has nothing to
     * contribute and must not cost the merged column its dictionary.
     */
    public void testSegmentWithoutTheFieldDoesNotDecideTheShape() throws IOException {
        final List<String> values = new ArrayList<>();
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int segment = 0; segment < 3; segment++) {
                    for (int i = 0; i < 1000; i++) {
                        final String value = randomFrom("alpha", "beta", "gamma");
                        values.add(value);
                        final Document doc = new Document();
                        doc.add(new Field(FIELD, new BytesRef(value), type));
                        writer.addDocument(doc);
                    }
                    writer.flush();
                }
                // A segment of documents that carry another field entirely.
                for (int i = 0; i < 1000; i++) {
                    final Document doc = new Document();
                    doc.add(new Field("other", new BytesRef("value"), type));
                    writer.addDocument(doc);
                }
                writer.flush();
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertTrue("the empty segment did not cost the dictionary", column(reader).hasDictionary());
                final IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                for (String term : List.of("alpha", "beta", "gamma")) {
                    final int expected = (int) values.stream().filter(term::equals).count();
                    assertEquals("term [" + term + "]", expected, searcher.count(ColumnarStringTermQuery.term(FIELD, new BytesRef(term))));
                }
            }
        }
    }

    /** The same values, merged: a decision the merge makes differently from the flush is a bug in one of them. */
    public void testHighCardinalityStaysPlainThroughAMerge() throws IOException {
        try (Directory dir = newDirectory()) {
            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int segment = 0; segment < 3; segment++) {
                    for (int i = 0; i < 100_000; i++) {
                        final Document doc = new Document();
                        final String value = "checkout-7d9f8b6c4-" + Integer.toString(random().nextInt(50_000), 36) + "xk";
                        doc.add(new Field(FIELD, new BytesRef(value), type));
                        writer.addDocument(doc);
                    }
                    writer.flush();
                    try (DirectoryReader flushed = DirectoryReader.open(writer)) {
                        for (LeafReaderContext leaf : flushed.leaves()) {
                            final BinaryDocValues binary = leaf.reader().getBinaryDocValues(FIELD);
                            final StringColumnReader flushedColumn = ((ColumnarStringBinaryDocValues) binary).reader();
                            assertFalse("a flushed segment stays plain", flushedColumn.hasDictionary());
                            assertTrue("and keeps a summary, so the merge need not survey again", flushedColumn.hasSummary());
                        }
                    }
                }
                writer.forceMerge(1);
                try (DirectoryReader merged = DirectoryReader.open(writer)) {
                    assertFalse("and so does the merge of them", column(merged).hasDictionary());
                }
            }
        }
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
