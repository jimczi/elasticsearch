/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Drives a string column over the whole space of shapes it can take — plain and dictionary, dense and
 * sparse, single- and multi-valued, both length layouts, both chunk codecs — and checks every way of
 * reading it against the values that were written.
 *
 * <p>Each shape is generated at random and checked exhaustively rather than at a few points, because the
 * combinations interact: a document's values are found through its rank, its rank through the presence
 * layer, its ordinal through the dictionary, and an escaped value through a count of the escapes before it.
 * A fault in any one of those only shows up in some combinations.
 */
public class StringColumnRandomizedTests extends ESTestCase {

    public void testRandomShapes() throws IOException {
        for (int iteration = 0; iteration < 30; iteration++) {
            final Shape shape = randomShape();
            assertColumn(shape);
        }
    }

    /** Values long enough to fill several chunks, so a block, a run and a value all cross chunk bounds. */
    public void testValuesSpanningChunks() throws IOException {
        final List<List<String>> perDoc = new ArrayList<>();
        for (int doc = 0; doc < 300; doc++) {
            perDoc.add(List.of(randomAlphaOfLength(between(1, 6000))));
        }
        assertColumn(new Shape(perDoc, allDocs(perDoc.size()), perDoc.size(), 4096, 128, anyPolicy()));
    }

    /** A single value larger than the chunk target, which a chunk has to hold whole. */
    public void testValueLargerThanChunk() throws IOException {
        final List<List<String>> perDoc = new ArrayList<>();
        for (int doc = 0; doc < 40; doc++) {
            perDoc.add(List.of(randomAlphaOfLength(between(2000, 9000))));
        }
        assertColumn(new Shape(perDoc, allDocs(perDoc.size()), perDoc.size(), 1024, 8, anyPolicy()));
    }

    /** Short values, which take the inline length layout, mixed with long ones, which do not. */
    public void testBothLengthLayouts() throws IOException {
        for (int meanLength : new int[] { 2, 8, 40, 200 }) {
            final List<List<String>> perDoc = new ArrayList<>();
            for (int doc = 0; doc < 2000; doc++) {
                perDoc.add(List.of(randomAlphaOfLength(between(0, 2 * meanLength))));
            }
            assertColumn(new Shape(perDoc, allDocs(perDoc.size()), perDoc.size(), 64 * 1024, 128, anyPolicy()));
        }
    }

    /** Escapes landing on the boundaries of the blocks their count is carried in. */
    public void testEscapesOnBlockBoundaries() throws IOException {
        final List<List<String>> perDoc = new ArrayList<>();
        for (int doc = 0; doc < 4000; doc++) {
            // Every 128th document escapes, so an escape sits at the head of each rank block.
            perDoc.add(List.of(doc % 128 == 0 ? "escaped-" + doc : "common-" + (doc % 4)));
        }
        assertColumn(
            new Shape(perDoc, allDocs(perDoc.size()), perDoc.size(), 64 * 1024, 128, new DictionaryPolicy(1 << 20, 0.0, Double.MAX_VALUE))
        );
    }

    /** Every document escaping, and none, which are the ends of the range the escape count covers. */
    public void testAllAndNoneEscaped() throws IOException {
        final List<List<String>> allEscape = new ArrayList<>();
        final List<List<String>> noneEscape = new ArrayList<>();
        for (int doc = 0; doc < 1500; doc++) {
            allEscape.add(List.of("unique-" + doc));
            noneEscape.add(List.of("v" + (doc % 4)));
        }
        assertColumn(new Shape(allEscape, allDocs(1500), 1500, 64 * 1024, 128, new DictionaryPolicy(1 << 20, 0.0, Double.MAX_VALUE)));
        assertColumn(new Shape(noneEscape, allDocs(1500), 1500, 64 * 1024, 128, anyPolicy()));
    }

    private record Shape(
        List<List<String>> perDoc,
        List<Integer> docs,
        int maxDoc,
        int chunkBytes,
        int valuesPerBlock,
        DictionaryPolicy policy
    ) {}

    private static DictionaryPolicy anyPolicy() {
        return new DictionaryPolicy(1 << 20, 0.0, Double.MAX_VALUE);
    }

    private static List<Integer> allDocs(int count) {
        final List<Integer> docs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            docs.add(i);
        }
        return docs;
    }

    private Shape randomShape() {
        final int maxDoc = between(1, 5000);
        final double density = randomFrom(0.02, 0.3, 0.8, 1.0);
        final int distinct = randomFrom(1, 3, 40, 900, Integer.MAX_VALUE);
        final boolean multiValued = randomBoolean();
        final List<Integer> docs = new ArrayList<>();
        final List<List<String>> perDoc = new ArrayList<>();
        for (int doc = 0; doc < maxDoc; doc++) {
            if (random().nextDouble() >= density) {
                continue;
            }
            docs.add(doc);
            final int count = multiValued ? between(1, 5) : 1;
            final List<String> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(
                    distinct == Integer.MAX_VALUE
                        ? "u-" + doc + "-" + i + randomAlphaOfLength(between(0, 30))
                        : "v-" + between(0, distinct - 1)
                );
            }
            perDoc.add(values);
        }
        return new Shape(
            perDoc,
            docs,
            maxDoc,
            randomFrom(512, 4096, 64 * 1024),
            randomFrom(8, 32, 128, 512),
            randomBoolean() ? anyPolicy() : DictionaryPolicy.NONE
        );
    }

    private void assertColumn(Shape shape) throws IOException {
        final String label = "docs="
            + shape.docs.size()
            + " maxDoc="
            + shape.maxDoc
            + " chunk="
            + shape.chunkBytes
            + " perBlock="
            + shape.valuesPerBlock
            + " dictionary="
            + shape.policy.enabled();
        int numValues = 0;
        for (List<String> values : shape.perDoc) {
            numValues += values.size();
        }
        if (shape.docs.isEmpty()) {
            return;
        }

        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata written;
            try (IndexOutput out = dir.createOutput("s.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(out, "ColumNARData", FormatVersion.CURRENT, segmentId, "");
                written = StringColumnWriter.write(
                    shape.maxDoc,
                    shape.docs.size(),
                    numValues,
                    cursors(shape),
                    randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD),
                    shape.chunkBytes,
                    shape.valuesPerBlock,
                    shape.policy,
                    null,
                    dir,
                    IOContext.DEFAULT,
                    out
                );
                ColumnarCodecUtil.writeFooter(out);
            }
            try (IndexOutput meta = dir.createOutput("s.cnm", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(meta, "ColumNARMeta", FormatVersion.CURRENT, segmentId, "");
                written.writeTo(meta);
                ColumnarCodecUtil.writeFooter(meta);
            }

            final StringColumnMetadata read;
            try (ChecksumIndexInput meta = dir.openChecksumInput("s.cnm")) {
                final FormatVersion version = ColumnarCodecUtil.checkHeader(meta, "ColumNARMeta", segmentId, "");
                read = StringColumnMetadata.readFrom(meta, shape.maxDoc, version);
                ColumnarCodecUtil.checkFooter(meta);
            }

            try (IndexInput data = dir.openInput("s.cnd", IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(data);
                ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
                final StringColumnReader reader = new StringColumnReader(read, data);

                assertPresence(label, reader, shape);
                assertValuesPerDocument(label, reader, shape);
                assertBlocks(label, reader, shape);
                assertLookups(label, reader, shape);
                assertMatches(label, reader, shape);
            }
        }
    }

    /** The presence layer must name exactly the documents that were written, in order. */
    private void assertPresence(String label, StringColumnReader reader, Shape shape) throws IOException {
        final ColumnIterator iterator = reader.iterator();
        for (int rank = 0; rank < shape.docs.size(); rank++) {
            assertEquals(label + " doc at rank " + rank, (int) shape.docs.get(rank), iterator.nextDoc());
            assertEquals(label + " rank", rank, iterator.index());
        }
        assertEquals(label + " exhausted", DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    /** Every document's values, found through its rank, in the order they were written. */
    private void assertValuesPerDocument(String label, StringColumnReader reader, Shape shape) throws IOException {
        final BytesRef scratch = new BytesRef();
        for (int rank = 0; rank < shape.perDoc.size(); rank++) {
            final List<String> expected = shape.perDoc.get(rank);
            assertEquals(label + " value count at rank " + rank, expected.size(), reader.valueCount(rank));
            final long first = reader.firstValue(rank);
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(label + " rank " + rank + " value " + i, expected.get(i), reader.valueAt(first + i, scratch).utf8ToString());
            }
        }
        // Backwards, so a lookup re-enters blocks and chunks it has already left.
        for (int rank = shape.perDoc.size() - 1; rank >= 0; rank--) {
            final long first = reader.firstValue(rank);
            assertEquals(label + " backwards rank " + rank, shape.perDoc.get(rank).get(0), reader.valueAt(first, scratch).utf8ToString());
        }
    }

    /** Both block forms, at several page sizes, must yield the values that were written. */
    private void assertBlocks(String label, StringColumnReader reader, Shape shape) throws IOException {
        if (reader.multiValued()) {
            // A multi-valued column has no block form; the per-document reads above cover it.
            return;
        }
        final int[] docs = new int[shape.docs.size()];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = shape.docs.get(i);
        }
        for (int page : new int[] { 1, 3, 128, 1024, 16384 }) {
            final List<String> seen = new ArrayList<>();
            for (int start = 0; start < docs.length; start += page) {
                final int count = Math.min(page, docs.length - start);
                assertTrue(label + " block served", reader.readBlock(docs, start, count, collector(seen)));
            }
            final List<String> expected = new ArrayList<>();
            for (List<String> values : shape.perDoc) {
                expected.add(values.get(0));
            }
            assertEquals(label + " page " + page, expected, seen);
        }
    }

    /** A column whose values arrive in term order, which is what an index sorted by the field produces. */
    public void testSortedColumn() throws IOException {
        final String[] vocabulary = { "alpha", "beta", "gamma", "delta", "epsilon" };
        final String[] sorted = vocabulary.clone();
        java.util.Arrays.sort(sorted);
        final List<List<String>> perDoc = new ArrayList<>();
        for (String term : sorted) {
            for (int i = 0, run = between(1, 400); i < run; i++) {
                perDoc.add(List.of(term));
            }
        }
        // Both shapes: with a dictionary the ordinals are ordered, without one the values are.
        assertColumn(new Shape(perDoc, allDocs(perDoc.size()), perDoc.size(), 64 * 1024, 128, anyPolicy()));
        assertColumn(new Shape(perDoc, allDocs(perDoc.size()), perDoc.size(), 64 * 1024, 128, DictionaryPolicy.NONE));
    }

    /** Values in term order with documents missing between them: both properties at once. */
    public void testSortedAndSparse() throws IOException {
        final String[] sorted = { "a", "b", "c", "d" };
        final List<List<String>> perDoc = new ArrayList<>();
        final List<Integer> docs = new ArrayList<>();
        int doc = 0;
        for (String term : sorted) {
            for (int i = 0, run = between(1, 300); i < run; i++) {
                doc += between(1, 4);
                docs.add(doc);
                perDoc.add(List.of(term));
            }
        }
        assertColumn(new Shape(perDoc, docs, doc + 1, 64 * 1024, 128, anyPolicy()));
        assertColumn(new Shape(perDoc, docs, doc + 1, 64 * 1024, 128, DictionaryPolicy.NONE));
    }

    /** Every dictionary term is found, and every prefix resolves to the run of ordinals that carries it. */
    private void assertLookups(String label, StringColumnReader reader, Shape shape) throws IOException {
        if (reader.hasDictionary() == false) {
            assertEquals(label + " a plain column knows no terms", -1, reader.lookupTerm(new BytesRef("anything")));
            return;
        }
        final BytesRef scratch = new BytesRef();
        String previous = null;
        for (int ordinal = 0; ordinal < reader.dictionarySize(); ordinal++) {
            // The dictionary is in term order, which is what makes a binary search and a prefix range work.
            final String term = reader.termAt(ordinal, scratch).utf8ToString();
            if (previous != null) {
                assertTrue(label + " dictionary sorted at " + ordinal, previous.compareTo(term) < 0);
            }
            previous = term;
            assertEquals(label + " term " + ordinal, ordinal, reader.lookupTerm(new BytesRef(term)));

            final int[] range = reader.lookupPrefix(new BytesRef(term));
            assertTrue(label + " a term is its own prefix", range[0] <= ordinal && ordinal < range[1]);
        }
        assertTrue(label + " absent term", reader.lookupTerm(new BytesRef("￿ absent")) < 0);
        final int[] everything = reader.lookupPrefix(new BytesRef(""));
        assertEquals(label + " the empty prefix covers the dictionary", 0, everything[0]);
        assertEquals(label + " the empty prefix covers the dictionary", reader.dictionarySize(), everything[1]);
    }

    /**
     * Every term and prefix must match exactly the documents that carry it — including terms the dictionary
     * does not hold, which only the exception stream can answer, and prefixes that both a dictionary term
     * and an escaped value carry.
     */
    private void assertMatches(String label, StringColumnReader reader, Shape shape) throws IOException {
        // A document matches when any of its values does.
        final List<List<String>> perDoc = shape.perDoc;
        final List<String> flat = new ArrayList<>();
        for (List<String> values : perDoc) {
            flat.add(values.get(0));
        }
        final List<String> distinct = new ArrayList<>(new java.util.TreeSet<>(flat));
        for (String term : distinct) {
            assertMatch(label + " term " + term, reader.matchTerm(new BytesRef(term)), shape, v -> v.equals(term));
        }
        for (String absent : new String[] { "", "zzz-absent", "v-", "u-" }) {
            assertMatch(label + " absent " + absent, reader.matchTerm(new BytesRef(absent)), shape, v -> v.equals(absent));
        }
        for (String prefix : new String[] { "", "v", "v-1", "u-", "zzz" }) {
            assertMatch(label + " prefix " + prefix, reader.matchPrefix(new BytesRef(prefix)), shape, v -> v.startsWith(prefix));
        }
    }

    private static void assertMatch(String label, DocIdSetIterator matches, Shape shape, java.util.function.Predicate<String> expected)
        throws IOException {
        assertNotNull(label + ": every column shape answers a match", matches);
        final List<Integer> actual = new ArrayList<>();
        for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
            actual.add(doc);
        }
        // The answer names documents, so a sparse column's matches are document ids and not ranks, and a
        // document with several values matches when any one of them does.
        final List<Integer> wanted = new ArrayList<>();
        for (int rank = 0; rank < shape.perDoc().size(); rank++) {
            for (String value : shape.perDoc().get(rank)) {
                if (expected.test(value)) {
                    wanted.add(shape.docs().get(rank));
                    break;
                }
            }
        }
        assertEquals(label, wanted, actual);
    }

    private static StringBlockSink collector(List<String> seen) {
        return new StringBlockSink() {
            @Override
            public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
                for (int i = 0; i < count; i++) {
                    seen.add(dictionary[ordinals[i]].utf8ToString());
                }
            }

            @Override
            public void appendValues(BytesRef[] values, int count) {
                for (int i = 0; i < count; i++) {
                    seen.add(values[i].utf8ToString());
                }
            }
        };
    }

    private static IOSupplier<StringColumnValues> cursors(Shape shape) {
        return () -> new StringColumnValues() {
            private int rank = -1;
            private int valueIndex;

            @Override
            public int valueCount() {
                return shape.perDoc.get(rank).size();
            }

            @Override
            public BytesRef nextValue() {
                return new BytesRef(shape.perDoc.get(rank).get(valueIndex++).getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public int docID() {
                return rank < 0 ? -1 : (rank < shape.docs.size() ? shape.docs.get(rank) : DocIdSetIterator.NO_MORE_DOCS);
            }

            @Override
            public int nextDoc() {
                rank++;
                valueIndex = 0;
                return docID();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return shape.docs.size();
            }
        };
    }
}
