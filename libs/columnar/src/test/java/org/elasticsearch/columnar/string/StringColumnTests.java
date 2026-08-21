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
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Round-trips a string column through a {@link Directory} in both shapes — plain, and dictionary with an
 * exception stream — checking per-value reads and both forms a block can be served in against the values
 * that were written.
 */
public class StringColumnTests extends ESTestCase {

    public void testEmptyColumn() throws IOException {
        assertColumn(List.of(), randomBoolean());
    }

    public void testAllValuesEmpty() throws IOException {
        assertColumn(nCopies(between(1, 500), ""), randomBoolean());
    }

    public void testSingleValue() throws IOException {
        assertColumn(List.of("only"), randomBoolean());
    }

    /** Few distinct values: every row hits the dictionary and nothing escapes. */
    public void testFullyCovered() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            values.add(randomFrom("INFO", "DEBUG", "WARN", "ERROR"));
        }
        assertColumn(values, true);
        assertColumn(values, false);
    }

    /** Every value distinct: the dictionary covers almost nothing and nearly every row escapes. */
    public void testAllDistinct() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            values.add("id-" + i + "-" + randomAlphaOfLength(8));
        }
        assertColumn(values, true);
        assertColumn(values, false);
    }

    /** Hot values plus a long tail, so a block mixes dictionary hits with escapes. */
    public void testHotValuesWithTail() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 8000; i++) {
            values.add(random().nextDouble() < 0.8 ? randomFrom("success", "timeout", "refused") : "err-" + i + "-detail");
        }
        assertColumn(values, true);
        assertColumn(values, false);
    }

    /** Values long enough that a run spans several chunks, and short ones alongside them. */
    public void testMixedValueLengths() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            values.add(randomBoolean() ? randomAlphaOfLength(between(0, 4)) : randomAlphaOfLength(between(500, 3000)));
        }
        assertColumn(values, true);
    }

    /** Documents without a value: a block's documents map to ranks before anything is resolved. */
    public void testSparse() throws IOException {
        for (double density : new double[] { 0.05, 0.5, 0.95 }) {
            final List<String> present = new ArrayList<>();
            final List<Integer> docs = new ArrayList<>();
            final int maxDoc = between(200, 4000);
            for (int doc = 0; doc < maxDoc; doc++) {
                if (random().nextDouble() < density) {
                    docs.add(doc);
                    present.add(randomFrom("a", "b", "c", "d-" + doc));
                }
            }
            if (present.isEmpty()) {
                continue;
            }
            try (Directory dir = newDirectory()) {
                final StringColumnMetadata written = writeSparse(dir, docs, present, maxDoc);
                try (IndexInput data = dir.openInput("str.cnd", IOContext.DEFAULT)) {
                    ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
                    final StringColumnReader reader = new StringColumnReader(readMeta(dir, maxDoc), data);
                    final int[] block = new int[docs.size()];
                    for (int i = 0; i < block.length; i++) {
                        block[i] = docs.get(i);
                    }
                    final List<String> seen = new ArrayList<>();
                    reader.readBlock(block, 0, block.length, collector(seen));
                    assertEquals("density " + density, present, seen);
                }
            }
        }
    }

    /** A column whose values arrive in term order records that, which is what lets a term be bisected for. */
    public void testSortedValuesAreRecorded() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int term = 0; term < 2000; term++) {
            final String value = "pod-" + String.format(java.util.Locale.ROOT, "%05d", term);
            for (int i = 0; i < 20; i++) {
                values.add(value);
            }
        }
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata plain = writeColumn(dir, values.size(), values.size(), values.size(), () -> cursor(values));
            assertTrue("values in term order are sorted", plain.valuesSorted());
        }
    }

    /** A single-valued column stores no value counts and no address table: its rank is its value index. */
    public void testSingleValuedPaysNoAddressCost() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            values.add(randomFrom("a", "b", "c"));
        }
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata written = write(dir, values);
            assertFalse(written.multiValued());
            assertNull("a single-valued column writes no value counts", written.valueCounts());
            assertEquals("and no address table", 0, written.docBlockBases().dataLength());
            final long single = dir.fileLength("str.cnd");

            // The same values, one document holding two of them: only then is either structure written.
            final List<List<String>> perDoc = new ArrayList<>();
            for (String value : values) {
                perDoc.add(List.of(value));
            }
            perDoc.get(0).size();
            perDoc.set(0, List.of(values.get(0), values.get(1)));
            try (Directory other = newDirectory()) {
                final StringColumnMetadata multi = writeMulti(other, perDoc);
                assertTrue(multi.multiValued());
                assertNotNull(multi.valueCounts());
                assertTrue("the address table costs bytes only when it is written", other.fileLength("str.cnd") > single);
            }
        }
    }

    /** A multi-valued column declines the block form, and its documents still resolve to their values. */
    public void testMultiValued() throws IOException {
        final List<List<String>> perDoc = new ArrayList<>();
        for (int doc = 0; doc < 500; doc++) {
            final List<String> values = new ArrayList<>();
            for (int i = 0, count = between(1, 4); i < count; i++) {
                values.add(randomFrom("x", "y", "z-" + doc));
            }
            perDoc.add(values);
        }
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata written = writeMulti(dir, perDoc);
            assertTrue("the column must know it is multi-valued", written.multiValued());
            try (IndexInput data = dir.openInput("str.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
                final StringColumnReader reader = new StringColumnReader(readMeta(dir, perDoc.size()), data);
                assertFalse(
                    "a multi-valued column has no block form yet",
                    reader.readBlock(new int[] { 0 }, 0, 1, collector(new ArrayList<>()))
                );
                final BytesRef scratch = new BytesRef();
                for (int rank = 0; rank < perDoc.size(); rank++) {
                    final List<String> expected = perDoc.get(rank);
                    assertEquals("value count at " + rank, expected.size(), reader.valueCount(rank));
                    final long first = reader.firstValue(rank);
                    for (int i = 0; i < expected.size(); i++) {
                        assertEquals("doc " + rank + " value " + i, bytes(expected.get(i)), reader.valueAt(first + i, scratch));
                    }
                }
            }
        }
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

    private StringColumnMetadata writeSparse(Directory dir, List<Integer> docs, List<String> values, int maxDoc) throws IOException {
        return writeColumn(dir, maxDoc, docs.size(), values.size(), () -> sparseCursor(docs, values));
    }

    private StringColumnMetadata writeMulti(Directory dir, List<List<String>> perDoc) throws IOException {
        int numValues = 0;
        for (List<String> values : perDoc) {
            numValues += values.size();
        }
        return writeColumn(dir, perDoc.size(), perDoc.size(), numValues, () -> multiCursor(perDoc));
    }

    private static StringColumnValues sparseCursor(List<Integer> docs, List<String> values) {
        final List<List<String>> perDoc = new ArrayList<>();
        for (String value : values) {
            perDoc.add(List.of(value));
        }
        return positioned(docs, perDoc);
    }

    private static StringColumnValues multiCursor(List<List<String>> perDoc) {
        final List<Integer> docs = new ArrayList<>();
        for (int i = 0; i < perDoc.size(); i++) {
            docs.add(i);
        }
        return positioned(docs, perDoc);
    }

    private static StringColumnValues positioned(List<Integer> docs, List<List<String>> perDoc) {
        return new StringColumnValues() {
            private int rank = -1;
            private int valueIndex;

            @Override
            public int valueCount() {
                return perDoc.get(rank).size();
            }

            @Override
            public BytesRef nextValue() {
                return bytes(perDoc.get(rank).get(valueIndex++));
            }

            @Override
            public int docID() {
                return rank < 0 ? -1 : (rank < docs.size() ? docs.get(rank) : DocIdSetIterator.NO_MORE_DOCS);
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
                return docs.size();
            }
        };
    }

    public void testRandom() throws IOException {
        for (int iter = 0; iter < 10; iter++) {
            final int distinct = between(1, 200);
            final List<String> values = new ArrayList<>();
            for (int i = 0; i < between(1, 4000); i++) {
                values.add("v-" + between(0, distinct - 1));
            }
            assertColumn(values, randomBoolean());
        }
    }

    private static List<String> nCopies(int count, String value) {
        final List<String> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(value);
        }
        return values;
    }

    private void assertColumn(List<String> values, boolean allowDictionary) throws IOException {
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata written = write(dir, values);
            try (IndexInput data = dir.openInput("str.cnd", IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(data);
                ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
                final StringColumnMetadata read = readMeta(dir, values.size());
                final StringColumnReader reader = new StringColumnReader(read, data);

                final BytesRef scratch = new BytesRef();
                for (int i = 0; i < values.size(); i++) {
                    assertEquals("value " + i, bytes(values.get(i)), reader.valueAt(i, scratch));
                }
                // Out of order, so a lookup re-enters blocks and chunks it has already left.
                for (int probe = 0; probe < Math.min(200, values.size()); probe++) {
                    final int i = between(0, values.size() - 1);
                    assertEquals("random value " + i, bytes(values.get(i)), reader.valueAt(i, scratch));
                }
                assertBlocks(reader, values);
            }
        }
    }

    /** Both block forms must yield the values that were written, whichever the column picks. */
    private void assertBlocks(StringColumnReader reader, List<String> values) throws IOException {
        for (int blockSize : new int[] { 1, 7, 128, 1024 }) {
            final List<String> seen = new ArrayList<>();
            final int[] indexes = new int[Math.min(blockSize, Math.max(1, values.size()))];
            for (int start = 0; start < values.size(); start += indexes.length) {
                final int count = Math.min(indexes.length, values.size() - start);
                for (int i = 0; i < count; i++) {
                    indexes[i] = start + i;
                }
                final boolean served = reader.readBlock(indexes, 0, count, new StringBlockSink() {
                    @Override
                    public void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize) {
                        for (int i = 0; i < count; i++) {
                            final int ordinal = ordinals[i];
                            assertTrue("ordinal in range", ordinal >= 0 && ordinal < dictionarySize);
                            seen.add(dictionary[ordinal].utf8ToString());
                        }
                        // Equal values must share an ordinal, or a consumer grouping on them would split them.
                        for (int a = 0; a < dictionarySize; a++) {
                            for (int b = a + 1; b < dictionarySize; b++) {
                                assertFalse("duplicate dictionary entry", dictionary[a].bytesEquals(dictionary[b]));
                            }
                        }
                    }

                    @Override
                    public void appendValues(BytesRef[] block, int count) {
                        for (int i = 0; i < count; i++) {
                            seen.add(block[i].utf8ToString());
                        }
                    }
                });
                assertTrue("a single-valued column must serve a block", served);
            }
            assertEquals("block size " + blockSize, values, seen);
        }
    }

    private byte[] segmentId;
    // One codec for every column a test writes: a test that compares two columns' sizes would otherwise be
    // comparing the codecs as much as the columns.
    private ChunkCodec chunkCodec;

    private ChunkCodec chunkCodec() {
        if (chunkCodec == null) {
            chunkCodec = randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
        }
        return chunkCodec;
    }

    private StringColumnMetadata write(Directory dir, List<String> values) throws IOException {
        return writeColumn(dir, Math.max(1, values.size()), values.size(), values.size(), () -> cursor(values));
    }

    private StringColumnMetadata writeColumn(Directory dir, int maxDoc, int numDocs, int numValues, IOSupplier<StringColumnValues> cursors)
        throws IOException {
        segmentId = new byte[16];
        random().nextBytes(segmentId);
        final StringColumnMetadata written;
        try (IndexOutput out = dir.createOutput("str.cnd", IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(out, "ColumNARData", FormatVersion.CURRENT, segmentId, "");
            written = StringColumnWriter.write(
                maxDoc,
                numDocs,
                numValues,
                cursors,
                chunkCodec(),
                64 * 1024,
                ValueStream.VALUES_PER_BLOCK,
                dir,
                IOContext.DEFAULT,
                out
            );
            ColumnarCodecUtil.writeFooter(out);
        }
        try (IndexOutput meta = dir.createOutput("str.cnm", IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(meta, "ColumNARMeta", FormatVersion.CURRENT, segmentId, "");
            written.writeTo(meta);
            ColumnarCodecUtil.writeFooter(meta);
        }
        return written;
    }

    private StringColumnMetadata readMeta(Directory dir, int maxDoc) throws IOException {
        try (ChecksumIndexInput meta = dir.openChecksumInput("str.cnm")) {
            final FormatVersion version = ColumnarCodecUtil.checkHeader(meta, "ColumNARMeta", segmentId, "");
            final StringColumnMetadata read = StringColumnMetadata.readFrom(meta, Math.max(1, maxDoc), version);
            ColumnarCodecUtil.checkFooter(meta);
            return read;
        }
    }

    private static BytesRef bytes(String value) {
        return new BytesRef(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertEquals(String message, BytesRef expected, BytesRef actual) {
        if (expected.bytesEquals(actual) == false) {
            throw new AssertionError(message + ": expected " + expected.utf8ToString() + " but was " + actual.utf8ToString());
        }
    }

    private static StringColumnValues cursor(List<String> values) {
        final BytesRef[] refs = new BytesRef[values.size()];
        for (int i = 0; i < refs.length; i++) {
            refs[i] = bytes(values.get(i));
        }
        return new StringColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public BytesRef nextValue() {
                return refs[doc];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return doc = (++doc < refs.length ? doc : DocIdSetIterator.NO_MORE_DOCS);
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return refs.length;
            }
        };
    }
}
