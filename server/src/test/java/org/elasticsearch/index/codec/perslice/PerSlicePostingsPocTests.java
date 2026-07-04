/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perslice;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Feasibility POC for the hard case the doc-values trick could NOT cover: <b>postings</b>.
 * <p>
 * Postings bake per-term statistics ({@code docFreq}/{@code totalTermFreq}) and per-field
 * ({@code sumDocFreq}/{@code docCount}) into the term dictionary, and terms with no docs in a slice must
 * be <em>dropped</em>. A "filter the PostingsEnum by doc id" wrapper (what we did for doc values) would
 * write wrong stats and ghost terms. This test proves the correct, <b>format-agnostic</b> mechanism:
 * present slice {@code s} as a {@link FilterCodecReader} exposing only slice {@code s}'s docs as live, and
 * let the <b>unmodified stock postings format</b> re-emit it via the normal merge path. The stock writer
 * then recomputes every statistic over the surviving docs and drops empty terms — for free.
 * <p>
 * Same mechanism (a slice-filtering live-docs / doc map at merge) is what gives per-slice HNSW graphs and
 * per-slice DiskBBQ centroids without touching those formats.
 */
public class PerSlicePostingsPocTests extends ESTestCase {

    private static final int NUM_SLICES = 3;
    private static final int[] DOCS_PER_SLICE = { 2, 3, 4 };
    private static final String FIELD = "body";
    private static final String SHARED_TERM = "common";

    public void testPerSlicePostingsViaStockMergeRecomputesStatsAndDropsEmptyTerms() throws IOException {
        final Directory source = new ByteBuffersDirectory();

        // One shared source segment: every slice contains the shared term plus a term unique to that slice.
        try (IndexWriter iw = new IndexWriter(source, new IndexWriterConfig(new StandardAnalyzer()))) {
            for (int s = 0; s < NUM_SLICES; s++) {
                for (int i = 0; i < DOCS_PER_SLICE[s]; i++) {
                    final Document doc = new Document();
                    doc.add(new StringField(FIELD, SHARED_TERM, StringField.Store.NO));
                    doc.add(new StringField(FIELD, "s" + s + "only", StringField.Store.NO));
                    doc.add(new NumericDocValuesField("slice_id", s));
                    iw.addDocument(doc);
                }
            }
            iw.forceMerge(1);
        }

        try (DirectoryReader sourceReader = DirectoryReader.open(source)) {
            final LeafReader leaf = sourceReader.leaves().get(0).reader();
            final int[] docSlice = readSliceAssignment(leaf);

            for (int s = 0; s < NUM_SLICES; s++) {
                final FixedBitSet live = new FixedBitSet(leaf.maxDoc());
                int liveCount = 0;
                for (int d = 0; d < leaf.maxDoc(); d++) {
                    if (docSlice[d] == s) {
                        live.set(d);
                        liveCount++;
                    }
                }

                // Re-emit ONLY slice s through the stock format, via the merge path (addIndexes runs SegmentMerger).
                final CodecReader sliceView = new SliceFilterCodecReader(SlowCodecReaderWrapper.wrap(leaf), live, liveCount);
                final Directory sliceDir = new ByteBuffersDirectory();
                try (IndexWriter sliceWriter = new IndexWriter(sliceDir, new IndexWriterConfig(new StandardAnalyzer()))) {
                    sliceWriter.addIndexes(sliceView);
                }

                try (DirectoryReader sliceReader = DirectoryReader.open(sliceDir)) {
                    assertEquals("slice " + s + " doc count", DOCS_PER_SLICE[s], sliceReader.numDocs());
                    final Terms terms = MultiTerms.getTerms(sliceReader, FIELD);
                    final TermsEnum te = terms.iterator();

                    // Shared term survives, with docFreq recomputed to THIS slice's doc count.
                    assertTrue(te.seekExact(new BytesRef(SHARED_TERM)));
                    assertEquals("recomputed docFreq for '" + SHARED_TERM + "' in slice " + s, DOCS_PER_SLICE[s], te.docFreq());

                    // This slice's own term is present with the right docFreq.
                    assertTrue(te.seekExact(new BytesRef("s" + s + "only")));
                    assertEquals(DOCS_PER_SLICE[s], te.docFreq());

                    // Every OTHER slice's unique term is DROPPED (zero docs here) — the thing the DV trick can't do.
                    for (int other = 0; other < NUM_SLICES; other++) {
                        if (other != s) {
                            assertFalse(
                                "term s" + other + "only must not leak into slice " + s,
                                te.seekExact(new BytesRef("s" + other + "only"))
                            );
                        }
                    }

                    // Field-level docCount is also recomputed to this slice only.
                    assertEquals("field docCount for slice " + s, DOCS_PER_SLICE[s], terms.getDocCount());
                }
            }
        }
    }

    private static int[] readSliceAssignment(LeafReader leaf) throws IOException {
        final int[] docSlice = new int[leaf.maxDoc()];
        final NumericDocValues sid = leaf.getNumericDocValues("slice_id");
        for (int d = sid.nextDoc(); d != NumericDocValues.NO_MORE_DOCS; d = sid.nextDoc()) {
            docSlice[d] = (int) sid.longValue();
        }
        return docSlice;
    }

    /** Exposes only a subset of docs as live, so a stock merge re-emits exactly that slice. */
    private static final class SliceFilterCodecReader extends FilterCodecReader {
        private final Bits liveDocs;
        private final int numDocs;

        SliceFilterCodecReader(CodecReader in, Bits liveDocs, int numDocs) {
            super(in);
            this.liveDocs = liveDocs;
            this.numDocs = numDocs;
        }

        @Override
        public Bits getLiveDocs() {
            return liveDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public IndexReader.CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public IndexReader.CacheHelper getReaderCacheHelper() {
            return null;
        }
    }
}
