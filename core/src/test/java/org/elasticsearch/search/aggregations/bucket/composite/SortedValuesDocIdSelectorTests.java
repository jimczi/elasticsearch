/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SortedValuesDocIdSelectorTests extends ESTestCase {
    public void testPoints() throws Exception {
        int numIter = atLeast(3);
        for (int i = 0; i < numIter; i++) {
            testNumericsCase(NumberFieldMapper.NumberType.INTEGER, LongUnaryOperator.identity());
            testNumericsCase(NumberFieldMapper.NumberType.LONG, LongUnaryOperator.identity());
            final Rounding rounding = Rounding.builder(TimeValue.timeValueMillis(randomLongBetween(1, 1000))).build();
            testNumericsCase(NumberFieldMapper.NumberType.INTEGER, rounding::round);
            testNumericsCase(NumberFieldMapper.NumberType.LONG, rounding::round);
        }
    }

    public void testTerms() throws Exception {
        int numIter = atLeast(3);
        for (int i = 0; i < numIter; i++) {
            testTermsCase();
        }
    }

    public void testCreateSelector() {
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType();
        assertThat(SortedValuesDocIdSelector.createSelector(fieldType, SortOrder.DESC, LongUnaryOperator.identity()),
            equalTo(SortedValuesDocIdSelector.disabled()));

        fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
        assertThat(SortedValuesDocIdSelector.createSelector(fieldType, SortOrder.DESC, LongUnaryOperator.identity()),
            equalTo(SortedValuesDocIdSelector.disabled()));

        fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.FLOAT);
        assertThat(SortedValuesDocIdSelector.createSelector(fieldType, SortOrder.DESC, LongUnaryOperator.identity()),
            equalTo(SortedValuesDocIdSelector.disabled()));
    }

    public void testApplicable() {
        MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType();
        keyword.setIndexOptions(IndexOptions.DOCS);
        assertFalse(SortedValuesDocIdSelector.createSelector(keyword, SortOrder.DESC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        assertFalse(SortedValuesDocIdSelector.createSelector(keyword, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new TermQuery(new Term("fake"))));
        keyword.setIndexOptions(IndexOptions.NONE);
        assertFalse(SortedValuesDocIdSelector.createSelector(keyword, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        keyword.setIndexOptions(IndexOptions.DOCS);
        assertTrue(SortedValuesDocIdSelector.createSelector(keyword, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));

        MappedFieldType number = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.INTEGER);
        number.setName("number");
        number.setIndexOptions(IndexOptions.DOCS);
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.DESC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new TermQuery(new Term("fake"))));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(IntPoint.newRangeQuery("fake", 0, 0)));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, (l) -> l+1)
            .isApplicable(IntPoint.newRangeQuery("number", 0, 0)));
        number.setIndexOptions(IndexOptions.NONE);
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        number.setIndexOptions(IndexOptions.DOCS);
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(IntPoint.newRangeQuery("number", Integer.MIN_VALUE, 0)));
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(IntPoint.newRangeQuery("number", 0, Integer.MAX_VALUE)));
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(IntPoint.newRangeQuery("number", Integer.MIN_VALUE, Integer.MAX_VALUE)));

        number = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        number.setName("number");
        number.setIndexOptions(IndexOptions.DOCS);
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.DESC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new TermQuery(new Term("fake"))));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(LongPoint.newRangeQuery("fake", 0, 0)));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, (l) -> l+1)
            .isApplicable(LongPoint.newRangeQuery("number", 0, 0)));
        number.setIndexOptions(IndexOptions.NONE);
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        number.setIndexOptions(IndexOptions.DOCS);
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(new MatchAllDocsQuery()));
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(LongPoint.newRangeQuery("number", Long.MIN_VALUE, 0)));
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(LongPoint.newRangeQuery("number", 0, Long.MAX_VALUE)));
        assertTrue(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, LongUnaryOperator.identity())
            .isApplicable(LongPoint.newRangeQuery("number", Long.MIN_VALUE, Long.MAX_VALUE)));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, (l) -> l+1)
            .isApplicable(LongPoint.newRangeQuery("number", Integer.MIN_VALUE, 0)));
        assertFalse(SortedValuesDocIdSelector.createSelector(number, SortOrder.ASC, (l) -> l+1)
            .isApplicable(LongPoint.newRangeQuery("number", 0, Integer.MAX_VALUE)));
    }

    private void testTermsCase() throws Exception {
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
        fieldType.setName("term");
        fieldType.setTokenized(false);
        int numDocs = randomIntBetween(100, 100);
        int numDistinctValues = randomIntBetween(1, numDocs);
        BytesRef[] values = new BytesRef[numDistinctValues];
        for (int i = 0; i < values.length; i++) {
            values[i] = new BytesRef(randomAlphaOfLengthBetween(1, 10));
        }
        List<BytesRef> indexedValues = new ArrayList<>();
        final SortedValuesDocIdSelector selector =
            SortedValuesDocIdSelector.createSelector(fieldType, SortOrder.ASC, LongUnaryOperator.identity());
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (int i = 0; i < numDocs; i++) {
                    int id = randomIntBetween(0, values.length-1);
                    indexedValues.add(values[id]);
                    document.add(new Field("term", values[id], fieldType));
                    document.add(new SortedDocValuesField("term", values[id]));
                    indexWriter.addDocument(document);
                    document.clear();
                }
                indexWriter.forceMerge(1);
            }
            Collections.sort(indexedValues);
            numDistinctValues = indexedValues.size();
            SortField sortField = new SortField("term", SortField.Type.STRING);
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(indexReader);
                assertThat(indexReader.leaves().size(), equalTo(1));
                LeafReaderContext context = indexReader.leaves().get(0);

                int size = randomIntBetween(1, numDistinctValues);
                MatchAllDocsQuery query = new MatchAllDocsQuery();
                assertTrue(selector.isApplicable(query));
                DocIdSet docIdSet =
                    selector.build(query, context, null, null, size);
                assertNotNull(docIdSet.iterator());
                assertThat((int) docIdSet.iterator().cost(), greaterThan(0));
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), (int) docIdSet.iterator().cost(), new Sort(sortField));
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, true);

                size = randomIntBetween(1, numDistinctValues);
                query = new MatchAllDocsQuery();
                assertTrue(selector.isApplicable(query));
                BytesRef lowerValue = indexedValues.get(0);
                BytesRef upperValue = indexedValues.get(values.length-1);

                docIdSet = selector.build(query, context, lowerValue, upperValue, size);
                assertNotNull(docIdSet.iterator());
                docs = searcher.search(new MatchAllDocsQuery(), (int) docIdSet.iterator().cost(), new Sort(sortField));
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, true);

                docIdSet = selector.build(query, context, null, upperValue, size);
                assertNotNull(docIdSet.iterator());
                docs = searcher.search(new MatchAllDocsQuery(), (int) docIdSet.iterator().cost(), new Sort(sortField));
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, true);

                docIdSet = selector.build(query, context, lowerValue, null, size);
                assertNotNull(docIdSet.iterator());
                docs = searcher.search(new MatchAllDocsQuery(), (int) docIdSet.iterator().cost(), new Sort(sortField));
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, true);
            }
        }
    }

    private void testNumericsCase(NumberFieldMapper.NumberType type, LongUnaryOperator rounding) throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(type);
        fieldType.setName("number");
        int numDocs = randomIntBetween(100, 100);
        int numDistinctValues = randomIntBetween(1, numDocs);
        boolean isLong = "long".equals(fieldType.typeName());
        Object[] values = new Object[numDistinctValues];
        for (int i = 0; i < values.length; i++) {
            if (isLong) {
                values[i] = randomLong();
            } else {
                values[i] = randomInt();
            }
        }
        final SortedValuesDocIdSelector selector =
            SortedValuesDocIdSelector.createSelector(fieldType, SortOrder.ASC, rounding);
        List<Number> indexedValues = new ArrayList<>();
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (int i = 0; i < numDocs; i++) {
                    int id = randomIntBetween(0, values.length-1);
                    indexedValues.add((Number) values[id]);
                    if (isLong) {
                        document.add(new LongPoint("number", (long) values[id]));
                        document.add(new NumericDocValuesField("number", (long) values[id]));
                    } else {
                        document.add(new IntPoint("number", (int) values[id]));
                        document.add(new NumericDocValuesField("number", (int) values[id]));
                    }
                    indexWriter.addDocument(document);
                    document.clear();
                }
                indexWriter.forceMerge(1);
            }
            Collections.sort(indexedValues, Comparator.comparingLong(Number::longValue));
            numDistinctValues = indexedValues.size();
            SortField.Type sortType = isLong ? SortField.Type.LONG : SortField.Type.INT;
            SortField sortField = new SortField("number", sortType);
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(indexReader);
                assertThat(indexReader.leaves().size(), equalTo(1));
                LeafReaderContext context = indexReader.leaves().get(0);

                int size = randomIntBetween(1, numDistinctValues);
                Query query = new MatchAllDocsQuery();
                assertTrue(selector.isApplicable(query));
                DocIdSet docIdSet =
                    selector.build(query, context, null, null, size);
                assertNotNull(docIdSet.iterator());
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), (int) docIdSet.iterator().cost(), new Sort(sortField));
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, true);

                int lowerId = randomIntBetween(0, indexedValues.size()-1);
                Object lowerValue = rounding.applyAsLong(indexedValues.get(lowerId).longValue());
                Object upperValue = rounding.applyAsLong(indexedValues.get(randomIntBetween(lowerId, indexedValues.size()-1)).longValue());
                if (isLong) {
                    query = LongPoint.newRangeQuery("number", (long) lowerValue, (long) upperValue);
                } else {
                    query = IntPoint.newRangeQuery("number", ((Number) lowerValue).intValue(), ((Number) upperValue).intValue());
                }
                assertTrue(selector.isApplicable(query));
                docIdSet =
                    selector.build(query, context, lowerValue, upperValue, size);
                assertNotNull(docIdSet.iterator());
                assertThat(docIdSet.iterator().cost(), greaterThan(0L));
                docs = searcher.search(query, (int) docIdSet.iterator().cost(), new Sort(sortField));
                // The selector can select more documents than the query
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, false);

                if (isLong) {
                    query = LongPoint.newRangeQuery("number", (long) lowerValue, Long.MAX_VALUE);
                } else {
                    query = IntPoint.newRangeQuery("number", ((Number) lowerValue).intValue(), Integer.MAX_VALUE);
                }
                assertTrue(selector.isApplicable(query));
                docIdSet =
                    selector.build(query, context, lowerValue, null, size);
                assertNotNull(docIdSet.iterator());
                docs = searcher.search(query, (int) docIdSet.iterator().cost(), new Sort(sortField));
                // The selector can select more documents than the query
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, false);

                if (isLong) {
                    query = LongPoint.newRangeQuery("number", Long.MIN_VALUE, (long) upperValue);
                } else {
                    query = IntPoint.newRangeQuery("number", Integer.MIN_VALUE, ((Number) upperValue).intValue());
                }
                assertTrue(selector.isApplicable(query));
                docIdSet =
                    selector.build(query, context, null, upperValue, size);
                assertNotNull(docIdSet.iterator());
                docs = searcher.search(query, (int) docIdSet.iterator().cost(), new Sort(sortField));
                // The selector can select more documents than the query
                assertSameDocs(context.reader().maxDoc(), docIdSet, docs.scoreDocs, false);
            }
        }
    }

    /**
     * Checks if the provided <code>bitSet</code> matches the document in the provided <code>docs</code>.
     * If <code>strictEquals</code> is true, all document in the bit set must be contained in the score docs,
     * otherwise we only check if all score docs are contained in the bit set.
     */
    private void assertSameDocs(int maxDoc, DocIdSet bitSet, ScoreDoc[] docs, boolean strictEquals) throws IOException {
        if (strictEquals) {
            assertThat((int) bitSet.iterator().cost(), equalTo(docs.length));
        } else {
            assertThat((int) bitSet.iterator().cost(), greaterThanOrEqualTo(docs.length));
        }

        FixedBitSet expectedBitSet = new FixedBitSet(maxDoc);
        FixedBitSet actualBitSet = new FixedBitSet(maxDoc);
        for (ScoreDoc doc : docs) {
            expectedBitSet.set(doc.doc);
        }
        DocIdSetIterator it = bitSet.iterator();
        while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            actualBitSet.set(it.docID());
        }

        DocIdSetIterator docIt = new BitDocIdSet(expectedBitSet).iterator();
        while (docIt.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            assertTrue(actualBitSet.get(docIt.docID()));
        }
    }
}
