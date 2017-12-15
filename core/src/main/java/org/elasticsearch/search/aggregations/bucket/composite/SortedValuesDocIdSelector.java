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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

/**
 * A selector that creates {@link DocIdSet} based on the ordering of values in
 * index data structures. It can be used to selects the first set of document
 * that contains the next N sorted distinct values from a specific field.
 */
abstract class SortedValuesDocIdSelector {
    private static final SortedValuesDocIdSelector DISABLED = new SortedValuesDocIdSelector() {
        @Override
        boolean isApplicable(Query query) {
            return false;
        }

        @Override
        DocIdSet build(Query query, LeafReaderContext context, Object lowerValue, Object upperValue, int size) throws IOException {
            throw new IllegalStateException("not applicable");
        }
    };

    /**
     * Returns true if this selector can be applied with the provided query.
     * @param query The original query.
     */
    abstract boolean isApplicable(Query query);

    /**
     * Returns a {@link DocIdSet} that contains the next N sorted distinct values from a specific field.
     * @param query The original query.
     * @param context The leaf context.
     * @param lowerValue The lower value to consider when selecting the documents (inclusive).
     * @param upperValue The upper value to consider when selecting the documents (inclusive).
     * @param size The number of distinct values to find.
     */
    abstract DocIdSet build(Query query, LeafReaderContext context, Object lowerValue, Object upperValue, int size) throws IOException;

    /**
     * Returns a selector that is always disabled.
     */
    static SortedValuesDocIdSelector disabled() {
        return DISABLED;
    }

    /**
     * Creates a selector based on the provided {@link FieldContext} and {@link SortOrder}.
     * @param fieldType The type of the field.
     * @param order The sort order for this selector.
     * @param rounding The rounding to apply to the numeric values extracted from the field.
     */
    static SortedValuesDocIdSelector createSelector(MappedFieldType fieldType, SortOrder order, LongUnaryOperator rounding) {
        if (fieldType.indexOptions() == IndexOptions.NONE || order == SortOrder.DESC) {
            // disables the selector if the field is not indexed or if the
            // sort order for the source is the inverse reversed of the natural ordering.
            return DISABLED;
        }
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
            return new WithTerms(fieldType.name());
        } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
            switch (fieldType.typeName()) {
                case "long":
                    return createLongSelector(fieldType, rounding);
                case "integer":
                    return createIntSelector(fieldType, rounding);
                default:
                    return DISABLED;
            }
        } else {
            return DISABLED;
        }
    }

    /**
     * Creates a selector for an integer field backed by an {@link IntPoint}.
     * @param fieldType The field type.
     * @param rounding The rounding to apply to the extracted values.
     */
    private static SortedValuesDocIdSelector createIntSelector(MappedFieldType fieldType, LongUnaryOperator rounding) {
        return new WithFixedPoints(fieldType.name(), true, rounding::applyAsLong);
    }

    /**
     * Creates a selector for a long field backed by a {@link LongPoint}.
     * @param fieldType The field type.
     * @param rounding The rounding to apply to the extracted values.
     */
    private static SortedValuesDocIdSelector createLongSelector(MappedFieldType fieldType, LongUnaryOperator rounding) {
        return new WithFixedPoints(fieldType.name(), false, rounding::applyAsLong);
    }

    /**
     * A selector that extract values from a field indexed with {@link PointValues}
     * when the query is a {@link MatchAllDocsQuery} or a {@link PointRangeQuery} that targets the same field.
     * It visits docs ids in the order of the values indexed for the field and terminates
     * when <code>size</code> different terms are fully visited.
     * Works only with single-dimension integer or long points.
     */
    private static class WithFixedPoints extends SortedValuesDocIdSelector {
        private final String field;
        private final ToLongFunction<byte[]> reader;
        private final LongUnaryOperator rounding;
        private final boolean isInteger;

        /**
         * Ctr
         * @param field The field name of the point field.
         * @param isInteger True if the field contains integers, false if it contains longs.
         * @param rounding Transforms extracted values to their rounding. The rounding must preserved the order.
         */
        private WithFixedPoints(String field, boolean isInteger, LongUnaryOperator rounding) {
            this.field = field;
            if (isInteger) {
                this.reader = (v) -> IntPoint.decodeDimension(v, 0);
            } else {
                this.reader = (v) -> LongPoint.decodeDimension(v, 0);
            }
            this.rounding = rounding;
            this.isInteger = isInteger;
        }

        @Override
        boolean isApplicable(Query query) {
            if (query instanceof MatchAllDocsQuery) {
                return true;
            } else if (query instanceof PointRangeQuery) {
                PointRangeQuery rangeQuery = (PointRangeQuery) query;
                if (rangeQuery.getField().equals(field) == false) {
                    return false;
                }
                long minValue = reader.applyAsLong(rangeQuery.getLowerPoint());
                long maxValue = reader.applyAsLong(rangeQuery.getUpperPoint());
                // we can use the lower and upper bound iff they are already rounded.
                if (isInteger) {
                    if ((minValue == Integer.MIN_VALUE || rounding.applyAsLong(minValue) == minValue) &&
                            (maxValue == Integer.MAX_VALUE || rounding.applyAsLong(maxValue) == maxValue)) {
                        return true;
                    }
                } else {
                    if ((minValue == Long.MIN_VALUE || rounding.applyAsLong(minValue) == minValue) &&
                            (maxValue == Long.MAX_VALUE || rounding.applyAsLong(maxValue) == maxValue)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private long extractLowerBound(Query query, Object lowerValue) {
            assert lowerValue == null || lowerValue instanceof Number;
            long lowerPoint = lowerValue == null ? Long.MIN_VALUE : ((Number) lowerValue).longValue();
            if (query instanceof PointRangeQuery) {
                PointRangeQuery rangeQuery = (PointRangeQuery) query;
                long value = reader.applyAsLong(rangeQuery.getLowerPoint());
                if (value > lowerPoint) {
                    return value;
                }
            }
            return lowerPoint;
        }

        private long extractUpperBound(Query query, Object upperValue) {
            assert upperValue == null || upperValue instanceof Number;
            long upperPoint = upperValue == null ? Long.MAX_VALUE : ((Number) upperValue).longValue();
            if (query instanceof PointRangeQuery) {
                PointRangeQuery rangeQuery = (PointRangeQuery) query;
                long value = reader.applyAsLong(rangeQuery.getUpperPoint());
                if (value < upperPoint) {
                    return value;
                }
            }
            return upperPoint;
        }

        @Override
        DocIdSet build(Query query, LeafReaderContext ctx, Object lowerValue, Object upperValue, int size) throws IOException {
            assert(isApplicable(query));
            final PointValues pointValues = ctx.reader().getPointValues(field);
            if (pointValues == null) {
                return DocIdSet.EMPTY;
            }
            final long lowerPoint = extractLowerBound(query, lowerValue);
            final long upperPoint = extractUpperBound(query, upperValue);
            final DocIdSetBuilder builder = new DocIdSetBuilder(ctx.reader().maxDoc(), pointValues, field);
            try {
                pointValues.intersect(new PointValues.IntersectVisitor() {
                    long lastValue;
                    int count;

                    @Override
                    public void visit(int docID) throws IOException {
                        throw new IllegalStateException("not applicable");
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) throws IOException {
                        long value = rounding.applyAsLong(reader.applyAsLong(packedValue));
                        if (value < lowerPoint) {
                            // Doc's value is too low, in this dimension
                            return;
                        }
                        if (value > upperPoint) {
                            // Doc's value is too high, in this dimension
                            return;
                        }
                        if (value != lastValue) {
                            if (++count > size+1) {
                                // the set of documents fully contains the next <code>size</code> distinct values for the field.
                                throw new CollectionTerminatedException();
                            }
                            lastValue = value;
                        }
                        builder.grow(1).add(docID);
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        // it is ok to round here since the lowerPoint is guaranteed to be at a rounding boundary.
                        long minValue = rounding.applyAsLong(reader.applyAsLong(minPackedValue));
                        // it is ok to round here, we will match more docs than the original range query but only docs
                        // that are greater than the upper bound.
                        long maxValue = rounding.applyAsLong(reader.applyAsLong(maxPackedValue));
                        if (minValue > upperPoint || maxValue < lowerPoint) {
                            return PointValues.Relation.CELL_OUTSIDE_QUERY;
                        }
                        // values are needed to early terminates the visit
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                });
            } catch (CollectionTerminatedException exc) {}
            return builder.build();
        }
    }

    /**
     * A selector that extract values from a field indexed with {@link Terms} when the query
     * is a {@link MatchAllDocsQuery}.
     * It visits docs ids in the order of the values indexed for the field and terminates
     * when <code>size</code> distinct terms are fully visited.
     */
    private static class WithTerms extends SortedValuesDocIdSelector {
        private final String field;

        private WithTerms(String field) {
            this.field = field;
        }

        @Override
        boolean isApplicable(Query query) {
            return query instanceof MatchAllDocsQuery;
        }

        @Override
        DocIdSet build(Query query, LeafReaderContext context, Object lowerValue, Object upperValue, int size) throws IOException {
            assert(isApplicable(query));
            final Terms terms = context.reader().terms(field);
            if (terms == null) {
                return DocIdSet.EMPTY;
            }
            final TermsEnum termsEnum = terms.iterator();
            if (lowerValue != null) {
                final BytesRef lowerTerm = (BytesRef) lowerValue;
                if (termsEnum.seekCeil(lowerTerm) == TermsEnum.SeekStatus.END) {
                    return DocIdSet.EMPTY;
                }
            } else {
                termsEnum.next();
            }
            final BytesRef upperTerm = upperValue != null ? (BytesRef) upperValue : null;
            final DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc(), terms);
            PostingsEnum postingsEnum = null;
            int count = 0;
            for (BytesRef term = termsEnum.term(); term != null; term = termsEnum.next()) {
                if (upperTerm != null && term.compareTo(upperTerm) > 0) {
                    return builder.build();
                }
                postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
                DocIdSetBuilder.BulkAdder adder = builder.grow((int) postingsEnum.cost());
                while (postingsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    adder.add(postingsEnum.docID());
                }
                if (++count > size) {
                    // the set of documents fully contains the next <code>size</code> distinct values for the field.
                   break;
                }
            }
            return builder.build();
        }
    }
}
