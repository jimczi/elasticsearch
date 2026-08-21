/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorerSupplier;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;

import java.io.IOException;
import java.util.Objects;

/**
 * Matches the documents of a ColumNAR string column whose value is a given term, or whose value starts with
 * a given prefix.
 *
 * <p>It reads the column at the binary surface and drives the column's own matching, which answers over
 * ordinals where the column has them and over the values where it does not. The iterator that comes back
 * carries a two-phase behind it, so a scorer collects it a window at a time rather than a document at a
 * time — which is what makes a filter cheap.
 */
public final class ColumnarStringTermQuery extends Query {

    private final String field;
    private final BytesRef term;
    private final boolean prefix;

    /** Matches documents whose value is exactly {@code term}. */
    public static ColumnarStringTermQuery term(String field, BytesRef term) {
        return new ColumnarStringTermQuery(field, term, false);
    }

    /** Matches documents whose value starts with {@code prefix}. */
    public static ColumnarStringTermQuery prefix(String field, BytesRef prefix) {
        return new ColumnarStringTermQuery(field, prefix, true);
    }

    private ColumnarStringTermQuery(String field, BytesRef term, boolean prefix) {
        this.field = Objects.requireNonNull(field);
        this.term = BytesRef.deepCopyOf(Objects.requireNonNull(term));
        this.prefix = prefix;
    }

    @Override
    public org.apache.lucene.search.Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final LeafReader reader = context.reader();
                final BinaryDocValues values = reader.getBinaryDocValues(field);
                if (values instanceof ColumnarStringBinaryDocValues column) {
                    final DocIdSetIterator iterator = prefix ? column.reader().matchPrefix(term) : column.reader().matchTerm(term);
                    return ConstantScoreScorerSupplier.fromIterator(iterator, score(), scoreMode, reader.maxDoc());
                }
                if (values == null) {
                    return null;
                }
                // Another format behind the same field: compare the values a document at a time.
                final TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        final BytesRef value = values.binaryValue();
                        return prefix ? startsWith(value, term) : value.bytesEquals(term);
                    }

                    @Override
                    public float matchCost() {
                        return 10f;
                    }
                };
                return ConstantScoreScorerSupplier.fromIterator(
                    TwoPhaseIterator.asDocIdSetIterator(twoPhase),
                    score(),
                    scoreMode,
                    reader.maxDoc()
                );
            }

            @Override
            public boolean isCacheable(LeafReaderContext context) {
                return org.apache.lucene.index.DocValues.isCacheable(context, field);
            }
        };
    }

    private static boolean startsWith(BytesRef value, BytesRef prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        return java.util.Arrays.equals(
            value.bytes,
            value.offset,
            value.offset + prefix.length,
            prefix.bytes,
            prefix.offset,
            prefix.offset + prefix.length
        );
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String defaultField) {
        return field + (prefix ? ":" + term.utf8ToString() + "*" : ":" + term.utf8ToString());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof ColumnarStringTermQuery query) {
            return prefix == query.prefix && field.equals(query.field) && term.bytesEquals(query.term);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, term, prefix);
    }
}
