/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DocumentPartitioner;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * A filter that restricts a search to a single slice (tenant) by <b>pruning whole segments</b>: for any leaf whose
 * segment does not belong to {@code slice} (per its {@link DocumentPartitioner#PARTITION_ATTRIBUTE} stamp) it
 * returns no scorer, so that leaf is never traversed. Combined as a {@code FILTER} clause, the query therefore
 * only ever touches the tenant's own segments — other tenants' segments are not scored, and in stateless their
 * blocks are never fetched from the object store. A matching leaf contributes all its docs (a constant-score
 * match-all), so as a filter it removes nothing within the tenant.
 * <p>
 * This is the search-path counterpart to slice-per-segment writes: because a slice-sticky buffer produces one
 * segment per tenant, "restrict to a slice" degenerates to segment selection — no per-document work and no per-doc
 * bitset (the same mechanism leaf-level security uses). It supersedes the coarse {@code _routing} term filter for
 * slice-partitioned indices, which post-filters documents over every segment rather than skipping segments.
 */
public final class SlicePruningQuery extends Query {

    private final Set<String> slices;

    public SlicePruningQuery(Set<String> slices) {
        this.slices = Set.copyOf(Objects.requireNonNull(slices, "slices"));
    }

    public Set<String> slices() {
        return slices;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final String segmentSlice = Lucene.segmentReader(context.reader()).getSegmentInfo().info.getAttribute(
                    DocumentPartitioner.PARTITION_ATTRIBUTE
                );
                if (slices.contains(segmentSlice) == false) {
                    // Prune: this tenant's query never traverses (nor fetches) another tenant's segment.
                    return null;
                }
                final Scorer scorer = new ConstantScoreScorer(score(), scoreMode, DocIdSetIterator.all(context.reader().maxDoc()));
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // Membership depends only on the leaf's own (immutable) slice attribute.
                return true;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "SlicePruningQuery(slices=" + slices + ")";
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && slices.equals(((SlicePruningQuery) other).slices);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + slices.hashCode();
    }
}
