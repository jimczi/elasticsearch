/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.ExactKnnQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class RankDocsRetrieverBuilder extends RetrieverBuilder {
    private static final Logger logger = LogManager.getLogger(RankDocsRetrieverBuilder.class);

    public static final String NAME = "rank_docs";
    private final int windowSize;
    private final List<SearchSourceBuilder> sources;
    private final Supplier<RankDoc[]> rankDocs;

    public RankDocsRetrieverBuilder(int windowSize, List<SearchSourceBuilder> sources, Supplier<RankDoc[]> rankDocs) {
        this.windowSize = windowSize;
        this.rankDocs = rankDocs;
        this.sources = sources;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        searchSourceBuilder.sort(Collections.singletonList(new RankDocsSortBuilder(rankDocs.get())));
        if (searchSourceBuilder.explain() != null && searchSourceBuilder.explain()) {
            searchSourceBuilder.trackScores(true);
        }
        var bq = new BoolQueryBuilder();
        var rankQuery = new RankDocsQueryBuilder(rankDocs.get());
        if (searchSourceBuilder.aggregations() != null) {
            bq.must(rankQuery);
            searchSourceBuilder.postFilter(rankQuery);
        } else {
            bq.should(rankQuery);
        }
        for (var preFilterQueryBuilder : preFilterQueryBuilders) {
            bq.filter(preFilterQueryBuilder);
        }

        DisMaxQueryBuilder disMax = new DisMaxQueryBuilder().tieBreaker(0f);
        for (var originalSource : sources) {
            if (originalSource.query() != null) {
                disMax.add(originalSource.query());
            }
            for (var knnSearch : originalSource.knnSearch()) {
                // TODO nested + inner_hits
                ExactKnnQueryBuilder knn = new ExactKnnQueryBuilder(knnSearch.getQueryVector(), knnSearch.getField());
                disMax.add(knn);
            }
        }
        bq.should(disMax);
        searchSourceBuilder.query(bq);
    }

    @Override
    protected boolean doEquals(Object o) {
        RankDocsRetrieverBuilder other = (RankDocsRetrieverBuilder) o;
        return Arrays.equals(rankDocs.get(), other.rankDocs.get());
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.hashCode(), windowSize, rankDocs.get());
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
}
