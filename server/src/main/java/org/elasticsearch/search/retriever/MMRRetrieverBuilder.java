/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class MMRRetrieverBuilder extends CompoundRetrieverBuilder<MMRRetrieverBuilder> {
    private final String vectorField;

    public MMRRetrieverBuilder(RetrieverBuilder retrieverBuilder, String vectorField, int rankWindowSize) {
        super(List.of(RetrieverSource.from(retrieverBuilder)), rankWindowSize);
        this.vectorField = vectorField;
    }

    public MMRRetrieverBuilder(List<RetrieverSource> retrieverSource, String vectorField, int rankWindowSize) {
        super(retrieverSource, rankWindowSize);
        this.vectorField = vectorField;
    }

    @Override
    public String getName() {
        return "mmr";
    }

    @Override
    protected MMRRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        return new MMRRetrieverBuilder(newChildRetrievers, vectorField, rankWindowSize);
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder sourceBuilder) {
        return super.finalizeSourceBuilder(sourceBuilder).docValueField(vectorField);
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDocAndHit[]> rankResults, boolean explain) {
        for (var topDocs : rankResults) {
            for (var topDoc : topDocs) {
                var field = topDoc.hit().getFields().getOrDefault(vectorField, null);
                List<Float> myVector = field.getValue();
            }
        }
        return null;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {

    }
}
