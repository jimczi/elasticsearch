/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortFieldAndFormat;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

public class RankDocsSortBuilder extends SortBuilder<RankDocsSortBuilder> {
    public static final String NAME = "rank_docs";

    private final RankDoc[] rankDocs;

    public RankDocsSortBuilder(RankDoc[] rankDocs) {
        this.rankDocs = rankDocs;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(StreamOutput::writeNamedWriteable, rankDocs);
    }

    @Override
    public SortBuilder<?> rewrite(QueryRewriteContext ctx) throws IOException {
        return this;
    }

    @Override
    protected SortFieldAndFormat build(SearchExecutionContext context) throws IOException {
        RankDoc[] shardRankDocs = Arrays.stream(rankDocs)
            .filter(r -> r.shardIndex == context.getShardRequestIndex())
            .toArray(RankDoc[]::new);
        return new SortFieldAndFormat(new RankDocsSortField(shardRankDocs), DocValueFormat.RAW);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // TODO
        return TransportVersion.current();
    }

    @Override
    public BucketedSort buildBucketedSort(SearchExecutionContext context, BigArrays bigArrays, int bucketSize, BucketedSort.ExtraData extra)
        throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
}
