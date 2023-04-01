/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.xcontent.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Defines a kNN search to run in the search request.
 */
public class HybridSearchBuilder implements Writeable, ToXContentFragment, Rewriteable<HybridSearchBuilder> {
    public static final ParseField QUERIES_FIELD = new ParseField("queries");
    public static final ParseField LIMIT_FIELD = new ParseField("limit");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<HybridSearchBuilder, Void> PARSER = new ConstructingObjectParser<>("hybrid", args -> {
        // TODO optimize parsing for when BYTE values are provided
        return new HybridSearchBuilder((int) args[0]);
    });

    static {
        PARSER.declareInt(constructorArg(), LIMIT_FIELD);
        PARSER.declareFieldArray(
            HybridSearchBuilder::addQueries,
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            QUERIES_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
    }

    public static HybridSearchBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final int limit;
    private List<QueryBuilder> queries;

    public HybridSearchBuilder(int limit) {
        this.limit = limit;
        this.queries = new ArrayList<>();
    }

    public HybridSearchBuilder(StreamInput in) throws IOException {
        this.limit = in.readVInt();
        this.queries = in.readNamedWriteableList(QueryBuilder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(limit);
        out.writeNamedWriteableList(queries);
    }

    public int limit() {
        return limit;
    }

    public List<QueryBuilder> getQueries() {
        return queries;
    }

    public HybridSearchBuilder addQuery(QueryBuilder query) {
        Objects.requireNonNull(query);
        this.queries.add(query);
        return this;
    }

    public HybridSearchBuilder addQueries(List<QueryBuilder> queryList) {
        Objects.requireNonNull(queryList);
        this.queries.addAll(queryList);
        return this;
    }

    @Override
    public HybridSearchBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        boolean changed = false;
        List<QueryBuilder> rewrittenQueries = new ArrayList<>(queries.size());
        for (QueryBuilder query : queries) {
            QueryBuilder rewrittenQuery = query.rewrite(ctx);
            if (rewrittenQuery != query) {
                changed = true;
            }
            rewrittenQueries.add(rewrittenQuery);
        }
        if (changed) {
            return new HybridSearchBuilder(limit).addQueries(rewrittenQueries);
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HybridSearchBuilder that = (HybridSearchBuilder) o;
        return limit == that.limit
            && Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            limit,
            Objects.hashCode(queries)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(LIMIT_FIELD.getPreferredName(), limit);
        builder.startArray(QUERIES_FIELD.getPreferredName());
        for (QueryBuilder query : queries) {
            query.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
