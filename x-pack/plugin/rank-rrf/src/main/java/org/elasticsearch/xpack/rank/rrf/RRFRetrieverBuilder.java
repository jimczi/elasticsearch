/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.RankDocsRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.search.rank.RankDoc.NO_RANK;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RRFRetrieverBuilder extends RetrieverBuilder {
    public static final String NAME = "rrf";
    public static final NodeFeature RRF_RETRIEVER_SUPPORTED = new NodeFeature("rrf_retriever_supported");

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");
    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<RRFRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            int rankWindowSize = args[1] == null ? RRFRankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[1];
            int rankConstant = args[2] == null ? RRFRankBuilder.DEFAULT_RANK_CONSTANT : (int) args[2];
            return new RRFRetrieverBuilder((List<RetrieverBuilder>) args[0], rankWindowSize, rankConstant);
        }
    );

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, name, c);
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static RRFRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(RRF_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    private record RetrieverSource(RetrieverBuilder retriever, SearchSourceBuilder source) {}

    private final List<RetrieverSource> retrievers;
    private final int rankWindowSize;
    private final int rankConstant;
    private final SetOnce<RRFRankDoc[]> rankDocs;

    public RRFRetrieverBuilder(List<RetrieverBuilder> retrieverBuilders, int rankWindowSize, int rankConstant) {
        this(
            retrieverBuilders.stream().map(r -> new RetrieverSource(r, null)).collect(Collectors.toList()),
            rankWindowSize,
            rankConstant,
            null
        );
    }

    private RRFRetrieverBuilder(List<RetrieverSource> retrievers, int rankWindowSize, int rankConstant, SetOnce<RRFRankDoc[]> rankDocs) {
        this.retrievers = retrievers;
        this.rankWindowSize = rankWindowSize;
        this.rankConstant = rankConstant;
        this.rankDocs = rankDocs;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean requiresPointInTime() {
        return true;
    }

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        if (ctx.pointInTimeBuilder() == null) {
            throw new IllegalStateException("PIT is required");
        }
        List<RetrieverSource> newRetrievers = new ArrayList<>();
        boolean hasChanged = false;
        for (var source : retrievers) {
            RetrieverBuilder rewritten = source.retriever.rewrite(ctx);
            if (rewritten != source.retriever) {
                newRetrievers.add(new RetrieverSource(rewritten, null));
                hasChanged |= rewritten != source.retriever;
            } else if (rewritten == source.retriever) {
                SearchSourceBuilder sourceBuilder;
                if (source.source == null) {
                    sourceBuilder = new SearchSourceBuilder().pointInTimeBuilder(ctx.pointInTimeBuilder()).size(rankWindowSize);
                    rewritten.extractToSearchSourceBuilder(sourceBuilder, false);
                    List<SortBuilder<?>> sortBuilders = sourceBuilder.sorts() != null
                        ? new ArrayList<>(sourceBuilder.sorts())
                        : new ArrayList<>();
                    if (sortBuilders.isEmpty()) {
                        sortBuilders.add(new ScoreSortBuilder());
                    }
                    sourceBuilder.sort(sortBuilders);
                } else {
                    sourceBuilder = source.source;
                }
                var rewrittenSource = sourceBuilder.rewrite(ctx);
                newRetrievers.add(new RetrieverSource(rewritten, rewrittenSource));
                hasChanged |= rewrittenSource != source.source;
            }
        }
        if (hasChanged) {
            return new RRFRetrieverBuilder(newRetrievers, rankWindowSize, rankConstant, null);
        }

        if (rankDocs != null) {
            return this;
        }

        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (var ret : retrievers) {
            SearchRequest searchRequest = new SearchRequest().source(ret.source);
            searchRequest.setPreFilterShardSize(Integer.MAX_VALUE);
            multiSearchRequest.add(searchRequest);
        }
        final SetOnce<RankDoc[]> results = new SetOnce<>();
        ctx.registerAsyncAction((client, listener) -> {
            client.execute(TransportMultiSearchAction.TYPE, multiSearchRequest, new ActionListener<>() {
                @Override
                public void onResponse(MultiSearchResponse items) {
                    List<ScoreDoc[]> topDocs = new ArrayList<>();
                    for (int i = 0; i < items.getResponses().length; i++) {
                        var item = items.getResponses()[i];
                        topDocs.add(getTopDocs(item.getResponse()));
                    }
                    results.set(combineQueryPhaseResults(topDocs));
                    listener.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        });
        return new RankDocsRetrieverBuilder(rankWindowSize, newRetrievers.stream().map(s -> s.source).toList(), results::get);
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        throw new IllegalStateException("Should not be called, missing a rewrite?");
    }

    // ---- FOR TESTING XCONTENT PARSING ----
    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (retrievers.isEmpty() == false) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());

            for (var entry : retrievers) {
                builder.startObject();
                builder.field(entry.retriever.getName());
                entry.retriever.toXContent(builder, params);
                builder.endObject();
            }

            builder.endArray();
        }

        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }

    @Override
    public boolean doEquals(Object o) {
        RRFRetrieverBuilder that = (RRFRetrieverBuilder) o;
        return rankWindowSize == that.rankWindowSize && rankConstant == that.rankConstant && Objects.equals(retrievers, that.retrievers);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(retrievers, rankWindowSize, rankConstant);
    }

    private ScoreDoc[] getTopDocs(SearchResponse searchResponse) {
        int size = Math.min(rankWindowSize, searchResponse.getHits().getHits().length);
        ScoreDoc[] docs = new ScoreDoc[size];
        for (int i = 0; i < size; i++) {
            var hit = searchResponse.getHits().getAt(i);
            long sortValue = (long) hit.getRawSortValues()[hit.getRawSortValues().length - 1];
            int shardIndex = (int) (sortValue >> 32);
            docs[i] = new ScoreDoc(hit.docId(), hit.getScore(), shardIndex);
        }
        return docs;
    }

    public RRFRankDoc[] combineQueryPhaseResults(List<ScoreDoc[]> rankResults) {
        // combine the disjointed sets of TopDocs into a single set or RRFRankDocs
        // each RRFRankDoc will have both the position and score for each query where
        // it was within the result set for that query
        // if a doc isn't part of a result set its position will be NO_RANK [0] and
        // its score is [0f]
        int queries = rankResults.size();
        Map<Long, RRFRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        int index = 0;
        for (var rrfRankResult : rankResults) {
            int rank = 1;
            for (ScoreDoc scoreDoc : rrfRankResult) {
                final int findex = index;
                final int frank = rank;
                long docAndShard = (((long) scoreDoc.shardIndex) << 32) | (scoreDoc.doc & 0xFFFFFFFFL);
                docsToRankResults.compute(docAndShard, (key, value) -> {
                    if (value == null) {
                        value = new RRFRankDoc(scoreDoc.doc, scoreDoc.shardIndex, queries);
                    }

                    // calculate the current rrf score for this document
                    // later used to sort and covert to a rank
                    value.score += 1.0f / (rankConstant + frank);

                    // record the position for each query
                    // for explain and debugging
                    value.positions[findex] = frank - 1;

                    // record the score for each query
                    // used to later re-rank on the coordinator
                    value.scores[findex] = scoreDoc.score;

                    return value;
                });
                ++rank;
            }
            ++index;
        }

        // sort the results based on rrf score, tiebreaker based on smaller doc id
        RRFRankDoc[] sortedResults = docsToRankResults.values().toArray(RRFRankDoc[]::new);
        Arrays.sort(sortedResults, (RRFRankDoc rrf1, RRFRankDoc rrf2) -> {
            if (rrf1.score != rrf2.score) {
                return rrf1.score < rrf2.score ? 1 : -1;
            }
            assert rrf1.positions.length == rrf2.positions.length;
            for (int qi = 0; qi < rrf1.positions.length; ++qi) {
                if (rrf1.positions[qi] != NO_RANK && rrf2.positions[qi] != NO_RANK) {
                    if (rrf1.scores[qi] != rrf2.scores[qi]) {
                        return rrf1.scores[qi] < rrf2.scores[qi] ? 1 : -1;
                    }
                } else if (rrf1.positions[qi] != NO_RANK) {
                    return -1;
                } else if (rrf2.positions[qi] != NO_RANK) {
                    return 1;
                }
            }
            return rrf1.doc < rrf2.doc ? -1 : 1;
        });
        // trim the results if needed, otherwise each shard will always return `rank_window_size` results.
        // pagination and all else will happen on the coordinator when combining the shard responses
        RRFRankDoc[] topResults = new RRFRankDoc[Math.min(rankWindowSize, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
            topResults[rank].score = Float.NaN;
        }
        return topResults;
    }
}
