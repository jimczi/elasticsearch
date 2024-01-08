/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.valuesource.ConstKnnFloatValueSource;
import org.apache.lucene.queries.function.valuesource.FloatKnnVectorFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SubSearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.vectors.KnnScoreDocQueryBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This search phase fans out to every shards to execute a distributed search with a pre-collected distributed frequencies for all
 * search terms used in the actual search query. This phase is very similar to a the default query-then-fetch search phase but it doesn't
 * retry on another shard if any of the shards are failing. Failures are treated as shard failures and are counted as a non-successful
 * operation.
 * @see CountedCollector#onFailure(int, SearchShardTarget, Exception)
 */
final class DfsQueryPhase extends SearchPhase {
    private final SearchPhaseResults<SearchPhaseResult> queryResult;
    private final List<DfsSearchResult> searchResults;
    private final AggregatedDfs dfs;
    private final List<DfsKnnResults> knnResults;
    private final Function<SearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final SearchTransportService searchTransportService;
    private final SearchProgressListener progressListener;

    DfsQueryPhase(
        List<DfsSearchResult> searchResults,
        AggregatedDfs dfs,
        List<DfsKnnResults> knnResults,
        SearchPhaseResults<SearchPhaseResult> queryResult,
        Function<SearchPhaseResults<SearchPhaseResult>, SearchPhase> nextPhaseFactory,
        SearchPhaseContext context
    ) {
        super("dfs_query");
        this.progressListener = context.getTask().getProgressListener();
        this.queryResult = queryResult;
        this.searchResults = searchResults;
        this.dfs = dfs;
        this.knnResults = knnResults;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.searchTransportService = context.getSearchTransport();

        // register the release of the query consumer to free up the circuit breaker memory
        // at the end of the search
        context.addReleasable(queryResult::decRef);
    }

    @Override
    public void run() {
        // TODO we can potentially also consume the actual per shard results from the initial phase here in the aggregateDfs
        // to free up memory early
        final CountedCollector<SearchPhaseResult> counter = new CountedCollector<>(
            queryResult,
            searchResults.size(),
            () -> context.executeNextPhase(this, nextPhaseFactory.apply(queryResult)),
            context
        );

        for (final DfsSearchResult dfsResult : searchResults) {
            final SearchShardTarget shardTarget = dfsResult.getSearchShardTarget();
            Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
            ShardSearchRequest shardRequest = rewriteShardSearchRequest(dfsResult.getShardSearchRequest());
            QuerySearchRequest querySearchRequest = new QuerySearchRequest(
                context.getOriginalIndices(dfsResult.getShardIndex()),
                dfsResult.getContextId(),
                shardRequest,
                dfs
            );
            final int shardIndex = dfsResult.getShardIndex();
            searchTransportService.sendExecuteQuery(
                connection,
                querySearchRequest,
                context.getTask(),
                new SearchActionListener<>(shardTarget, shardIndex) {

                    @Override
                    protected void innerOnResponse(QuerySearchResult response) {
                        try {
                            response.setSearchProfileDfsPhaseResult(dfsResult.searchProfileDfsPhaseResult());
                            counter.onResult(response);
                        } catch (Exception e) {
                            context.onPhaseFailure(DfsQueryPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        try {
                            context.getLogger()
                                .debug(() -> "[" + querySearchRequest.contextId() + "] Failed to execute query phase", exception);
                            progressListener.notifyQueryFailure(shardIndex, shardTarget, exception);
                            counter.onFailure(shardIndex, shardTarget, exception);
                        } finally {
                            if (context.isPartOfPointInTime(querySearchRequest.contextId()) == false) {
                                // the query might not have been executed at all (for example because thread pool rejected
                                // execution) and the search context that was created in dfs phase might not be released.
                                // release it again to be in the safe side
                                context.sendReleaseSearchContext(
                                    querySearchRequest.contextId(),
                                    connection,
                                    context.getOriginalIndices(shardIndex)
                                );
                            }
                        }
                    }
                }
            );
        }
    }

    // package private for testing
    ShardSearchRequest rewriteShardSearchRequest(ShardSearchRequest request) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.knnSearch().isEmpty()) {
            return request;
        }

        List<SubSearchSourceBuilder> subSearchSourceBuilders = new ArrayList<>(source.subSearches());

        int i = 0;
        for (DfsKnnResults dfsKnnResults : knnResults) {
            List<ScoreDoc> scoreDocs = new ArrayList<>();
            for (ScoreDoc scoreDoc : dfsKnnResults.scoreDocs()) {
                if (scoreDoc.shardIndex == request.shardRequestIndex()) {
                    scoreDocs.add(scoreDoc);
                }
            }
            scoreDocs.sort(Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
            String nestedPath = dfsKnnResults.getNestedPath();
            QueryBuilder query = new KnnScoreDocQueryBuilder(scoreDocs.toArray(new ScoreDoc[0]));
            if (nestedPath != null) {
                NestedQueryBuilder delegate =
                        new NestedQueryBuilder(nestedPath, query, ScoreMode.Max).innerHit(source.knnSearch().get(i).innerHit());
                // TODO: extract the knn search correctly
                query = new KnnNestedQueryBuilder(source.knnSearch().get(0), delegate);
            }
            subSearchSourceBuilders.add(new SubSearchSourceBuilder(query));
            i++;
        }

        source = source.shallowCopy().subSearches(subSearchSourceBuilders).knnSearch(List.of());
        request.source(source);

        return request;
    }

    private static class KnnNestedQueryBuilder extends AbstractQueryBuilder<KnnNestedQueryBuilder> {
        private final KnnSearchBuilder knnQuery;
        private final NestedQueryBuilder delegate;

        private KnnNestedQueryBuilder(KnnSearchBuilder knnQuery, NestedQueryBuilder nested) {
            this.knnQuery = knnQuery;
            this.delegate = nested;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            knnQuery.writeTo(out);
            delegate.writeTo(out);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            throw new IllegalStateException("");
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            return delegate.toQuery(context);
        }

        @Override
        protected boolean doEquals(KnnNestedQueryBuilder other) {
            return delegate.equals(other);
        }

        @Override
        protected int doHashCode() {
            return delegate.hashCode();
        }

        @Override
        protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
            if (delegate.innerHit() != null) {
                InnerHitBuilder innerHitBuilder = delegate.innerHit();
                String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : delegate.getPath();
                if (innerHits.containsKey(name)) {
                    throw new IllegalArgumentException("[inner_hits] already contains an entry for key [" + name + "]");
                }

                QueryBuilder query = new ExactKnnQueryBuilder(knnQuery.getField(), knnQuery.getQueryVector());
                InnerHitContextBuilder innerHitContextBuilder =
                        new NestedQueryBuilder.NestedInnerHitContextBuilder(delegate.getPath(), query, innerHitBuilder, Map.of());
                innerHits.put(name, innerHitContextBuilder);
            }
        }

        @Override
        public String getWriteableName() {
            return "knn_nested";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }
    };

    // TODO: Replace with a KnnVectorQueryBuilder that forces exact search
    private static class ExactKnnQueryBuilder extends AbstractQueryBuilder<ExactKnnQueryBuilder> {
        private final String field;
        private final float[] queryVector;

        private ExactKnnQueryBuilder(String field, float[] queryVector) {
            this.field = field;
            this.queryVector = queryVector;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new IllegalStateException("");
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            throw new IllegalStateException("");
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            var v1 = new FloatKnnVectorFieldSource(field);
            var v2 = new ConstKnnFloatValueSource(queryVector);
            return new FunctionQuery(new FloatVectorSimilarityFunction(VectorSimilarityFunction.COSINE, v1, v2));
        }

        @Override
        protected boolean doEquals(ExactKnnQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            throw new IllegalStateException("");
        }

        @Override
        public String getWriteableName() {
            throw new IllegalStateException("");
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            throw new IllegalStateException("");
        }
    };


}
