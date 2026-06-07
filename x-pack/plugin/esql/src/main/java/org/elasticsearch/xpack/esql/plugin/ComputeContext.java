/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.function.Supplier;

/**
 * @param blockFactory the per-query {@link BlockFactory} this compute must allocate through, or {@code null} to use the
 *                     node's shared block factory. A non-null factory is built over the admission's per-query memory
 *                     breaker so the query's allocations are bounded by its reserved budget (see
 *                     {@code SearchService.SearchAdmission}).
 */
record ComputeContext(
    String sessionId,
    String description,
    String clusterAlias,
    EsqlFlags flags,
    IndexedByShardId<ComputeSearchContext> searchContexts,
    Configuration configuration,
    FoldContext foldCtx,
    Supplier<ExchangeSource> exchangeSourceSupplier,
    Supplier<ExchangeSink> exchangeSinkSupplier,
    BlockFactory blockFactory
) {
    /** Builds a context that uses the node's shared block factory (no per-query memory budget). */
    ComputeContext(
        String sessionId,
        String description,
        String clusterAlias,
        EsqlFlags flags,
        IndexedByShardId<ComputeSearchContext> searchContexts,
        Configuration configuration,
        FoldContext foldCtx,
        Supplier<ExchangeSource> exchangeSourceSupplier,
        Supplier<ExchangeSink> exchangeSinkSupplier
    ) {
        this(
            sessionId,
            description,
            clusterAlias,
            flags,
            searchContexts,
            configuration,
            foldCtx,
            exchangeSourceSupplier,
            exchangeSinkSupplier,
            null
        );
    }

    IndexedByShardId<? extends SearchExecutionContext> searchExecutionContexts() {
        return searchContexts.map(s -> s.searchContext().getSearchExecutionContext());
    }
}
