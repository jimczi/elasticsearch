/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ResolvedIndices {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(ResolvedIndices.class);
    public static final String FROZEN_INDICES_DEPRECATION_MESSAGE = "Searching frozen indices [{}] is deprecated."
        + " Consider cold or frozen tiers in place of frozen indices. The frozen feature will be removed in a feature release.";

    @Nullable
    private final SearchContextId searchContext;
    private final Index[] localIndices;

    private final String[] localIndicesNames;
    private final Map<Index, IndexMetadata> localIndicesMetadata;
    private final Map<String, OriginalIndices> remoteIndices;

    private ResolvedIndices(Map<Index, IndexMetadata> localIndices, Map<String, OriginalIndices> remoteIndices) {
        this(localIndices, remoteIndices, null);
    }

    private ResolvedIndices(
        Map<Index, IndexMetadata> localIndices,
        Map<String, OriginalIndices> remoteIndices,
        @Nullable SearchContextId searchContext
    ) {
        this.localIndices = localIndices.keySet().toArray(Index[]::new);
        this.localIndicesNames = localIndices.keySet().toArray(String[]::new);
        this.localIndicesMetadata = localIndices;
        this.remoteIndices = remoteIndices;
        this.searchContext = searchContext;
    }

    public Index[] getLocalIndices() {
        return localIndices;
    }

    /**
     *
     */
    public @Nullable SearchContextId getSearchContext() {
        return searchContext;
    }

    public String[] getLocalIndexNames() {
        return localIndicesNames;
    }

    /**
     * TODO
     */
    public Map<String, OriginalIndices> getRemoteClusters() {
        return remoteIndices;
    }

    /**
     * TODO Entries with null metadata represent deleted indices
     */
    public Map<Index, IndexMetadata> getIndicesMetadata() {
        return localIndicesMetadata;
    }

    /**
     * TODO
     */
    public static ResolvedIndices resolveWithIndicesRequest(
        ClusterState clusterState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        IndicesRequest request,
        long startInMillis
    ) {
        final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
            request.indicesOptions(),
            request.indices()
        );
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        Index[] indices = localIndices == null
            ? new Index[0]
            : indexNameExpressionResolver.concreteIndices(clusterState, localIndices, startInMillis);
        Map<Index, IndexMetadata> localIndicesMetadata = resolveLocalIndexMetadata(clusterState, indices);
        return new ResolvedIndices(localIndicesMetadata, remoteClusterIndices);
    }

    /**
     * TODO
     */
    public static ResolvedIndices resolveWithPIT(
        ClusterState clusterState,
        NamedWriteableRegistry namedWriteableRegistry,
        PointInTimeBuilder pit,
        IndicesOptions indicesOptions
    ) {
        final SearchContextId searchContext = pit.getSearchContextId(namedWriteableRegistry);
        final Map<String, Set<Index>> indices = new HashMap<>();
        for (Map.Entry<ShardId, SearchContextIdForNode> entry : searchContext.shards().entrySet()) {
            String clusterAlias = entry.getValue().getClusterAlias() == null
                ? RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY
                : entry.getValue().getClusterAlias();
            indices.computeIfAbsent(clusterAlias, k -> new HashSet<>()).add(entry.getKey().getIndex());
        }

        Map<String, OriginalIndices> remoteClusterIndices = indices.entrySet()
            .stream()
            .filter(e -> e.getKey().equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new OriginalIndices(e.getValue().toArray(String[]::new), indicesOptions)));
        final Index[] localIndices = indices.getOrDefault(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, Set.of()).toArray(Index[]::new);
        Map<Index, IndexMetadata> localIndicesMetadata = resolveLocalIndexMetadata(clusterState, localIndices);
        return new ResolvedIndices(localIndicesMetadata, remoteClusterIndices);
    }

    private static Map<Index, IndexMetadata> resolveLocalIndexMetadata(ClusterState clusterState, Index[] localIndices) {
        Map<Index, IndexMetadata> result = new HashMap<>();
        List<String> frozenIndices = null;
        for (var index : localIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexMetadata != null && indexMetadata.getSettings().getAsBoolean("index.frozen", false)) {
                if (frozenIndices == null) {
                    frozenIndices = new ArrayList<>();
                }
                frozenIndices.add(index.getName());
            }
            result.put(index, clusterState.metadata().index(index));
        }
        if (frozenIndices != null) {
            DEPRECATION_LOGGER.warn(
                DeprecationCategory.INDICES,
                "search-frozen-indices",
                FROZEN_INDICES_DEPRECATION_MESSAGE,
                String.join(",", frozenIndices)
            );
        }
        return result;
    }
}
