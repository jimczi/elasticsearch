/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Proves a slice-partitioned index (one segment per tenant, via {@code index.slice.enabled}) and an ordinary index
 * run side by side on the same node without interference: the slice engine's per-tenant machinery is
 * per-shard/per-engine and gated on the setting, so an ordinary shard is entirely unaffected. Requires the patched
 * lucene-core (the slice index uses the keyed-DWPT); the slice feature flag is enabled by default in snapshot builds.
 */
public class SliceMixedShardTests extends ESSingleNodeTestCase {

    private void indexPlain(String index, String value) {
        var r = client().index(new IndexRequest(index).source("f", value)).actionGet();
        r.decRef();
    }

    private void indexSlice(String index, String slice, String value) {
        // _slice provenance: routing derived from the _slice API parameter (required when slice is enabled).
        var r = client().index(new IndexRequest(index).routing(slice).setRoutingFromSlice(true).source("f", value)).actionGet();
        r.decRef();
    }

    private void assertCount(SearchRequest request, long expected) throws Exception {
        assertResponse(client().search(request), response -> assertEquals(expected, response.getHits().getTotalHits().value()));
    }

    private static SearchRequest matchAll(String index) {
        return new SearchRequest(index).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
    }

    /**
     * Diagnostic: does the bounded pool's {@code acquireSliceSearcher} (which opens from the last COMMIT) see data
     * made visible by an NRT {@code refresh} but not yet flushed? On stateful this is expected to LAG (refresh != commit),
     * which is why the general search path must not naively use the commit-based pool.
     */
    public void testAcquireSliceSearcherVsNrtRefresh() throws Exception {
        createIndex(
            "slicediag",
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.slice.enabled", true).build()
        );
        for (int i = 0; i < 3; i++) {
            indexSlice("slicediag", "tenantA", "a" + i);
        }
        client().admin().indices().prepareRefresh("slicediag").get(); // NRT refresh, no flush

        final IndexShard shard = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex("slicediag")).getShard(0);
        final int commitBased;
        try (var searcher = shard.acquireSliceSearcher("diag", "tenantA")) {
            commitBased = searcher.getIndexReader().numDocs();
        }
        // A normal (NRT) search sees all 3.
        assertCount(matchAll("slicediag").searchSlice("tenantA"), 3);
        // Record what the commit-based pool sees; a flush should reconcile it.
        logger.info("SLICE POOL NRT-DIAG: commit-based pool saw {} of 3 refreshed docs before flush", commitBased);
        client().admin().indices().prepareFlush("slicediag").get();
        try (var searcher = shard.acquireSliceSearcher("diag", "tenantA")) {
            assertEquals("after flush the commit-based pool must see all docs", 3, searcher.getIndexReader().numDocs());
        }
    }

    public void testSlicedAndNonSlicedIndicesCoexistOnOneNode() throws Exception {
        createIndex("ordinary", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        createIndex(
            "sliced",
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).put("index.slice.enabled", true).build()
        );

        for (int i = 0; i < 5; i++) {
            indexPlain("ordinary", "v" + i);
        }
        for (int i = 0; i < 3; i++) {
            indexSlice("sliced", "tenantA", "a" + i);
        }
        for (int i = 0; i < 2; i++) {
            indexSlice("sliced", "tenantB", "b" + i);
        }
        client().admin().indices().prepareRefresh("ordinary", "sliced").get();

        // Both indices are independently searchable on the same node — the slice engine did not disturb the ordinary one.
        assertCount(matchAll("ordinary"), 5);
        assertCount(matchAll("sliced"), 5); // no _slice -> _slice=_all -> all tenants

        // A per-tenant (_slice) search on the slice index returns only that tenant's docs.
        assertCount(matchAll("sliced").searchSlice("tenantA"), 3);
        assertCount(matchAll("sliced").searchSlice("tenantB"), 2);

        // Ongoing coexistence: more writes to both after searching, no shared/stuck state.
        indexPlain("ordinary", "v5");
        indexSlice("sliced", "tenantA", "a3");
        client().admin().indices().prepareRefresh("ordinary", "sliced").get();
        assertCount(matchAll("ordinary"), 6);
        assertCount(matchAll("sliced"), 6);
        assertCount(matchAll("sliced").searchSlice("tenantA"), 4);
    }
}
