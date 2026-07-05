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
