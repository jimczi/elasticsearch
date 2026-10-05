/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Drives the search paths that build their own terms rather than going through a field type: highlighting and the term
 * vectors API. {@link SliceTermsVerifier} is interposed under assertions, so a path that reached the terms dictionary
 * without the slice fails here with the field and term named rather than silently matching nothing.
 *
 * <p>{@code more_like_this} is knowingly left out: it builds term queries straight from the analysed text and the
 * verifier reports it, but slice indices are not expected to support it.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class SliceSearchPathsIT extends ESIntegTestCase {

    private static final String INDEX = "slices";

    @Before
    public void setUpSliceIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put("index.number_of_shards", 1).put(IndexSettings.SLICE_ENABLED.getKey(), true)
            ).setMapping("body", "type=text,term_vector=with_positions_offsets", "kw", "type=keyword")
        );
        ensureGreen(INDEX);
        indexInSlice("a", "the quick brown fox jumps", "alpha");
        indexInSlice("b", "the quick red fox sleeps", "beta");
        indexInSlice("c", "a lazy brown dog waits", "gamma");
        refresh(INDEX);
    }

    private void indexInSlice(String slice, String body, String keyword) {
        client().index(new IndexRequest(INDEX).source(Map.of("body", body, "kw", keyword)).routing(slice).setRoutingFromSlice(true))
            .actionGet();
    }

    /** A highlighter re-analyses the text and matches it against the index. */
    public void testHighlightingOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("body", "brown")).highlighter(new HighlightBuilder().field("body"))
        );
        builder.request().searchSlice("a");
        SearchResponse response = builder.get();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            final var highlight = response.getHits().getHits()[0].getHighlightFields().get("body");
            assertNotNull("the highlighter produced no field", highlight);
            // The fragment must show the plain term, not the slice-prefixed one.
            assertThat(highlight.fragments()[0].string(), containsString("<em>brown</em>"));
        } finally {
            response.decRef();
        }
    }

    /** The fast vector highlighter reads the stored term vectors, which carry the slice. */
    public void testFastVectorHighlightingOnASliceIndex() {
        final var builder = prepareSearch(INDEX).setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("body", "quick"))
                .highlighter(new HighlightBuilder().field("body").highlighterType("fvh"))
        );
        builder.request().searchSlice("a");
        SearchResponse response = builder.get();
        try {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            final var highlight = response.getHits().getHits()[0].getHighlightFields().get("body");
            assertNotNull("the fast vector highlighter produced no field", highlight);
            assertThat(highlight.fragments()[0].string(), containsString("<em>quick</em>"));
        } finally {
            response.decRef();
        }
    }

    /** The term vectors API hands terms back to the user, so they must come without the slice. */
    public void testTermVectorsApiOnASliceIndex() throws java.io.IOException {
        final var search = prepareSearch(INDEX).setQuery(QueryBuilders.matchAllQuery());
        search.request().searchSlice("a");
        final var hits = search.get();
        final String id;
        try {
            id = hits.getHits().getHits()[0].getId();
        } finally {
            hits.decRef();
        }
        final var request = client().prepareTermVectors(INDEX, id).setFieldStatistics(false);
        request.request().routing("a").setRoutingFromSlice(true);
        final var response = request.get();
        final var terms = response.getFields().terms("body");
        assertNotNull(terms);
        final var iterator = terms.iterator();
        boolean sawPlainTerm = false;
        for (var term = iterator.next(); term != null; term = iterator.next()) {
            assertThat("term vectors leaked the slice: " + term.utf8ToString(), term.utf8ToString(), not(containsString("|")));
            sawPlainTerm = true;
        }
        assertTrue("no term vectors were returned", sawPlainTerm);
    }
}
