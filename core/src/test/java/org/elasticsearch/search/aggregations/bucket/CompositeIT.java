/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLongBetween;
import static org.hamcrest.Matchers.equalTo;

public class CompositeIT extends ESIntegTestCase {
    public void testRandomDateHistoAndTerms() throws IOException {
        long interval = randomLongBetween(2, TimeValue.timeValueHours(24 * 30).getMillis());
        Rounding dateRounding = Rounding.builder(TimeValue.timeValueMillis(interval))
            .build();
        Set<String> expectedBuckets = new HashSet<>();
        BulkRequestBuilder bulk = client().prepareBulk();
        int numDocs = randomIntBetween(1000, 10000);
        for (int i = 0; i < numDocs; i++) {
            long date = randomNonNegativeLong();
            String term = randomAlphaOfLengthBetween(5, 10);
            bulk.add(
                new IndexRequest("test", "doc")
                    .source("date", date, "term", term, "fake", "fake")
            );
            expectedBuckets.add(term + "-" + Long.toString(dateRounding.round(date)));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        for (SortOrder order : SortOrder.values()) {
            CompositeAggregationBuilder builder = new CompositeAggregationBuilder("stream",
                Arrays.asList(
                    new TermsValuesSourceBuilder("term").field("term.keyword").order(order),
                    new DateHistogramValuesSourceBuilder("date").field("date").interval(interval).order(order)
                )
            );
            testCase(new MatchAllQueryBuilder(), builder, numDocs, expectedBuckets.size());
            builder.aggregateAfter(null);
            testCase(new TermQueryBuilder("fake", "fake"), builder, numDocs, expectedBuckets.size());

            builder = new CompositeAggregationBuilder("stream",
                Arrays.asList(
                    new DateHistogramValuesSourceBuilder("date").field("date").interval(interval).order(order),
                    new TermsValuesSourceBuilder("term").field("term.keyword").order(order)
                )
            );
            testCase(new MatchAllQueryBuilder(), builder, numDocs, expectedBuckets.size());
            builder.aggregateAfter(null);
            testCase(new TermQueryBuilder("fake", "fake"), builder, numDocs, expectedBuckets.size());
        }
    }

    public void testRandomNumericAndTerms() throws IOException {
        Set<String> expectedBuckets = new HashSet<>();
        BulkRequestBuilder bulk = client().prepareBulk();
        int numDocs = randomIntBetween(1000, 10000);
        for (int i = 0; i < numDocs; i++) {
            long value = randomLong();
            String term = randomAlphaOfLengthBetween(5, 10);
            bulk.add(
                new IndexRequest("test", "doc")
                    .source("long", value, "term", term, "fake", "fake")
            );
            expectedBuckets.add(term + "-" + value);
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        for (SortOrder order : SortOrder.values()) {
            CompositeAggregationBuilder builder = new CompositeAggregationBuilder("stream",
                Arrays.asList(
                    new TermsValuesSourceBuilder("term").field("term.keyword").order(order),
                    new TermsValuesSourceBuilder("long").field("long").order(order)
                )
            );
            testCase(new MatchAllQueryBuilder(), builder, numDocs, expectedBuckets.size());
            builder.aggregateAfter(null);
            testCase(new TermQueryBuilder("fake", "fake"), builder, numDocs, expectedBuckets.size());

            builder = new CompositeAggregationBuilder("stream",
                Arrays.asList(
                    new TermsValuesSourceBuilder("long").field("long").order(order),
                    new TermsValuesSourceBuilder("term").field("term.keyword").order(order)
                )
            );
            testCase(new MatchAllQueryBuilder(), builder, numDocs, expectedBuckets.size());
            builder.aggregateAfter(null);
            testCase(new RangeQueryBuilder("long").gte(Long.MIN_VALUE).lte(Long.MAX_VALUE),
                builder, numDocs, expectedBuckets.size());
            builder.aggregateAfter(null);
            testCase(new TermQueryBuilder("fake", "fake"), builder, numDocs, expectedBuckets.size());
        }
    }

    private void testCase(QueryBuilder query, CompositeAggregationBuilder composite, int expectedSumDocCount, int expectedNumBuckets) {
        int size = randomIntBetween(1, expectedNumBuckets-1);
        composite.size(size);
        SearchResponse response =client().prepareSearch("test")
            .setTrackTotalHits(false)
            .setSize(0)
            .setQuery(query)
            .addAggregation(composite)
            .get();
        CompositeAggregation agg = response.getAggregations().get(composite.getName());
        Set<String> buckets = new HashSet<>();
        int docCount = 0;
        while (agg.getBuckets().isEmpty() == false) {
            for (CompositeAggregation.Bucket bucket : agg.getBuckets()) {
                docCount += bucket.getDocCount();
                assertThat(buckets.add(bucket.getKeyAsString()), equalTo(true));
            }
            agg.getBuckets().stream()
                .map((b) -> b.getKeyAsString())
                .forEach(buckets::add);
            composite.aggregateAfter(agg.afterKey());
            response = client().prepareSearch("test")
                .setTrackTotalHits(false)
                .setSize(0)
                .setQuery(query)
                .addAggregation(composite)
                .get();
            agg = response.getAggregations().get(composite.getName());
        }
        assertThat(docCount, equalTo(expectedSumDocCount));
        assertThat(buckets.size(), equalTo(expectedNumBuckets));
    }
}
