/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;
import org.elasticsearch.xpack.rollup.Rollup;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class RollupActionSingleNodeTests extends ESSingleNodeTestCase {

    private static final DateFormatter DATE_FORMATTER = DateFormatter.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private String index;
    private String rollupIndex;
    private long startTime;
    private int docCount;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Rollup.class, AnalyticsPlugin.class, AggregateMetricMapperPlugin.class);
    }

    @Before
    public void setup() {
        index = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        rollupIndex = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        startTime = randomLongBetween(946769284000L, 1607470084000L); // random date between 2000-2020
        docCount = randomIntBetween(10, 1000);

        client().admin().indices().prepareCreate(index)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).build())
            .setMapping(
                "date_1", "type=date",
                "numeric_1", "type=double",
                "numeric_2", "type=float",
                "categorical_1", "type=keyword",
                "histogram_1", "type=histogram").get();
    }

    public void testDateHistogramGrouping() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.emptyList(), null, rollupIndex);
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("date");
        aggBuilder.field("date_1");
        if (dateHistogramGroupConfig instanceof DateHistogramGroupConfig.FixedInterval) {
            aggBuilder.fixedInterval(dateHistogramGroupConfig.getInterval());
        } else if (dateHistogramGroupConfig instanceof DateHistogramGroupConfig.CalendarInterval) {
            aggBuilder.calendarInterval(dateHistogramGroupConfig.getInterval());
        } else {
            aggBuilder.interval(dateHistogramGroupConfig.getInterval().estimateMillis());
            assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
        }
        assertAggregation(aggBuilder);
    }

    public void testTermsGrouping() {

    }

    public void testHistogramGrouping() {

    }

    public void testMaxMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("max"))), null, rollupIndex);
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new MaxAggregationBuilder("max_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
    }

    public void testMinMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("min"))), null, rollupIndex);
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new MinAggregationBuilder("min_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
    }

    public void testValueCountMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("value_count"))), null, rollupIndex);
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new ValueCountAggregationBuilder("value_count_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
    }

    public void testAvgMetric() throws IOException {
        DateHistogramGroupConfig dateHistogramGroupConfig = randomDateHistogramGroupConfig();
        SourceSupplier sourceSupplier = () -> XContentFactory.jsonBuilder().startObject()
            .field("date_1", randomDateForInterval(dateHistogramGroupConfig.getInterval()))
            .field("numeric_1", randomDouble())
            .endObject();
        RollupActionConfig config = new RollupActionConfig(
            new GroupConfig(dateHistogramGroupConfig, null, null),
            Collections.singletonList(new MetricConfig("numeric_1", Collections.singletonList("avg"))), null, rollupIndex);
        bulkIndex(sourceSupplier);
        rollup(config);
        assertRollupIndex(config);
        DateHistogramAggregationBuilder aggBuilder = dateHistogramBuilder("date", dateHistogramGroupConfig);
        aggBuilder.subAggregation(new ValueCountAggregationBuilder("avg_numeric_1").field("numeric_1"));
        assertAggregation(aggBuilder);
    }

    public void testAllGroupingAllMetrics() {

    }

    private DateHistogramAggregationBuilder dateHistogramBuilder(String name, DateHistogramGroupConfig dateHistogramGroupConfig) {
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder(name);
        aggBuilder.field(dateHistogramGroupConfig.getField());
        aggBuilder.timeZone(ZoneId.of(dateHistogramGroupConfig.getTimeZone()));
        if (dateHistogramGroupConfig instanceof DateHistogramGroupConfig.FixedInterval) {
            aggBuilder.fixedInterval(dateHistogramGroupConfig.getInterval());
        } else if (dateHistogramGroupConfig instanceof DateHistogramGroupConfig.CalendarInterval) {
            aggBuilder.calendarInterval(dateHistogramGroupConfig.getInterval());
        } else {
            aggBuilder.interval(dateHistogramGroupConfig.getInterval().estimateMillis());
            assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] or [calendar_interval] in the future.");
        }
        return aggBuilder;
    }

    private DateHistogramGroupConfig randomDateHistogramGroupConfig() {
        final String timezone = randomBoolean() ? randomDateTimeZone().toString() : null;
        int i = randomIntBetween(0,1); // TODO(talevy): move to 0-2 once calendar interval is supported
        final DateHistogramInterval interval;
        switch (i) {
            case 0:
                interval = new DateHistogramInterval(randomTimeValue(2, 1000, new String[]{"d", "h", "ms", "s", "m"}));
                return new DateHistogramGroupConfig.FixedInterval("date_1", interval, null, timezone);
            // TODO(talevy): uncomment once calendar intervals are supported
            //     case 1:
            //         interval = new DateHistogramInterval(randomTimeValue(1,1, "m", "h", "d", "w"));
            //         return new DateHistogramGroupConfig.CalendarInterval("date_1", interval, null, timezone);
            default:
                interval = new DateHistogramInterval(randomTimeValue(2, 1000, new String[]{"d", "h", "ms", "s", "m"}));
                return new DateHistogramGroupConfig("date_1", interval, null, timezone);
        }
    }

    private String randomDateForInterval(DateHistogramInterval interval) {
        final long maxNumBuckets = 10;
        final long endTime = startTime + maxNumBuckets * interval.estimateMillis();
        return DATE_FORMATTER.formatMillis(randomLongBetween(startTime, endTime));
    }

    private void bulkIndex(SourceSupplier sourceSupplier) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < docCount; i++) {
            IndexRequest indexRequest = new IndexRequest(index);
            XContentBuilder source = sourceSupplier.get();
            indexRequest.source(source);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
        assertHitCount(client().prepareSearch(index).setSize(0).get(), docCount);
    }

    private void rollup(RollupActionConfig config) {
        RollupAction.Response rollupResponse = client().execute(RollupAction.INSTANCE, new RollupAction.Request(index, config)).actionGet();
        assertTrue(rollupResponse.isCreated());
    }

    private void assertRollupIndex(RollupActionConfig config) {
        GetIndexResponse indexResponse = client().admin().indices().prepareGetIndex().setIndices(rollupIndex).get();
        assertTrue(true);
        // TODO(talevy): assert doc count
        // TODO(talevy): assert mapping
        // TODO(talevy): assert settings
    }

    private void assertAggregation(AggregationBuilder builder) {
        List<Aggregation> indexAggregations = client().prepareSearch(index).addAggregation(builder).get().getAggregations().asList();
        List<Aggregation> rollupAggregations = client().prepareSearch(rollupIndex).addAggregation(builder).get().getAggregations().asList();
        assertThat(rollupAggregations.size(), equalTo(indexAggregations.size()));
        for (int i = 0; i < rollupAggregations.size(); i++) {
            Aggregation indexAggObj = indexAggregations.get(i);
            Aggregation rollupAggObj = rollupAggregations.get(i);
            if (indexAggObj instanceof InternalDateHistogram && rollupAggObj instanceof InternalDateHistogram) {
                InternalDateHistogram indexAgg = (InternalDateHistogram) indexAggObj;
                InternalDateHistogram rollupAgg = (InternalDateHistogram) rollupAggObj;
                assertThat(rollupAgg.getBuckets().size(), equalTo(indexAgg.getBuckets().size()));
                for (int j = 0; j < rollupAggregations.size(); j++) {
                    assertThat(rollupAgg.getBuckets().get(j).getKey(), equalTo(indexAgg.getBuckets().get(j).getKey()));
                    assertThat(rollupAgg.getBuckets().get(j).getDocCount(), equalTo(indexAgg.getBuckets().get(j).getDocCount()));
                }
            }
        }
    }

    @FunctionalInterface
    public interface SourceSupplier {
        XContentBuilder get() throws IOException;
    }
}

