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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DelayedAggregationBuilder extends AbstractAggregationBuilder<DelayedAggregationBuilder> {
    public static final String NAME = "delay";

    private String index;
    private TimeValue delay;

    public DelayedAggregationBuilder(String name, String index, TimeValue delay) {
        super(name);
        this.index = index;
        this.delay = delay;
    }

    public DelayedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.delay = in.readTimeValue();
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new DelayedAggregationBuilder(name, index, delay);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeTimeValue(delay);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("value", delay.toString());
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<DelayedAggregationBuilder, String> PARSER =
        new ConstructingObjectParser<>(NAME, false,
            (args, name) -> new DelayedAggregationBuilder(name, (String) args[0],
                TimeValue.parseTimeValue((String) args[1], "value")));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("index"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("value"));
    }

    public static DelayedAggregationBuilder fromXContent(String aggName, XContentParser parser) {
        try {
            return PARSER.apply(parser, aggName);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent,
                                        AggregatorFactories.Builder subfactoriesBuilder) throws IOException {
        final FilterAggregationBuilder filterAgg = new FilterAggregationBuilder(name, QueryBuilders.matchAllQuery());
        filterAgg.subAggregations(subfactoriesBuilder);
        final AggregatorFactory factory = filterAgg.build(queryShardContext, parent);
        return new AggregatorFactory(name, queryShardContext, parent, subfactoriesBuilder, metaData) {
            @Override
            protected Aggregator createInternal(SearchContext searchContext,
                                                Aggregator parent,
                                                boolean collectsFromSingleBucket,
                                                List<PipelineAggregator> pipelineAggregators,
                                                Map<String, Object> metaData) throws IOException {
                if (Regex.simpleMatch(index, queryShardContext.index().getName())) {
                    long start = searchContext.getRelativeTimeInMillis();
                    long sleepTime = Math.min(delay.getMillis(), 100);
                    do {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                        if (searchContext.isCancelled()) {
                            break;
                        }
                    } while (searchContext.getRelativeTimeInMillis() - start < delay.getMillis());
                }
                return factory.create(searchContext, parent, collectsFromSingleBucket);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DelayedAggregationBuilder that = (DelayedAggregationBuilder) o;
        return Objects.equals(index, that.index) &&
            Objects.equals(delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index, delay);
    }
}
