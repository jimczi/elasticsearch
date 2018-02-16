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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class CompositeAggregator extends DeferableBucketAggregator {
    private final int size;
    private final CompositeValuesSourceConfig[] sources;
    private final List<String> sourceNames;
    private final List<DocValueFormat> formats;
    private final boolean canEarlyTerminate;

    private final CompositeValuesCollectorQueue queue;

    CompositeAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                        int size, CompositeValuesSourceConfig[] sources, CompositeKey rawAfterKey) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.size = size;
        this.sources = sources;
        this.sourceNames = Arrays.stream(sources).map(CompositeValuesSourceConfig::name).collect(Collectors.toList());
        this.formats = Arrays.stream(sources).map(CompositeValuesSourceConfig::format).collect(Collectors.toList());
        this.queue = new CompositeValuesCollectorQueue(context.searcher().getIndexReader(), sources, size);
        if (rawAfterKey != null) {
            queue.setAfter(rawAfterKey.values());
        }
        this.canEarlyTerminate = Arrays.stream(sources)
            .allMatch(CompositeValuesSourceConfig::canEarlyTerminate);
    }

    boolean canEarlyTerminate() {
        return canEarlyTerminate;
    }

    private int[] getReverseMuls() {
        return Arrays.stream(sources).mapToInt(CompositeValuesSourceConfig::reverseMul).toArray();
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        // Always defers the execution of sub-aggregators
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        return new BestCompositeBucketsDeferringCollector(context, queue, descendsFromGlobalAggregator(parent()));
    }

    @Override
    public InternalAggregation buildAggregation(long zeroBucket) throws IOException {
        assert zeroBucket == 0L;
        consumeBucketsAndMaybeBreak(queue.size());

        // Replay all documents that contain at least one top bucket (collected during the first pass).
        runDeferredCollections(new long[0]);

        int num = Math.min(size, queue.size());
        final InternalComposite.InternalBucket[] buckets = new InternalComposite.InternalBucket[num];
        final int[] reverseMuls = getReverseMuls();
        int pos = 0;
        for (int slot : queue.getSortedSlot()) {
            CompositeKey key = queue.toCompositeKey(slot);
            InternalAggregations aggs = bucketAggregations(slot);
            int docCount = queue.getDocCount(slot);
            buckets[pos++] = new InternalComposite.InternalBucket(sourceNames, formats, key, reverseMuls, docCount, aggs);
        }
        CompositeKey lastBucket = num > 0 ? buckets[num-1].getRawKey() : null;
        return new InternalComposite(name, size, sourceNames, formats, Arrays.asList(buckets), lastBucket, reverseMuls,
            pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        final int[] reverseMuls = getReverseMuls();
        return new InternalComposite(name, size, sourceNames, formats, Collections.emptyList(), null, reverseMuls,
            pipelineAggregators(), metaData());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final CompositeValuesSource.Collector inner = queue.getLeafCollector(ctx, getFirstPassCollector(sub));
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long zeroBucket) throws IOException {
                assert zeroBucket == 0L;
                inner.collect(doc);
            }
        };
    }

    /**
     * The first pass selects the top composite buckets from all matching documents.
     */
    private CompositeValuesSource.Collector getFirstPassCollector(LeafBucketCollector sub) {
        return doc -> {
            int bucket = queue.addIfCompetitive(canEarlyTerminate);
            if (bucket != -1) {
                sub.collect(doc, bucket);
            }
        };
    }
}

