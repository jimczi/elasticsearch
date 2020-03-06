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
package org.elasticsearch.benchmark.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.query.QuerySearchResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TermsReduceBenchmark {
    private final SearchPhaseController controller = new SearchPhaseController(finalReduce ->
        new InternalAggregation.ReduceContext(null, null, finalReduce));

    @State(Scope.Benchmark)
    public static class TermsList extends AbstractList<InternalAggregations> {
        @Param({"1000"})
        int numShards;

        @Param({"1000"})
        int topNSize;

        @Param({"100000"})
        int cardinality;

        List<InternalAggregations> aggsList;

        @Setup
        public void setup() {
            this.aggsList = new ArrayList<>();
            Random rand = new Random();
            BytesRef[] dict = new BytesRef[cardinality];
            for (int i = 0; i < dict.length; i++) {
                dict[i] = new BytesRef(Long.toString(rand.nextLong()));
            }
            for (int i = 0; i < numShards; i++) {
                aggsList.add(new InternalAggregations(Collections.singletonList(newTerms(rand, dict))));
            }
        }

        private StringTerms newTerms(Random rand, BytesRef[] dict) {
            Set<BytesRef> randomTerms = new HashSet<>();
            for (int i = 0; i < topNSize; i++) {
                randomTerms.add(dict[rand.nextInt(dict.length)]);
            }
            List<StringTerms.Bucket> buckets = new ArrayList<>();
            for (BytesRef term : randomTerms) {
                buckets.add(new StringTerms.Bucket(term,
                    rand.nextInt(10000), InternalAggregations.EMPTY, true, 0L, DocValueFormat.RAW));
            }
            Collections.sort(buckets, Comparator.comparingLong(a -> a.getDocCount()));
            return new StringTerms("terms", BucketOrder.key(true), topNSize, 1, Collections.emptyList(), Collections.emptyMap(),
                DocValueFormat.RAW, numShards, true, 0, buckets, 0);
        }

        @Override
        public InternalAggregations get(int index) {
            return aggsList.get(index);
        }

        @Override
        public int size() {
            return aggsList.size();
        }
    }

    @Param({"5", "16", "32", "128", "512"})
    private int bufferSize;

    @Benchmark
    public  SearchPhaseController.ReducedQueryPhase reduceTopHits(TermsList candidateList) {
        List<QuerySearchResult> shards = new ArrayList<>();
        for (int i = 0; i < candidateList.size(); i++) {
            QuerySearchResult result = new QuerySearchResult();
            result.setShardIndex(0);
            result.from(0);
            result.size(candidateList.topNSize);
            result.aggregations(candidateList.get(i));
            result.setSearchShardTarget(new SearchShardTarget("node",
                new ShardId(new Index("index", "index"), i), null, OriginalIndices.NONE));
            shards.add(result);
        }
        SearchPhaseController.QueryPhaseResultConsumer consumer =
            new SearchPhaseController.QueryPhaseResultConsumer(SearchProgressListener.NOOP,
                controller, shards.size(), bufferSize, false, true, 0, candidateList.topNSize, true);
        for (int i = 0; i < shards.size(); i++) {
            consumer.consumeResult(shards.get(i));
        }
        return consumer.reduce();
    }
}
