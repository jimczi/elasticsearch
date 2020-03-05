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

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchProgressListener;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TopDocsReduceBenchmark {
    private final SearchPhaseController controller = new SearchPhaseController(finalReduce ->
        new InternalAggregation.ReduceContext(null, null, finalReduce));

    @State(Scope.Benchmark)
    public static class TopDocsList extends AbstractList<TopDocs> {
        @Param({"100000"})
        int numShards;

        @Param({"1"})
        int topNSize;

        List<TopDocs> topDocsList;

        @Setup
        public void setup() {
            this.topDocsList = new ArrayList<>();
            for (int i = 0; i < numShards; i++) {
                topDocsList.add(newTopDocs());
            }
        }

        private TopDocs newTopDocs() {
            Random rand = new Random();
            FieldDoc[] fieldDocs = new FieldDoc[topNSize];
            for (int i = 0; i < fieldDocs.length; i++) {
                fieldDocs[i] = new FieldDoc(i, Float.NaN, new Object[] { rand.nextLong() });
            }
            Arrays.sort(fieldDocs, Comparator.comparingLong(a -> (long) a.fields[0]));
            SortField sortField = new SortField("sort", SortField.Type.LONG);
            return new TopFieldDocs(new TotalHits(rand.nextInt(100000),
                TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
                fieldDocs, new SortField[] { sortField });
        }

        @Override
        public TopDocs get(int index) {
            return topDocsList.get(index);
        }

        @Override
        public int size() {
            return topDocsList.size();
        }
    }

    @Param({"2", "32", "512"})
    private int bufferSize;

    @Benchmark
    public  SearchPhaseController.ReducedQueryPhase reduceTopHits(TopDocsList candidateList) {
        List<QuerySearchResult> shards = new ArrayList<>();
        for (int i = 0; i < candidateList.size(); i++) {
            QuerySearchResult result = new QuerySearchResult();
            result.setShardIndex(0);
            result.from(0);
            result.size(candidateList.topNSize);
            //result.aggregations(aggsList.get(i));
            result.topDocs(new TopDocsAndMaxScore(candidateList.get(0), Float.NaN),
                new DocValueFormat[]{ DocValueFormat.RAW });
            result.setSearchShardTarget(new SearchShardTarget("node",
                new ShardId(new Index("index", "index"), i), null, OriginalIndices.NONE));
            shards.add(result);
        }
        SearchPhaseController.QueryPhaseResultConsumer consumer =
            new SearchPhaseController.QueryPhaseResultConsumer(SearchProgressListener.NOOP,
                controller, shards.size(), bufferSize, true, false, 0, candidateList.topNSize, true);
        int pos = 0;
        for (int i = 0; i < shards.size(); i++) {
            consumer.consumeResult(shards.get(i));
        }
        return consumer.reduce();
    }
}
