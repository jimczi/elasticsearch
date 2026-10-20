/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.index.AssertingLeafReader;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.scheduling.QueryAdmission;
import org.elasticsearch.search.scheduling.QueryScheduler;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Runs concurrent queries on several nodes with {@link QueryScheduler} and {@link QueryAdmission} at their tightest.
 * Every shard reader is wrapped in Lucene's asserting reader, which fails when a reader is used on a thread other
 * than the one it was created on. After each test, the framework checks that every search context, exchange and block
 * was released.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class EsqlQuerySchedulingIT extends AbstractEsqlIntegTestCase {

    /**
     * Queries reading doc values by ordinal and by value, postings and stored fields. {@link #createIndex} returns the
     * result of each.
     */
    private static final String[] QUERIES = {
        "FROM test | STATS c = COUNT(*) BY group | STATS total = SUM(c)",
        "FROM test | STATS s = SUM(value)",
        "FROM test | WHERE MATCH(message, \"message\") | STATS s = SUM(value)",
        "FROM test | EVAL l = LENGTH(message) | STATS s = SUM(l)" };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), AssertingReaderPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    /**
     * Small pages, so a driver reads a segment over many iterations and can move to another thread in the middle.
     */
    @Override
    protected QueryPragmas getPragmas() {
        if (canUseQueryPragmas() == false) {
            return super.getPragmas();
        }
        return new QueryPragmas(
            Settings.builder().put(super.getPragmas().getSettings()).put(QueryPragmas.PAGE_SIZE.getKey(), between(1, 16)).build()
        );
    }

    @After
    public void resetSettings() {
        updateClusterSettings(
            Settings.builder()
                .putNull(QueryScheduler.ENABLED.getKey())
                .putNull(QueryScheduler.YIELD_INTERVAL.getKey())
                .putNull(QueryAdmission.ENABLED.getKey())
                .putNull(QueryAdmission.MAX_RUNNING_QUERIES.getKey())
                .putNull(QueryAdmission.MAX_WAITING_QUERIES.getKey())
        );
    }

    /**
     * One running query per node and the shortest yield interval: every query completes with the right result.
     */
    public void testConcurrentQueriesComplete() throws Exception {
        long[] expected = createIndex();
        enable(100);
        assertThat(runConcurrently(between(4, 12), expected, false), equalTo(0));
    }

    /**
     * Every query runs at once with the shortest yield interval, so drivers give their thread back at nearly every
     * iteration and resume on another. Reads of postings, doc values and stored fields stay on the thread that
     * created them.
     */
    public void testDriversMoveBetweenThreads() throws Exception {
        long[] expected = createIndex();
        updateClusterSettings(
            Settings.builder()
                .put(QueryScheduler.ENABLED.getKey(), true)
                .put(QueryScheduler.YIELD_INTERVAL.getKey(), TimeValue.timeValueMillis(1))
        );
        assertThat(runConcurrently(between(8, 16), expected, false), equalTo(0));
    }

    /**
     * No query may wait for admission, so concurrent queries are rejected. Rejected queries release everything they held.
     */
    public void testRejectedQueriesReleaseEverything() throws Exception {
        long[] expected = createIndex();
        enable(0);
        int rejected = 0;
        for (int round = 0; round < 10 && rejected == 0; round++) {
            rejected = runConcurrently(between(8, 16), expected, true);
        }
        assertThat("no query was rejected", rejected, greaterThan(0));
    }

    private long[] createIndex() {
        int docs = between(1000, 5000);
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", between(1, 5))
                    .put("index.number_of_replicas", 0)
                    .put(MockEngineSupport.WRAP_READER_RATIO.getKey(), 1.0d)
            ).setMapping("value", "type=long", "group", "type=keyword", "message", "type=text")
        );
        long sum = 0;
        long length = 0;
        // Several refreshes, so shards have several segments.
        for (int start = 0; start < docs;) {
            int end = Math.min(docs, start + between(1, 500));
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = start; i < end; i++) {
                bulk.add(prepareIndex("test").setSource("value", i, "group", "g" + (i % 7), "message", "message " + i));
                sum += i;
                length += ("message " + i).length();
            }
            bulk.get();
            start = end;
        }
        return new long[] { docs, sum, sum, length };
    }

    /**
     * Wraps shard readers in Lucene's {@link AssertingLeafReader}. Elasticsearch reads stored fields in order through
     * {@link SequentialStoredFieldsLeafReader}, which the asserting reader does not support, so those reads go to the
     * wrapped reader.
     */
    public static class AssertingReaderPlugin extends MockEngineFactoryPlugin {
        @Override
        protected Class<? extends FilterDirectoryReader> getReaderWrapperClass() {
            return AssertingSequentialDirectoryReader.class;
        }
    }

    public static class AssertingSequentialDirectoryReader extends FilterDirectoryReader {
        public AssertingSequentialDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new AssertingSequentialLeafReader(reader);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new AssertingSequentialDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static class AssertingSequentialLeafReader extends SequentialStoredFieldsLeafReader {
        private final LeafReader wrapped;

        AssertingSequentialLeafReader(LeafReader wrapped) {
            super(new AssertingLeafReader(wrapped));
            this.wrapped = wrapped;
        }

        @Override
        public StoredFieldsReader getSequentialStoredFieldsReader() {
            if (wrapped instanceof SequentialStoredFieldsLeafReader reader) {
                return reader.getSequentialStoredFieldsReader();
            }
            return ((CodecReader) wrapped).getFieldsReader().getMergeInstance();
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private void enable(int maxWaitingQueries) {
        updateClusterSettings(
            Settings.builder()
                .put(QueryScheduler.ENABLED.getKey(), true)
                .put(QueryScheduler.YIELD_INTERVAL.getKey(), TimeValue.timeValueMillis(1))
                .put(QueryAdmission.ENABLED.getKey(), true)
                .put(QueryAdmission.MAX_RUNNING_QUERIES.getKey(), 1)
                .put(QueryAdmission.MAX_WAITING_QUERIES.getKey(), maxWaitingQueries)
        );
    }

    /**
     * Runs {@code queries} queries at once and returns how many were rejected. Each returns the right result, or, when
     * {@code rejectionsAllowed}, fails or returns partial results because it was rejected.
     */
    private int runConcurrently(int queries, long[] expected, boolean rejectionsAllowed) throws Exception {
        CyclicBarrier start = new CyclicBarrier(queries);
        AtomicInteger rejected = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        for (int q = 0; q < queries; q++) {
            int kind = q % QUERIES.length;
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    try (EsqlQueryResponse response = run(QUERIES[kind])) {
                        // A shard rejected by admission can make the result partial rather than fail the query.
                        if (rejectionsAllowed && response.isPartial()) {
                            rejected.incrementAndGet();
                        } else {
                            assertThat(getValuesList(response), equalTo(List.of(List.of(expected[kind]))));
                        }
                    }
                } catch (Exception e) {
                    if (rejectionsAllowed && ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
                        rejected.incrementAndGet();
                    } else {
                        failure.compareAndSet(null, e);
                    }
                } catch (AssertionError e) {
                    failure.compareAndSet(null, new RuntimeException(e));
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw failure.get();
        }
        return rejected.get();
    }
}
