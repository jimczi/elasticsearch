/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class QueryAdmissionTests extends ESTestCase {

    public void testDisabledAdmitsEverything() {
        QueryAdmission admission = admission(Settings.builder().put(QueryAdmission.ENABLED.getKey(), false).build());
        for (int i = 0; i < 5; i++) {
            assertTrue(admit(admission, "q" + i, true).isDone());
        }
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    public void testPartsThatMayNotWaitNeverWait() {
        QueryAdmission admission = admission(enabled(1, 10));
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> reduce = admit(admission, "b", false);
        assertTrue(reduce.isDone());
        assertThat(admission.stats().runningQueries(), equalTo(1));
        a.close();
        reduce.actionGet().close();
    }

    public void testQueriesWaitAndPartsOfRunningQueriesPass() {
        QueryAdmission admission = admission(enabled(1, 10));
        Releasable a1 = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> b = admit(admission, "b", true);
        assertFalse(b.isDone());
        assertThat(admission.stats().waitingQueries(), equalTo(1));

        Releasable a2 = admit(admission, "a", true).actionGet();
        assertThat(admission.stats().runningQueries(), equalTo(1));

        a1.close();
        assertFalse("a still runs a part", b.isDone());
        a2.close();
        assertTrue(b.isDone());
        assertThat(admission.stats().waitingQueries(), equalTo(0));

        Releasable permit = b.actionGet();
        permit.close();
        permit.close();
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    public void testWaitingPartOfARunningQueryPassesWhenTheNodeIsFull() {
        QueryAdmission admission = admission(enabled(1, 10));
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> b = admit(admission, "b", true);
        Releasable a2 = admit(admission, "a", true).actionGet();
        a.close();
        a2.close();
        // b runs now; another of its parts passes while c waits.
        PlainActionFuture<Releasable> c = admit(admission, "c", true);
        PlainActionFuture<Releasable> b2 = admit(admission, "b", true);
        assertTrue(b2.isDone());
        assertFalse(c.isDone());
        b.actionGet().close();
        b2.actionGet().close();
        c.actionGet().close();
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    public void testPartsOfAWaitingQueryWaitTogether() {
        QueryAdmission admission = admission(enabled(1, 1));
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> b1 = admit(admission, "b", true);
        PlainActionFuture<Releasable> b2 = admit(admission, "b", true);
        assertFalse(b2.isDone());
        assertThat(admission.stats().waitingQueries(), equalTo(1));
        expectThrows(EsRejectedExecutionException.class, admit(admission, "c", true)::actionGet);

        a.close();
        assertTrue(b1.isDone());
        assertTrue(b2.isDone());
        assertThat(admission.stats().runningQueries(), equalTo(1));
        b1.actionGet().close();
        assertThat(admission.stats().runningQueries(), equalTo(1));
        b2.actionGet().close();
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    public void testMetrics() {
        AtomicLong now = new AtomicLong();
        QueryAdmission admission = new QueryAdmission(
            THREADS,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            THREAD_CONTEXT,
            new ClusterSettings(enabled(1, 1), Set.copyOf(QueryAdmission.settings())),
            now::get
        );
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        admission.registerMetrics(meterRegistry);
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> b = admit(admission, "b", true);
        expectThrows(EsRejectedExecutionException.class, admit(admission, "c", true)::actionGet);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(7));
        a.close();
        b.actionGet().close();

        var recorder = meterRegistry.getRecorder();
        List<Measurement> waits = recorder.getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.search.query_admission.wait_time.histogram");
        assertThat(waits.stream().map(Measurement::getLong).toList(), contains(7L));
        recorder.collect();
        assertThat(
            lastValue(recorder.getMeasurements(InstrumentType.LONG_ASYNC_COUNTER, "es.search.query_admission.rejected.total")),
            equalTo(1L)
        );
        assertThat(
            lastValue(recorder.getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.search.query_admission.queries.running.current")),
            equalTo(0L)
        );
    }

    private static long lastValue(List<Measurement> measurements) {
        return measurements.getLast().getLong();
    }

    public void testDefaultLimit() {
        QueryAdmission admission = admission(enabled(0, 10));
        for (int i = 0; i < THREADS; i++) {
            assertTrue(admit(admission, "q" + i, true).isDone());
        }
        assertFalse(admit(admission, "one-too-many", true).isDone());
    }

    public void testRejectsWhenTooManyQueriesWait() {
        QueryAdmission admission = admission(enabled(1, 0));
        Releasable a = admit(admission, "a", true).actionGet();
        expectThrows(EsRejectedExecutionException.class, admit(admission, "b", true)::actionGet);
        assertThat(admission.stats().rejected(), equalTo(1L));
        a.close();
    }

    public void testCancelledPartStopsWaiting() {
        QueryAdmission admission = admission(enabled(1, 10));
        Releasable a = admit(admission, "a", true).actionGet();
        CancellableTask task = task();
        PlainActionFuture<Releasable> b = new PlainActionFuture<>();
        admission.admit("b", true, task, b);
        TaskCancelHelper.cancel(task, "test");
        expectThrows(TaskCancelledException.class, b::actionGet);
        assertThat(admission.stats().waitingQueries(), equalTo(0));
        a.close();
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    public void testWaiterResumesInItsOwnContext() {
        QueryAdmission admission = admission(enabled(1, 10));
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<String> user = new PlainActionFuture<>();
        try (var ignored = THREAD_CONTEXT.stashContext()) {
            THREAD_CONTEXT.putHeader("user", "alice");
            admission.admit("b", true, task(), user.delegateFailureAndWrap((l, permit) -> {
                l.onResponse(THREAD_CONTEXT.getHeader("user"));
                permit.close();
            }));
        }
        try (var ignored = THREAD_CONTEXT.stashContext()) {
            THREAD_CONTEXT.putHeader("user", "bob");
            a.close();
        }
        assertThat(user.actionGet(), equalTo("alice"));
    }

    public void testRaisingTheLimitAdmitsWaiters() {
        ClusterSettings clusterSettings = new ClusterSettings(enabled(1, 10), Set.copyOf(QueryAdmission.settings()));
        QueryAdmission admission = admission(clusterSettings);
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> b = admit(admission, "b", true);
        assertFalse(b.isDone());
        clusterSettings.applySettings(Settings.builder().put(enabled(1, 10)).put(QueryAdmission.MAX_RUNNING_QUERIES.getKey(), 2).build());
        assertTrue(b.isDone());
        b.actionGet().close();
        a.close();
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    public void testDisablingAdmitsWaiters() {
        ClusterSettings clusterSettings = new ClusterSettings(enabled(1, 10), Set.copyOf(QueryAdmission.settings()));
        QueryAdmission admission = admission(clusterSettings);
        Releasable a = admit(admission, "a", true).actionGet();
        PlainActionFuture<Releasable> b = admit(admission, "b", true);
        assertFalse(b.isDone());
        clusterSettings.applySettings(Settings.builder().put(enabled(1, 10)).put(QueryAdmission.ENABLED.getKey(), false).build());
        assertTrue(b.isDone());
        b.actionGet().close();
        a.close();
        assertThat(admission.stats().runningQueries(), equalTo(0));
    }

    /**
     * Parts of a few queries are admitted, released, rejected and cancelled from many threads, and waiters resume on a
     * real pool. The limit on running queries always holds, every part completes once, and nothing is left running.
     */
    public void testConcurrentParts() throws Exception {
        int maxRunning = between(1, 4);
        int threads = between(2, 8);
        int partsPerThread = between(10, 100);
        boolean[][] mayWait = new boolean[threads][partsPerThread];
        boolean[][] cancel = new boolean[threads][partsPerThread];
        String[][] keys = new String[threads][partsPerThread];
        CancellableTask[][] tasks = new CancellableTask[threads][partsPerThread];
        for (int i = 0; i < threads; i++) {
            for (int p = 0; p < partsPerThread; p++) {
                mayWait[i][p] = randomBoolean();
                cancel[i][p] = rarely();
                keys[i][p] = "q" + between(0, 6);
                tasks[i][p] = task();
            }
        }
        ExecutorService resumer = EsExecutors.newFixed(
            "resume",
            between(1, 4),
            -1,
            EsExecutors.daemonThreadFactory("test", "resume"),
            THREAD_CONTEXT,
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
        try {
            QueryAdmission admission = new QueryAdmission(
                THREADS,
                resumer,
                THREAD_CONTEXT,
                new ClusterSettings(enabled(maxRunning, between(1, 50)), Set.copyOf(QueryAdmission.settings())),
                System::nanoTime
            );
            AtomicInteger errors = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(threads * partsPerThread);
            runInParallel(threads, i -> {
                for (int p = 0; p < partsPerThread; p++) {
                    AtomicBoolean completed = new AtomicBoolean();
                    Runnable complete = () -> {
                        if (completed.compareAndSet(false, true)) {
                            done.countDown();
                        } else {
                            errors.incrementAndGet();
                        }
                    };
                    CancellableTask task = tasks[i][p];
                    admission.admit(keys[i][p], mayWait[i][p], task, ActionListener.wrap(permit -> {
                        if (admission.stats().runningQueries() > maxRunning) {
                            errors.incrementAndGet();
                        }
                        permit.close();
                        complete.run();
                    }, e -> {
                        if ((e instanceof EsRejectedExecutionException || e instanceof TaskCancelledException) == false) {
                            errors.incrementAndGet();
                        }
                        complete.run();
                    }));
                    if (cancel[i][p]) {
                        TaskCancelHelper.cancel(task, "test");
                    }
                }
            });
            safeAwait(done);
            assertThat(errors.get(), equalTo(0));
            QueryAdmission.Stats stats = admission.stats();
            assertThat(stats.runningQueries(), equalTo(0));
            assertThat(stats.waitingQueries(), equalTo(0));
        } finally {
            terminate(resumer);
        }
    }

    private static final int THREADS = 3;
    private static final ThreadContext THREAD_CONTEXT = new ThreadContext(Settings.EMPTY);

    private static QueryAdmission admission(Settings settings) {
        return admission(new ClusterSettings(settings, Set.copyOf(QueryAdmission.settings())));
    }

    private static QueryAdmission admission(ClusterSettings clusterSettings) {
        return new QueryAdmission(THREADS, EsExecutors.DIRECT_EXECUTOR_SERVICE, THREAD_CONTEXT, clusterSettings, System::nanoTime);
    }

    private static Settings enabled(int maxRunning, int maxWaiting) {
        return Settings.builder()
            .put(QueryAdmission.ENABLED.getKey(), true)
            .put(QueryAdmission.MAX_RUNNING_QUERIES.getKey(), maxRunning)
            .put(QueryAdmission.MAX_WAITING_QUERIES.getKey(), maxWaiting)
            .build();
    }

    private static PlainActionFuture<Releasable> admit(QueryAdmission admission, String queryId, boolean mayWait) {
        PlainActionFuture<Releasable> future = new PlainActionFuture<>();
        admission.admit(queryId, mayWait, task(), future);
        return future;
    }

    private static CancellableTask task() {
        return new CancellableTask(randomNonNegativeLong(), "transport", "action", "", TaskId.EMPTY_TASK_ID, Map.of());
    }
}
