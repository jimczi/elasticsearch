/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.YieldingExecutor;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Drives {@link QueryScheduler} with a pool that only runs tasks when the test says so, so every dispatch
 * decision is observable.
 */
public class QuerySchedulerTests extends ESTestCase {
    /** Tasks handed to the pool and not run yet. */
    private final List<Runnable> pool = new ArrayList<>();
    /** Names of the tasks the pool ran, in order. */
    private final List<String> ran = new ArrayList<>();
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    public void testDisabledSubmitsStraightToThePool() {
        QueryScheduler scheduler = scheduler(2, false);
        QueryScheduler.QueryExecutor executor = scheduler.executor("a", pool::add);
        assertThat(executor, not(instanceOf(YieldingExecutor.class)));
        for (int i = 0; i < 5; i++) {
            executor.execute(task("a" + i));
        }
        assertThat(pool.size(), equalTo(5));
        assertThat(scheduler.stats().queries(), equalTo(0));
        executor.close();
    }

    public void testMetrics() {
        AtomicLong now = new AtomicLong();
        QueryScheduler scheduler = new QueryScheduler(1, threadContext, clusterSettings(true), now::get);
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        scheduler.registerMetrics(meterRegistry);
        QueryScheduler.QueryExecutor executor = scheduler.executor("a", pool::add);
        executor.execute(task("a0"));
        executor.execute(task("a1"));
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(5));
        runAll();
        executor.close();

        var recorder = meterRegistry.getRecorder();
        List<Measurement> waits = recorder.getMeasurements(
            InstrumentType.LONG_HISTOGRAM,
            "es.search.query_scheduling.tasks.wait_time.histogram"
        );
        assertThat(waits.stream().map(Measurement::getLong).toList(), contains(0L, 5L));
        recorder.collect();
        assertThat(
            lastValue(recorder.getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.search.query_scheduling.queries.current")),
            equalTo(0L)
        );
    }

    private static long lastValue(List<Measurement> measurements) {
        return measurements.getLast().getLong();
    }

    public void testPoolOnlyReceivesAsManyTasksAsItHasThreads() {
        QueryScheduler scheduler = scheduler(2, true);
        QueryScheduler.QueryExecutor executor = scheduler.executor("a", pool::add);
        YieldingExecutor yielding = (YieldingExecutor) executor;
        for (int i = 0; i < 5; i++) {
            executor.execute(task("a" + i));
        }
        assertThat(pool.size(), equalTo(2));
        assertThat(scheduler.stats().runningTasks(), equalTo(2));
        assertThat(scheduler.stats().waitingTasks(), equalTo(3));
        assertFalse("only the query's own tasks wait", yielding.hasWaitingTasks());
        QueryScheduler.QueryExecutor other = scheduler.executor("b", pool::add);
        other.execute(task("b0"));
        assertTrue("b waits", yielding.hasWaitingTasks());
        assertTrue("a waits too", ((YieldingExecutor) other).hasWaitingTasks());

        runAll();
        assertThat(ran, contains("a0", "a1", "b0", "a2", "a3", "a4"));
        assertThat(scheduler.stats().runningTasks(), equalTo(0));
        assertFalse(yielding.hasWaitingTasks());
        executor.close();
        other.close();
    }

    public void testFreedThreadGoesToTheQueryRunningFewest() {
        QueryScheduler scheduler = scheduler(2, true);
        QueryScheduler.QueryExecutor a = scheduler.executor("a", pool::add);
        QueryScheduler.QueryExecutor b = scheduler.executor("b", pool::add);
        for (int i = 0; i < 4; i++) {
            a.execute(task("a" + i));
        }
        b.execute(task("b0"));
        // a holds both threads. b arrived after all of a's tasks but gets the first thread a frees.
        runAll();
        assertThat(ran, contains("a0", "a1", "b0", "a2", "a3"));
        a.close();
        b.close();
    }

    public void testExecutorsOfAQueryShareItsQueue() {
        QueryScheduler scheduler = scheduler(2, true);
        QueryScheduler.QueryExecutor first = scheduler.executor("a", pool::add);
        QueryScheduler.QueryExecutor second = scheduler.executor("a", pool::add);
        QueryScheduler.QueryExecutor other = scheduler.executor("b", pool::add);
        first.execute(task("a0"));
        second.execute(task("a1"));
        second.execute(task("a2"));
        other.execute(task("b0"));
        // Both executors count as one query running two tasks, so b runs before a2.
        runAll();
        assertThat(ran, contains("a0", "a1", "b0", "a2"));
        first.close();
        second.close();
        other.close();
    }

    public void testRejectionGivesTheThreadBack() {
        AtomicReference<Exception> rejected = new AtomicReference<>();
        Executor rejecting = r -> {
            AbstractRunnable task = (AbstractRunnable) r;
            try {
                task.onRejection(new EsRejectedExecutionException("rejected"));
            } finally {
                task.onAfter();
            }
        };
        QueryScheduler scheduler = new QueryScheduler(1, threadContext, clusterSettings(true), System::nanoTime);
        QueryScheduler.QueryExecutor executor = scheduler.executor("a", rejecting);
        executor.execute(new AbstractRunnable() {
            @Override
            public void onRejection(Exception e) {
                rejected.set(e);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            protected void doRun() {
                fail("rejected tasks do not run");
            }
        });
        assertThat(rejected.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(scheduler.stats().runningTasks(), equalTo(0));
        executor.close();
    }

    public void testDefaultMaxRunningTasks() {
        QueryScheduler scheduler = new QueryScheduler(2, threadContext, clusterSettings(true), System::nanoTime);
        QueryScheduler.QueryExecutor executor = scheduler.executor("a", pool::add);
        for (int i = 0; i < 4; i++) {
            executor.execute(task("a" + i));
        }
        assertThat(pool.size(), equalTo(2));
        runAll();
        executor.close();
    }

    public void testTaskRunsInTheContextItWasSubmittedFrom() {
        QueryScheduler scheduler = scheduler(1, true);
        QueryScheduler.QueryExecutor a = scheduler.executor("a", pool::add);
        QueryScheduler.QueryExecutor b = scheduler.executor("b", pool::add);
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader("user", "alice");
            a.execute(() -> ran.add("a0 as " + threadContext.getHeader("user")));
        }
        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader("user", "bob");
            b.execute(() -> ran.add("b0 as " + threadContext.getHeader("user")));
        }
        // b0 is dispatched when a0 finishes, on a0's thread, but runs as bob.
        runAll();
        assertThat(ran, contains("a0 as alice", "b0 as bob"));
        a.close();
        b.close();
    }

    public void testNeverRunsMoreTasksOnAPoolThanItHasThreads() {
        QueryScheduler scheduler = scheduler(10, true);
        StubPool small = new StubPool(2);
        QueryScheduler.QueryExecutor executor = scheduler.executor("a", small);
        for (int i = 0; i < 5; i++) {
            executor.execute(task("a" + i));
        }
        assertThat(small.submitted.size(), equalTo(2));
        assertThat(scheduler.stats().waitingTasks(), equalTo(3));
        assertThat(scheduler.waitingTasks(small), equalTo(3));
        StubPool other = new StubPool(1);
        QueryScheduler.QueryExecutor otherExecutor = scheduler.executor("b", other);
        otherExecutor.execute(task("b0"));
        otherExecutor.execute(task("b1"));
        assertThat(scheduler.waitingTasks(other), equalTo(1));
        assertThat(scheduler.waitingTasks(small), equalTo(3));
        while (small.submitted.isEmpty() == false) {
            small.submitted.removeFirst().run();
        }
        assertThat(ran, contains("a0", "a1", "a2", "a3", "a4"));
        assertThat(scheduler.waitingTasks(small), equalTo(0));
        while (other.submitted.isEmpty() == false) {
            other.submitted.removeFirst().run();
        }
        assertThat(scheduler.waitingTasks(other), equalTo(0));
        executor.close();
        otherExecutor.close();
    }

    public void testDoesNotYieldToTasksWaitingForAnotherPool() {
        QueryScheduler scheduler = scheduler(10, true);
        StubPool busy = new StubPool(1);
        StubPool other = new StubPool(1);
        YieldingExecutor a = (YieldingExecutor) scheduler.executor("a", busy);
        a.execute(task("a0"));
        QueryScheduler.QueryExecutor b = scheduler.executor("b", busy);
        QueryScheduler.QueryExecutor c = scheduler.executor("c", other);
        c.execute(task("c0"));
        c.execute(task("c1"));
        assertThat(scheduler.waitingTasks(other), equalTo(1));
        assertFalse("c waits for another pool", a.hasWaitingTasks());
        b.execute(task("b0"));
        assertTrue("b waits for a's pool", a.hasWaitingTasks());
        for (StubPool p : List.of(busy, other)) {
            while (p.submitted.isEmpty() == false) {
                p.submitted.removeFirst().run();
            }
        }
        assertThat(ran, contains("a0", "b0", "c0", "c1"));
        ((QueryScheduler.QueryExecutor) a).close();
        b.close();
        c.close();
    }

    /**
     * Queries submit, pause and resume tasks from many threads on real pools. Their queues are small, so a task handed
     * to a thread that is still finishing its previous task may be rejected. The limit on running tasks always holds,
     * every task completes once without error, and the scheduler ends up empty.
     */
    public void testConcurrentQueries() throws Exception {
        int maxRunningTasks = between(1, 6);
        QueryScheduler scheduler = scheduler(maxRunningTasks, true);
        List<ExecutorService> pools = List.of(pool("a", between(1, 4), between(1, 2)), pool("b", between(1, 4), -1));
        int submitters = between(2, 8);
        int tasksPerSubmitter = between(10, 200);
        int[][] runs = new int[submitters][tasksPerSubmitter];
        for (int[] s : runs) {
            for (int t = 0; t < s.length; t++) {
                s[t] = between(1, 5);
            }
        }
        AtomicInteger running = new AtomicInteger();
        AtomicInteger mostRunning = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        try {
            runInParallel(submitters, i -> {
                ExecutorService p = pools.get(i % pools.size());
                QueryScheduler.QueryExecutor executor = scheduler.executor("q" + (i % 3), p);
                CountDownLatch done = new CountDownLatch(tasksPerSubmitter);
                for (int t = 0; t < tasksPerSubmitter; t++) {
                    executor.execute(new YieldingTask(executor, runs[i][t], running, mostRunning, errors, done));
                }
                safeAwait(done);
                executor.close();
            });
            assertThat(errors.get(), equalTo(0));
            assertThat(mostRunning.get(), lessThanOrEqualTo(maxRunningTasks));
            // A task counts down before the pool tells the scheduler it finished.
            assertBusy(() -> {
                QueryScheduler.Stats stats = scheduler.stats();
                assertThat(stats.runningTasks(), equalTo(0));
                assertThat(stats.waitingTasks(), equalTo(0));
                assertThat(stats.queries(), equalTo(0));
            });
            for (ExecutorService p : pools) {
                assertThat(scheduler.waitingTasks(p), equalTo(0));
            }
        } finally {
            terminate(pools.toArray(ExecutorService[]::new));
        }
    }

    /**
     * Runs {@code runs} times, giving its thread back between runs while another task waits, as a driver does.
     */
    private static final class YieldingTask extends AbstractRunnable {
        private final QueryScheduler.QueryExecutor executor;
        private final AtomicInteger running;
        private final AtomicInteger mostRunning;
        private final AtomicInteger errors;
        private final CountDownLatch done;
        private final AtomicBoolean completed = new AtomicBoolean();
        private int runs;

        YieldingTask(
            QueryScheduler.QueryExecutor executor,
            int runs,
            AtomicInteger running,
            AtomicInteger mostRunning,
            AtomicInteger errors,
            CountDownLatch done
        ) {
            this.executor = executor;
            this.runs = runs;
            this.running = running;
            this.mostRunning = mostRunning;
            this.errors = errors;
            this.done = done;
        }

        @Override
        protected void doRun() {
            mostRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                do {
                    runs--;
                } while (runs > 0 && ((YieldingExecutor) executor).hasWaitingTasks() == false);
            } finally {
                running.decrementAndGet();
            }
            if (runs > 0) {
                executor.execute(this);
            } else {
                complete();
            }
        }

        @Override
        public void onRejection(Exception e) {
            complete();
        }

        @Override
        public void onFailure(Exception e) {
            // Thrown here, it would end in the pool, not in the test.
            errors.incrementAndGet();
            complete();
        }

        private void complete() {
            if (completed.compareAndSet(false, true)) {
                done.countDown();
            } else {
                errors.incrementAndGet();
            }
        }
    }

    private ExecutorService pool(String name, int threads, int queueCapacity) {
        return EsExecutors.newFixed(
            name,
            threads,
            queueCapacity,
            EsExecutors.daemonThreadFactory("test", name),
            threadContext,
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
    }

    public void testYieldsToTasksSubmittedToThePoolDirectly() {
        QueryScheduler scheduler = scheduler(1, true);
        StubPool busy = new StubPool(1);
        YieldingExecutor executor = (YieldingExecutor) scheduler.executor("a", busy);
        assertFalse(executor.hasWaitingTasks());
        busy.getQueue().add(() -> {});
        assertTrue(executor.hasWaitingTasks());
        ((QueryScheduler.QueryExecutor) executor).close();
    }

    /**
     * A pool with a fixed number of threads that keeps the tasks the scheduler hands it, so the test runs them.
     */
    @SuppressForbidden(reason = "never starts a thread, so it does not need EsExecutors to report errors")
    private static class StubPool extends ThreadPoolExecutor {
        final List<Runnable> submitted = new ArrayList<>();

        StubPool(int threads) {
            super(threads, threads, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable command) {
            submitted.add(command);
        }
    }

    private QueryScheduler scheduler(int maxRunningTasks, boolean enabled) {
        return new QueryScheduler(maxRunningTasks, threadContext, clusterSettings(enabled), System::nanoTime);
    }

    private static ClusterSettings clusterSettings(boolean enabled) {
        Settings settings = Settings.builder().put(QueryScheduler.ENABLED.getKey(), enabled).build();
        return new ClusterSettings(settings, Set.copyOf(QueryScheduler.settings()));
    }

    private Runnable task(String name) {
        return () -> ran.add(name);
    }

    private void runAll() {
        while (pool.isEmpty() == false) {
            pool.removeFirst().run();
        }
    }
}
