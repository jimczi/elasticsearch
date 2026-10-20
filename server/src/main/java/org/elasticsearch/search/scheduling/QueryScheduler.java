/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.YieldingExecutor;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Shares the processors of a node between search queries. Each query queues its tasks in a {@link QueryExecutor}, which
 * hands a task to its pool only while fewer than {@link #MAX_RUNNING_TASKS} tasks run, and never more than the pool has
 * threads. The next task to run comes from the query running the fewest tasks. Tasks that can pause yield their
 * thread every {@link #YIELD_INTERVAL} while another task waits, see {@link YieldingExecutor}.
 * <p>
 * While disabled, tasks go straight to their pool. Behind {@link #FEATURE_FLAG}, which also covers {@link QueryAdmission}.
 */
public final class QueryScheduler {
    private static final Logger logger = LogManager.getLogger(QueryScheduler.class);

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("search_query_scheduling");

    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "search.query_scheduling.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> YIELD_INTERVAL = Setting.timeSetting(
        "search.query_scheduling.yield_interval",
        TimeValue.timeValueMillis(100),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Tasks running at once. {@code 0} means the size of the {@code search} pool.
     */
    public static final Setting<Integer> MAX_RUNNING_TASKS = Setting.intSetting(
        "search.query_scheduling.max_running_tasks",
        0,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The settings of this class, none without {@link #FEATURE_FLAG}.
     */
    public static List<Setting<?>> settings() {
        return FEATURE_FLAG.isEnabled() ? List.of(ENABLED, YIELD_INTERVAL, MAX_RUNNING_TASKS) : List.of();
    }

    private final ThreadContext threadContext;
    private final LongSupplier nanoTime;

    private volatile boolean enabled;
    private volatile TimeValue yieldInterval;
    private volatile int maxRunningTasks;

    /** Queries with an open executor or a task, by id. Guarded by {@code this}. */
    private final Map<String, QueryState> queries = new HashMap<>();
    /** Running tasks, by the pool that runs them. Guarded by {@code this}. */
    private final Map<Executor, Integer> runningByPool = new IdentityHashMap<>();
    /** Tasks waiting to run, by the pool that will run them. Changed under {@code this}, read without it. */
    private final Map<Executor, AtomicInteger> waitingByPool = new ConcurrentHashMap<>();
    /** Running tasks. Changed under {@code this}, read without it. */
    private volatile int running;
    /** Tasks waiting to run. */
    private final AtomicInteger waiting = new AtomicInteger();
    /** Counts dispatches to break ties between queries. Guarded by {@code this}. */
    private long dispatches;
    /** Requests to {@link #dispatch()}, so that one thread at a time dispatches, without recursing. */
    private final AtomicInteger dispatchRequests = new AtomicInteger();
    /** Time each dispatched task waited to run. */
    private volatile LongHistogram waitTime = LongHistogram.NOOP;
    /** Intervals at the end of which a task kept its thread because no other task waited. */
    private final LongAdder skippedYields = new LongAdder();
    /** Iterations that ran longer than an interval. */
    private final LongAdder lateYields = new LongAdder();

    /**
     * @param defaultMaxRunningTasks the limit when {@link #MAX_RUNNING_TASKS} is {@code 0}
     */
    public QueryScheduler(int defaultMaxRunningTasks, ThreadContext threadContext, ClusterSettings clusterSettings, LongSupplier nanoTime) {
        this.threadContext = threadContext;
        this.nanoTime = nanoTime;
        if (FEATURE_FLAG.isEnabled()) {
            clusterSettings.initializeAndWatch(ENABLED, v -> enabled = v);
            clusterSettings.initializeAndWatch(YIELD_INTERVAL, v -> yieldInterval = v);
            clusterSettings.initializeAndWatch(MAX_RUNNING_TASKS, v -> {
                maxRunningTasks = v > 0 ? v : defaultMaxRunningTasks;
                dispatch();
            });
        }
    }

    /**
     * Returns an executor for the tasks of the query {@code queryId} that run on {@code pool}. Close it once those
     * tasks completed. While disabled, the executor hands tasks straight to {@code pool} and the scheduler does not
     * track the query.
     */
    public QueryExecutor executor(String queryId, Executor pool) {
        if (enabled == false) {
            return new QueryExecutor(pool);
        }
        QueryState state;
        synchronized (this) {
            state = queries.computeIfAbsent(queryId, QueryState::new);
            state.executors++;
        }
        return new ScheduledQueryExecutor(state, pool);
    }

    /**
     * Runs the tasks of a query on its pool.
     */
    public static class QueryExecutor implements Executor, Releasable {
        final Executor pool;

        private QueryExecutor(Executor pool) {
            this.pool = pool;
        }

        @Override
        public void execute(Runnable task) {
            pool.execute(task);
        }

        @Override
        public void close() {}
    }

    /**
     * Queues the tasks of a query until they can run.
     */
    private final class ScheduledQueryExecutor extends QueryExecutor implements YieldingExecutor {
        final QueryState state;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ScheduledQueryExecutor(QueryState state, Executor pool) {
            super(pool);
            this.state = state;
        }

        @Override
        public void execute(Runnable task) {
            // Another thread runs the task later: it keeps the context of the thread that submitted it.
            Runnable preserved = threadContext.preserveContext(task);
            synchronized (QueryScheduler.this) {
                state.queue.add(new QueuedTask(preserved, pool, nanoTime.getAsLong()));
                state.waiting.incrementAndGet();
                state.waitingOn(pool).incrementAndGet();
                waiting.incrementAndGet();
                waitingOn(pool).incrementAndGet();
            }
            dispatch();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                synchronized (QueryScheduler.this) {
                    state.executors--;
                    maybeRemove(state);
                }
            }
        }

        @Override
        public TimeValue yieldInterval() {
            return yieldInterval;
        }

        /**
         * A task of another query could take this thread: it waits for this pool, or for any pool while the limit on
         * running tasks is reached. Or a task submitted to the pool directly waits for one of its threads. The tasks of
         * one query share its work, so it gains nothing from rotating them.
         */
        @Override
        public boolean hasWaitingTasks() {
            return waitingTasks(pool) > state.waitingOn(pool).get()
                || (running >= maxRunningTasks && waiting.get() > state.waiting.get())
                || (pool instanceof ThreadPoolExecutor tpe && tpe.getQueue().isEmpty() == false);
        }

        @Override
        public void onYieldSkipped() {
            skippedYields.increment();
        }

        @Override
        public void onLateYield(long iterationNanos) {
            lateYields.increment();
        }
    }

    /**
     * Hands waiting tasks to their pools while fewer than {@link #MAX_RUNNING_TASKS} run, from the query running the fewest.
     */
    private void dispatch() {
        if (dispatchRequests.getAndIncrement() != 0) {
            return;
        }
        int requests = 1;
        do {
            for (DispatchedTask task : nextTasks()) {
                waitTime.record(TimeValue.nsecToMSec(task.waitedNanos));
                task.queued.pool.execute(task);
            }
            requests = dispatchRequests.addAndGet(-requests);
        } while (requests != 0);
    }

    private synchronized List<DispatchedTask> nextTasks() {
        List<DispatchedTask> toRun = new ArrayList<>();
        long now = nanoTime.getAsLong();
        while (running < maxRunningTasks && waiting.get() > 0) {
            QueryState next = null;
            for (QueryState q : queries.values()) {
                QueuedTask head = q.queue.peek();
                if (head != null && hasFreeThread(head.pool) && (next == null || q.compareTo(next) < 0)) {
                    next = q;
                }
            }
            if (next == null) {
                break;
            }
            QueuedTask queued = next.queue.poll();
            next.waiting.decrementAndGet();
            next.waitingOn(queued.pool).decrementAndGet();
            waiting.decrementAndGet();
            waitingOn(queued.pool).decrementAndGet();
            next.running++;
            next.lastDispatch = ++dispatches;
            long waited = now - queued.queuedAtNanos;
            running++;
            runningByPool.merge(queued.pool, 1, Integer::sum);
            toRun.add(new DispatchedTask(next, queued, waited));
        }
        return toRun;
    }

    /** Whether {@code pool} runs fewer of our tasks than it has threads. Called with the lock held. */
    private boolean hasFreeThread(Executor pool) {
        if ((pool instanceof ThreadPoolExecutor) == false) {
            return true;
        }
        return runningByPool.getOrDefault(pool, 0) < ((ThreadPoolExecutor) pool).getMaximumPoolSize();
    }

    private void finished(QueryState state, Executor pool) {
        synchronized (this) {
            state.running--;
            running--;
            runningByPool.merge(pool, -1, QueryScheduler::sumOrRemove);
            maybeRemove(state);
        }
        dispatch();
    }

    /** Merges a count, removing it at zero. */
    private static Integer sumOrRemove(Integer count, Integer delta) {
        return count + delta == 0 ? null : count + delta;
    }

    /** The count of tasks waiting for {@code pool}. Pools live as long as the node, so their counts are kept. */
    private AtomicInteger waitingOn(Executor pool) {
        return waitingByPool.computeIfAbsent(pool, p -> new AtomicInteger());
    }

    /** Forgets a query once it has no executor and no task. Called with the lock held. */
    private void maybeRemove(QueryState state) {
        if (state.executors == 0 && state.running == 0 && state.queue.isEmpty()) {
            queries.remove(state.queryId, state);
        }
    }

    /**
     * A running task. Its pool calls {@link #onAfter()} whether the task ran or was rejected.
     */
    private final class DispatchedTask extends AbstractRunnable {
        private final QueryState state;
        private final QueuedTask queued;
        private final long waitedNanos;

        private DispatchedTask(QueryState state, QueuedTask queued, long waitedNanos) {
            this.state = state;
            this.queued = queued;
            this.waitedNanos = waitedNanos;
        }

        @Override
        public boolean isForceExecution() {
            return queued.task instanceof AbstractRunnable r && r.isForceExecution();
        }

        @Override
        protected void doRun() {
            queued.task.run();
        }

        @Override
        public void onRejection(Exception e) {
            if (queued.task instanceof AbstractRunnable r) {
                r.onRejection(e);
            } else {
                logger.error(() -> Strings.format("rejected [%s]", queued.task), e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (queued.task instanceof AbstractRunnable r) {
                r.onFailure(e);
            } else {
                logger.error(() -> Strings.format("failed to run [%s]", queued.task), e);
            }
        }

        @Override
        public void onAfter() {
            finished(state, queued.pool);
        }
    }

    private record QueuedTask(Runnable task, Executor pool, long queuedAtNanos) {}

    /** The tasks of one query. Fields other than the atomic ones are guarded by the scheduler. */
    static final class QueryState implements Comparable<QueryState> {
        final String queryId;
        final ArrayDeque<QueuedTask> queue = new ArrayDeque<>();
        /** The size of {@link #queue}, read without the lock. */
        final AtomicInteger waiting = new AtomicInteger();
        /** The tasks of {@link #queue}, by the pool that will run them, read without the lock. */
        final Map<Executor, AtomicInteger> waitingByPool = new ConcurrentHashMap<>();
        int executors;
        int running;
        long lastDispatch;

        QueryState(String queryId) {
            this.queryId = queryId;
        }

        AtomicInteger waitingOn(Executor pool) {
            return waitingByPool.computeIfAbsent(pool, p -> new AtomicInteger());
        }

        /** Fewer running tasks first, then the query dispatched least recently. */
        @Override
        public int compareTo(QueryState o) {
            int cmp = Integer.compare(running, o.running);
            return cmp != 0 ? cmp : Long.compare(lastDispatch, o.lastDispatch);
        }
    }

    /**
     * The tasks running and waiting to run and the queries holding either, then totals since the node started.
     */
    public synchronized Stats stats() {
        return new Stats(running, waiting.get(), queries.size(), skippedYields.sum(), lateYields.sum());
    }

    /**
     * @param skippedYields intervals at the end of which a task kept its thread because no other task waited
     * @param lateYields    iterations that ran longer than an interval
     */
    public record Stats(int runningTasks, int waitingTasks, int queries, long skippedYields, long lateYields) {}

    /**
     * Reports {@link #stats()} and the time each task waited to run, none without {@link #FEATURE_FLAG}.
     */
    public void registerMetrics(MeterRegistry meterRegistry) {
        if (FEATURE_FLAG.isEnabled() == false) {
            return;
        }
        String prefix = "es.search.query_scheduling.";
        meterRegistry.registerLongAsyncGauge(prefix + "tasks.running.current", "Tasks running", "count", () -> stats().runningTasks());
        meterRegistry.registerLongAsyncGauge(
            prefix + "tasks.waiting.current",
            "Tasks waiting to run",
            "count",
            () -> stats().waitingTasks()
        );
        meterRegistry.registerLongAsyncGauge(
            prefix + "queries.current",
            "Queries with a running or waiting task",
            "count",
            () -> stats().queries()
        );
        waitTime = meterRegistry.registerLongHistogram(prefix + "tasks.wait_time.histogram", "Time tasks waited to run", "ms");
        meterRegistry.registerLongAsyncCounter(
            prefix + "yields.skipped.total",
            "Intervals at the end of which a task kept its thread because no other task waited",
            "count",
            () -> stats().skippedYields()
        );
        meterRegistry.registerLongAsyncCounter(
            prefix + "yields.late.total",
            "Iterations that ran longer than an interval",
            "count",
            () -> stats().lateYields()
        );
    }

    /**
     * The tasks waiting here to run on {@code pool}. They are not in the pool's queue, so a reading of its queue
     * should add them.
     */
    public int waitingTasks(Executor pool) {
        AtomicInteger count = waitingByPool.get(pool);
        return count == null ? 0 : count.get();
    }
}
