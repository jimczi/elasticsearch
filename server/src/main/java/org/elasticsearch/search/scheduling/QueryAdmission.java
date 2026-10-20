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
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;

/**
 * Caps how many search queries read data on a node at once. A query waits before it allocates anything, so a waiting
 * query holds no memory and no thread. Behind {@link QueryScheduler#FEATURE_FLAG}.
 * <p>
 * Only the parts of a query that read data may wait, never the parts that consume them. Waits on different nodes
 * therefore cannot form a cycle. Parts of an admitted query never wait.
 */
public final class QueryAdmission {
    public static final Setting<Boolean> ENABLED = Setting.boolSetting(
        "search.query_admission.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Queries reading data at once. {@code 0} means half the allocated processors.
     */
    public static final Setting<Integer> MAX_RUNNING_QUERIES = Setting.intSetting(
        "search.query_admission.max_running_queries",
        0,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Queries waiting for admission at once. More are rejected.
     */
    public static final Setting<Integer> MAX_WAITING_QUERIES = Setting.intSetting(
        "search.query_admission.max_waiting_queries",
        1000,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The settings of this class, none without {@link QueryScheduler#FEATURE_FLAG}.
     */
    public static List<Setting<?>> settings() {
        return QueryScheduler.FEATURE_FLAG.isEnabled() ? List.of(ENABLED, MAX_RUNNING_QUERIES, MAX_WAITING_QUERIES) : List.of();
    }

    private final int defaultRunning;
    private final Executor admittedExecutor;
    private final ThreadContext threadContext;
    private final LongSupplier nanoTime;

    private volatile boolean enabled;
    private volatile int maxRunningQueries;
    private volatile int maxWaitingQueries;

    /** The number of running parts of admitted queries, by id. Guarded by {@code this}. */
    private final Map<String, Integer> running = new HashMap<>();
    /**
     * Waiting parts by query, in the arrival order of each query's first part. A query has waiting parts only while it
     * does not run, since all its parts are admitted together. Guarded by {@code this}.
     */
    private final LinkedHashMap<String, List<Waiter>> waiting = new LinkedHashMap<>();
    /** Parts rejected because too many waited. Guarded by {@code this}. */
    private long rejected;
    /** Time each part admitted after waiting waited. */
    private volatile LongHistogram waitTime = LongHistogram.NOOP;

    /**
     * @param defaultRunning   the limit when {@link #MAX_RUNNING_QUERIES} is {@code 0}
     * @param admittedExecutor runs parts that waited for admission
     */
    public QueryAdmission(
        int defaultRunning,
        Executor admittedExecutor,
        ThreadContext threadContext,
        ClusterSettings clusterSettings,
        LongSupplier nanoTime
    ) {
        this.defaultRunning = defaultRunning;
        this.admittedExecutor = admittedExecutor;
        this.threadContext = threadContext;
        this.nanoTime = nanoTime;
        if (QueryScheduler.FEATURE_FLAG.isEnabled()) {
            clusterSettings.initializeAndWatch(ENABLED, v -> {
                enabled = v;
                admitWaiting();
            });
            clusterSettings.initializeAndWatch(MAX_RUNNING_QUERIES, v -> {
                maxRunningQueries = v;
                admitWaiting();
            });
            clusterSettings.initializeAndWatch(MAX_WAITING_QUERIES, v -> maxWaitingQueries = v);
        }
    }

    /**
     * Admits one part of a query, then calls {@code listener} with a permit to close once the part completed. Parts
     * that may not wait are admitted immediately. A waiting part resumes in the thread context it was admitted from,
     * and fails if its task is cancelled.
     * <p>
     * {@code mayWait} must be {@code false} for a part that an admitted part waits for, such as one that consumes the
     * results of other parts, or a request that a running query sends to another node. Otherwise two queries could
     * each hold the permit, on different nodes, that the other's part waits for.
     */
    public void admit(String queryId, boolean mayWait, CancellableTask task, ActionListener<Releasable> listener) {
        if (enabled == false || mayWait == false) {
            listener.onResponse(() -> {});
            return;
        }
        Waiter waiter = null;
        Exception rejection = null;
        synchronized (this) {
            List<Waiter> parts = waiting.get(queryId);
            if (parts != null || tryAdmit(queryId) == false) {
                if (parts == null && waiting.size() >= maxWaitingQueries) {
                    rejected++;
                    rejection = new EsRejectedExecutionException(
                        "too many search queries on this node: [" + running.size() + "] running and [" + waiting.size() + "] waiting"
                    );
                } else {
                    waiter = new Waiter(
                        queryId,
                        ContextPreservingActionListener.wrapPreservingContext(listener, threadContext),
                        nanoTime.getAsLong()
                    );
                    waiting.computeIfAbsent(queryId, k -> new ArrayList<>()).add(waiter);
                }
            }
        }
        if (rejection != null) {
            listener.onFailure(rejection);
        } else if (waiter == null) {
            listener.onResponse(permit(queryId));
        } else {
            Waiter w = waiter;
            task.addListener(() -> cancel(w, task));
        }
    }

    /** Admits {@code queryId} if it already runs here or the node has room. Called with the lock held. */
    private boolean tryAdmit(String queryId) {
        Integer parts = running.get(queryId);
        if (parts != null) {
            running.put(queryId, parts + 1);
            return true;
        }
        if (hasRoom()) {
            running.put(queryId, 1);
            return true;
        }
        return false;
    }

    private Releasable permit(String queryId) {
        return Releasables.releaseOnce(() -> release(queryId));
    }

    /** Called with the lock held. */
    private boolean hasRoom() {
        return running.size() < (maxRunningQueries > 0 ? maxRunningQueries : defaultRunning);
    }

    private void release(String queryId) {
        List<Waiter> admitted;
        synchronized (this) {
            running.merge(queryId, -1, (parts, delta) -> parts + delta == 0 ? null : parts + delta);
            admitted = pollAdmitted();
        }
        admitted.forEach(this::resume);
    }

    private void admitWaiting() {
        List<Waiter> admitted;
        synchronized (this) {
            admitted = pollAdmitted();
        }
        admitted.forEach(this::resume);
    }

    /**
     * Admits the queries that waited longest, with all their waiting parts, while the node has room, or all of them once
     * admission is disabled. Called with the lock held.
     */
    private List<Waiter> pollAdmitted() {
        List<Waiter> admitted = new ArrayList<>();
        long now = nanoTime.getAsLong();
        Iterator<Map.Entry<String, List<Waiter>>> it = waiting.entrySet().iterator();
        while (it.hasNext() && (enabled == false || hasRoom())) {
            Map.Entry<String, List<Waiter>> query = it.next();
            it.remove();
            running.merge(query.getKey(), query.getValue().size(), Integer::sum);
            for (Waiter w : query.getValue()) {
                w.waitedNanos = now - w.queuedAtNanos;
                admitted.add(w);
            }
        }
        return admitted;
    }

    /**
     * Resumes a part removed from {@link #waiting} and counted as running. Removal under the lock means it is resumed or
     * cancelled, not both.
     */
    private void resume(Waiter w) {
        waitTime.record(TimeValue.nsecToMSec(w.waitedNanos));
        Releasable permit = permit(w.queryId);
        admittedExecutor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                w.listener.onResponse(permit);
            }

            @Override
            public void onFailure(Exception e) {
                permit.close();
                w.listener.onFailure(e);
            }
        });
    }

    private void cancel(Waiter w, CancellableTask task) {
        boolean removed;
        synchronized (this) {
            List<Waiter> parts = waiting.get(w.queryId);
            removed = parts != null && parts.remove(w);
            if (removed && parts.isEmpty()) {
                waiting.remove(w.queryId);
            }
        }
        if (removed) {
            w.listener.onFailure(new TaskCancelledException(task.getReasonCancelled()));
        }
    }

    /** Compares by identity, so a cancelled part only removes itself from {@link #waiting}. */
    private static final class Waiter {
        final String queryId;
        final ActionListener<Releasable> listener;
        final long queuedAtNanos;
        /** Set when admitted, under the lock. */
        long waitedNanos;

        Waiter(String queryId, ActionListener<Releasable> listener, long queuedAtNanos) {
            this.queryId = queryId;
            this.listener = listener;
            this.queuedAtNanos = queuedAtNanos;
        }
    }

    /**
     * The queries running and waiting on this node, then totals since the node started.
     */
    public synchronized Stats stats() {
        return new Stats(running.size(), waiting.size(), rejected);
    }

    /**
     * @param rejected parts rejected because too many queries waited
     */
    public record Stats(int runningQueries, int waitingQueries, long rejected) {}

    /**
     * Reports {@link #stats()} and the time each part waited for admission, none without {@link QueryScheduler#FEATURE_FLAG}.
     */
    public void registerMetrics(MeterRegistry meterRegistry) {
        if (QueryScheduler.FEATURE_FLAG.isEnabled() == false) {
            return;
        }
        String prefix = "es.search.query_admission.";
        meterRegistry.registerLongAsyncGauge(
            prefix + "queries.running.current",
            "Queries admitted and running",
            "count",
            () -> stats().runningQueries()
        );
        meterRegistry.registerLongAsyncGauge(
            prefix + "queries.waiting.current",
            "Queries waiting for admission",
            "count",
            () -> stats().waitingQueries()
        );
        meterRegistry.registerLongAsyncCounter(
            prefix + "rejected.total",
            "Parts rejected because too many queries waited",
            "count",
            () -> stats().rejected()
        );
        waitTime = meterRegistry.registerLongHistogram(prefix + "wait_time.histogram", "Time parts waited for admission", "ms");
    }
}
