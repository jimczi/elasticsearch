/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.Executor;

/**
 * An {@link Executor} whose tasks yield their thread to others. A task that can pause checks {@link #hasWaitingTasks()}
 * every {@link #yieldInterval()}: it keeps its thread while nothing waits, and yields it, to be resubmitted, otherwise.
 */
public interface YieldingExecutor extends Executor {
    /**
     * How long a task runs between checks of {@link #hasWaitingTasks()}.
     */
    TimeValue yieldInterval();

    /**
     * Whether another task waits for a thread.
     */
    boolean hasWaitingTasks();

    /**
     * Called when a task keeps its thread at the end of an interval because nothing waits.
     */
    default void onYieldSkipped() {}

    /**
     * Called when a task ran for {@code iterationNanos}, longer than an interval, without a chance to yield.
     */
    default void onLateYield(long iterationNanos) {}
}
