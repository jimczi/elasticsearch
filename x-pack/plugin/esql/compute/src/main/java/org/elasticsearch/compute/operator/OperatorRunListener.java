/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.core.Nullable;

import java.util.Objects;

/**
 * Listener invoked by the Driver when an Operator has been "run"
 * (i.e., it made progress until it blocked, finished, or the driver exhausted the step budget).
 *
 * The listener is intentionally low-level and synchronous: keep implementations lightweight.
 *
 * Operator status is included to allow operator-specific details (e.g. LuceneOperator.Status
 * exposes processed shards, query fingerprints, and process time)
 */
@FunctionalInterface
public interface OperatorRunListener {
    /**
     * Called after an operator has been run.
     */
    void onOperatorRun(Operator event);

    interface Provider {
        OperatorRunListener get();
    }
}
