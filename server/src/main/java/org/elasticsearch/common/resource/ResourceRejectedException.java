/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

/**
 * Thrown when a {@link ResourcePool} declines a request for capacity.
 *
 * <p>Extends {@link EsRejectedExecutionException} so that callers and the REST layer treat resource-manager backpressure
 * the same way they already treat thread-pool and indexing-pressure rejections. Rejection is the pool's normal, expected
 * way of signalling overload — it is preferred over letting unbounded work start and failing late.
 */
public class ResourceRejectedException extends EsRejectedExecutionException {

    public ResourceRejectedException(String message) {
        super(message, false);
    }
}
