/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.admission;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.resource.ResourcePriority;

/**
 * Reserves and releases shard-search admission capacity on a (possibly remote) node under a lease id. Implemented by
 * {@link SearchAdmissionService} over the reserve/release transport; abstracted as an interface so the coordinator
 * admission state machine ({@link CoordinatorSearchAdmission}) can be unit-tested with a fake.
 */
public interface NodeAdmissionClient {

    /** Reserves {@code slots} on {@code node} under {@code leaseId}; the listener fails with a rejection if it cannot. */
    void reserve(DiscoveryNode node, String leaseId, int slots, ResourcePriority priority, ActionListener<Void> listener);

    /** Releases the lease {@code leaseId} on {@code node}. */
    void release(DiscoveryNode node, String leaseId, ActionListener<Void> listener);
}
