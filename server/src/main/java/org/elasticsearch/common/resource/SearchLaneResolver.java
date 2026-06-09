/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

/**
 * Maps a unit of search work to the admission {@link ResourcePriority lane} it should run in. This is the pluggable
 * policy seam over the {@link ResourcePool} mechanism (floors, borrow, reclaim): the pool enforces per-lane budgets, but
 * <em>which</em> lane a search lands in is policy and lives here, so different deployments (serverless search-power,
 * on-prem tiers, system-index isolation) can choose different strategies without touching the pool.
 *
 * <p>The {@link Work} description starts minimal and is expected to grow (index boost / search power, data source kind,
 * project) as more strategies are added; resolvers must tolerate unknown future fields by falling back to
 * {@link ResourcePriority#NORMAL}.
 */
public interface SearchLaneResolver {

    /** A self-describing unit of admitted search work. Extend with new fields as strategies need them. */
    record Work(boolean systemIndex) {}

    /** The lane this work should be admitted into. */
    ResourcePriority resolve(Work work);

    /** Default policy: everything shares the {@link ResourcePriority#NORMAL} lane, so the lane machinery stays inert. */
    SearchLaneResolver NORMAL_ONLY = work -> ResourcePriority.NORMAL;

    /** Isolates system-index searches into the dedicated {@link ResourcePriority#SYSTEM} lane; everything else NORMAL. */
    SearchLaneResolver SYSTEM_AWARE = work -> work.systemIndex() ? ResourcePriority.SYSTEM : ResourcePriority.NORMAL;
}
