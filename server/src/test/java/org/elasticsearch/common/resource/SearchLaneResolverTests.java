/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.test.ESTestCase;

public class SearchLaneResolverTests extends ESTestCase {

    private static SearchLaneResolver.Work work(boolean system, String tier) {
        return new SearchLaneResolver.Work(system, tier);
    }

    public void testNormalOnlyKeepsEverythingInNormalLane() {
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.NORMAL_ONLY.resolve(work(true, "boosted")));
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.NORMAL_ONLY.resolve(work(false, "")));
    }

    public void testSystemAwareIsolatesSystemIndices() {
        assertEquals(ResourcePriority.SYSTEM, SearchLaneResolver.SYSTEM_AWARE.resolve(work(true, "")));
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.SYSTEM_AWARE.resolve(work(false, "boosted")));
    }

    public void testTierRoutesBoostedAndUnboosted() {
        assertEquals(ResourcePriority.SYSTEM, SearchLaneResolver.TIER.resolve(work(true, "boosted")));
        assertEquals(ResourcePriority.HIGH, SearchLaneResolver.TIER.resolve(work(false, "boosted")));
        assertEquals(ResourcePriority.LOW, SearchLaneResolver.TIER.resolve(work(false, "unboosted")));
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.TIER.resolve(work(false, "")));
    }
}
