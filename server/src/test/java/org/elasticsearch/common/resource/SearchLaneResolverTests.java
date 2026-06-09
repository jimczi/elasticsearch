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

    public void testNormalOnlyKeepsEverythingInNormalLane() {
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.NORMAL_ONLY.resolve(new SearchLaneResolver.Work(true)));
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.NORMAL_ONLY.resolve(new SearchLaneResolver.Work(false)));
    }

    public void testSystemAwareIsolatesSystemIndices() {
        assertEquals(ResourcePriority.SYSTEM, SearchLaneResolver.SYSTEM_AWARE.resolve(new SearchLaneResolver.Work(true)));
        assertEquals(ResourcePriority.NORMAL, SearchLaneResolver.SYSTEM_AWARE.resolve(new SearchLaneResolver.Work(false)));
    }
}
