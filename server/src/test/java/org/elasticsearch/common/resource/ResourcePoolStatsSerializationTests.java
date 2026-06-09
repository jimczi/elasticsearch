/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.resource;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class ResourcePoolStatsSerializationTests extends ESTestCase {

    public void testLaneStatsRoundTrip() throws IOException {
        ResourceLaneStats lane = new ResourceLaneStats(
            randomFrom(ResourcePriority.values()),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        assertEquals(lane, copyWriteable(lane, new NamedWriteableRegistry(List.of()), ResourceLaneStats::new));
    }

    public void testPoolStatsRoundTrip() throws IOException {
        List<ResourceLaneStats> lanes = randomList(
            0,
            4,
            () -> new ResourceLaneStats(
                randomFrom(ResourcePriority.values()),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            )
        );
        ResourcePoolStats stats = new ResourcePoolStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            lanes
        );
        assertEquals(stats, copyWriteable(stats, new NamedWriteableRegistry(List.of()), ResourcePoolStats::new));
    }
}
