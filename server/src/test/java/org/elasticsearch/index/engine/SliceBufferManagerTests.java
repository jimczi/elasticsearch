/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class SliceBufferManagerTests extends ESTestCase {

    public void testDrainsOnlyIdleSlices() {
        final SliceBufferManager manager = new SliceBufferManager(100);
        manager.onWrite("tenantA", 0);
        manager.onWrite("tenantB", 0);
        manager.onWrite("tenantC", 50);

        // At t=120, A and B have been idle 120ns (>=100) but C only 70ns.
        final List<Object> idle = manager.drainIdle(120);
        assertThat(idle, containsInAnyOrder("tenantA", "tenantB"));
        assertEquals("only the active slice remains tracked", 1, manager.trackedSlices());

        // Drained slices are gone; draining again returns nothing new for them.
        assertThat(manager.drainIdle(120), empty());
    }

    public void testActivityResetsIdleTimer() {
        final SliceBufferManager manager = new SliceBufferManager(100);
        manager.onWrite("tenantA", 0);
        manager.onWrite("tenantA", 80); // fresh write refreshes A
        assertThat("A is not yet idle at t=150 (last write 80)", manager.drainIdle(150), empty());
        assertThat(manager.drainIdle(200), containsInAnyOrder("tenantA"));
    }

    public void testDisabledWhenIntervalZero() {
        final SliceBufferManager manager = new SliceBufferManager(0);
        manager.onWrite("tenantA", 0);
        assertThat(manager.drainIdle(Long.MAX_VALUE), empty());
        assertEquals(1, manager.trackedSlices());
    }

    public void testForgetStopsTracking() {
        final SliceBufferManager manager = new SliceBufferManager(100);
        manager.onWrite("tenantA", 0);
        manager.forget("tenantA");
        assertEquals(0, manager.trackedSlices());
        assertThat(manager.drainIdle(1000), empty());
    }

    public void testNullSliceIgnored() {
        final SliceBufferManager manager = new SliceBufferManager(100);
        manager.onWrite(null, 0);
        assertEquals(0, manager.trackedSlices());
    }
}
