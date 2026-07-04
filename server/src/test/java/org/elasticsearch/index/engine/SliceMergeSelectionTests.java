/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.index.engine.PartitionedManifest.Unit;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

/**
 * Shows the manifest drives slice-scoped merge <em>selection</em> in O(#dirty slices), and consolidating a slice's
 * own segments (the real per-slice merge is proven isolated in {@code SlicePartitionedStressBenchmarkTests}) is a
 * remove-N/add-1 delta that never touches other tenants. This is #2 built on top of the LSM manifest.
 */
public class SliceMergeSelectionTests extends ESTestCase {

    /** Consolidate one slice's segments into a single merged segment (docCount preserved) via a manifest delta. */
    private static long consolidate(PartitionedManifest m, String slice, String mergedName) {
        final List<Unit> segs = m.units(slice);
        final List<String> removed = new ArrayList<>(segs.size());
        long docs = 0;
        for (Unit s : segs) {
            removed.add(s.name());
            docs += s.weight();
        }
        return m.commit(removed, List.of(new Unit(mergedName, slice, docs)));
    }

    public void testManifestDrivesSliceScopedConsolidation() {
        final PartitionedManifest m = new PartitionedManifest();
        // A fragmented into 5 segments, B into 1, C into 3.
        m.commit(List.of(), List.of(seg("a0", "A", 10), seg("a1", "A", 10), seg("a2", "A", 10), seg("a3", "A", 10), seg("a4", "A", 10)));
        m.commit(List.of(), List.of(seg("b0", "B", 100)));
        m.commit(List.of(), List.of(seg("c0", "C", 5), seg("c1", "C", 5), seg("c2", "C", 5)));

        // Selection off the manifest: only A and C are fragmented at threshold 3.
        assertThat(m.dirtyPartitions(3), containsInAnyOrder("A", "C"));

        // Consolidate A — remove its 5 segments, add 1. B and C untouched.
        consolidate(m, "A", "a_merged");
        assertEquals(1, m.unitCount("A"));
        assertThat(names(m.units("A")), containsInAnyOrder("a_merged"));
        assertEquals("docs preserved across the merge", 50, m.weight("A"));
        assertThat(names(m.units("B")), containsInAnyOrder("b0")); // untouched
        assertEquals(3, m.unitCount("C")); // untouched

        // Now only C is dirty; consolidate it too.
        assertThat(m.dirtyPartitions(3), containsInAnyOrder("C"));
        consolidate(m, "C", "c_merged");
        assertEquals(15, m.weight("C"));

        // No fragmented slices remain.
        assertThat(m.dirtyPartitions(3), empty());
        assertThat(m.dirtyPartitions(2), empty());
        assertEquals(3, m.partitionCount()); // A, B, C all still present, each with 1 segment
        assertEquals(3, m.unitCount());
    }

    private static Unit seg(String name, String slice, long docs) {
        return new Unit(name, slice, docs);
    }

    private static java.util.Set<String> names(List<Unit> units) {
        return units.stream().map(Unit::name).collect(java.util.stream.Collectors.toSet());
    }
}
