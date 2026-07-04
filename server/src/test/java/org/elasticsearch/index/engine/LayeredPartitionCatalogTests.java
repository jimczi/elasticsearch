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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class LayeredPartitionCatalogTests extends ESTestCase {

    private static Unit u(String name, String slice, long w) {
        return new Unit(name, slice, w);
    }

    public void testIncrementalCommitsMergeOverBaseWithoutRebuild() throws Exception {
        // Base snapshot: A={_0,_1}, B={_2}.
        final FstPartitionCatalog base = FstPartitionCatalog.build(List.of(u("_0", "A", 3), u("_1", "A", 2), u("_2", "B", 5)));
        try (LayeredPartitionCatalog cat = new LayeredPartitionCatalog(base)) {
            // Commit 1 adds only _3 to A (a delta — base FST untouched).
            cat.apply(List.of(), List.of(u("_3", "A", 4)));
            assertThat(names(cat.units("A")), containsInAnyOrder("_0", "_1", "_3"));
            assertThat(names(cat.units("B")), containsInAnyOrder("_2")); // untouched partition = pure base read

            // Commit 2 merges A's _0,_1 into _4 (remove from base + add to delta).
            cat.apply(List.of("_0", "_1"), List.of(u("_4", "A", 5)));
            assertThat("base units _0,_1 hidden; delta _3,_4 shown", names(cat.units("A")), containsInAnyOrder("_3", "_4"));

            // Re-add of a base name: remove _2 then add _2 with a new weight — delta wins.
            cat.apply(List.of("_2"), List.of(u("_2", "B", 99)));
            final List<Unit> b = cat.units("B");
            assertThat(names(b), containsInAnyOrder("_2"));
            assertEquals(99L, b.get(0).weight());

            assertEquals("delta holds _3,_4,_2 pending compaction", 3, cat.pendingDeltaUnits());

            // Compaction folds base+delta into a fresh base; queries via the new base are identical, delta empty.
            try (FstPartitionCatalog compacted = cat.compact(); LayeredPartitionCatalog swapped = new LayeredPartitionCatalog(compacted)) {
                assertThat(names(compacted.units("A")), containsInAnyOrder("_3", "_4"));
                assertThat(names(compacted.units("B")), containsInAnyOrder("_2"));
                assertEquals(99L, compacted.units("B").get(0).weight());
                assertEquals(3, compacted.unitCount()); // _3,_4,_2
                assertEquals(0, swapped.pendingDeltaUnits());
                assertThat(names(swapped.units("A")), containsInAnyOrder("_3", "_4"));
            }
        }
    }

    private static Set<String> names(List<Unit> units) {
        return units.stream().map(Unit::name).collect(Collectors.toSet());
    }
}
