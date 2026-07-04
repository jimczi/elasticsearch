/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.index.engine.PartitionedManifest.Unit;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThan;

public class FstPartitionCatalogTests extends ESTestCase {

    public void testPartitionScopedQueries() throws Exception {
        final FstPartitionCatalog catalog = FstPartitionCatalog.build(
            List.of(
                new Unit("_0", "tenantA", 3),
                new Unit("_1", "tenantA", 2),
                new Unit("_2", "tenantB", 5),
                new Unit("_3", "tenantAB", 7) // prefix of neither; guards the separator boundary
            )
        );
        assertThat(names(catalog.units("tenantA")), containsInAnyOrder("_0", "_1"));
        assertThat(names(catalog.units("tenantB")), containsInAnyOrder("_2"));
        assertThat(names(catalog.units("tenantAB")), containsInAnyOrder("_3")); // NOT swallowed by "tenantA"
        assertThat(catalog.units("missing"), empty());
        assertEquals(2, catalog.unitCount("tenantA"));
        assertEquals(4, catalog.unitCount());
        // weight carried through as the FST output
        assertEquals(3L, catalog.units("tenantA").stream().filter(u -> u.name().equals("_0")).findFirst().orElseThrow().weight());
    }

    public void testSeparatorBoundaryExactness() throws Exception {
        // "a" must not leak "ab"/"a1"'s units. The 0x00 separator makes "a\0*" exclude "ab\0*".
        final FstPartitionCatalog catalog = FstPartitionCatalog.build(
            List.of(new Unit("s1", "a", 1), new Unit("s2", "ab", 1), new Unit("s3", "a1", 1), new Unit("s4", "a", 1))
        );
        assertThat(names(catalog.units("a")), containsInAnyOrder("s1", "s4"));
        assertThat(names(catalog.units("ab")), containsInAnyOrder("s2"));
        assertThat(names(catalog.units("a1")), containsInAnyOrder("s3"));
    }

    public void testMillionUnitCompactHeap() throws Exception {
        final int units = 1_000_000;
        final List<Unit> all = new ArrayList<>(units);
        for (int i = 0; i < units; i++) {
            all.add(new Unit("_" + i, "tenant" + i, 100));
        }
        final FstPartitionCatalog catalog = FstPartitionCatalog.build(all);
        assertEquals(units, catalog.unitCount());

        // Spot-check correctness at random.
        for (int i = 0; i < 1000; i++) {
            final int t = randomIntBetween(0, units - 1);
            final List<Unit> got = catalog.units("tenant" + t);
            assertEquals(1, got.size());
            assertEquals("_" + t, got.get(0).name());
        }

        // The compact FST holds 1M units in far less than the ~1.3 GB the naive maps needed (see
        // PartitionedManifestScaleBenchmarkTests). Assert a generous ceiling so this is a real regression guard.
        final long fstMb = catalog.ramBytesUsed() / (1024 * 1024);
        logger.info("FST CATALOG: {} units, on-heap FST ~{} MB (naive maps were ~1300 MB)", units, fstMb);
        assertThat("compact FST must be well under the naive footprint", catalog.ramBytesUsed(), lessThan(200L * 1024 * 1024));
    }

    public void testOffHeapCatalogMmap() throws Exception {
        final int units = 1_000_000;
        final List<Unit> all = new ArrayList<>(units);
        for (int i = 0; i < units; i++) {
            all.add(new Unit("_" + i, "tenant" + i, 100));
        }
        final Path dir = createTempDir();
        final long onHeapBytes;
        try (Directory directory = new MMapDirectory(dir)) {
            final FstPartitionCatalog built = FstPartitionCatalog.build(all);
            onHeapBytes = built.ramBytesUsed();
            built.save(directory, "catalog");

            // Reopen off-heap: FST bytes are mmap'd, not on the Java heap.
            try (FstPartitionCatalog offHeap = FstPartitionCatalog.openOffHeap(directory, "catalog")) {
                assertEquals(units, offHeap.unitCount());
                for (int i = 0; i < 1000; i++) {
                    final int t = randomIntBetween(0, units - 1);
                    final List<Unit> got = offHeap.units("tenant" + t);
                    assertEquals(1, got.size());
                    assertEquals("_" + t, got.get(0).name());
                    assertEquals(100L, got.get(0).weight());
                }
                // Off-heap retained heap is the FST metadata only — orders of magnitude below the on-heap FST.
                logger.info(
                    "OFF-HEAP CATALOG: {} units | on-heap FST {} KB vs off-heap retained {} KB (data mmap'd)",
                    units,
                    onHeapBytes / 1024,
                    offHeap.ramBytesUsed() / 1024
                );
                assertThat("off-heap retained must be far below on-heap FST", offHeap.ramBytesUsed(), lessThan(onHeapBytes / 10));
            }
        }
    }

    private static Set<String> names(List<Unit> units) {
        return units.stream().map(Unit::name).collect(Collectors.toSet());
    }
}
