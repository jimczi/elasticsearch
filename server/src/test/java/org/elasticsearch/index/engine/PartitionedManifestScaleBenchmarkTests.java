/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.engine.PartitionedManifest.Unit;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Scale harness for {@link PartitionedManifest}: shows the slice/columnar catalog holds ~1M partitions on a single
 * node with append-only O(#changed) commits and O(#partition) queries — i.e. the mechanism that lets a shard carry
 * millions of tenants without an O(#segments) monolithic commit. (In-memory maps here; a shard at ~10M+ backs the
 * snapshot with an off-heap FST — see PartitionedManifest#writeSnapshot. Multiple shards multiply the doc ceiling
 * past 2B.) Timings are logged, not asserted.
 */
public class PartitionedManifestScaleBenchmarkTests extends ESTestCase {

    public void testMillionSliceCatalog() throws Exception {
        final int slices = 1_000_000;
        final int commitBatch = 10_000; // realistic: many slices land per commit

        // Build the catalog: one segment per slice, appended in batches (each batch = one incremental commit).
        final PartitionedManifest manifest = new PartitionedManifest();
        long addNanos = 0;
        for (int start = 0; start < slices; start += commitBatch) {
            final int end = Math.min(start + commitBatch, slices);
            final List<Unit> batch = new ArrayList<>(end - start);
            for (int s = start; s < end; s++) {
                batch.add(new Unit("_" + s, "tenant" + s, 100));
            }
            final long t0 = System.nanoTime();
            manifest.commit(List.of(), batch);
            addNanos += System.nanoTime() - t0;
        }

        // Query latency: per-slice discovery + admin stats, over random slices.
        final Random random = new Random(7);
        final long q0 = System.nanoTime();
        long touched = 0;
        for (int i = 0; i < 100_000; i++) {
            final String slice = "tenant" + random.nextInt(slices);
            touched += manifest.units(slice).size();  // O(#slice segments) discovery
            touched += manifest.unitCount(slice);
        }
        final long queryNanos = System.nanoTime() - q0;

        final long dirtyStart = System.nanoTime();
        final int dirty = manifest.dirtyPartitions(2).size(); // merge candidates (none here: 1 seg each)
        final long dirtyNanos = System.nanoTime() - dirtyStart;

        // Persistence: compact snapshot + recover.
        long snapshotNanos, recoverNanos, snapshotBytes;
        try (Directory dir = new ByteBuffersDirectory()) {
            final long s0 = System.nanoTime();
            manifest.writeSnapshot(dir);
            snapshotNanos = System.nanoTime() - s0;
            snapshotBytes = dir.fileLength(dir.listAll()[0]);
            final long r0 = System.nanoTime();
            final PartitionedManifest recovered = PartitionedManifest.recover(dir);
            recoverNanos = System.nanoTime() - r0;
            assertEquals(slices, recovered.partitionCount());
            assertEquals(slices, recovered.unitCount());
        }

        assertEquals(slices, manifest.partitionCount());
        assertEquals(slices, manifest.unitCount());
        assertEquals(0, dirty);

        final long usedMb = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
        logger.info("MANIFEST SCALE: {} slices (1 segment each), commit batch {}", slices, commitBatch);
        logger.info(
            "  build(append): {} ms ({} slices/s) | 100k random queries: {} ms ({} us/query) | dirtyPartitions scan: {} ms",
            addNanos / 1_000_000,
            (long) (slices / (addNanos / 1e9)),
            queryNanos / 1_000_000,
            (queryNanos / 1000) / 100_000.0,
            dirtyNanos / 1_000_000
        );
        logger.info(
            "  snapshot: {} ms, {} MB | recover: {} ms ({} slices/s) | heap in use ~{} MB | touched={}",
            snapshotNanos / 1_000_000,
            snapshotBytes / (1024 * 1024),
            recoverNanos / 1_000_000,
            (long) (slices / (recoverNanos / 1e9)),
            usedMb,
            touched
        );
    }
}
