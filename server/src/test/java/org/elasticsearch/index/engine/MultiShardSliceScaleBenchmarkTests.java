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

/**
 * Node-scale harness: many shards, each with its own off-heap FST slice catalog, so a single node holds
 * <b>millions of slices representing billions of docs</b> at negligible heap. Multiple shards are used because a
 * Lucene shard is docID-bounded at 2^31; the catalog cost is what we prove bounded. Timings/sizes are logged.
 */
public class MultiShardSliceScaleBenchmarkTests extends ESTestCase {

    public void testMillionsOfSlicesBillionsOfDocsAcrossShards() throws Exception {
        final int shards = 8;
        final int slicesPerShard = 250_000;
        final int docsPerSlice = 1000;
        final long totalSlices = (long) shards * slicesPerShard;
        final long totalDocs = totalSlices * docsPerSlice;

        final Path root = createTempDir();
        final List<FstPartitionCatalog> openCatalogs = new ArrayList<>(shards);
        long buildNanos = 0, totalOffHeapBytes = 0, totalOnDiskBytes = 0;
        try {
            for (int shard = 0; shard < shards; shard++) {
                final List<Unit> units = new ArrayList<>(slicesPerShard);
                for (int s = 0; s < slicesPerShard; s++) {
                    units.add(new Unit("_" + s, "shard" + shard + "-tenant" + s, docsPerSlice));
                }
                final long t0 = System.nanoTime();
                final FstPartitionCatalog built = FstPartitionCatalog.build(units);
                buildNanos += System.nanoTime() - t0;

                final Directory dir = new MMapDirectory(root.resolve("shard" + shard));
                built.save(dir, "catalog");
                built.close();
                for (String f : dir.listAll()) {
                    totalOnDiskBytes += dir.fileLength(f);
                }
                // Reopen off-heap — this is what a live node keeps resident.
                final FstPartitionCatalog offHeap = FstPartitionCatalog.openOffHeap(dir, "catalog");
                openCatalogs.add(offHeap);
                totalOffHeapBytes += offHeap.ramBytesUsed();
            }

            // Spot-check cross-shard correctness.
            final FstPartitionCatalog shard3 = openCatalogs.get(3);
            final List<Unit> got = shard3.units("shard3-tenant42");
            assertEquals(1, got.size());
            assertEquals(docsPerSlice, got.get(0).weight());
            assertEquals(slicesPerShard, shard3.unitCount());

            logger.info(
                "MULTI-SHARD SCALE: {} shards x {} slices = {} slices, {} docs/slice = {} total docs",
                shards,
                slicesPerShard,
                totalSlices,
                docsPerSlice,
                totalDocs
            );
            logger.info(
                "  build: {} ms | catalog on disk: {} MB total | resident heap for ALL shard catalogs: {} KB (off-heap mmap)",
                buildNanos / 1_000_000,
                totalOnDiskBytes / (1024 * 1024),
                totalOffHeapBytes / 1024
            );
        } finally {
            for (FstPartitionCatalog c : openCatalogs) {
                c.close();
            }
        }

        assertTrue("billions of docs represented", totalDocs >= 2_000_000_000L);
        // The whole point: the node-resident catalog for millions of slices costs ~nothing in heap.
        assertTrue("resident heap for all catalogs stays tiny", totalOffHeapBytes < 5L * 1024 * 1024);
    }
}
