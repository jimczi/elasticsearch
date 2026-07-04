/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.engine.PartitionedManifest.Unit;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class PartitionedManifestTests extends ESTestCase {

    private static Unit seg(String name, String slice, long docs) {
        return new Unit(name, slice, docs);
    }

    private static long commitAndWrite(PartitionedManifest m, Directory dir, List<String> removed, List<Unit> added) throws IOException {
        final long gen = m.commit(removed, added);
        PartitionedManifest.writeEdit(dir, gen, removed, added);
        return gen;
    }

    public void testCommitQueryAndMergeDelta() {
        final PartitionedManifest m = new PartitionedManifest();
        m.commit(List.of(), List.of(seg("_0", "tenantA", 3), seg("_1", "tenantA", 2), seg("_2", "tenantB", 5)));

        assertThat(m.partitions(), containsInAnyOrder("tenantA", "tenantB"));
        assertEquals(2, m.partitionCount());
        assertEquals(3, m.unitCount());
        assertEquals(2, m.unitCount("tenantA"));
        assertEquals(5, m.weight("tenantB")); // docCount for slices
        assertEquals(5, m.weight("tenantA")); // 3 + 2
        assertThat(names(m.units("tenantA")), containsInAnyOrder("_0", "_1"));

        // Merge tenantA's two segments into one — remove _0,_1, add _3. Never touches tenantB.
        m.commit(List.of("_0", "_1"), List.of(seg("_3", "tenantA", 5)));
        assertEquals(1, m.unitCount("tenantA"));
        assertThat(names(m.units("tenantA")), containsInAnyOrder("_3"));
        assertEquals(5, m.weight("tenantA"));
        assertEquals(2, m.unitCount()); // _3 (A) + _2 (B)
        assertThat(names(m.units("tenantB")), containsInAnyOrder("_2"));
    }

    public void testDirtyPartitions() {
        final PartitionedManifest m = new PartitionedManifest();
        m.commit(List.of(), List.of(seg("_0", "A", 1), seg("_1", "A", 1), seg("_2", "A", 1), seg("_3", "B", 1)));
        assertThat(m.dirtyPartitions(3), containsInAnyOrder("A")); // A has 3, B has 1
        assertThat(m.dirtyPartitions(1), containsInAnyOrder("A", "B"));
        assertThat(m.dirtyPartitions(4), empty());
    }

    public void testEdgeCases() {
        final PartitionedManifest m = new PartitionedManifest();
        assertThat(m.units("nope"), empty());
        assertEquals(0, m.unitCount("nope"));
        assertEquals(0, m.weight("nope"));
        m.commit(List.of("does-not-exist"), List.of()); // remove unknown -> no-op
        assertEquals(0, m.unitCount());
        m.commit(List.of(), List.of(seg("_0", "A", 1)));
        m.commit(List.of(), List.of(seg("_0", "A", 9))); // re-add same name -> overwrite weight
        assertEquals(1, m.unitCount("A"));
        assertEquals(9, m.weight("A"));
    }

    public void testPersistenceReplayFromEdits() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            final PartitionedManifest m = new PartitionedManifest();
            commitAndWrite(m, dir, List.of(), List.of(seg("_0", "A", 3), seg("_1", "B", 4)));
            commitAndWrite(m, dir, List.of(), List.of(seg("_2", "A", 2)));
            commitAndWrite(m, dir, List.of("_0", "_2"), List.of(seg("_5", "A", 5))); // merge A

            final PartitionedManifest recovered = PartitionedManifest.recover(dir);
            assertEquals(m.generation(), recovered.generation());
            assertThat(recovered.partitions(), containsInAnyOrder("A", "B"));
            assertThat(names(recovered.units("A")), containsInAnyOrder("_5"));
            assertThat(names(recovered.units("B")), containsInAnyOrder("_1"));
            assertEquals(5, recovered.weight("A"));
        }
    }

    public void testSnapshotCompactionAndRecover() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            final PartitionedManifest m = new PartitionedManifest();
            for (int i = 0; i < 5; i++) {
                commitAndWrite(m, dir, List.of(), List.of(seg("_" + i, "tenant" + (i % 3), i + 1)));
            }
            m.writeSnapshot(dir);
            PartitionedManifest.deleteObsoleteEdits(dir, m.generation());
            // A couple of commits after the snapshot.
            commitAndWrite(m, dir, List.of(), List.of(seg("_9", "tenant0", 100)));

            final PartitionedManifest recovered = PartitionedManifest.recover(dir);
            assertEquals(m.generation(), recovered.generation());
            assertEquals(m.unitCount(), recovered.unitCount());
            assertEquals(m.weight("tenant0"), recovered.weight("tenant0"));
            assertThat(recovered.partitions(), containsInAnyOrder("tenant0", "tenant1", "tenant2"));
            // Obsolete edits are gone; recovery came from the snapshot + the post-snapshot edit.
            for (String f : dir.listAll()) {
                if (f.startsWith(PartitionedManifest.EDIT_PREFIX)) {
                    assertTrue(
                        "only post-snapshot edits remain: " + f,
                        Long.parseLong(f.substring(PartitionedManifest.EDIT_PREFIX.length())) > 5
                    );
                }
            }
        }
    }

    public void testCrashTruncatedTrailingEditIsIgnored() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            final PartitionedManifest m = new PartitionedManifest();
            commitAndWrite(m, dir, List.of(), List.of(seg("_0", "A", 1)));
            commitAndWrite(m, dir, List.of(), List.of(seg("_1", "B", 1)));
            // Simulate a crash mid-write of the next commit: an edit blob with a header but no footer.
            final long badGen = m.generation() + 1;
            try (IndexOutput out = dir.createOutput(PartitionedManifest.EDIT_PREFIX + badGen, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(out, "PartitionedManifestEdit", 1);
                out.writeVLong(badGen);
                out.writeVInt(0); // removed
                out.writeVInt(1); // claims 1 added unit but writes nothing more -> truncated
                // no unit bytes, no footer
            }
            final PartitionedManifest recovered = PartitionedManifest.recover(dir);
            // The truncated edit is ignored; state reflects only the two good commits.
            assertEquals(2, recovered.generation());
            assertThat(recovered.partitions(), containsInAnyOrder("A", "B"));
            assertEquals(2, recovered.unitCount());
        }
    }

    public void testCorruptMiddleEditFailsRecovery() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            final PartitionedManifest m = new PartitionedManifest();
            commitAndWrite(m, dir, List.of(), List.of(seg("_0", "A", 1)));
            commitAndWrite(m, dir, List.of(), List.of(seg("_1", "B", 1)));
            commitAndWrite(m, dir, List.of(), List.of(seg("_2", "C", 1)));
            // Corrupt a MIDDLE edit (gen 2) while a later valid edit (gen 3) exists — genuine corruption,
            // not a crash-truncated tail, so recovery must fail loudly rather than silently drop gen 3.
            dir.deleteFile(PartitionedManifest.EDIT_PREFIX + 2);
            try (IndexOutput out = dir.createOutput(PartitionedManifest.EDIT_PREFIX + 2, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(out, "PartitionedManifestEdit", 1);
                out.writeVLong(2);
                out.writeVInt(0);
                out.writeVInt(1); // claims a unit but writes nothing -> truncated
            }
            expectThrows(org.apache.lucene.index.CorruptIndexException.class, () -> PartitionedManifest.recover(dir));
        }
    }

    public void testConcurrentCommitsAreThreadSafe() throws Exception {
        final PartitionedManifest m = new PartitionedManifest();
        final int threads = 8;
        final int perThread = 2000;
        final Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    m.commit(List.of(), List.of(seg("t" + tid + "_" + i, "tenant" + tid, 1)));
                }
            });
        }
        for (Thread w : workers) {
            w.start();
        }
        for (Thread w : workers) {
            w.join();
        }
        assertEquals(threads * perThread, m.unitCount());
        assertEquals(threads, m.partitionCount());
        for (int t = 0; t < threads; t++) {
            assertEquals(perThread, m.unitCount("tenant" + t));
        }
        assertEquals(threads * perThread, m.generation()); // every commit advanced the generation exactly once
    }

    public void testColumnarUsageSameManifest() throws Exception {
        // Columnar: partition = field, unit = file, weight = bytes. Same API.
        try (Directory dir = new ByteBuffersDirectory()) {
            final PartitionedManifest m = new PartitionedManifest();
            commitAndWrite(
                m,
                dir,
                List.of(),
                List.of(new Unit("f0.postings", "title", 4096), new Unit("f0.dv", "price", 2048), new Unit("f1.postings", "title", 1024))
            );
            assertThat(m.partitions(), containsInAnyOrder("title", "price"));
            assertEquals(2, m.unitCount("title"));
            assertEquals(4096 + 1024, m.weight("title")); // bytes
            // Compact the "title" column: replace its two files with one.
            m.commit(List.of("f0.postings", "f1.postings"), List.of(new Unit("f2.postings", "title", 5000)));
            assertEquals(1, m.unitCount("title"));
            assertEquals(5000, m.weight("title"));

            final PartitionedManifest recovered = PartitionedManifest.recover(dir);
            assertThat(recovered.partitions(), containsInAnyOrder("title", "price"));
        }
    }

    private static Set<String> names(List<Unit> units) {
        return units.stream().map(Unit::name).collect(java.util.stream.Collectors.toSet());
    }
}
