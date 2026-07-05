/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

/**
 * Verifies the read-side active-set bound: only N tenant readers open at once, LRU/idle eviction, ref-count safety
 * (an in-use reader is never closed), and per-tenant isolation (each reader sees only its slice's docs).
 */
public class SliceReaderPoolTests extends ESTestCase {

    private static IndexWriterConfig config() {
        final IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setDocumentPartitioner(doc -> {
            for (IndexableField f : doc) {
                if (f.name().equals("slice")) {
                    return f.stringValue();
                }
            }
            return null;
        });
        iwc.setMergePolicy(new SlicePartitionedMergePolicy(new TieredMergePolicy()));
        return iwc;
    }

    private static Document doc(String slice, String id) {
        final Document d = new Document();
        d.add(new StringField("slice", slice, Field.Store.NO));
        d.add(new StringField("id", id, Field.Store.NO));
        return d;
    }

    private static IndexCommit commitOf(Directory dir) throws Exception {
        return DirectoryReader.listCommits(dir).get(DirectoryReader.listCommits(dir).size() - 1);
    }

    public void testBoundedLruAndIdleEvictionWithRefCountSafety() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, config())) {
                // 5 tenants, differing sizes so isolation is checkable.
                for (int s = 0; s < 5; s++) {
                    for (int i = 0; i <= s; i++) { // tenant s has s+1 docs
                        w.addDocument(doc("tenant" + s, "t" + s + "_" + i));
                    }
                }
                w.commit();
            }
            final IndexCommit commit = commitOf(dir);

            try (SliceReaderPool pool = new SliceReaderPool(dir, commit, 2)) {
                // Acquire A and B -> 2 open, each isolated to its own docs.
                final var a = pool.acquire("tenant0", 1000);
                final var b = pool.acquire("tenant1", 1001);
                assertEquals(2, pool.openReaderCount());
                assertEquals(1, a.reader().numDocs()); // tenant0 has 1 doc
                assertEquals(2, b.reader().numDocs()); // tenant1 has 2 docs
                assertOnlySlice(a.reader(), "tenant0");
                assertOnlySlice(b.reader(), "tenant1");

                // Release A (now idle). Acquiring C exceeds maxActive(2) -> evicts the LRU idle reader (A).
                a.close();
                final var c = pool.acquire("tenant2", 1002);
                assertEquals(2, pool.openReaderCount());
                assertEquals(3, c.reader().numDocs()); // tenant2 has 3 docs

                // B is still in use, so it must NOT have been evicted; re-acquiring B reuses the open reader.
                final var b2 = pool.acquire("tenant1", 1003);
                assertEquals(2, pool.openReaderCount());
                assertSame(b.reader(), b2.reader());

                // Both B refs + C are in use: acquiring D cannot evict anyone -> bound exceeded transiently.
                final var d = pool.acquire("tenant3", 1004);
                assertEquals(3, pool.openReaderCount());

                // Release everything; drainIdle past the idle window closes all idle readers.
                b.close();
                b2.close();
                c.close();
                d.close();
                pool.drainIdle(2000, 100); // now=2000, idle>=100 -> all (last access <= 1004) are idle
                assertEquals(0, pool.openReaderCount());
            }
        }
    }

    public void testRefreshRetiresOldCommitReaders() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, config())) {
                w.addDocument(doc("tenantA", "a0"));
                w.commit();
                final IndexCommit c1 = commitOf(dir);

                try (SliceReaderPool pool = new SliceReaderPool(dir, c1, 4)) {
                    final var r1 = pool.acquire("tenantA", 1);
                    assertEquals(1, r1.reader().numDocs());

                    // New commit adds a doc to tenantA.
                    w.addDocument(doc("tenantA", "a1"));
                    w.commit();
                    final IndexCommit c2 = commitOf(dir);

                    // While the old reader is still in use, refresh marks it for retirement but does not close it.
                    pool.refresh(c2);
                    assertEquals(1, pool.openReaderCount());
                    assertEquals(1, r1.reader().numDocs()); // old reader still valid & in use

                    // Next acquire opens against the NEW commit (sees 2 docs); the old reader retires on release.
                    final var r2 = pool.acquire("tenantA", 2);
                    assertEquals(2, r2.reader().numDocs());
                    r1.close(); // releasing the retired reader closes it
                    assertEquals(1, pool.openReaderCount());
                    r2.close();
                }
            }
        }
    }

    public void testAcquireSearcherIsolatedEsWrappedAndReleasesToPool() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, config())) {
                for (int i = 0; i < 4; i++) {
                    w.addDocument(doc("tenantA", "a" + i));
                }
                for (int i = 0; i < 6; i++) {
                    w.addDocument(doc("tenantB", "b" + i));
                }
                w.commit();
            }
            final IndexCommit commit = commitOf(dir);
            final ShardId shardId = new ShardId("idx", "uuid", 0);

            try (SliceReaderPool pool = new SliceReaderPool(dir, commit, 2)) {
                final Engine.Searcher searcher = pool.acquireSearcher(
                    "test",
                    "tenantB",
                    1000,
                    shardId,
                    new BM25Similarity(),
                    org.apache.lucene.search.IndexSearcher.getDefaultQueryCache(),
                    org.apache.lucene.search.IndexSearcher.getDefaultQueryCachingPolicy()
                );
                assertEquals(1, pool.openReaderCount());
                // It is a valid ES searcher over only tenantB's docs.
                assertNotNull(ElasticsearchDirectoryReader.getElasticsearchDirectoryReader(searcher.getDirectoryReader()));
                assertEquals(6, searcher.getIndexReader().numDocs());
                assertEquals(6, searcher.count(new org.apache.lucene.search.MatchAllDocsQuery()));

                // Closing the searcher releases the pool ref but must NOT close the pool-owned reader: it stays usable.
                searcher.close();
                assertEquals(1, pool.openReaderCount());
                try (var ref = pool.acquire("tenantB", 1001)) {
                    assertEquals("pool reader still valid after searcher close", 6, ref.reader().numDocs());
                }

                // Idle-drain then closes it.
                pool.drainIdle(5000, 100);
                assertEquals(0, pool.openReaderCount());
            }
        }
    }

    public void testSearcherSurvivesRefreshWhileInUse() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, config())) {
                w.addDocument(doc("tenantA", "a0"));
                w.commit();
            }
            final IndexCommit c1 = commitOf(dir);
            try (SliceReaderPool pool = new SliceReaderPool(dir, c1, 4)) {
                final Engine.Searcher searcher = pool.acquireSearcher(
                    "s",
                    "tenantA",
                    1,
                    new ShardId("idx", "uuid", 0),
                    new BM25Similarity(),
                    org.apache.lucene.search.IndexSearcher.getDefaultQueryCache(),
                    org.apache.lucene.search.IndexSearcher.getDefaultQueryCachingPolicy()
                );
                // A new commit + refresh retires the old reader, but the open searcher keeps working on it.
                try (IndexWriter w2 = new IndexWriter(dir, config())) {
                    w2.addDocument(doc("tenantA", "a1"));
                    w2.commit();
                }
                pool.refresh(commitOf(dir));
                assertEquals("old reader still counted while the searcher holds it", 1, pool.openReaderCount());
                assertEquals("searcher still sees its snapshot", 1, searcher.getIndexReader().numDocs());
                searcher.close();
                assertEquals("retired reader closed once released", 0, pool.openReaderCount());
            }
        }
    }

    public void testConcurrentAcquireSearcherStaysBoundedAndLeakFree() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, config())) {
                for (int s = 0; s < 20; s++) {
                    w.addDocument(doc("tenant" + s, "d" + s));
                }
                w.commit();
            }
            final IndexCommit commit = commitOf(dir);
            final ShardId shardId = new ShardId("idx", "uuid", 0);
            final int maxActive = 4;
            try (SliceReaderPool pool = new SliceReaderPool(dir, commit, maxActive)) {
                final int threads = 8;
                final Thread[] workers = new Thread[threads];
                final java.util.concurrent.atomic.AtomicReference<Exception> failure = new java.util.concurrent.atomic.AtomicReference<>();
                for (int t = 0; t < threads; t++) {
                    final int tid = t;
                    workers[t] = new Thread(() -> {
                        try {
                            for (int i = 0; i < 200; i++) {
                                final String slice = "tenant" + ((tid + i) % 20);
                                try (
                                    Engine.Searcher searcher = pool.acquireSearcher(
                                        "s",
                                        slice,
                                        i,
                                        shardId,
                                        new BM25Similarity(),
                                        org.apache.lucene.search.IndexSearcher.getDefaultQueryCache(),
                                        org.apache.lucene.search.IndexSearcher.getDefaultQueryCachingPolicy()
                                    )
                                ) {
                                    assertEquals(1, searcher.count(new org.apache.lucene.search.MatchAllDocsQuery()));
                                }
                            }
                        } catch (Exception e) {
                            failure.set(e);
                        }
                    });
                }
                for (Thread w : workers) {
                    w.start();
                }
                for (Thread w : workers) {
                    w.join();
                }
                assertNull("concurrent acquire/search/close must not fail: " + failure.get(), failure.get());
                // All handles released; drain closes everything -> no leaked readers.
                pool.drainIdle(Long.MAX_VALUE / 2, 0);
                assertEquals(0, pool.openReaderCount());
            }
        }
    }

    private static void assertOnlySlice(CompositeReader reader, String slice) throws Exception {
        for (var leaf : reader.leaves()) {
            final String segSlice = Lucene.segmentReader(leaf.reader()).getSegmentInfo().info.getAttribute("lucene.partition.key");
            assertEquals("reader must contain only " + slice + "'s segments", slice, segSlice);
        }
    }
}
