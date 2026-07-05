/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The read-side active-set bound: a shard may hold millions of tenants ("slices"), but only a bounded number of
 * their readers are kept open at once. Each open reader covers exactly one tenant's segments (via
 * {@link SliceScopedReader}), so an inactive tenant's segments are never materialized — the read-side mirror of the
 * write-side {@code maxActivePartitions} / {@link SliceBufferManager}. Without this, the shard's single reader would
 * open every tenant's segments (millions of {@code SegmentReader}s → heap wall); segment-pruning at query time
 * ({@link SlicePruningQuery}) isolates a query but does not stop those segments from being opened — this does.
 * <p>
 * Lifecycle:
 * <ul>
 *   <li>{@link #acquire} returns a ref-counted handle over the tenant's reader, opening it on first access.</li>
 *   <li>When the open count would exceed {@code maxActive}, the least-recently-used <b>idle</b> (ref-count 0) reader
 *       is closed. If all open readers are in use the bound is exceeded transiently rather than blocking a query.</li>
 *   <li>{@link #drainIdle} closes idle readers untouched for longer than an idle interval.</li>
 *   <li>A reader marked for eviction while still in use is closed when its last handle is released.</li>
 *   <li>{@link #refresh} advances the commit the pool serves; readers opened against an older commit are retired
 *       (closed once idle), and the next {@link #acquire} reopens against the new commit.</li>
 * </ul>
 * Callers pass {@code nowNanos} (production: {@code System.nanoTime()}) so tests can drive time deterministically,
 * matching {@link SliceBufferManager}. Not thread-safe externally beyond its own synchronization.
 */
public final class SliceReaderPool implements Closeable {

    private final Directory directory;
    private final int maxActive;

    private IndexCommit commit;
    private long accessClock;
    /** Current-commit readers, keyed by slice. */
    private final Map<String, Holder> open = new HashMap<>();
    /** Old-commit readers marked for retirement but still in use; closed when their last handle is released. */
    private final List<Holder> retiring = new ArrayList<>();

    public SliceReaderPool(Directory directory, IndexCommit commit, int maxActive) {
        if (maxActive < 1) {
            throw new IllegalArgumentException("maxActive must be >= 1, got " + maxActive);
        }
        this.directory = directory;
        this.commit = commit;
        this.maxActive = maxActive;
    }

    /** Advances the commit served by the pool. Readers on the old commit are closed if idle, else retired. */
    public synchronized void refresh(IndexCommit newCommit) throws IOException {
        this.commit = newCommit;
        final List<Holder> stale = new ArrayList<>();
        for (Holder h : open.values()) {
            if (h.commit != newCommit) {
                stale.add(h);
            }
        }
        for (Holder h : stale) {
            open.remove(h.slice);
            if (h.refCount == 0) {
                h.reader.close();
            } else {
                retiring.add(h); // still in use — close on last release
            }
        }
    }

    /**
     * Acquires a ref-counted reader over {@code slice}'s segments in the current commit, opening it if needed and
     * evicting an idle LRU reader if that would exceed {@code maxActive}. The caller must {@link Ref#close()} it.
     */
    public synchronized Ref acquire(String slice, long nowNanos) throws IOException {
        Holder h = open.get(slice);
        if (h != null) {
            h.touch(++accessClock, nowNanos);
            h.refCount++;
            return new Ref(h);
        }
        if (open.size() >= maxActive) {
            evictLruIdle();
        }
        final DirectoryReader reader = SliceScopedReader.open(directory, commit, slice);
        h = new Holder(slice, reader, commit);
        h.touch(++accessClock, nowNanos);
        h.refCount = 1;
        open.put(slice, h);
        return new Ref(h);
    }

    /**
     * Acquires a bounded per-tenant reader (as {@link #acquire}) and returns it as an {@link Engine.Searcher} over an
     * {@link ElasticsearchDirectoryReader}, ready for the shard search path. The returned searcher holds the pool ref
     * and an extra reader ref for its own lifetime; closing it releases both, leaving the pool to own the reader.
     */
    public synchronized Engine.Searcher acquireSearcher(
        String source,
        String slice,
        long nowNanos,
        ShardId shardId,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy
    ) throws IOException {
        final Ref ref = acquire(slice, nowNanos);
        boolean success = false;
        try {
            // Extra ref balances the ElasticsearchDirectoryReader's close() below, so closing the searcher does not
            // close the pool-owned reader — the pool closes it on eviction.
            ref.reader().incRef();
            final ElasticsearchDirectoryReader esReader = ElasticsearchDirectoryReader.wrap(ref.reader(), shardId);
            final Engine.Searcher searcher = new Engine.Searcher(
                source,
                esReader,
                similarity,
                queryCache,
                queryCachingPolicy,
                () -> IOUtils.close(esReader, ref) // esReader.close() decRefs the reader (undo incRef); ref releases the pool acquisition
            );
            success = true;
            return searcher;
        } finally {
            if (success == false) {
                ref.reader().decRef();
                ref.close();
            }
        }
    }

    /** Closes idle (ref-count 0) readers not accessed within {@code idleNanos}. */
    public synchronized void drainIdle(long nowNanos, long idleNanos) throws IOException {
        final List<Holder> toClose = new ArrayList<>();
        for (Holder h : open.values()) {
            if (h.refCount == 0 && nowNanos - h.lastAccessNanos >= idleNanos) {
                toClose.add(h);
            }
        }
        for (Holder h : toClose) {
            closeAndRemove(h);
        }
    }

    /** Number of tenant readers currently open (materialized) — current-commit plus any still-in-use retiring ones. */
    public synchronized int openReaderCount() {
        return open.size() + retiring.size();
    }

    @Override
    public synchronized void close() throws IOException {
        final List<DirectoryReader> readers = new ArrayList<>(open.size() + retiring.size());
        for (Holder h : open.values()) {
            readers.add(h.reader);
        }
        for (Holder h : retiring) {
            readers.add(h.reader);
        }
        open.clear();
        retiring.clear();
        IOUtils.close(readers);
    }

    private void evictLruIdle() throws IOException {
        Holder victim = null;
        for (Holder h : open.values()) {
            if (h.refCount == 0 && (victim == null || h.lastAccessClock < victim.lastAccessClock)) {
                victim = h;
            }
        }
        if (victim != null) {
            closeAndRemove(victim);
        }
        // else: all open readers are in use — the bound is exceeded transiently rather than blocking a query.
    }

    private void closeAndRemove(Holder h) throws IOException {
        open.remove(h.slice);
        h.reader.close();
    }

    private void release(Holder h) {
        h.refCount--;
        assert h.refCount >= 0 : "over-released slice reader " + h.slice;
        if (h.refCount == 0 && retiring.remove(h)) {
            try {
                h.reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** A borrowed reference to a tenant's reader; releasing it decrements the ref-count. */
    public final class Ref implements Closeable {
        private final Holder holder;
        private boolean closed;

        private Ref(Holder holder) {
            this.holder = holder;
        }

        public DirectoryReader reader() {
            return holder.reader;
        }

        @Override
        public void close() {
            synchronized (SliceReaderPool.this) {
                if (closed == false) {
                    closed = true;
                    release(holder);
                }
            }
        }
    }

    private static final class Holder {
        private final String slice;
        private final DirectoryReader reader;
        private final IndexCommit commit;
        private int refCount;
        private long lastAccessClock;
        private long lastAccessNanos;

        Holder(String slice, DirectoryReader reader, IndexCommit commit) {
            this.slice = slice;
            this.reader = reader;
            this.commit = commit;
        }

        void touch(long clock, long nanos) {
            this.lastAccessClock = clock;
            this.lastAccessNanos = nanos;
        }
    }
}
