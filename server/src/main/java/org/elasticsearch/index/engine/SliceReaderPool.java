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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Bounds the read-side active set: a shard may hold millions of tenants ("slices") but keeps only {@code maxActive}
 * of their readers open at once. Each reader covers exactly one tenant's segments (via {@link SliceScopedReader}),
 * so inactive tenants are never materialized — the read-side mirror of the write-side {@link SliceBufferManager}.
 * Query-time {@link SlicePruningQuery} isolates a query but does not stop segments being opened; this does.
 * <p>
 * {@link #acquire} returns a ref-counted handle, opening the reader on demand and evicting the LRU <b>idle</b>
 * reader when the bound would be exceeded (if all are in use the bound is exceeded transiently rather than blocking).
 * {@link #drainIdle} closes long-idle readers; {@link #refresh} advances the served commit, retiring old-commit
 * readers once idle. Callers pass {@code nowNanos} so time is deterministic in tests. Thread-safe via its own lock.
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
    /**
     * A deletion-policy pin per commit the pool has open readers on, so the engine cannot delete a commit's files
     * while a (possibly retiring) reader still uses them. Released when a commit's last reader drains. Identity-keyed
     * because {@link IndexCommit} equality is by generation. Values may be null when no pinning is wired (tests).
     */
    private final Map<IndexCommit, Releasable> commitPins = new IdentityHashMap<>();

    /**
     * Charges a newly opened reader against a heap budget (the stateless SearchEngine's reader-heap ledger + breaker)
     * and returns a {@link Releasable} that refunds it when the pool closes the reader. Null when no budget is wired.
     */
    private final Function<DirectoryReader, Releasable> heapCharger;

    public SliceReaderPool(Directory directory, IndexCommit commit, int maxActive) {
        this(directory, commit, null, null, maxActive);
    }

    public SliceReaderPool(Directory directory, IndexCommit commit, Releasable commitPin, int maxActive) {
        this(directory, commit, commitPin, null, maxActive);
    }

    public SliceReaderPool(
        Directory directory,
        IndexCommit commit,
        Releasable commitPin,
        Function<DirectoryReader, Releasable> heapCharger,
        int maxActive
    ) {
        if (maxActive < 1) {
            throw new IllegalArgumentException("maxActive must be >= 1, got " + maxActive);
        }
        this.directory = directory;
        this.commit = commit;
        this.maxActive = maxActive;
        this.heapCharger = heapCharger;
        commitPins.put(commit, commitPin);
    }

    /** Advances the commit served by the pool (no new pin). */
    public synchronized void refresh(IndexCommit newCommit) throws IOException {
        refresh(newCommit, null);
    }

    /**
     * Advances the commit served by the pool, pinning {@code newCommit} with {@code newCommitPin} (released when the
     * pool no longer reads it). Readers on the old commit are closed if idle, else retired; a commit's pin is released
     * once its last reader drains.
     */
    public synchronized void refresh(IndexCommit newCommit, Releasable newCommitPin) throws IOException {
        if (newCommit != commit) {
            commitPins.putIfAbsent(newCommit, newCommitPin);
        } else if (newCommitPin != null) {
            // Same commit already pinned; drop the redundant new pin.
            newCommitPin.close();
        }
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
                IOUtils.close(h.reader, h.heapCharge);
                maybeReleasePin(h.commit);
            } else {
                retiring.add(h); // still in use — close on last release
            }
        }
    }

    /** Releases the pin for {@code commit} if it is no longer the current commit and has no remaining open readers. */
    private void maybeReleasePin(IndexCommit forCommit) {
        if (forCommit == commit) {
            return;
        }
        for (Holder h : open.values()) {
            if (h.commit == forCommit) {
                return;
            }
        }
        for (Holder h : retiring) {
            if (h.commit == forCommit) {
                return;
            }
        }
        final Releasable pin = commitPins.remove(forCommit);
        if (pin != null) {
            pin.close();
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
        boolean charged = false;
        try {
            // Charge the reader's segments against the heap budget before it is handed out, so the budget accounts
            // for every reader the pool holds. Released in closeAndRemove/release/refresh/close.
            h.heapCharge = heapCharger != null ? heapCharger.apply(reader) : null;
            h.touch(++accessClock, nowNanos);
            h.refCount = 1;
            open.put(slice, h);
            charged = true;
            return new Ref(h);
        } finally {
            if (charged == false) {
                IOUtils.closeWhileHandlingException(reader, h.heapCharge);
            }
        }
    }

    /**
     * Acquires a bounded per-tenant reader (as {@link #acquire}) as an {@link Engine.Searcher} over an
     * {@link ElasticsearchDirectoryReader}. Closing the searcher releases the pool ref but leaves the pool owning
     * the reader.
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

    /**
     * Returns an {@link Engine.SearcherSupplier} holding one tenant reader <b>stable</b> for its whole lifetime, so
     * every {@code acquireSearcher} (query phase, fetch phase, ...) sees the same reader — the point-in-time contract
     * the search path requires. The held pool ref keeps the reader unevictable until the supplier is closed, at which
     * point the ref and {@code onClose} are released. The {@code wrapper} is applied by the base class on each acquire.
     */
    public synchronized Engine.SearcherSupplier acquireSearcherSupplier(
        Function<Engine.Searcher, Engine.Searcher> wrapper,
        String slice,
        long nowNanos,
        ShardId shardId,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        Releasable onClose
    ) throws IOException {
        final Ref ref = acquire(slice, nowNanos); // held for the supplier's lifetime -> reader is stable & unevictable
        boolean success = false;
        try {
            final Engine.SearcherSupplier supplier = new Engine.SearcherSupplier(wrapper) {
                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    try {
                        ref.reader().incRef(); // balance the ES wrapper's close(); the pool ref keeps the reader alive
                        final var esReader = ElasticsearchDirectoryReader.wrap(ref.reader(), shardId);
                        return new Engine.Searcher(source, esReader, similarity, queryCache, queryCachingPolicy, esReader::close);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                protected void doClose() {
                    IOUtils.closeWhileHandlingException(ref, onClose);
                }
            };
            success = true;
            return supplier;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(ref, onClose);
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
        final List<Closeable> resources = new ArrayList<>();
        for (Holder h : open.values()) {
            resources.add(h.reader);
            resources.add(h.heapCharge);
        }
        for (Holder h : retiring) {
            resources.add(h.reader);
            resources.add(h.heapCharge);
        }
        for (Releasable pin : commitPins.values()) {
            resources.add(pin);
        }
        open.clear();
        retiring.clear();
        commitPins.clear();
        IOUtils.close(resources);
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
        IOUtils.close(h.reader, h.heapCharge);
        maybeReleasePin(h.commit);
    }

    private void release(Holder h) {
        h.refCount--;
        assert h.refCount >= 0 : "over-released slice reader " + h.slice;
        if (h.refCount == 0 && retiring.remove(h)) {
            try {
                IOUtils.close(h.reader, h.heapCharge);
                maybeReleasePin(h.commit);
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
        private Releasable heapCharge; // budget refund for this reader's segments, or null when no budget is wired
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
