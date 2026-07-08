/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which slices (tenants) have recently-buffered documents so that <em>idle</em> slices can be flushed to
 * segments, letting inactive tenants stop consuming indexing memory (in stateless, pushing their data to object
 * storage). This complements Lucene's <b>count</b>-based bound ({@code IndexWriterConfig#setMaxActivePartitions})
 * with a <b>time</b>-based one: {@link #drainIdle} surfaces slices write-idle for {@code idleIntervalNanos} to flush.
 * Time is passed in (the engine's relative-nanos clock) to keep this deterministically testable.
 */
public final class SliceBufferManager {

    private final Map<Object, Long> lastWriteNanos = new ConcurrentHashMap<>();
    private final long idleIntervalNanos;

    /** @param idleIntervalNanos how long a slice may be write-idle before it is eligible to flush; 0 disables. */
    public SliceBufferManager(long idleIntervalNanos) {
        this.idleIntervalNanos = idleIntervalNanos;
    }

    /** Records that a document was buffered for {@code slice} at {@code nowNanos}. Null slices are ignored. */
    public void onWrite(Object slice, long nowNanos) {
        if (slice != null) {
            lastWriteNanos.put(slice, nowNanos);
        }
    }

    /**
     * Returns the slices that have been write-idle for at least {@code idleIntervalNanos} as of
     * {@code nowNanos}, atomically removing them from tracking so each is flushed at most once. A slice
     * that received a write concurrently (its timestamp advanced) is left tracked and not returned.
     */
    public List<Object> drainIdle(long nowNanos) {
        if (idleIntervalNanos <= 0 || lastWriteNanos.isEmpty()) {
            return List.of();
        }
        final List<Object> idle = new ArrayList<>();
        for (Map.Entry<Object, Long> entry : lastWriteNanos.entrySet()) {
            final long last = entry.getValue();
            if (nowNanos - last >= idleIntervalNanos && lastWriteNanos.remove(entry.getKey(), last)) {
                idle.add(entry.getKey());
            }
        }
        return idle;
    }

    /** Stops tracking {@code slice} (e.g. after it was flushed or evicted for another reason). */
    public void forget(Object slice) {
        lastWriteNanos.remove(slice);
    }

    /** Number of slices currently tracked as having buffered documents. */
    public int trackedSlices() {
        return lastWriteNanos.size();
    }
}
