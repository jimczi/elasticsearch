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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The LSM read view of the slice/columnar catalog: an <b>immutable off-heap FST base</b> (the last compacted
 * snapshot) with a small <b>in-memory delta</b> of the commits since. This is what answers "when a new commit
 * adds segments, do we resend/rebuild the whole list?" — no:
 * <ul>
 *   <li>A commit calls {@link #apply(Collection, Collection)} with only its added/removed units; that mutates the
 *       in-memory delta in O(#changed). The immutable base FST is untouched, and nothing re-serializes the full
 *       segment list.</li>
 *   <li>A query ({@link #units(String)}) reads the partition's units from the base FST (prefix scan) and merges
 *       the delta on top — base units hidden if the delta removed them, delta units added, delta weights winning.
 *       O(#partition units), never O(#total).</li>
 *   <li>Only {@link #compact()} folds delta into a fresh base FST (O(#total)), run periodically — the LSM's
 *       amortized rebuild, not a per-commit cost.</li>
 * </ul>
 * So the searcher <b>incrementally adds</b> from the current state; it neither receives the full list per commit
 * nor rebuilds it per commit.
 */
public final class LayeredPartitionCatalog implements Closeable {

    private final FstPartitionCatalog base;
    /** Units added since the base snapshot (in-memory, partition-indexed). */
    private final PartitionedManifest delta = new PartitionedManifest();
    /** Base unit names hidden (removed) since the base snapshot. */
    private final Set<String> removedFromBase = new HashSet<>();

    public LayeredPartitionCatalog(FstPartitionCatalog base) {
        this.base = base;
    }

    /**
     * Applies one commit's delta (only its changed units) to the in-memory layer. O(#changed). A name that also
     * exists in the base stays hidden via {@code removedFromBase} — the delta owns the current version — so
     * base and delta never both surface the same name.
     */
    public synchronized void apply(Collection<String> removed, Collection<Unit> added) {
        for (String name : removed) {
            removedFromBase.add(name); // hides it if it lives in the base
        }
        for (Unit u : added) {
            removedFromBase.add(u.name()); // if this name also lives in the base, the delta version supersedes it
        }
        delta.commit(removed, added); // delta owns units added after the base; re-remove drops them from the delta
    }

    /** Live units of one partition: base (minus removed) merged with the delta (which wins on name collision). */
    public synchronized List<Unit> units(String partition) throws IOException {
        final List<Unit> result = new ArrayList<>(delta.units(partition));
        final Set<String> deltaNames = new HashSet<>();
        for (Unit u : result) {
            deltaNames.add(u.name());
        }
        for (Unit b : base.units(partition)) {
            if (removedFromBase.contains(b.name()) == false && deltaNames.contains(b.name()) == false) {
                result.add(b);
            }
        }
        return result;
    }

    public synchronized int unitCount(String partition) throws IOException {
        return units(partition).size();
    }

    /** Number of in-memory delta commits' worth of units — a proxy for "time to compact". */
    public synchronized int pendingDeltaUnits() {
        return delta.unitCount();
    }

    /**
     * Folds base (minus removed) + delta into a fresh immutable base FST and returns it. O(#total); run
     * periodically. This is a pure function of current state — the caller swaps to a {@code new
     * LayeredPartitionCatalog(returned)} (with an empty delta) and discards this view, mirroring an immutable-SST
     * swap. Would typically be {@link FstPartitionCatalog#save}d as the new snapshot.
     */
    public synchronized FstPartitionCatalog compact() throws IOException {
        final List<Unit> merged = new ArrayList<>();
        for (Unit b : base.allUnits()) {
            if (removedFromBase.contains(b.name()) == false) {
                merged.add(b);
            }
        }
        for (String p : delta.partitions()) {
            merged.addAll(delta.units(p));
        }
        return FstPartitionCatalog.build(merged);
    }

    @Override
    public void close() throws IOException {
        base.close();
    }
}
