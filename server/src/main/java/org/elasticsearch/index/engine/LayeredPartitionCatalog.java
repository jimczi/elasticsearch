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
 * The LSM read view of the catalog: an immutable off-heap FST base (the last compacted snapshot) plus a small
 * in-memory delta of the commits since. {@link #apply(Collection, Collection)} folds a commit's changed units into
 * the delta in O(#changed) without touching the base; {@link #units(String)} reads the base's partition slice and
 * merges the delta on top (delta wins on name collision), O(#partition units); {@link #compact()} periodically folds
 * base + delta into a fresh base FST (O(#total)). So a new commit never resends or rebuilds the full unit list.
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
     * Folds base (minus removed) + delta into a fresh immutable base FST (O(#total); run periodically). The caller
     * swaps to a {@code new LayeredPartitionCatalog(returned)} with an empty delta and typically
     * {@link FstPartitionCatalog#save}s it as the new snapshot.
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
