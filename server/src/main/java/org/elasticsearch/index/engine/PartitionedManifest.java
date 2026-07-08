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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A partition-indexed, append-delta catalog of physical units — the LSM {@code MANIFEST} for a shard holding very
 * many small units without paying O(#units) per commit. Used for slices ({@code partition = slice}, {@code unit =
 * segment}, {@code weight = docCount}) and columnar ({@code partition = field}, {@code unit = file}, {@code weight =
 * bytes}). {@link #commit} records only changed units (O(#changed)), {@link #units(String)} lists one partition's
 * units, and {@link #dirtyPartitions(int)} finds partitions to compact — never crossing partitions.
 * <p>
 * Persistence is one {@code edit_<gen>} blob per commit plus periodic {@link #writeSnapshot} compaction;
 * {@link #recover} loads the latest snapshot then later edits, ignoring a crash-truncated trailing edit. In-memory
 * state is a plain map; the compacted snapshot's sorted output is the build input for an off-heap FST at scale.
 */
public final class PartitionedManifest {

    static final String EDIT_PREFIX = "manifest_edit_";
    static final String SNAPSHOT_PREFIX = "manifest_snapshot_";
    private static final String EDIT_CODEC = "PartitionedManifestEdit";
    private static final String SNAPSHOT_CODEC = "PartitionedManifestSnapshot";
    private static final int VERSION = 1;

    /** A physical unit (a segment, or a file) and the partition (slice, field, or {@code slice/field}) that owns it. */
    public record Unit(String name, String partition, long weight) {}

    /** name -> unit (all live units). */
    private final Map<String, Unit> byName = new HashMap<>();
    /** partition -> live unit names of that partition (insertion order kept for deterministic listing). */
    private final Map<String, Set<String>> byPartition = new HashMap<>();
    private long generation;

    public PartitionedManifest() {}

    // ---- mutation ---------------------------------------------------------------------------------------------

    /**
     * Applies a commit delta: remove {@code removed} unit names, then add {@code added} units. Returns the new
     * generation. Removing an unknown name and re-adding an existing name are idempotent, so replay is safe.
     */
    public synchronized long commit(Collection<String> removed, Collection<Unit> added) {
        for (String name : removed) {
            removeUnit(name);
        }
        for (Unit unit : added) {
            addUnit(unit);
        }
        return ++generation;
    }

    private void addUnit(Unit unit) {
        final Unit previous = byName.put(unit.name(), unit);
        if (previous != null && previous.partition().equals(unit.partition()) == false) {
            final Set<String> old = byPartition.get(previous.partition());
            if (old != null) {
                old.remove(unit.name());
                if (old.isEmpty()) {
                    byPartition.remove(previous.partition());
                }
            }
        }
        byPartition.computeIfAbsent(unit.partition(), k -> new LinkedHashSet<>()).add(unit.name());
    }

    private void removeUnit(String name) {
        final Unit removed = byName.remove(name);
        if (removed != null) {
            final Set<String> names = byPartition.get(removed.partition());
            if (names != null) {
                names.remove(name);
                if (names.isEmpty()) {
                    byPartition.remove(removed.partition());
                }
            }
        }
    }

    // ---- queries ----------------------------------------------------------------------------------------------

    /** Live units of one partition. O(#partition units), not O(#total). */
    public synchronized List<Unit> units(String partition) {
        final Set<String> names = byPartition.get(partition);
        if (names == null) {
            return List.of();
        }
        final List<Unit> result = new ArrayList<>(names.size());
        for (String name : names) {
            result.add(byName.get(name));
        }
        return result;
    }

    /** All partitions with at least one live unit. */
    public synchronized Set<String> partitions() {
        return new LinkedHashSet<>(byPartition.keySet());
    }

    public synchronized int partitionCount() {
        return byPartition.size();
    }

    public synchronized int unitCount() {
        return byName.size();
    }

    public synchronized int unitCount(String partition) {
        final Set<String> names = byPartition.get(partition);
        return names == null ? 0 : names.size();
    }

    /** Total weight (docs for slices, bytes for columnar) of a partition. */
    public synchronized long weight(String partition) {
        final Set<String> names = byPartition.get(partition);
        if (names == null) {
            return 0;
        }
        long total = 0;
        for (String name : names) {
            total += byName.get(name).weight();
        }
        return total;
    }

    /** Partitions whose own unit count is at least {@code minUnits} — the merge/compaction candidates. */
    public synchronized List<String> dirtyPartitions(int minUnits) {
        final List<String> dirty = new ArrayList<>();
        for (Map.Entry<String, Set<String>> e : byPartition.entrySet()) {
            if (e.getValue().size() >= minUnits) {
                dirty.add(e.getKey());
            }
        }
        return dirty;
    }

    public synchronized long generation() {
        return generation;
    }

    // ---- persistence (object-store-native: one blob per commit + periodic snapshot) ---------------------------

    /** Writes the per-commit delta blob {@code edit_<generation>}. */
    public static void writeEdit(Directory dir, long generation, Collection<String> removed, Collection<Unit> added) throws IOException {
        try (IndexOutput out = dir.createOutput(EDIT_PREFIX + generation, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, EDIT_CODEC, VERSION);
            out.writeVLong(generation);
            out.writeVInt(removed.size());
            for (String r : removed) {
                out.writeString(r);
            }
            writeUnits(out, added);
            CodecUtil.writeFooter(out);
        }
    }

    /** Writes a compacted full-state snapshot {@code snapshot_<generation>} (sorted → FST build input). */
    public synchronized void writeSnapshot(Directory dir) throws IOException {
        try (IndexOutput out = dir.createOutput(SNAPSHOT_PREFIX + generation, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(out, SNAPSHOT_CODEC, VERSION);
            out.writeVLong(generation);
            final TreeSet<String> names = new TreeSet<>(byName.keySet());
            out.writeVInt(names.size());
            for (String n : names) {
                writeUnit(out, byName.get(n));
            }
            CodecUtil.writeFooter(out);
        }
    }

    private static void writeUnits(IndexOutput out, Collection<Unit> units) throws IOException {
        out.writeVInt(units.size());
        for (Unit u : units) {
            writeUnit(out, u);
        }
    }

    private static void writeUnit(IndexOutput out, Unit u) throws IOException {
        out.writeString(u.name());
        out.writeString(u.partition());
        out.writeVLong(u.weight());
    }

    /**
     * Recovers by loading the latest snapshot (if any) then replaying every {@code edit_<gen>} with a greater
     * generation, in order. A partially-written trailing edit (missing/invalid footer) is ignored — crash-safe.
     */
    public static PartitionedManifest recover(Directory dir) throws IOException {
        final PartitionedManifest manifest = new PartitionedManifest();
        long snapshotGen = -1;
        for (String file : dir.listAll()) {
            if (file.startsWith(SNAPSHOT_PREFIX)) {
                snapshotGen = Math.max(snapshotGen, Long.parseLong(file.substring(SNAPSHOT_PREFIX.length())));
            }
        }
        if (snapshotGen >= 0) {
            manifest.loadSnapshot(dir, SNAPSHOT_PREFIX + snapshotGen);
        }
        final TreeSet<Long> editGens = new TreeSet<>();
        for (String file : dir.listAll()) {
            if (file.startsWith(EDIT_PREFIX)) {
                final long gen = Long.parseLong(file.substring(EDIT_PREFIX.length()));
                if (gen > snapshotGen) {
                    editGens.add(gen);
                }
            }
        }
        final List<Long> ordered = new ArrayList<>(editGens);
        for (int i = 0; i < ordered.size(); i++) {
            if (manifest.replayEdit(dir, EDIT_PREFIX + ordered.get(i)) == false) {
                // A truncated/corrupt edit is only acceptable as the very last one (a crash mid-write). If
                // later edits exist, the log is genuinely corrupt and silently dropping them would lose data.
                if (i != ordered.size() - 1) {
                    throw new CorruptIndexException(
                        "corrupt manifest edit gen=" + ordered.get(i) + " with " + (ordered.size() - 1 - i) + " later edit(s) present",
                        EDIT_PREFIX + ordered.get(i)
                    );
                }
                break;
            }
        }
        return manifest;
    }

    private void loadSnapshot(Directory dir, String name) throws IOException {
        try (ChecksumIndexInput in = dir.openChecksumInput(name)) {
            CodecUtil.checkHeader(in, SNAPSHOT_CODEC, VERSION, VERSION);
            generation = in.readVLong();
            final int count = in.readVInt();
            for (int i = 0; i < count; i++) {
                addUnit(new Unit(in.readString(), in.readString(), in.readVLong()));
            }
            CodecUtil.checkFooter(in);
        }
    }

    /** Replays one edit. Returns false (applying nothing) if the edit is truncated/corrupt, so the caller can
     *  decide whether that is a tolerable trailing crash or genuine corruption. */
    private boolean replayEdit(Directory dir, String name) throws IOException {
        final List<String> removed;
        final List<Unit> added;
        final long editGen;
        try (ChecksumIndexInput in = dir.openChecksumInput(name)) {
            CodecUtil.checkHeader(in, EDIT_CODEC, VERSION, VERSION);
            editGen = in.readVLong();
            final int removedCount = in.readVInt();
            removed = new ArrayList<>(removedCount);
            for (int i = 0; i < removedCount; i++) {
                removed.add(in.readString());
            }
            final int addedCount = in.readVInt();
            added = new ArrayList<>(addedCount);
            for (int i = 0; i < addedCount; i++) {
                added.add(new Unit(in.readString(), in.readString(), in.readVLong()));
            }
            CodecUtil.checkFooter(in);
        } catch (Exception corruptOrTruncated) {
            return false;
        }
        for (String r : removed) {
            removeUnit(r);
        }
        for (Unit u : added) {
            addUnit(u);
        }
        generation = editGen;
        return true;
    }

    /** Deletes edit blobs made obsolete by a snapshot at {@code throughGeneration} (compaction cleanup). */
    public static void deleteObsoleteEdits(Directory dir, long throughGeneration) throws IOException {
        final List<String> toDelete = new ArrayList<>();
        for (String file : dir.listAll()) {
            if (file.startsWith(EDIT_PREFIX) && Long.parseLong(file.substring(EDIT_PREFIX.length())) <= throughGeneration) {
                toDelete.add(file);
            }
        }
        IOUtils.deleteFilesIgnoringExceptions(dir, toDelete.toArray(new String[0]));
    }
}
