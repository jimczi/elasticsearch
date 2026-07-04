/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perslice;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * POC of the <b>selection</b> half of a slice-aware merge policy. Execution merges whole segments (see
 * {@link PerSliceDocValues#mergeNumeric}, which keeps each slice physically isolated), so the policy's job
 * is to choose <em>which segments</em> to merge using a per-slice-aware cost model:
 * <ul>
 *   <li><b>Whale isolation</b> — a segment holding a slice at/above {@code whaleBytes} is never selected, so
 *       an already-large tenant is not rewritten again. This is what bounds write amplification: a slice
 *       stops being merged once it is big, so its bytes are rewritten O(log) times, not O(n).</li>
 *   <li><b>Tail batching</b> — among the remaining "pure tail" segments (all slices small), merge the
 *       {@code maxMergeAtOnce} smallest, so the long tail of tiny tenants consolidates into reasonably
 *       sized segments without ever mixing tenants.</li>
 * </ul>
 * Maps onto {@link org.apache.lucene.index.MergePolicy#findMerges}: a real implementation reads per-slice
 * sizes from segment metadata (the slice directory) instead of the {@link Segment} model here.
 * <p>
 * Simplification: a segment that holds a whale is frozen <em>entirely</em>, so tail slices co-resident with
 * a whale do not consolidate. The design answer is to route whales into their own segments at flush (so a
 * whale segment is ~only the whale); that write-path routing is future work, not part of this POC.
 */
public final class SliceAwareMergePlanner {

    /** A segment as a vector of per-slice sizes (index = slice id). */
    public record Segment(String name, long[] sliceBytes) {
        public long totalBytes() {
            long t = 0;
            for (long b : sliceBytes) {
                t += b;
            }
            return t;
        }

        public long maxSliceBytes() {
            long m = 0;
            for (long b : sliceBytes) {
                m = Math.max(m, b);
            }
            return m;
        }
    }

    /** The chosen segments to merge into one. */
    public record MergeSpec(List<Segment> segments) {}

    private final long whaleBytes;
    private final int maxMergeAtOnce;

    public SliceAwareMergePlanner(long whaleBytes, int maxMergeAtOnce) {
        if (maxMergeAtOnce < 2) {
            throw new IllegalArgumentException("maxMergeAtOnce must be >= 2");
        }
        this.whaleBytes = whaleBytes;
        this.maxMergeAtOnce = maxMergeAtOnce;
    }

    /** Returns the next merge to run, or empty if nothing is worth merging. */
    public Optional<MergeSpec> findMerge(List<Segment> segments) {
        // Eligible = pure-tail segments: no slice has grown into a whale.
        final List<Segment> eligible = new ArrayList<>();
        for (Segment s : segments) {
            if (s.maxSliceBytes() < whaleBytes) {
                eligible.add(s);
            }
        }
        if (eligible.size() < 2) {
            return Optional.empty();
        }
        // Batch the smallest tail segments first.
        eligible.sort(Comparator.comparingLong(Segment::totalBytes));
        return Optional.of(new MergeSpec(List.copyOf(eligible.subList(0, Math.min(maxMergeAtOnce, eligible.size())))));
    }

    /** Combines a merge's inputs into the resulting segment (per-slice sizes add up — tenants stay separate). */
    public static Segment combine(String name, List<Segment> inputs) {
        int numSlices = 0;
        for (Segment s : inputs) {
            numSlices = Math.max(numSlices, s.sliceBytes().length);
        }
        final long[] merged = new long[numSlices];
        for (Segment s : inputs) {
            final long[] b = s.sliceBytes();
            for (int i = 0; i < b.length; i++) {
                merged[i] += b[i];
            }
        }
        return new Segment(name, merged);
    }
}
