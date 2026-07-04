/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perslice;

import org.elasticsearch.index.codec.perslice.SliceAwareMergePlanner.MergeSpec;
import org.elasticsearch.index.codec.perslice.SliceAwareMergePlanner.Segment;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * M3 selection heuristic: proves the slice-aware merge policy (1) isolates whales, (2) batches the tail,
 * and (3) keeps write amplification bounded when the tail is repeatedly consolidated.
 */
public class SliceAwareMergePlannerTests extends ESTestCase {

    private static final long WHALE = 50;
    private static final int MAX_AT_ONCE = 8;

    public void testWhaleSegmentIsNeverSelected() {
        final SliceAwareMergePlanner planner = new SliceAwareMergePlanner(WHALE, MAX_AT_ONCE);
        final List<Segment> segments = List.of(
            new Segment("tailA", new long[] { 3, 2, 1 }),
            new Segment("tailB", new long[] { 1, 4, 2 }),
            new Segment("whaleC", new long[] { 80, 1, 1 }) // slice 0 is a whale
        );

        final Optional<MergeSpec> merge = planner.findMerge(segments);
        assertTrue(merge.isPresent());
        final List<String> names = merge.get().segments().stream().map(Segment::name).toList();
        assertEquals(List.of("tailA", "tailB"), names);
        assertFalse("a whale-bearing segment must never be re-merged", names.contains("whaleC"));
    }

    public void testNoMergeWhenFewerThanTwoEligible() {
        final SliceAwareMergePlanner planner = new SliceAwareMergePlanner(WHALE, MAX_AT_ONCE);
        // One whale + one tail: only the tail is eligible, which is not enough to merge.
        assertTrue(planner.findMerge(List.of(new Segment("whale", new long[] { 99 }), new Segment("tail", new long[] { 2 }))).isEmpty());
    }

    public void testTailBatchingPicksTheSmallest() {
        final SliceAwareMergePlanner planner = new SliceAwareMergePlanner(WHALE, 3);
        final List<Segment> segments = List.of(
            new Segment("s5", new long[] { 5 }),
            new Segment("s1", new long[] { 1 }),
            new Segment("s4", new long[] { 4 }),
            new Segment("s2", new long[] { 2 }),
            new Segment("s3", new long[] { 3 })
        );
        final List<String> names = planner.findMerge(segments).orElseThrow().segments().stream().map(Segment::name).toList();
        assertEquals("should batch the three smallest tail segments", List.of("s1", "s2", "s3"), names);
    }

    /**
     * Repeatedly consolidating a long tail of tiny single-slice segments rewrites each byte only a few
     * times (O(log_M) levels), and a slice that grows into a whale is never rewritten again.
     */
    public void testWriteAmplificationIsBounded() {
        final SliceAwareMergePlanner planner = new SliceAwareMergePlanner(WHALE, MAX_AT_ONCE);

        // 64 tiny single-slice tail segments of 1 byte each.
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            segments.add(new Segment("t" + i, new long[] { 1 }));
        }
        final long initialTotal = segments.stream().mapToLong(Segment::totalBytes).sum();

        long bytesWritten = 0;
        int mergeCount = 0;
        Optional<MergeSpec> next;
        while ((next = planner.findMerge(segments)).isPresent()) {
            final List<Segment> inputs = next.get().segments();
            final Segment merged = SliceAwareMergePlanner.combine("m" + mergeCount++, inputs);
            bytesWritten += merged.totalBytes();
            segments = new ArrayList<>(segments);
            segments.removeAll(inputs);
            segments.add(merged);
            assertTrue("must not exceed maxMergeAtOnce inputs", inputs.size() <= MAX_AT_ONCE);
        }

        // Every remaining segment is either a whale or there is nothing left to pair it with.
        long remainingEligible = segments.stream().filter(s -> s.maxSliceBytes() < WHALE).count();
        assertTrue("tail should be fully consolidated", remainingEligible < 2);

        // The single consolidated whale holds all the original data, and was frozen once it crossed WHALE.
        final Segment whale = segments.stream().max((a, b) -> Long.compare(a.totalBytes(), b.totalBytes())).orElseThrow();
        assertEquals(initialTotal, whale.totalBytes());
        assertTrue(whale.maxSliceBytes() >= WHALE);

        // Amplification: total bytes rewritten stays a small multiple of the data (here 2x), not O(n).
        final double amplification = (double) bytesWritten / initialTotal;
        logger.info("slice-aware tail consolidation: {} merges, amplification={}x", mergeCount, amplification);
        assertTrue("write amplification should be small, was " + amplification + "x", amplification <= 3.0);
    }
}
