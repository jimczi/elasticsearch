/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.primitive;

/**
 * Run-length encoding for blocks of {@code long} values.
 *
 * <p>Encodes a sequence into two parallel streams: one of distinct run values and one of run
 * lengths. Both streams are independent of each other and can be further compressed by downstream
 * primitives (for example, run lengths typically bit-pack well with a small {@code bitsPerValue}).
 *
 * <p>Run lengths are always positive; a run of length zero is never emitted.
 *
 * <p>This codec writes to caller-supplied output arrays. The output arrays must be at least
 * {@code len} long because in the worst case (no adjacent duplicates) every input element forms
 * its own run.
 */
public final class Rle {

    public Rle() {}

    /**
     * Encode {@code in[inOffset, inOffset + len)} into parallel value / count streams. Returns the
     * number of runs produced. {@code outValues} and {@code outCounts} must each have capacity
     * for at least {@code len} entries starting at their respective offsets.
     */
    public static int encode(
        long[] in,
        int inOffset,
        int len,
        long[] outValues,
        int outValuesOffset,
        int[] outCounts,
        int outCountsOffset
    ) {
        if (len <= 0) {
            return 0;
        }
        int runs = 0;
        long current = in[inOffset];
        int count = 1;
        for (int i = 1; i < len; i++) {
            final long v = in[inOffset + i];
            if (v == current) {
                count++;
            } else {
                outValues[outValuesOffset + runs] = current;
                outCounts[outCountsOffset + runs] = count;
                runs++;
                current = v;
                count = 1;
            }
        }
        outValues[outValuesOffset + runs] = current;
        outCounts[outCountsOffset + runs] = count;
        return runs + 1;
    }

    /**
     * Decode parallel value / count streams produced by {@link #encode} back into a flat sequence.
     * The caller is responsible for ensuring that {@code out} has capacity for the sum of the run
     * lengths in {@code inCounts[inCountsOffset, inCountsOffset + runs)}.
     */
    public static int decode(long[] inValues, int inValuesOffset, int[] inCounts, int inCountsOffset, int runs, long[] out, int outOffset) {
        int written = 0;
        for (int r = 0; r < runs; r++) {
            final long v = inValues[inValuesOffset + r];
            final int c = inCounts[inCountsOffset + r];
            if (c <= 0) {
                throw new IllegalArgumentException("run length must be positive, got " + c + " at run " + r);
            }
            final int end = written + c;
            for (int i = written; i < end; i++) {
                out[outOffset + i] = v;
            }
            written = end;
        }
        return written;
    }
}
