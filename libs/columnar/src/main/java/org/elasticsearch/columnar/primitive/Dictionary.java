/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.primitive;

import java.util.Arrays;

/**
 * Block-local dictionary encoding for blocks of {@code long} values.
 *
 * <p>Builds a sorted dictionary of the distinct values in the input block, then rewrites every
 * input value to its dictionary index. The dictionary order is ascending which lets callers feed
 * the dictionary itself into delta encoding if they choose.
 *
 * <p>The encoded form has two streams:
 * <ul>
 *   <li>{@code dict[0..numDistinct-1]} — sorted distinct values.</li>
 *   <li>{@code indices[0..len-1]} — index into {@code dict} for each input value.
 *       Indices are non-negative and bounded by {@code numDistinct - 1}, so they bit-pack
 *       with {@code bitsPerValue = ceil(log2(numDistinct))}.</li>
 * </ul>
 *
 * <p>This primitive is most useful when the block cardinality is significantly smaller than the
 * block size; callers should still encode the dictionary and indices even when cardinality is
 * close to the block size, but the win will be small in that regime.
 */
public final class Dictionary {

    public Dictionary() {}

    /**
     * Encode {@code in[inOffset, inOffset + len)} into a sorted dictionary written into
     * {@code outDict} and a parallel index stream written into {@code outIndices}.
     *
     * <p>{@code outDict} must have capacity for at least {@code len} entries because in the worst
     * case (all distinct values) the dictionary is the same size as the input.
     *
     * @return the dictionary size {@code numDistinct} — the meaningful prefix of {@code outDict}.
     */
    public static int encode(long[] in, int inOffset, int len, int[] outIndices, int outIndicesOffset, long[] outDict, int outDictOffset) {
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(in, inOffset, outDict, outDictOffset, len);
        Arrays.sort(outDict, outDictOffset, outDictOffset + len);

        int dictSize = 1;
        long prev = outDict[outDictOffset];
        for (int i = 1; i < len; i++) {
            final long v = outDict[outDictOffset + i];
            if (v != prev) {
                outDict[outDictOffset + dictSize++] = v;
                prev = v;
            }
        }

        for (int i = 0; i < len; i++) {
            final int idx = Arrays.binarySearch(outDict, outDictOffset, outDictOffset + dictSize, in[inOffset + i]);
            assert idx >= 0 : "value not found in dictionary: " + in[inOffset + i];
            outIndices[outIndicesOffset + i] = idx;
        }
        return dictSize;
    }

    /**
     * Inverse of {@link #encode}: rewrite {@code indices} back into values using {@code dict}.
     */
    public static void decode(int[] indices, int indicesOffset, int len, long[] dict, int dictOffset, long[] out, int outOffset) {
        for (int i = 0; i < len; i++) {
            out[outOffset + i] = dict[dictOffset + indices[indicesOffset + i]];
        }
    }
}
