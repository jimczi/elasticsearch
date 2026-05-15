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
 * Fixed-width bit-packing for blocks of {@code long} values.
 *
 * <p>Packs {@code valuesLen} values of {@code bitsPerValue} bits each into a destination
 * {@code long[]} buffer, low-bits-first within each word. Callers must ensure that every input
 * value already fits in {@code bitsPerValue} bits; this class does not mask on the encode path.
 *
 * <p>The encoded layout is identical to a stream of {@code valuesLen * bitsPerValue} bits emitted
 * least-significant-bit-first into a sequence of 64-bit little-endian words, which is the same
 * byte-order-independent convention used by {@link org.apache.lucene.util.packed.PackedInts}.
 *
 * <p>Supported {@code bitsPerValue} range is {@code [1, 63]}. A width of {@code 0} or {@code 64}
 * is rejected to keep shift semantics well-defined; callers that need either case should special
 * case it themselves (constant zero, or a raw {@code long[]} copy).
 */
public final class BitPacking {

    public BitPacking() {}

    /**
     * Returns the number of 64-bit words required to hold {@code valuesLen} packed values.
     */
    public static int requiredLongs(int valuesLen, int bitsPerValue) {
        checkBitsPerValue(bitsPerValue);
        final long totalBits = (long) valuesLen * bitsPerValue;
        return (int) ((totalBits + 63L) >>> 6);
    }

    /**
     * Pack {@code valuesLen} values starting at {@code valuesOffset} into {@code out} starting
     * at word index 0. Each value is expected to be in {@code [0, 2^bitsPerValue)}; values
     * with set bits above {@code bitsPerValue} will corrupt neighboring packed slots.
     *
     * @return the number of 64-bit words written to {@code out}.
     */
    public static int pack(long[] values, int valuesOffset, int valuesLen, int bitsPerValue, long[] out) {
        checkBitsPerValue(bitsPerValue);
        long buffer = 0L;
        int bitsFilled = 0;
        int outPos = 0;
        for (int i = 0; i < valuesLen; i++) {
            final long v = values[valuesOffset + i];
            buffer |= v << bitsFilled;
            bitsFilled += bitsPerValue;
            if (bitsFilled >= 64) {
                out[outPos++] = buffer;
                bitsFilled -= 64;
                buffer = bitsFilled == 0 ? 0L : v >>> (bitsPerValue - bitsFilled);
            }
        }
        if (bitsFilled > 0) {
            out[outPos++] = buffer;
        }
        return outPos;
    }

    /**
     * Inverse of {@link #pack}. Reads {@code valuesLen} packed values starting at word index
     * {@code inOffset} of {@code in} and writes them to {@code out} starting at {@code outOffset}.
     */
    public static void unpack(long[] in, int inOffset, int valuesLen, int bitsPerValue, long[] out, int outOffset) {
        checkBitsPerValue(bitsPerValue);
        final long mask = (1L << bitsPerValue) - 1L;
        long buffer = 0L;
        int bitsAvailable = 0;
        int inPos = inOffset;
        for (int i = 0; i < valuesLen; i++) {
            if (bitsAvailable >= bitsPerValue) {
                out[outOffset + i] = buffer & mask;
                buffer >>>= bitsPerValue;
                bitsAvailable -= bitsPerValue;
            } else {
                final long next = in[inPos++];
                out[outOffset + i] = (buffer | (next << bitsAvailable)) & mask;
                final int consumed = bitsPerValue - bitsAvailable;
                buffer = next >>> consumed;
                bitsAvailable = 64 - consumed;
            }
        }
    }

    private static void checkBitsPerValue(int bitsPerValue) {
        if (bitsPerValue < 1 || bitsPerValue > 63) {
            throw new IllegalArgumentException("bitsPerValue must be in [1, 63], got " + bitsPerValue);
        }
    }
}
