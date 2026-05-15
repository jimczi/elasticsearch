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
 * Delta and delta-of-delta encoding for blocks of {@code long} values.
 *
 * <p>Both transforms produce a signed output of the same length as the input. The first element
 * of the output is the base value; subsequent elements are differences. The differences may be
 * negative, so callers that want to bit-pack the result must first apply a {@link #zigZagEncode}
 * pass to map signed values to unsigned.
 *
 * <p>All methods accept overlapping {@code in == out} as long as {@code inOffset == outOffset};
 * the encode path walks backwards and the decode path walks forwards so the transform is safe
 * in-place.
 *
 * <p>For inputs of length 0 or 1 the encoders write the trivially-correct output (zero or one
 * element copied) and the decoders are their inverses.
 */
public final class Delta {

    public Delta() {}

    /**
     * First-order delta encoding. {@code out[0] = in[0]}; {@code out[i] = in[i] - in[i-1]} for
     * {@code 1 <= i < len}.
     */
    public static void encode(long[] in, int inOffset, int len, long[] out, int outOffset) {
        if (len <= 0) {
            return;
        }
        // Walk backwards so in-place encoding (in == out, inOffset == outOffset) is safe.
        for (int i = len - 1; i >= 1; i--) {
            out[outOffset + i] = in[inOffset + i] - in[inOffset + i - 1];
        }
        out[outOffset] = in[inOffset];
    }

    /**
     * Inverse of {@link #encode}: prefix-sum reconstruction of the original sequence.
     */
    public static void decode(long[] in, int inOffset, int len, long[] out, int outOffset) {
        if (len <= 0) {
            return;
        }
        long acc = in[inOffset];
        out[outOffset] = acc;
        for (int i = 1; i < len; i++) {
            acc += in[inOffset + i];
            out[outOffset + i] = acc;
        }
    }

    /**
     * Second-order delta (delta-of-delta) encoding.
     * <ul>
     *   <li>{@code out[0] = in[0]}</li>
     *   <li>{@code out[1] = in[1] - in[0]} (first delta, preserved verbatim)</li>
     *   <li>{@code out[i] = (in[i] - in[i-1]) - (in[i-1] - in[i-2])} for {@code 2 <= i < len}</li>
     * </ul>
     */
    public static void encode2(long[] in, int inOffset, int len, long[] out, int outOffset) {
        if (len <= 0) {
            return;
        }
        if (len == 1) {
            out[outOffset] = in[inOffset];
            return;
        }
        for (int i = len - 1; i >= 2; i--) {
            out[outOffset + i] = in[inOffset + i] - 2L * in[inOffset + i - 1] + in[inOffset + i - 2];
        }
        out[outOffset + 1] = in[inOffset + 1] - in[inOffset];
        out[outOffset] = in[inOffset];
    }

    /**
     * Inverse of {@link #encode2}: double prefix-sum reconstruction.
     */
    public static void decode2(long[] in, int inOffset, int len, long[] out, int outOffset) {
        if (len <= 0) {
            return;
        }
        if (len == 1) {
            out[outOffset] = in[inOffset];
            return;
        }
        final long base = in[inOffset];
        long delta = in[inOffset + 1];
        long value = base + delta;
        out[outOffset] = base;
        out[outOffset + 1] = value;
        for (int i = 2; i < len; i++) {
            delta += in[inOffset + i];
            value += delta;
            out[outOffset + i] = value;
        }
    }

    /**
     * ZigZag encoding: maps a signed long to an unsigned long where small absolute values produce
     * small unsigned results. Use this to feed signed deltas to {@link BitPacking#pack}.
     */
    public static long zigZagEncode(long value) {
        return (value << 1) ^ (value >> 63);
    }

    /**
     * Inverse of {@link #zigZagEncode}.
     */
    public static long zigZagDecode(long value) {
        return (value >>> 1) ^ -(value & 1L);
    }

    /**
     * Apply {@link #zigZagEncode} to every element of {@code values} in place over
     * {@code [offset, offset + len)}.
     */
    public static void zigZagEncodeAll(long[] values, int offset, int len) {
        for (int i = 0; i < len; i++) {
            final long v = values[offset + i];
            values[offset + i] = (v << 1) ^ (v >> 63);
        }
    }

    /**
     * Apply {@link #zigZagDecode} to every element of {@code values} in place over
     * {@code [offset, offset + len)}.
     */
    public static void zigZagDecodeAll(long[] values, int offset, int len) {
        for (int i = 0; i < len; i++) {
            final long v = values[offset + i];
            values[offset + i] = (v >>> 1) ^ -(v & 1L);
        }
    }
}
