/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.elasticsearch.columnar.primitive.BitPacking;
import org.elasticsearch.columnar.primitive.Delta;
import org.elasticsearch.columnar.primitive.Gcd;

import java.io.IOException;

/**
 * Numeric {@link NumericBlockEncoder} that combines a GCD divisor with first-order delta encoding
 * and min-subtracted bit-packing. Designed for data that has a large common factor — day-
 * or hour-granularity timestamps in milliseconds, fixed-increment counters, monetary values
 * in cents, etc. The GCD reduces the value range by orders of magnitude before bit-packing.
 *
 * <p>How it differs from {@link DeltaPackedBlockEncoder}:
 * <ul>
 *   <li>{@code DeltaPackedBlockEncoder} computes deltas of raw values, then bit-packs.
 *       Tight on data that has small consecutive differences in raw units.</li>
 *   <li>{@code GcdDeltaPackedBlockEncoder} divides every value by the per-block GCD first,
 *       then takes deltas, then bit-packs. Tight on data where consecutive differences
 *       share a large common factor (day-granularity timestamps step by 86,400,000 ms in
 *       raw units, by {@code 1} after GCD reduction).</li>
 * </ul>
 *
 * <p>Auto-pick in {@link BitPackBlockEncoder#specializeForSegment} chooses this encoder
 * when the sample has a non-trivial GCD AND the divided-then-delta-then-bitpack savings
 * exceed the framing overhead.
 *
 * <p>Encoded block layout (Lucene {@code DataOutput} byte order):
 * <pre>
 *   gcd               : long (8 bytes)         // non-zero divisor; values are multiples of gcd
 *   base              : long (8 bytes)         // values[0] / gcd
 *   bitsPerValue      : byte                   // for the deltas of divided values
 *     0  = no deltas (valuesLen == 1) or all deltas are zero (constant sequence)
 *     1..63 = bit-packed zigzag deltas of divided values
 *     64 = raw fallback (deltas don't fit; store values / gcd as plain longs)
 *   minDelta          : long (8 bytes, only when bitsPerValue is 1..63)
 *   payload:
 *     bitsPerValue == 0   : empty
 *     bitsPerValue 1..63  : ceil((valuesLen-1) * bitsPerValue / 64) * 8 bytes of bit-packed
 *                            zigzag deltas of divided values
 *     bitsPerValue == 64  : (valuesLen - 1) * 8 bytes of raw values / gcd
 * </pre>
 *
 * <p>Reserved id {@code 3}.
 */
public final class GcdDeltaPackedBlockEncoder implements NumericBlockEncoder {

    public static final String NAME = "GcdDeltaPack";
    public static final GcdDeltaPackedBlockEncoder INSTANCE = new GcdDeltaPackedBlockEncoder();

    private static final int HEADER_BYTES = Long.BYTES + Long.BYTES + 1; // gcd + base + bitsPerValue
    private static final int MINDELTA_BYTES = Long.BYTES;

    public GcdDeltaPackedBlockEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int maxEncodedSize(int valuesLen) {
        if (valuesLen == 0) return 0;
        return HEADER_BYTES + MINDELTA_BYTES + Math.multiplyExact(Math.max(0, valuesLen - 1), Long.BYTES);
    }

    @Override
    public int scratchLongs(int valuesLen) {
        return Math.max(0, valuesLen - 1);
    }

    @Override
    public int encode(long[] values, int valuesOffset, int valuesLen, byte[] dest, int destOffset) {
        if (valuesLen == 0) {
            return 0;
        }
        // Compute GCD across the block. Auto-pick should have ensured gcd > 1 already,
        // but defend against direct calls by falling back to gcd = 1 (which makes this
        // encoder pay 8 bytes of overhead vs DeltaPackedBlockEncoder but stays correct).
        final long gcd = Gcd.gcdOfBlock(values, valuesOffset, valuesLen);
        final long divisor = gcd > 0 ? gcd : 1L;

        final long base = values[valuesOffset] / divisor;
        final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
        out.writeLong(divisor);
        out.writeLong(base);

        if (valuesLen == 1) {
            out.writeByte((byte) 0);
            return out.getPosition() - destOffset;
        }

        final int deltaCount = valuesLen - 1;
        // Compute zigzag-encoded deltas of divided values.
        final long[] deltas = new long[deltaCount];
        long prev = base;
        for (int i = 0; i < deltaCount; i++) {
            final long cur = values[valuesOffset + i + 1] / divisor;
            final long d = cur - prev;
            deltas[i] = Delta.zigZagEncode(d);
            prev = cur;
        }

        long min = deltas[0];
        long max = deltas[0];
        for (int i = 1; i < deltaCount; i++) {
            final long d = deltas[i];
            if (d < min) min = d;
            if (d > max) max = d;
        }

        final long range = max - min;
        final boolean rawMode;
        final int bitsPerValue;
        if (range < 0) {
            rawMode = true;
            bitsPerValue = 64;
        } else if (range == 0) {
            rawMode = false;
            bitsPerValue = 0;
        } else {
            final int b = 64 - Long.numberOfLeadingZeros(range);
            if (b >= 64) {
                rawMode = true;
                bitsPerValue = 64;
            } else {
                rawMode = false;
                bitsPerValue = b;
            }
        }

        out.writeByte((byte) bitsPerValue);

        if (rawMode) {
            // Raw fallback: store divided values for i=1..valuesLen-1.
            for (int i = 1; i < valuesLen; i++) {
                out.writeLong(values[valuesOffset + i] / divisor);
            }
            return out.getPosition() - destOffset;
        }

        out.writeLong(min);

        if (bitsPerValue == 0) {
            return out.getPosition() - destOffset;
        }

        for (int i = 0; i < deltaCount; i++) {
            deltas[i] -= min;
        }
        final int packedLongs = BitPacking.requiredLongs(deltaCount, bitsPerValue);
        final long[] packed = new long[packedLongs];
        BitPacking.pack(deltas, 0, deltaCount, bitsPerValue, packed);
        for (int i = 0; i < packedLongs; i++) {
            out.writeLong(packed[i]);
        }
        return out.getPosition() - destOffset;
    }

    @Override
    public void decode(int formatVersion, DataInput in, long[] dest, int destOffset, int valuesLen, long[] scratch) throws IOException {
        if (valuesLen == 0) {
            return;
        }
        final long gcd = in.readLong();
        final long base = in.readLong();
        dest[destOffset] = base * gcd;
        if (valuesLen == 1) {
            in.readByte();
            return;
        }
        final int bitsPerValue = in.readByte() & 0xff;
        final int deltaCount = valuesLen - 1;

        if (bitsPerValue == 64) {
            for (int i = 1; i < valuesLen; i++) {
                dest[destOffset + i] = in.readLong() * gcd;
            }
            return;
        }
        if (bitsPerValue < 0 || bitsPerValue > 63) {
            throw new IOException("invalid bitsPerValue: " + bitsPerValue);
        }

        final long minDelta = in.readLong();

        if (bitsPerValue == 0) {
            // All deltas equal minDelta (zigzag-encoded) → constant step in divided space.
            final long step = Delta.zigZagDecode(minDelta);
            long acc = base;
            for (int i = 1; i < valuesLen; i++) {
                acc += step;
                dest[destOffset + i] = acc * gcd;
            }
            return;
        }

        final int packedLongs = BitPacking.requiredLongs(deltaCount, bitsPerValue);
        for (int i = 0; i < packedLongs; i++) {
            scratch[i] = in.readLong();
        }
        final long[] deltas = new long[deltaCount];
        BitPacking.unpack(scratch, 0, deltaCount, bitsPerValue, deltas, 0);
        long acc = base;
        for (int i = 0; i < deltaCount; i++) {
            final long zz = deltas[i] + minDelta;
            acc += Delta.zigZagDecode(zz);
            dest[destOffset + i + 1] = acc * gcd;
        }
    }
}
