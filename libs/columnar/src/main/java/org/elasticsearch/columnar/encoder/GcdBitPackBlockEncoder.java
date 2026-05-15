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
import org.elasticsearch.columnar.primitive.Gcd;

import java.io.IOException;

/**
 * Numeric {@link NumericBlockEncoder} that combines a per-block GCD divisor with min-subtracted
 * bit-packing — same shape as {@link BitPackBlockEncoder} but applied AFTER dividing every
 * value by the block's GCD. Designed for non-monotonic data with a non-trivial common
 * factor: random doc-publication dates at day granularity, random monetary amounts in
 * cents, gauges quantised to a coarser grid, …
 *
 * <p>How it differs from the two existing GCD-aware paths:
 * <ul>
 *   <li>{@link GcdDeltaPackedBlockEncoder} divides by GCD, then takes deltas, then bit-packs.
 *       Tight on data that's BOTH multiple-of-GCD AND monotonic.</li>
 *   <li>{@link BitPackBlockEncoder} min-subtracts then bit-packs. Tight on bounded data but
 *       loses a factor-of-GCD's worth of bits when every value is a multiple of e.g. 86 M.</li>
 *   <li>This encoder min-subtracts the DIVIDED values, then bit-packs. Saves the GCD's
 *       contribution without paying the delta-inflation penalty that hits random data.</li>
 * </ul>
 *
 * <p>Auto-pick in {@link BitPackBlockEncoder#specializeForSegment} chooses this encoder when
 * the sample has a non-trivial GCD but is not monotonic (so the delta-then-bit-pack variant
 * wouldn't beat plain bit-pack on the divided values).
 *
 * <p>Encoded block layout (Lucene {@code DataOutput} byte order):
 * <pre>
 *   gcd               : long (8 bytes)   // non-zero divisor; values are multiples of gcd
 *   min               : long (8 bytes)   // min of (value / gcd) across the block
 *   bitsPerValue      : byte (0..64)
 *   payload:
 *     bitsPerValue == 0  : empty (every divided value equals min)
 *     bitsPerValue 1..63 : ceil(valuesLen * bitsPerValue / 64) * 8 bytes of bit-packed
 *                          (value/gcd - min) values
 *     bitsPerValue == 64 : valuesLen * 8 bytes of raw (value/gcd) values
 * </pre>
 *
 * <p>Reserved id {@code 4}.
 */
public final class GcdBitPackBlockEncoder implements NumericBlockEncoder {

    public static final String NAME = "GcdBitPack";
    public static final GcdBitPackBlockEncoder INSTANCE = new GcdBitPackBlockEncoder();

    private static final int HEADER_BYTES = Long.BYTES + Long.BYTES + 1; // gcd + min + bitsPerValue

    public GcdBitPackBlockEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int maxEncodedSize(int valuesLen) {
        return HEADER_BYTES + Math.multiplyExact(valuesLen, Long.BYTES);
    }

    @Override
    public int scratchLongs(int valuesLen) {
        return valuesLen;
    }

    @Override
    public int encode(long[] values, int valuesOffset, int valuesLen, byte[] dest, int destOffset) {
        if (valuesLen == 0) {
            return 0;
        }
        final long gcd = Gcd.gcdOfBlock(values, valuesOffset, valuesLen);
        final long divisor = gcd > 0 ? gcd : 1L;

        long min = values[valuesOffset] / divisor;
        long max = min;
        for (int i = 1; i < valuesLen; i++) {
            final long v = values[valuesOffset + i] / divisor;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        final long range = max - min;
        final boolean rawMode;
        final int bitsPerValue;
        if (range < 0) {
            // Signed overflow on the divided values — fall back to raw. Unusual when gcd > 1
            // but defended against.
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

        final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
        out.writeLong(divisor);
        out.writeLong(rawMode ? 0L : min);
        out.writeByte((byte) bitsPerValue);

        if (bitsPerValue == 0) {
            return out.getPosition() - destOffset;
        }
        if (rawMode) {
            for (int i = 0; i < valuesLen; i++) {
                out.writeLong(values[valuesOffset + i] / divisor);
            }
            return out.getPosition() - destOffset;
        }

        final long[] deltas = new long[valuesLen];
        for (int i = 0; i < valuesLen; i++) {
            deltas[i] = (values[valuesOffset + i] / divisor) - min;
        }
        final int packedLongs = BitPacking.requiredLongs(valuesLen, bitsPerValue);
        final long[] packed = new long[packedLongs];
        BitPacking.pack(deltas, 0, valuesLen, bitsPerValue, packed);
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
        final long min = in.readLong();
        final int bitsPerValue = in.readByte() & 0xff;

        if (bitsPerValue == 0) {
            final long value = min * gcd;
            for (int i = 0; i < valuesLen; i++) {
                dest[destOffset + i] = value;
            }
            return;
        }
        if (bitsPerValue == 64) {
            for (int i = 0; i < valuesLen; i++) {
                dest[destOffset + i] = in.readLong() * gcd;
            }
            return;
        }
        if (bitsPerValue < 0 || bitsPerValue > 63) {
            throw new IOException("invalid bitsPerValue: " + bitsPerValue);
        }
        final int packedLongs = BitPacking.requiredLongs(valuesLen, bitsPerValue);
        for (int i = 0; i < packedLongs; i++) {
            scratch[i] = in.readLong();
        }
        BitPacking.unpack(scratch, 0, valuesLen, bitsPerValue, dest, destOffset);
        for (int i = 0; i < valuesLen; i++) {
            dest[destOffset + i] = (dest[destOffset + i] + min) * gcd;
        }
    }
}
