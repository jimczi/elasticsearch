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
import org.elasticsearch.columnar.LongValuesIterator;
import org.elasticsearch.columnar.LongValuesSupplier;
import org.elasticsearch.columnar.NumericType;
import org.elasticsearch.columnar.primitive.BitPacking;
import org.elasticsearch.columnar.primitive.Gcd;

import java.io.IOException;

/**
 * Core numeric {@link NumericBlockEncoder} for the Elasticsearch columnar codec. Encodes a
 * block of {@code long} values by subtracting the per-block minimum and bit-packing the
 * resulting non-negative deltas at the narrowest fixed bit width that fits
 * {@code max - min}.
 *
 * <p>This is the production-default encoder. The combination of min-delta and bit-packing is
 * the workhorse for typical numeric data (timestamps, counters, gauges, ids); specialised
 * encoders for monotonic / GCD-able / dictionary-friendly data are picked through
 * {@link #specializeForSegment(LongValuesSupplier)}.
 *
 * <p>Encoded block layout (Lucene {@code DataOutput} byte order):
 * <pre>
 *   min               : long (8 bytes)
 *   bitsPerValue      : byte (0..64)
 *   payload           : depends on bitsPerValue
 *     bitsPerValue == 0  : empty (every value equals min)
 *     bitsPerValue 1..63 : ceil(valuesLen * bitsPerValue / 64) * 8 bytes of bit-packed deltas
 *     bitsPerValue == 64 : valuesLen * 8 bytes of raw values (deltas would not fit in 64
 *                          bits; min is conventionally written as 0 in this case)
 * </pre>
 *
 * <p>Decode reads sequentially from a {@link DataInput} — zero-copy when paired with
 * {@link IdentityBlockEncoding} and an mmap'd {@code IndexInput}. A caller-provided
 * {@code long[] scratch} of at least {@code valuesLen} slots holds the packed words on the
 * inner bit-unpack pass.
 */
public final class BitPackBlockEncoder implements NumericBlockEncoder {

    public static final String NAME = "BitPack";
    public static final BitPackBlockEncoder INSTANCE = new BitPackBlockEncoder();

    private static final int HEADER_BYTES = Long.BYTES + 1;
    /**
     * Minimum number of values we want to inspect before specialising. Below this threshold
     * the per-encoder framing overhead can outweigh any bit-width win, so we stick with
     * plain bit-pack.
     */
    private static final int SPECIALIZE_MIN_VALUES = 16;

    public BitPackBlockEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public NumericType numericType() {
        return NumericType.LONG;
    }

    @Override
    public NumericBlockEncoder specializeForSegment(LongValuesSupplier values) throws IOException {
        // Picks among four encoders by walking the segment's full value sequence (off-heap)
        // through {@link LongValuesSupplier}:
        // - this (BitPackBlockEncoder) for generic numeric data
        // - DeltaPackedBlockEncoder for monotonic / near-monotonic data
        // - GcdDeltaPackedBlockEncoder for monotonic data sharing a non-trivial GCD
        // (coarse-granularity timestamps, fixed-step counters)
        // - GcdBitPackBlockEncoder for non-monotonic data sharing a non-trivial GCD
        // (random doc-dates at day granularity, gauges quantised to a grid)
        //
        // Pass 1 over the supplier gathers everything except the GCD-divided ranges, which
        // need GCD known up front. Pass 2 (only when gcd > 1) walks again to compute the
        // divided range and divided-delta range. Most workloads have gcd=1 so a single pass
        // is enough.
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long gcd = 0L; // gcd(0, x) == |x| — accumulates correctly from first value
        long prev = 0L;
        int nonDecreasing = 0;
        long deltaMin = Long.MAX_VALUE;
        long deltaMax = Long.MIN_VALUE;
        int count = 0;
        {
            final LongValuesIterator it = values.open();
            while (it.next()) {
                final long v = it.longValue();
                if (count == 0) {
                    min = max = v;
                    gcd = Math.abs(v);
                } else {
                    if (v < min) min = v;
                    if (v > max) max = v;
                    if (gcd != 1L) {
                        gcd = Gcd.gcd(gcd, v);
                    }
                    if (v >= prev) nonDecreasing++;
                    final long d = v - prev;
                    final long zz = (d << 1) ^ (d >> 63);
                    if (zz < deltaMin) deltaMin = zz;
                    if (zz > deltaMax) deltaMax = zz;
                }
                prev = v;
                count++;
            }
        }

        if (count < SPECIALIZE_MIN_VALUES) {
            return this;
        }

        final long absRange = max - min;
        if (absRange < 0) {
            // Signed overflow — fall back to plain bit-pack which handles this gracefully.
            return this;
        }
        final int bitsBitPack = absRange == 0 ? 0 : 64 - Long.numberOfLeadingZeros(absRange);

        // 90% non-decreasing — same threshold as before; tolerates a small amount of jitter
        // in otherwise monotonic streams.
        final boolean monotonic = nonDecreasing * 10 >= (count - 1) * 9;

        int bitsDeltaPack = Integer.MAX_VALUE;
        if (monotonic) {
            final long deltaRange = deltaMax - deltaMin;
            if (deltaRange >= 0) {
                bitsDeltaPack = deltaRange == 0 ? 0 : 64 - Long.numberOfLeadingZeros(deltaRange);
            }
        }

        int bitsGcdBitPack = Integer.MAX_VALUE;
        int bitsGcdDeltaPack = Integer.MAX_VALUE;
        if (gcd > 1L) {
            // Pass 2 — compute the divided range (always) and the divided-delta range
            // (only when monotonic, so the GCD+delta variant is a candidate). We keep this
            // pass independent of pass 1 because the supplier guarantees a fresh walk; we
            // intentionally don't try to fuse it (encoders that walk only once are simpler
            // to reason about, and this pass runs only for the small fraction of fields
            // with a non-trivial GCD).
            long divMin = Long.MAX_VALUE;
            long divMax = Long.MIN_VALUE;
            long gdMin = Long.MAX_VALUE;
            long gdMax = Long.MIN_VALUE;
            long prevDivided = 0L;
            int idx = 0;
            final LongValuesIterator it2 = values.open();
            while (it2.next()) {
                final long divided = it2.longValue() / gcd;
                if (divided < divMin) divMin = divided;
                if (divided > divMax) divMax = divided;
                if (idx > 0 && monotonic) {
                    final long d = divided - prevDivided;
                    final long zz = (d << 1) ^ (d >> 63);
                    if (zz < gdMin) gdMin = zz;
                    if (zz > gdMax) gdMax = zz;
                }
                prevDivided = divided;
                idx++;
            }
            final long divRange = divMax - divMin;
            if (divRange >= 0) {
                bitsGcdBitPack = divRange == 0 ? 0 : 64 - Long.numberOfLeadingZeros(divRange);
            }
            if (monotonic) {
                final long gdRange = gdMax - gdMin;
                if (gdRange >= 0) {
                    bitsGcdDeltaPack = gdRange == 0 ? 0 : 64 - Long.numberOfLeadingZeros(gdRange);
                }
            }
        }

        // 2-bit margin against the simpler baseline so we don't flip-flop on marginal
        // savings. We test (a + margin < b) via (b - a > margin) to avoid Integer.MAX_VALUE
        // wrap-around when a candidate's bit width was never computed.
        final int margin = 2;
        if (bitsBitPack - bitsGcdDeltaPack > margin && bitsDeltaPack - bitsGcdDeltaPack > margin) {
            return GcdDeltaPackedBlockEncoder.INSTANCE;
        }
        if (bitsBitPack - bitsDeltaPack > margin) {
            return DeltaPackedBlockEncoder.INSTANCE;
        }
        if (bitsBitPack - bitsGcdBitPack > margin) {
            return GcdBitPackBlockEncoder.INSTANCE;
        }
        return this;
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
        long min = values[valuesOffset];
        long max = min;
        for (int i = 1; i < valuesLen; i++) {
            final long v = values[valuesOffset + i];
            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
        }

        final long range = max - min;
        final boolean rawMode;
        final int bitsPerValue;
        if (range < 0) {
            // Signed subtraction overflowed: deltas don't fit in a positive long. Fall back to raw.
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
        out.writeLong(rawMode ? 0L : min);
        out.writeByte((byte) bitsPerValue);

        if (bitsPerValue == 0) {
            return out.getPosition() - destOffset;
        }
        if (rawMode) {
            for (int i = 0; i < valuesLen; i++) {
                out.writeLong(values[valuesOffset + i]);
            }
            return out.getPosition() - destOffset;
        }

        final long[] deltas = new long[valuesLen];
        for (int i = 0; i < valuesLen; i++) {
            deltas[i] = values[valuesOffset + i] - min;
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
        final long min = in.readLong();
        final int bitsPerValue = in.readByte() & 0xff;

        if (bitsPerValue == 0) {
            for (int i = 0; i < valuesLen; i++) {
                dest[destOffset + i] = min;
            }
            return;
        }
        if (bitsPerValue == 64) {
            for (int i = 0; i < valuesLen; i++) {
                dest[destOffset + i] = in.readLong();
            }
            return;
        }
        if (bitsPerValue < 0 || bitsPerValue > 63) {
            throw new IllegalArgumentException("invalid bitsPerValue: " + bitsPerValue);
        }
        final int packedLongs = BitPacking.requiredLongs(valuesLen, bitsPerValue);
        for (int i = 0; i < packedLongs; i++) {
            scratch[i] = in.readLong();
        }
        BitPacking.unpack(scratch, 0, valuesLen, bitsPerValue, dest, destOffset);
        for (int i = 0; i < valuesLen; i++) {
            dest[destOffset + i] += min;
        }
    }

}
