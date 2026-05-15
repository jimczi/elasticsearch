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
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.primitive.BitPacking;
import org.elasticsearch.columnar.primitive.Delta;

import java.io.IOException;

/**
 * Numeric {@link NumericBlockEncoder} that combines first-order delta encoding with min-subtracted
 * bit-packing. Designed for monotonic or near-monotonic sequences — timestamps, monotonic
 * counters, ascending ids — where consecutive values differ by a small range even though the
 * absolute values are large.
 *
 * <p>How it compares to {@link BitPackBlockEncoder} (the format's production-default numeric
 * encoder):
 * <ul>
 *   <li>{@code BitPackBlockEncoder} subtracts the per-block <em>min value</em> from every
 *       entry, then bit-packs the remainder. Tight on gauges and uniformly-distributed
 *       fields; loose on monotonic data because the range
 *       {@code max - min} grows with the block.</li>
 *   <li>{@code DeltaPackedBlockEncoder} computes consecutive differences first, then
 *       min-subtracts the zigzag-encoded deltas, then bit-packs the remainder. Tight on
 *       monotonic data because the delta range is independent of block size (typically
 *       the step size between consecutive entries) and stays small as the block grows.</li>
 * </ul>
 *
 * <p>Encoded block layout:
 * <pre>
 *   base              : long (8 bytes)        // values[0]
 *   bitsPerValue      : byte                  // for the deltas
 *     0  = no deltas (valuesLen == 1) or all deltas are zero (constant sequence)
 *     1..63 = bit-packed zigzag deltas
 *     64 = deltas don't fit; stored as raw values[1..valuesLen-1]
 *   minDelta          : long (8 bytes, only when bitsPerValue is 1..63)
 *   payload:
 *     bitsPerValue == 0   : empty
 *     bitsPerValue 1..63  : ceil((valuesLen-1) * bitsPerValue / 64) * 8 bytes of bit-packed zigzag deltas
 *     bitsPerValue == 64  : (valuesLen - 1) * 8 bytes of raw values
 * </pre>
 *
 * <p>Reserved id {@code 2}. Production code that wants this encoder constructs a
 * {@link ColumNARDocValuesFormat} with it explicitly via the 6-arg constructor; the SPI
 * default still selects {@link BitPackBlockEncoder} until a per-segment auto-pick mechanism
 * lands that can choose between them based on a sample.
 */
public final class DeltaPackedBlockEncoder implements NumericBlockEncoder {

    public static final String NAME = "DeltaPack";
    public static final DeltaPackedBlockEncoder INSTANCE = new DeltaPackedBlockEncoder();

    private static final int HEADER_BYTES = Long.BYTES + 1; // base + bitsPerValue
    private static final int MINDELTA_BYTES = Long.BYTES;

    public DeltaPackedBlockEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int maxEncodedSize(int valuesLen) {
        if (valuesLen == 0) return 0;
        // base + bitsPerValue + (minDelta) + (valuesLen-1) * 8 raw bytes max.
        return HEADER_BYTES + MINDELTA_BYTES + Math.multiplyExact(Math.max(0, valuesLen - 1), Long.BYTES);
    }

    @Override
    public int scratchLongs(int valuesLen) {
        // Same scratch needs as BitPackBlockEncoder — one entry per delta for bit-unpack.
        return Math.max(0, valuesLen - 1);
    }

    @Override
    public int encode(long[] values, int valuesOffset, int valuesLen, byte[] dest, int destOffset) {
        if (valuesLen == 0) {
            return 0;
        }
        final long base = values[valuesOffset];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
        out.writeLong(base);

        // Single value: just base. No deltas, no bits.
        if (valuesLen == 1) {
            out.writeByte((byte) 0);
            return out.getPosition() - destOffset;
        }

        final int deltaCount = valuesLen - 1;

        // Compute first-order deltas, zigzag-encode them in place. ZigZag maps signed values
        // to non-negative longs so the min/max range can be bit-packed without further sign
        // handling.
        final long[] deltas = new long[deltaCount];
        for (int i = 0; i < deltaCount; i++) {
            final long d = values[valuesOffset + i + 1] - values[valuesOffset + i];
            deltas[i] = Delta.zigZagEncode(d);
        }

        // Find min/max of the zigzag'd deltas.
        long min = deltas[0];
        long max = deltas[0];
        for (int i = 1; i < deltaCount; i++) {
            final long d = deltas[i];
            if (d < min) min = d;
            if (d > max) max = d;
        }

        // Note: zigzag output is always >= 0, so range >= 0 unless overflow.
        final long range = max - min;
        final boolean rawMode;
        final int bitsPerValue;
        if (range < 0) {
            // Overflow — fall back to raw.
            rawMode = true;
            bitsPerValue = 64;
        } else if (range == 0) {
            // All deltas equal — could be monotonic constant offset (e.g. 1000ms steps).
            // We need 0 bits to store them, but we DO need to know the value to reconstruct.
            // Use bitsPerValue=0 + minDelta to carry the delta value.
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
            // Raw fallback: store values[1..valuesLen-1] as plain longs.
            for (int i = 1; i < valuesLen; i++) {
                out.writeLong(values[valuesOffset + i]);
            }
            return out.getPosition() - destOffset;
        }

        out.writeLong(min); // minDelta

        if (bitsPerValue == 0) {
            // All deltas == min. Decode reconstructs by zigzag-decoding min and adding to base
            // (valuesLen-1) times.
            return out.getPosition() - destOffset;
        }

        // Subtract min from each delta so they fit in bitsPerValue bits.
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
        final long base = in.readLong();
        dest[destOffset] = base;
        if (valuesLen == 1) {
            // Consume the trailing bitsPerValue=0 byte we wrote.
            in.readByte();
            return;
        }
        final int bitsPerValue = in.readByte() & 0xff;
        final int deltaCount = valuesLen - 1;

        if (bitsPerValue == 64) {
            // Raw mode — values[1..valuesLen-1] stored verbatim.
            for (int i = 1; i < valuesLen; i++) {
                dest[destOffset + i] = in.readLong();
            }
            return;
        }
        if (bitsPerValue < 0 || bitsPerValue > 63) {
            throw new IOException("invalid bitsPerValue: " + bitsPerValue);
        }

        final long minDelta = in.readLong();

        if (bitsPerValue == 0) {
            // All deltas equal minDelta (zigzag-encoded). Reconstruct.
            final long delta = Delta.zigZagDecode(minDelta);
            long acc = base;
            for (int i = 1; i < valuesLen; i++) {
                acc += delta;
                dest[destOffset + i] = acc;
            }
            return;
        }

        // Read packed words, unpack into scratch, add minDelta, zigzag-decode, prefix-sum.
        final int packedLongs = BitPacking.requiredLongs(deltaCount, bitsPerValue);
        // Reuse the start of scratch for the packed words (we know scratchLongs >= deltaCount,
        // and packedLongs <= deltaCount * bitsPerValue / 64 + 1 <= deltaCount, so it fits).
        for (int i = 0; i < packedLongs; i++) {
            scratch[i] = in.readLong();
        }
        // Unpack from the start of scratch into dest[destOffset+1..destOffset+valuesLen-1].
        // BitPacking.unpack reads from in[inOffset..] and writes to out[outOffset..]. We need
        // the input slot to be different from the output slot — use a temporary delta array.
        final long[] deltas = new long[deltaCount];
        BitPacking.unpack(scratch, 0, deltaCount, bitsPerValue, deltas, 0);
        long acc = base;
        for (int i = 0; i < deltaCount; i++) {
            final long zz = deltas[i] + minDelta;
            acc += Delta.zigZagDecode(zz);
            dest[destOffset + i + 1] = acc;
        }
    }
}
