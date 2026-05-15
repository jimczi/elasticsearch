/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numericpipeline;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.primitive.BitPacking;

import java.io.IOException;

/**
 * Terminal payload stage of the numeric pipeline. Bit-packs the (post-transform) values
 * at the narrowest fixed width that fits the OR of all values.
 *
 * <p>Inline layout:
 * <pre>
 *   VInt(bitsPerValue)                   // 0..63
 *   ceil(valuesLen * bitsPerValue / 64) longs of packed data (bpv &gt; 0)
 * </pre>
 *
 * <p>When {@code bitsPerValue == 0} every value is zero — typically the result of
 * upstream stages collapsing a constant or fully-deltable sequence — and the payload is
 * just the header byte. Decode fills the destination with zeros so upstream reverse stages
 * reconstruct the original sequence.
 */
final class BitPackStage implements PayloadStage {

    static final BitPackStage INSTANCE = new BitPackStage();

    public BitPackStage() {}

    @Override
    public StageId stageId() {
        return StageId.BITPACK_PAYLOAD;
    }

    @Override
    public void encode(long[] values, int valueCount, DataOutput dataOut) throws IOException {
        long or = 0L;
        for (int i = 0; i < valueCount; i++) {
            or |= values[i];
        }
        final int bitsPerValue;
        if (or == 0L) {
            bitsPerValue = 0;
        } else {
            // Values that occupy the sign bit need the raw-long path (bpv=64); the
            // bit-pack primitive caps at 63 bits per value.
            bitsPerValue = 64 - Long.numberOfLeadingZeros(or);
        }
        dataOut.writeVInt(bitsPerValue);
        if (bitsPerValue == 0) {
            return;
        }
        if (bitsPerValue == 64) {
            // Raw mode: every value pays a full 8 bytes. Used when transform stages
            // couldn't tame a sign-bit-bearing range (e.g. uniform random longs).
            for (int i = 0; i < valueCount; i++) {
                dataOut.writeLong(values[i]);
            }
            return;
        }
        final int packedLongs = BitPacking.requiredLongs(valueCount, bitsPerValue);
        final long[] packed = new long[packedLongs];
        BitPacking.pack(values, 0, valueCount, bitsPerValue, packed);
        for (int i = 0; i < packedLongs; i++) {
            dataOut.writeLong(packed[i]);
        }
    }

    @Override
    public void decode(long[] values, int valuesOffset, int valueCount, DataInput dataIn, long[] scratch) throws IOException {
        final int bitsPerValue = dataIn.readVInt();
        if (bitsPerValue == 0) {
            for (int i = 0; i < valueCount; i++) {
                values[valuesOffset + i] = 0L;
            }
            return;
        }
        if (bitsPerValue == 64) {
            for (int i = 0; i < valueCount; i++) {
                values[valuesOffset + i] = dataIn.readLong();
            }
            return;
        }
        if (bitsPerValue < 0 || bitsPerValue > 63) {
            throw new IOException("invalid bitsPerValue: " + bitsPerValue);
        }
        final int packedLongs = BitPacking.requiredLongs(valueCount, bitsPerValue);
        for (int i = 0; i < packedLongs; i++) {
            scratch[i] = dataIn.readLong();
        }
        BitPacking.unpack(scratch, 0, valueCount, bitsPerValue, values, valuesOffset);
    }

    /**
     * Worst-case longs the payload may write per block. Accounts for both the bit-pack
     * mode (bpv 1..63, payload size {@code ceil(valueCount * bpv / 64)} longs) and the
     * raw mode (bpv 64, payload size {@code valueCount} longs) — whichever is larger.
     */
    static int maxPackedLongs(int valueCount) {
        return Math.max(BitPacking.requiredLongs(valueCount, 63), valueCount);
    }
}
