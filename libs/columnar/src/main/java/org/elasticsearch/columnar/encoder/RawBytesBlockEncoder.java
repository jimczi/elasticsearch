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

import java.io.IOException;

/**
 * Baseline {@link BytesBlockEncoder}: writes one {@code [vint length][bytes]} pair per
 * value, concatenated. The encoded length is {@code sum(vint(length_i)) + sum(length_i)};
 * for typical field-length distributions this is within ~1 byte of the theoretical minimum
 * without compression. Future encoders (prefix compression, block-local dictionary) sit on
 * the same extension point and ship under new ids.
 */
public final class RawBytesBlockEncoder implements BytesBlockEncoder {

    public static final String NAME = "RawBytes";
    public static final RawBytesBlockEncoder INSTANCE = new RawBytesBlockEncoder();

    // Worst-case bytes a vint takes to encode a non-negative int. Lucene caps at 5 bytes
    // (5 * 7 bits = 35, covers full int range).
    private static final int MAX_VINT_BYTES = 5;

    public RawBytesBlockEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int maxEncodedSize(int valuesLen, int totalValueBytes) {
        return Math.addExact(Math.multiplyExact(valuesLen, MAX_VINT_BYTES), totalValueBytes);
    }

    @Override
    public int encode(byte[] valueBytes, int[] valueOffsets, int valuesLen, byte[] dest, int destOffset) throws IOException {
        final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
        for (int i = 0; i < valuesLen; i++) {
            final int start = valueOffsets[i];
            final int len = valueOffsets[i + 1] - start;
            out.writeVInt(len);
            out.writeBytes(valueBytes, start, len);
        }
        return out.getPosition() - destOffset;
    }

    @Override
    public void decode(
        int formatVersion,
        DataInput in,
        byte[] valueBytes,
        int valueBytesOffset,
        int[] valueOffsets,
        int valueOffsetsOffset,
        int valuesLen
    ) throws IOException {
        int writePos = valueBytesOffset;
        valueOffsets[valueOffsetsOffset] = writePos;
        for (int i = 0; i < valuesLen; i++) {
            final int len = in.readVInt();
            in.readBytes(valueBytes, writePos, len);
            writePos += len;
            valueOffsets[valueOffsetsOffset + i + 1] = writePos;
        }
    }
}
