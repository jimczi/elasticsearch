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

import java.io.IOException;

/**
 * Pipeline stage that replaces a monotonic value sequence with its first-order differences.
 * Applies only to strictly increasing or strictly decreasing sequences with at least two
 * monotonic transitions (ignores constant runs).
 *
 * <p>When the stage applies, {@code values[0..valueCount)} becomes the delta stream and
 * the metadata records the recovered first value (zig-zag long) so decode can reconstruct
 * the original sequence via a prefix-sum pass.
 */
final class DeltaStage implements NumericStage {

    static final DeltaStage INSTANCE = new DeltaStage();

    public DeltaStage() {}

    @Override
    public StageId stageId() {
        return StageId.DELTA_STAGE;
    }

    @Override
    public boolean encode(long[] values, int valueCount, DataOutput metaOut) throws IOException {
        if (valueCount < 2 || isMonotonic(values, valueCount) == false) {
            return false;
        }
        // Compute deltas in reverse so we don't lose values[i-1] mid-loop.
        for (int i = valueCount - 1; i > 0; i--) {
            values[i] -= values[i - 1];
        }
        // Stash the original first value (recoverable from values[0] - values[1] after the
        // shift below) so decode can rebuild the sequence from a running sum.
        final long first = values[0] - values[1];
        values[0] = values[1];
        writeZLong(metaOut, first);
        return true;
    }

    @Override
    public void decode(long[] values, int valuesOffset, int valueCount, DataInput metaIn) throws IOException {
        long sum = readZLong(metaIn);
        for (int i = 0; i < valueCount; i++) {
            sum += values[valuesOffset + i];
            values[valuesOffset + i] = sum;
        }
    }

    private static boolean isMonotonic(long[] values, int valueCount) {
        int increases = 0;
        int decreases = 0;
        for (int i = 1; i < valueCount; i++) {
            increases += (values[i] > values[i - 1]) ? 1 : 0;
            decreases += (values[i] < values[i - 1]) ? 1 : 0;
        }
        return (increases >= 2 && decreases == 0) || (decreases >= 2 && increases == 0);
    }

    static void writeZLong(DataOutput out, long v) throws IOException {
        // Manual zig-zag + variable-length encoding (bypasses Lucene's writeVLong's
        // non-negative assertion so the encoding survives v == Long.MIN_VALUE, where the
        // zigzag result has the sign bit set).
        long zz = (v << 1) ^ (v >> 63);
        while ((zz & ~0x7FL) != 0L) {
            out.writeByte((byte) ((zz & 0x7FL) | 0x80L));
            zz >>>= 7;
        }
        out.writeByte((byte) zz);
    }

    static long readZLong(DataInput in) throws IOException {
        long b = in.readByte();
        long zz = b & 0x7FL;
        int shift = 7;
        while ((b & 0x80L) != 0L) {
            b = in.readByte();
            zz |= (b & 0x7FL) << shift;
            shift += 7;
        }
        return (zz >>> 1) ^ -(zz & 1L);
    }
}
