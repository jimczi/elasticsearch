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
import org.elasticsearch.columnar.primitive.Gcd;

import java.io.IOException;

/**
 * Pipeline stage that divides every value by the block's greatest common divisor — the
 * decisive optimisation for day-granularity timestamps, monetary values in cents, gauges
 * quantised to a grid, etc. Runs only when the GCD is at least 2.
 *
 * <p>Includes a power-of-two fast path that replaces division and multiplication with
 * arithmetic shifts.
 */
final class GcdStage implements NumericStage {

    static final GcdStage INSTANCE = new GcdStage();

    public GcdStage() {}

    @Override
    public StageId stageId() {
        return StageId.GCD_STAGE;
    }

    @Override
    public boolean encode(long[] values, int valueCount, DataOutput metaOut) throws IOException {
        if (valueCount < 1) {
            return false;
        }
        long gcd = values[0];
        for (int i = 1; i < valueCount; i++) {
            gcd = Gcd.gcd(gcd, values[i]);
            if (gcd == 1L) break;
        }
        // Skip when there's no useful divisor (gcd <= 1) AND defend against the
        // {@code Math.abs(Long.MIN_VALUE)} overflow case in the gcd computation, which
        // can produce a negative running gcd. Bit-pack will fall back to raw 64-bit mode
        // for those workloads.
        if (gcd < 2L) {
            return false;
        }
        divideByGcd(values, valueCount, gcd);
        // gcd is always >= 2 when the stage applies — store (gcd - 2) as VLong so the most
        // common case (gcd == 2) uses a single byte.
        metaOut.writeVLong(gcd - 2);
        return true;
    }

    @Override
    public void decode(long[] values, int valuesOffset, int valueCount, DataInput metaIn) throws IOException {
        final long gcd = metaIn.readVLong() + 2L;
        multiplyByGcd(values, valuesOffset, valueCount, gcd);
    }

    private static void divideByGcd(long[] values, int valueCount, long gcd) {
        if ((gcd & (gcd - 1)) == 0) {
            final int shift = Long.numberOfTrailingZeros(gcd);
            for (int i = 0; i < valueCount; i++) {
                values[i] >>= shift;
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                values[i] /= gcd;
            }
        }
    }

    private static void multiplyByGcd(long[] values, int valuesOffset, int valueCount, long gcd) {
        if ((gcd & (gcd - 1)) == 0) {
            final int shift = Long.numberOfTrailingZeros(gcd);
            for (int i = 0; i < valueCount; i++) {
                values[valuesOffset + i] <<= shift;
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                values[valuesOffset + i] *= gcd;
            }
        }
    }
}
