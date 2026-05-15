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
 * Pipeline stage that subtracts the per-block minimum from every value so the bit-pack
 * stage downstream sees a tighter range. Three predicates gate when the stage runs:
 *
 * <ul>
 *   <li>skip when {@code max - min < 0} (unsigned overflow — caller can't shift safely),</li>
 *   <li>skip when {@code min == 0} (subtracting zero is a no-op),</li>
 *   <li>skip when {@code |min| < |max| >>> 2} unsigned — the minimum is small relative to
 *       the maximum, so the subtract barely buys any bits and the metadata overhead
 *       outweighs the win.</li>
 * </ul>
 *
 * <p>When the stage applies, every value gets {@code -min} added; the metadata records
 * {@code min} as a zig-zag long so decode can undo it. Plays well with a preceding
 * {@link DeltaStage} (delta produces a sequence that may have a non-zero minimum delta).
 */
final class OffsetStage implements NumericStage {

    static final OffsetStage INSTANCE = new OffsetStage();

    public OffsetStage() {}

    @Override
    public StageId stageId() {
        return StageId.OFFSET_STAGE;
    }

    @Override
    public boolean encode(long[] values, int valueCount, DataOutput metaOut) throws IOException {
        if (valueCount < 1) {
            return false;
        }
        long min = values[0];
        long max = values[0];
        for (int i = 1; i < valueCount; i++) {
            if (values[i] < min) min = values[i];
            if (values[i] > max) max = values[i];
        }
        if (max - min < 0) return false;
        if (min == 0) return false;
        final long absMin = min < 0 ? -min : min;
        final long absMax = max < 0 ? -max : max;
        if (Long.compareUnsigned(absMin, absMax >>> 2) < 0) {
            return false;
        }
        for (int i = 0; i < valueCount; i++) {
            values[i] -= min;
        }
        DeltaStage.writeZLong(metaOut, min);
        return true;
    }

    @Override
    public void decode(long[] values, int valuesOffset, int valueCount, DataInput metaIn) throws IOException {
        final long min = DeltaStage.readZLong(metaIn);
        for (int i = 0; i < valueCount; i++) {
            values[valuesOffset + i] += min;
        }
    }
}
