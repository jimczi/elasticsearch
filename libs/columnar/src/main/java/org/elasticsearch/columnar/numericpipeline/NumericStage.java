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
 * One transform step in a numeric encoder pipeline. A stage inspects a block of values,
 * decides whether running itself would shrink the encoded size, and (if so) mutates the
 * values in place and writes its per-block metadata. Stages are composed into a pipeline
 * by {@link NumericPipelineEncoder}; each stage runs in pipeline order on encode and the
 * applied stages are reversed in opposite order on decode.
 *
 * <p>The pipeline records a per-block bitmap of which stages applied; on decode only the
 * applied stages run their reverse transform.
 */
interface NumericStage {

    /** Stable identity of this stage; doubles as the bit position in the pipeline bitmap. */
    StageId stageId();

    /**
     * Apply the stage if the input warrants it. Returns {@code true} when the stage
     * mutated the values and wrote metadata (the pipeline flips the corresponding bit in
     * the stage bitmap); {@code false} when the stage was a no-op.
     *
     * <p>Operates on {@code values[0..valueCount)} — encode always sees a fresh work copy
     * starting at offset 0.
     */
    boolean encode(long[] values, int valueCount, DataOutput metaOut) throws IOException;

    /**
     * Reverse the stage's transform on {@code values[valuesOffset..valuesOffset+valueCount)}.
     * Only invoked when the bitmap indicates this stage applied during encode; reads the
     * metadata its corresponding {@link #encode} wrote.
     */
    void decode(long[] values, int valuesOffset, int valueCount, DataInput metaIn) throws IOException;
}
