/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numericpipeline;

/**
 * Stable identifier for each pipeline stage. The id doubles as the bit position in the
 * per-block stage bitmap that {@link NumericPipelineEncoder} writes — bit {@code id} is set
 * when the corresponding stage applied to the block.
 *
 * <p>Once a stage id is published in a release, the wire format produced by that stage is
 * frozen forever. New stages take new ids; deprecated stages keep theirs so old segments
 * stay readable.
 */
enum StageId {

    /** Transform stage: replace a monotonic sequence with first-order deltas. */
    DELTA_STAGE((byte) 0),

    /** Transform stage: subtract the per-block minimum so the bit-pack stage sees a tighter range. */
    OFFSET_STAGE((byte) 1),

    /** Transform stage: divide every value by the block's greatest common divisor. */
    GCD_STAGE((byte) 2),

    /** Terminal payload stage: bit-pack the (post-transform) values into the block's data bytes. */
    BITPACK_PAYLOAD((byte) 3);

    final byte id;

    StageId(byte id) {
        this.id = id;
    }
}
