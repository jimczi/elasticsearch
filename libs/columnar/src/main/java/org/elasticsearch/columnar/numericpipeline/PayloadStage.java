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
 * Terminal stage of a numeric encoder pipeline — the one stage that actually serialises
 * the (post-transform) values as a payload. Unlike {@link NumericStage}, a payload stage always
 * runs (it produces the block's value bytes) and writes its output directly to the data
 * stream rather than to the metadata buffer.
 */
interface PayloadStage {

    /** Stable identity of this payload stage. */
    StageId stageId();

    /**
     * Serialise the values into {@code dataOut}. The implementation writes its inline
     * header (e.g. bit width) followed by the packed payload.
     */
    void encode(long[] values, int valueCount, DataOutput dataOut) throws IOException;

    /**
     * Deserialise the payload bytes into {@code values[valuesOffset..valuesOffset+valueCount)}.
     * {@code scratch} is a caller-owned reusable buffer the implementation may use to avoid
     * per-block heap allocations.
     */
    void decode(long[] values, int valuesOffset, int valueCount, DataInput dataIn, long[] scratch) throws IOException;
}
