/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.encoder.NumericBlockEncoder;

import java.io.IOException;

/**
 * Forward, single-pass iterator over the {@code long} values of a column in the segment, in
 * doc order. Encoders use this to walk the values when {@link NumericBlockEncoder#specializeForSegment
 * specialising for a segment} without materialising them on heap.
 *
 * <p>The iterator carries no doc-id information — encoders only care about the value sequence.
 * Sparse and multi-valued fields are flattened to the value sequence by the caller; the
 * encoder sees one long per "position" with no notion of which doc produced it.
 *
 * <p>To re-walk the values for a multi-pass analysis, open a fresh iterator via
 * {@link LongValuesSupplier#open()}.
 */
public interface LongValuesIterator {

    /**
     * Advance to the next value. Returns {@code true} if a value is available and
     * {@link #longValue()} returns a valid result; {@code false} when the iterator is
     * exhausted. After the first {@code false} the iterator is closed and any further call
     * is undefined.
     */
    boolean next() throws IOException;

    /**
     * The value at the current position. Only valid after {@link #next()} returned
     * {@code true}; behaviour before the first {@code next()} or after exhaustion is
     * undefined.
     */
    long longValue();
}
