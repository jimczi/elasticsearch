/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.encoder.BytesBlockEncoder;

import java.io.IOException;

/**
 * Forward, single-pass iterator over the variable-length byte values of a column. The
 * counterpart to {@link LongValuesIterator} for {@link BytesBlockEncoder}; encoders use it
 * to walk values without materialising them on heap.
 *
 * <p>The {@link BytesRef} returned by {@link #bytesValue()} is owned by the iterator and is
 * valid only until the next {@link #next()} call. Encoders that need to retain a value
 * across iterations must copy the bytes themselves.
 *
 * <p>To re-walk the values for a multi-pass analysis (e.g. cardinality counting before
 * deciding between a dictionary and raw encoding), open a fresh iterator via
 * {@link BytesRefValuesSupplier#open()}.
 */
public interface BytesRefValuesIterator {

    /**
     * Advance to the next value. Returns {@code true} if a value is available and
     * {@link #bytesValue()} returns a valid {@code BytesRef}; {@code false} when the iterator
     * is exhausted.
     */
    boolean next() throws IOException;

    /**
     * The value at the current position. Only valid until the next {@link #next()} call.
     */
    BytesRef bytesValue();
}
