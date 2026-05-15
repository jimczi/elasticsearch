/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import java.io.IOException;

/**
 * Replayable source of long-valued iterators over a column. Encoders can call
 * {@link #open()} multiple times to re-walk the same value sequence — useful for multi-pass
 * analyses that probe range, GCD, monotonicity, cardinality, etc. without buffering the
 * values on heap. Each call returns a <em>fresh</em> iterator positioned before the first
 * value.
 *
 * <p>The format's consumer constructs a supplier from the source {@code DocValuesProducer}
 * so that each {@code open()} call gives the encoder a brand-new {@link
 * org.apache.lucene.index.NumericDocValues}-style walk.
 */
@FunctionalInterface
public interface LongValuesSupplier {

    /**
     * Open a fresh iterator positioned before the first value. May be called multiple times.
     * The values returned across calls represent the same logical sequence in the same order.
     */
    LongValuesIterator open() throws IOException;

    /**
     * Adapter that exposes a slice of a {@code long[]} as a replayable supplier. Useful for
     * unit tests that exercise {@code specializeForSegment} on a hand-crafted value sequence
     * without standing up a {@link org.apache.lucene.index.NumericDocValues}.
     */
    static LongValuesSupplier fromArray(long[] values, int offset, int length) {
        return () -> new LongValuesIterator() {
            private int pos = -1;

            @Override
            public boolean next() {
                pos++;
                return pos < length;
            }

            @Override
            public long longValue() {
                return values[offset + pos];
            }
        };
    }
}
