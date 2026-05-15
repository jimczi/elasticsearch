/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.bridge;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.util.BytesRef;

/**
 * Indexable field for one or more {@code double} values, encoded as IEEE 754 bit patterns
 * in the binary doc-values payload. Use {@link Double#longBitsToDouble} on the long read
 * by {@link ColumNARLongValues} to recover the double.
 */
public final class ColumNARDoubleField extends BinaryDocValuesField {

    public ColumNARDoubleField(String name, double value) {
        super(name, new BytesRef(PackedLongBinaryPacker.encodeSingle(Double.doubleToRawLongBits(value))));
    }

    public ColumNARDoubleField(String name, double... values) {
        super(name, new BytesRef(pack(values)));
    }

    private static byte[] pack(double[] values) {
        final long[] asLongs = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            asLongs[i] = Double.doubleToRawLongBits(values[i]);
        }
        return PackedLongBinaryPacker.encode(asLongs, asLongs.length);
    }
}
