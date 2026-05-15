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
 * Indexable field for one or more {@code float} values, encoded as IEEE 754 bit patterns
 * sign-extended to longs in the binary doc-values payload. Use {@link Float#intBitsToFloat}
 * on the {@code int} cast of the long read by {@link ColumNARLongValues} to recover the
 * float.
 */
public final class ColumNARFloatField extends BinaryDocValuesField {

    public ColumNARFloatField(String name, float value) {
        super(name, new BytesRef(PackedLongBinaryPacker.encodeSingle((long) Float.floatToRawIntBits(value))));
    }

    public ColumNARFloatField(String name, float... values) {
        super(name, new BytesRef(pack(values)));
    }

    private static byte[] pack(float[] values) {
        final long[] asLongs = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            asLongs[i] = Float.floatToRawIntBits(values[i]);
        }
        return PackedLongBinaryPacker.encode(asLongs, asLongs.length);
    }
}
