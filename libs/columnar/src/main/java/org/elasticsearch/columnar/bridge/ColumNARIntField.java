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
 * Indexable field for one or more {@code int} values, encoded as sign-extended longs in the
 * binary doc-values payload. Bridge counterpart of {@link ColumNARLongField} for the
 * narrower 32-bit signed type.
 *
 * <p>Readers materialise the values via {@link ColumNARLongValues}; the high 32 bits are
 * the sign-extension of the original int, so callers cast back via {@code (int) longValue}
 * or use Java's narrowing conversion.
 */
public final class ColumNARIntField extends BinaryDocValuesField {

    public ColumNARIntField(String name, int value) {
        super(name, new BytesRef(PackedLongBinaryPacker.encodeSingle((long) value)));
    }

    public ColumNARIntField(String name, int... values) {
        super(name, new BytesRef(pack(values)));
    }

    private static byte[] pack(int[] values) {
        final long[] asLongs = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            asLongs[i] = values[i];
        }
        return PackedLongBinaryPacker.encode(asLongs, asLongs.length);
    }
}
