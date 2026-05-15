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

import java.nio.charset.StandardCharsets;

/**
 * Indexable field that stores one or more keyword / text / IP / binary values for a doc,
 * packed as a single binary doc-values payload. Bridge counterpart of
 * {@link ColumNARLongField} for variable-length byte values.
 *
 * <p>Multi-valued entries are stored in insertion order — no sort, no dedup, no ordinals.
 * Read side: open a {@link ColumNARBytesValues} (e.g. via
 * {@link PackedBytesFromBinaryDocValues}) over the same field's binary doc values.
 */
public final class ColumNARKeywordField extends BinaryDocValuesField {

    public ColumNARKeywordField(String name, BytesRef value) {
        super(name, new BytesRef(PackedBytesBinaryPacker.encodeSingle(value)));
    }

    public ColumNARKeywordField(String name, String value) {
        this(name, new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
    }

    public ColumNARKeywordField(String name, BytesRef... values) {
        super(name, new BytesRef(PackedBytesBinaryPacker.encode(values)));
    }

    /** Convenience multi-valued constructor for UTF-8 strings. */
    public static ColumNARKeywordField of(String name, String... values) {
        final BytesRef[] refs = new BytesRef[values.length];
        for (int i = 0; i < values.length; i++) {
            refs[i] = new BytesRef(values[i].getBytes(StandardCharsets.UTF_8));
        }
        return new ColumNARKeywordField(name, refs);
    }
}
