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
 * Indexable field that stores one or more {@code long} values for a doc, packed as a
 * binary doc-values payload. The columnar format's lineage is binary-only; this field is
 * the typed write-side adapter that turns a {@code long} (or {@code long[]}) into the
 * binary blob the codec stores.
 *
 * <p>Insertion order is preserved exactly — multi-valued entries come back out in the
 * same order they went in, with no sort and no dedup. There are no exposed ordinals, no
 * {@code SortedNumericDocValues}, no {@code NumericDocValues} type — Lucene sees a pure
 * {@link BinaryDocValuesField}.
 *
 * <p>Read side: open a {@link ColumNARLongValues} iterator (e.g. via
 * {@link PackedLongsFromBinaryDocValues}) over the same field's binary doc values.
 */
public final class ColumNARLongField extends BinaryDocValuesField {

    /** Single-valued constructor. */
    public ColumNARLongField(String name, long value) {
        super(name, new BytesRef(PackedLongBinaryPacker.encodeSingle(value)));
    }

    /** Multi-valued constructor; values are stored in the given order. */
    public ColumNARLongField(String name, long... values) {
        super(name, new BytesRef(PackedLongBinaryPacker.encode(values, values.length)));
    }
}
