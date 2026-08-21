/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ColumnIterator;

import java.io.IOException;

/**
 * A string column at the binary surface. A query that knows this format takes {@link #reader()} and drives
 * the column's own matching instead of comparing values a document at a time.
 */
public final class ColumnarStringBinaryDocValues extends BinaryDocValues {

    private final StringColumnReader reader;
    private final ColumnIterator iterator;
    private final BytesRef value = new BytesRef();

    ColumnarStringBinaryDocValues(StringColumnReader reader, ColumnIterator iterator) {
        this.reader = reader;
        this.iterator = iterator;
    }

    public StringColumnReader reader() {
        return reader;
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return reader.valueAt(reader.firstValue(iterator.index()), value);
    }

    /**
     * The dictionary ordinal of this document's value, without reading the value itself. A merge that
     * already knows what each of the input's ordinals becomes needs nothing more than this, and resolving
     * the bytes only to look them up again is the expensive part of merging a dictionary column.
     */
    public int ordinal() throws IOException {
        return reader.ordinalAt(reader.firstValue(iterator.index()));
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        return iterator.advanceExact(target);
    }

    @Override
    public int docID() {
        return iterator.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return iterator.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        return iterator.advance(target);
    }

    @Override
    public long cost() {
        return iterator.cost();
    }
}
