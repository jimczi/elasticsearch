/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.elasticsearch.columnar.bridge.ColumNARLongValues;
import org.elasticsearch.columnar.bridge.PackedLongsFromBinaryDocValues;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Map;

/**
 * Minimal {@link MappedFieldType} that exposes columnar long values via the binary
 * substrate. Drives the iteration benchmark: indexing emits a single
 * {@link org.apache.lucene.document.BinaryDocValuesField} per doc (single- or multi-valued
 * via {@link org.elasticsearch.columnar.bridge.PackedLongBinaryPacker}), and reads route
 * through {@link #longValues(LeafReader)} which returns a {@link ColumNARLongValues}
 * iterator over the binary payload — never {@code NumericDocValues}, never
 * {@code SortedNumericDocValues}, never ordinals.
 *
 * <p>This proves the format's MFT integration is shape-agnostic: the same field type
 * handles single- and multi-valued data, ordering inside a multi-valued doc is an
 * implementation detail, and the read API never leaks any Lucene typed-DV abstraction.
 *
 * <p>Stubs the rest of the MFT surface (queries, value fetcher) since this type exists
 * only to demonstrate the binary-substrate access pattern from a benchmark.
 */
public final class ColumNARLongFieldType extends MappedFieldType {

    /** Field name in the index. */
    public static final String FIELD = "value";

    public ColumNARLongFieldType() {
        super(FIELD, IndexType.docValuesOnly(), false, Map.of());
    }

    @Override
    public String typeName() {
        return "columnar_long";
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        // Benchmark doesn't exercise the fetch path — return a sentinel.
        throw new UnsupportedOperationException("valueFetcher is not used in this benchmark");
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        // Benchmark doesn't exercise the term-query path — return a sentinel.
        throw new UnsupportedOperationException("termQuery is not used in this benchmark");
    }

    /**
     * Open the long bridge for this field on a leaf reader. Returns {@code null} when the
     * field is absent from the segment. This is the only API the benchmark touches — the
     * caller never sees {@code BinaryDocValues}, {@code NumericDocValues}, or any other
     * Lucene typed-DV abstraction.
     */
    public ColumNARLongValues longValues(LeafReader reader) throws IOException {
        final BinaryDocValues source = reader.getBinaryDocValues(name());
        if (source == null) {
            return null;
        }
        return new PackedLongsFromBinaryDocValues(source);
    }
}
