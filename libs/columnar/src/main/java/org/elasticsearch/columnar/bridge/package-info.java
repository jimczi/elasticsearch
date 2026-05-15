/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Typed indexing + read wrappers over the format's single binary substrate. The format
 * itself only writes binary doc values; this package gives downstream code typed entry
 * points without re-introducing typed Lucene doc-values shapes (which would force
 * sorting, dedup, or ordinal materialisation the format deliberately avoids).
 *
 * <h2>Indexing wrappers (extend {@code BinaryDocValuesField})</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.columnar.bridge.ColumNARLongField} (also
 *       {@link org.elasticsearch.columnar.bridge.ColumNARIntField} /
 *       {@link org.elasticsearch.columnar.bridge.ColumNARFloatField} /
 *       {@link org.elasticsearch.columnar.bridge.ColumNARDoubleField}) — pack typed
 *       numerics into the {@code 'L'}-shape payload via
 *       {@link org.elasticsearch.columnar.bridge.PackedLongBinaryPacker}.</li>
 *   <li>{@link org.elasticsearch.columnar.bridge.ColumNARKeywordField} — pack
 *       {@code BytesRef} values into the {@code 'B'}-shape payload via
 *       {@link org.elasticsearch.columnar.bridge.PackedBytesBinaryPacker}.</li>
 * </ul>
 *
 * <h2>Read iterators (extend {@code DocIdSetIterator})</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.columnar.bridge.ColumNARLongValues} —
 *       {@code longAt(i)} + bit-reinterpreting accessors for int / float / double, plus
 *       a {@code readValues(long[], int)} bulk seam.</li>
 *   <li>{@link org.elasticsearch.columnar.bridge.ColumNARBytesValues} —
 *       {@code bytesAt(i)} / {@code stringAt(i)} + {@code readValues(byte[], int, int[],
 *       int)} bulk seam.</li>
 * </ul>
 *
 * <h2>Per-doc payload format</h2>
 *
 * Every payload starts with a one-byte shape marker
 * ({@link org.elasticsearch.columnar.bridge.PayloadShape}: {@code 'L' = 0x4C} for longs,
 * {@code 'B' = 0x42} for bytes). The marker is verified at decode time and fails fast
 * if a {@code BinaryDocValuesField} was written without going through one of the
 * packers — single- and multi-valued docs share the same shape.
 *
 * <ul>
 *   <li>{@code 'L'} payload: {@code [byte 'L'][vint count][LE long]*count}.</li>
 *   <li>{@code 'B'} payload: {@code [byte 'B'][vint count]([vint len][bytes])*count}.</li>
 * </ul>
 *
 * Single-valued documents pay one extra byte (the marker) and one varint (the count = 1)
 * over the raw value bytes. Multi-valued documents preserve insertion order; the bridge
 * never sorts, never dedups, never exposes ordinals.
 */
package org.elasticsearch.columnar.bridge;
