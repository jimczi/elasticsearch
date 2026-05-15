/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Public root of the Elasticsearch columnar doc-values library. Hosts the Lucene-facing
 * {@link org.elasticsearch.columnar.ColumNARDocValuesFormat} plus the small set of types
 * downstream code uses directly: the {@link org.elasticsearch.columnar.NumericType} enum
 * and the iterator + supplier APIs the bridge consumes ({@link
 * org.elasticsearch.columnar.LongValuesIterator},
 * {@link org.elasticsearch.columnar.LongValuesSupplier},
 * {@link org.elasticsearch.columnar.BytesRefValuesIterator},
 * {@link org.elasticsearch.columnar.BytesRefValuesSupplier}).
 *
 * <h2>Two files per segment</h2>
 *
 * <ul>
 *   <li>{@code .cdv} — block data. Codec header, then per-field per-block sequence of
 *       payload bytes (encoded by the chosen {@code NumericBlockEncoder} /
 *       {@code BytesBlockEncoder}, then wrapped by the chosen {@code BlockEncoding}),
 *       footer.</li>
 *   <li>{@code .cdvm} — per-field metadata. Codec header, then a sequence of per-field
 *       records, then the end sentinel ({@code fieldNumber = -1}), then footer.</li>
 * </ul>
 *
 * <h2>Per-field metadata record</h2>
 *
 * <p>Every field record starts with {@code [int fieldNumber][byte fieldType]}, then a
 * type-specific body. Two field types are written today:
 *
 * <ul>
 *   <li>{@code FIELD_TYPE_BINARY = 1} — direct binary block storage.</li>
 *   <li>{@code FIELD_TYPE_DICT_BINARY = 2} — per-segment dictionary + ordinal blocks
 *       (the consumer auto-selects on the first block's distinct-count).</li>
 * </ul>
 *
 * Type-specific bodies share the same shape: {@code [vint encoderId][vint encodingId][vint blockSize]
 * [vlong valueCount][vint blockCount]} then per-type summary fields, then the fixed-width
 * block table (offsets into {@code .cdv}, per-block lengths, per-block value counts), then
 * the type's auxiliary sections (e.g. the dictionary entries for {@code FIELD_TYPE_DICT_BINARY}).
 * Per-block table records are uniform-width so the producer can seek into them by
 * arithmetic without loading the table on heap. See
 * {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer} and
 * {@link org.elasticsearch.columnar.ColumNARDocValuesProducer} for the byte-exact layout.
 *
 * <h2>Block boundaries</h2>
 *
 * Each format instance is configured for one field with two ints: the target encoded
 * bytes per block and a row-count safety cap. Whichever fires first closes the block.
 * Both ints are persisted in the field's metadata; the reader sees the same boundaries
 * the writer chose. Different fields (driven by separate format instances under
 * {@code PerFieldDocValuesFormat}) flush at different doc-id positions.
 *
 * <h2>Format versioning</h2>
 *
 * Files carry {@code CodecUtil.writeIndexHeader(...)} with
 * {@link org.elasticsearch.columnar.ColumNARDocValuesFormat#VERSION_CURRENT}, and the
 * reader validates against {@code [VERSION_START, VERSION_CURRENT]}. Encoder, encoding,
 * and skip-index ids carry the long-term backwards-compatibility contract — see the
 * {@code org.elasticsearch.columnar.encoder} package for the rules.
 */
package org.elasticsearch.columnar;
