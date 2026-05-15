/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Extension points the format wires together — numeric block encoders, bytes block
 * encoders, outer encodings, and per-column skip indexes. Each is registered through a
 * {@link java.util.ServiceLoader} registry and identified at write time by a stable
 * {@code int} id persisted in per-field metadata.
 *
 * <h2>Four seams</h2>
 *
 * <ul>
 *   <li>{@link org.elasticsearch.columnar.encoder.NumericBlockEncoder} — block of longs to
 *       bytes. Resolved through {@link
 *       org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry}.</li>
 *   <li>{@link org.elasticsearch.columnar.encoder.BytesBlockEncoder} — block of
 *       variable-length byte sequences to bytes, with a flat {@code byte[]} value buffer
 *       and {@code int[]} offsets. Resolved through {@link
 *       org.elasticsearch.columnar.encoder.BytesBlockEncoderRegistry}.</li>
 *   <li>{@link org.elasticsearch.columnar.encoder.BlockEncoding} — outer wrap (LZ4 today,
 *       identity for measurement). Stateful per-consumer via {@code newEncoder()};
 *       stateless on decode. Resolved through {@link
 *       org.elasticsearch.columnar.encoder.BlockEncodingRegistry}.</li>
 *   <li>{@link org.elasticsearch.columnar.encoder.SkipIndex} — per-column doc-id-range
 *       index that surfaces through Lucene's {@code DocValuesSkipper}. Resolved through
 *       {@link org.elasticsearch.columnar.encoder.SkipIndexRegistry}.</li>
 * </ul>
 *
 * <h2>Block-payload layout</h2>
 *
 * Per-field metadata records the chosen encoder + encoding + skip-index ids; the block
 * table records, per block, {@code [vlong dataOffset][int payloadLen][int encodedLen]
 * [int valuesInBlock]} (numeric) or {@code [vlong dataOffset][int payloadLen][int
 * encodedLen][int valuesInBlock][int totalValueBytes]} (binary). On read the producer
 * resolves the encoder/encoding by id, seeks to {@code dataOffset}, hands the encoding's
 * {@code decode()} an {@code IndexInput} sliced to {@code encodedLen}, and feeds the
 * decoded {@code DataInput} of length {@code payloadLen} to the encoder's
 * {@code decode()}.
 *
 * <h2>Wire-format invariant</h2>
 *
 * <strong>Each id, once published in a shipped release, is frozen forever.</strong> New
 * behaviour ships as a new id, never as a silent change to an existing one. Built-in ids
 * sit at the low end of each registry's id space; downstream registrations pick higher
 * ids. Duplicate ids are a startup error — the registries fail fast, so a
 * misconfigured deployment never silently corrupts bytes.
 *
 * <p>Framing changes (per-field record shape, file header structure) ship through
 * {@link org.elasticsearch.columnar.ColumNARDocValuesFormat#VERSION_CURRENT}. Layout
 * changes that aren't backward-compatible ship as a fresh format class with a fresh SPI
 * name (Lucene's {@code Lucene90DocValuesFormat} → {@code Lucene104DocValuesFormat}
 * precedent); the SPI-registered name is the long-term BWC contract.
 *
 * <h2>Caller-provided buffers</h2>
 *
 * Decode hot paths take caller-provided {@code byte[]} / {@code long[]} scratches. The
 * producer allocates one scratch per Lucene DocValues instance and reuses it across every
 * block. Identity encoding decodes return the underlying {@code IndexInput} directly so
 * reads pull from mmap with no intermediate heap copy.
 */
package org.elasticsearch.columnar.encoder;
