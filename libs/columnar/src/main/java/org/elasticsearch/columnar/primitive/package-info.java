/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Low-level encoding primitives shared across encoders. Pure utilities, no Lucene
 * dependency, usable by any
 * {@link org.elasticsearch.columnar.encoder.NumericBlockEncoder} /
 * {@link org.elasticsearch.columnar.encoder.BytesBlockEncoder} implementation — including
 * downstream ones outside this library.
 *
 * <ul>
 *   <li>{@link org.elasticsearch.columnar.primitive.BitPacking} — fixed-width
 *       bit-pack/unpack against {@code long[]}.</li>
 *   <li>{@link org.elasticsearch.columnar.primitive.Delta} — first-order /
 *       second-order delta + zigzag.</li>
 *   <li>{@link org.elasticsearch.columnar.primitive.Gcd} — greatest-common-divisor
 *       detection over a window of longs.</li>
 *   <li>{@link org.elasticsearch.columnar.primitive.Rle} — run-length encoding for
 *       constant-run-heavy sequences.</li>
 *   <li>{@link org.elasticsearch.columnar.primitive.Dictionary} — sorted dictionary +
 *       bit-packed ordinals.</li>
 * </ul>
 *
 * <p>Every primitive operates on caller-provided arrays — no allocation on the hot path.
 * Each one has a dedicated JMH bench under {@code libs/columnar/benchmark/}.
 */
package org.elasticsearch.columnar.primitive;
