/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

/**
 * Per-column skip-index configuration. Two thresholds bound a skip interval — whichever fires
 * first closes the interval and emits its on-disk record:
 *
 * <ul>
 *   <li>{@link #intervalDocs()} — maximum number of docs per interval. Bounds worst-case scan
 *       cost when many docs fit in a small byte range (tightly bit-packed numerics).</li>
 *   <li>{@link #intervalMaxBytes()} — maximum accumulated indexed-value bytes per interval.
 *       Bounds the byte-range the interval covers for variable-length payloads (long
 *       keywords, multi-valued binary).</li>
 * </ul>
 *
 * <p>Both values are recorded once per segment in the column's metadata so segments written
 * with one configuration stay readable when the codec defaults change. The skip-index
 * implementation may carry additional, type-specific parameters (e.g. a bloom filter's
 * false-positive rate) as private state that does not appear in this record — see the
 * wire-format-vs-config separation rule documented on {@link BlockEncoding}.
 */
public record SkipIndexParams(int intervalDocs, int intervalMaxBytes) {

    /**
     * Default doc threshold for a skip interval. Fine-grained skip intervals let range
     * filters short-circuit a much larger fraction of a segment than block boundaries
     * could on their own; at ~24 bytes per interval, the metadata overhead stays small
     * relative to the latency win on filtered scans.
     */
    public static final int DEFAULT_INTERVAL_DOCS = 256;

    /** Default byte threshold for a skip interval — 8 KB of source value bytes. */
    public static final int DEFAULT_INTERVAL_MAX_BYTES = 8 * 1024;

    public static final SkipIndexParams DEFAULTS = new SkipIndexParams(DEFAULT_INTERVAL_DOCS, DEFAULT_INTERVAL_MAX_BYTES);

    public SkipIndexParams {
        if (intervalDocs <= 0) {
            throw new IllegalArgumentException("intervalDocs must be positive, got " + intervalDocs);
        }
        if (intervalMaxBytes <= 0) {
            throw new IllegalArgumentException("intervalMaxBytes must be positive, got " + intervalMaxBytes);
        }
    }
}
