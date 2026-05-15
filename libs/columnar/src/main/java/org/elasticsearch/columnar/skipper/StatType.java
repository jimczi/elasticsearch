/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.skipper;

/**
 * Stat tracked per skip interval. Stats serve two distinct purposes:
 *
 * <ol>
 *   <li><b>Filter pushdown.</b> Range / equality filters use {@link #MIN_MAX} (or
 *       membership stats like {@code BLOOM_FILTER}) to prove a filter is absent over a
 *       skip-interval's doc-id range and skip the whole range without touching the value
 *       payload.</li>
 *   <li><b>Aggregation pushdown.</b> {@link #COUNT}, {@link #SUM} let block-level ES|QL
 *       aggregations (e.g. {@code SUM(field)}, {@code COUNT(field)}, {@code AVG(field)})
 *       answer over whole skip intervals without iterating individual docs.</li>
 * </ol>
 *
 * <p>Stats are an explicit feature set: each stat can be enabled or disabled per field
 * through {@link SkipperConfig}. Disabled stats occupy zero bytes on disk and return
 * {@link Long#MIN_VALUE} / {@link Long#MAX_VALUE} / {@code 0} sentinels from the reader so
 * a query path can detect "stat not present" and fall through to per-doc evaluation.
 *
 * <p>Stat ids are stable: once a stat ships in a release its bit position in the on-disk
 * stat bitmap is frozen. New stats take new bit positions; deprecated stats keep theirs so
 * old segments stay readable.
 */
public enum StatType {

    /**
     * Per-interval doc count. Always implicitly tracked because most readers need it; the
     * config keeps the enum value for completeness and to support a "stats bitmap" that
     * is full-width.
     */
    COUNT(0),

    /**
     * Per-interval value min and max. For numeric skippers this is two {@code long}s; for
     * bytes skippers it is two lexicographic {@code BytesRef}s.
     */
    MIN_MAX(1),

    /**
     * Per-interval value sum. Numeric only. Used by {@code SUM} / {@code AVG} aggregations
     * and by ES|QL block-level reducers.
     */
    SUM(2),

    /**
     * Per-interval count of null / missing entries (sparse columns). Lets {@code COUNT}
     * over sparse fields skip whole intervals where every doc is null.
     */
    NULL_COUNT(3);

    private final int bit;

    StatType(int bit) {
        this.bit = bit;
    }

    /** Bit position in the stat bitmap persisted on disk. Stable forever. */
    public int bit() {
        return bit;
    }
}
