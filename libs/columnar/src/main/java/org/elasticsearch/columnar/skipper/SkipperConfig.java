/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.skipper;

import java.util.EnumSet;
import java.util.Set;

/**
 * Per-field skipper configuration. The format's consumer asks the resolver once per field
 * for a {@code SkipperConfig}; the configuration drives every aspect of how the skipper
 * builds itself for that field:
 *
 * <ul>
 *   <li>{@link #enabled()} — fields with raw binary payloads or extremely wide values can
 *       skip the skipper entirely. Saves on-disk bytes and write-time CPU; readers behave
 *       as if no skip-interval boundaries match (every doc must be scanned).</li>
 *   <li>{@link #level0Granularity()} — docs per level-0 (most granular) interval.
 *       Finer = more skip-index bytes, finer-grained filter pushdown.</li>
 *   <li>{@link #levels()} — number of skip-list levels. {@code 1} = flat (no skip-list);
 *       higher levels let bulk scorers leap across many intervals at once when a filter
 *       prunes huge doc ranges.</li>
 *   <li>{@link #levelFanOut()} — each upper-level interval aggregates {@code fanOut}
 *       intervals from the level below.</li>
 *   <li>{@link #stats()} — explicit set of stats tracked per interval. {@link StatType#COUNT}
 *       is always tracked; the others are opt-in.</li>
 * </ul>
 *
 * <p>The defaults are tuned for the common case (numeric or dictionary-encoded keyword
 * field): 256-doc level-0 intervals, 3 levels with fan-out 8 (so level-1 covers ~2K docs
 * and level-2 covers ~16K docs), min/max + count + sum stats.
 */
public record SkipperConfig(boolean enabled, int level0Granularity, int levels, int levelFanOut, Set<StatType> stats) {

    /** Default per-interval doc count at level 0 — 256 docs per interval. */
    public static final int DEFAULT_LEVEL0_GRANULARITY = 256;

    /** Default number of skip-list levels — three levels covers up to ~16K docs per top-level interval. */
    public static final int DEFAULT_LEVELS = 3;

    /** Default fan-out — each upper-level interval covers 8 intervals from the level below. */
    public static final int DEFAULT_LEVEL_FAN_OUT = 8;

    /** Default stat set — min/max + count + sum (count and sum power aggregation pushdown). */
    public static final Set<StatType> DEFAULT_STATS = EnumSet.of(StatType.COUNT, StatType.MIN_MAX, StatType.SUM);

    /** Production-default config: enabled, 3-level skip list, 256-doc level-0 granularity, min/max+sum stats. */
    public static final SkipperConfig DEFAULT = new SkipperConfig(
        true,
        DEFAULT_LEVEL0_GRANULARITY,
        DEFAULT_LEVELS,
        DEFAULT_LEVEL_FAN_OUT,
        DEFAULT_STATS
    );

    /** Sentinel "skipper disabled" config — writer is a no-op, reader returns "no intervals". */
    public static final SkipperConfig DISABLED = new SkipperConfig(false, 1, 1, 2, EnumSet.of(StatType.COUNT));

    public SkipperConfig {
        if (level0Granularity <= 0) {
            throw new IllegalArgumentException("level0Granularity must be positive, got " + level0Granularity);
        }
        if (levels < 1) {
            throw new IllegalArgumentException("levels must be >= 1, got " + levels);
        }
        if (levels > 1 && levelFanOut < 2) {
            throw new IllegalArgumentException("levelFanOut must be >= 2 when levels > 1, got " + levelFanOut);
        }
        if (levels > 8) {
            throw new IllegalArgumentException("levels capped at 8, got " + levels);
        }
        stats = EnumSet.copyOf(stats);
        // Count is always tracked.
        stats.add(StatType.COUNT);
        stats = java.util.Collections.unmodifiableSet(stats);
    }

    /** {@code true} if the given stat is tracked. {@link StatType#COUNT} is always tracked. */
    public boolean tracks(StatType stat) {
        return stats.contains(stat);
    }
}
