/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import java.util.Map;

/**
 * What the mapping says about a dense vector field, for a directory deciding how to open its files.
 * The shard hands one of these to its directory, so the directory can ask about a field without
 * knowing how fields are mapped.
 */
@FunctionalInterface
public interface VectorFieldOptions {

    /** The options a directory acts on. */
    record Options(boolean onDiskRescore, boolean onDiskMerge) {
        public static final Options NONE = new Options(false, false);
    }

    /** A lookup for an index with no dense vector fields. */
    VectorFieldOptions NONE = field -> Options.NONE;

    /** The options of {@code field}, or {@link Options#NONE} if it is not a dense vector field. */
    Options get(String field);

    /** Fixed options, for a directory built without a mapping behind it. */
    static VectorFieldOptions of(Map<String, Options> byField) {
        Map<String, Options> copy = Map.copyOf(byField);
        return field -> copy.getOrDefault(field, Options.NONE);
    }
}
