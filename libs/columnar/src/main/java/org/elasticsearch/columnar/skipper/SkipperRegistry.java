/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.skipper;

import org.apache.lucene.util.NamedSPILoader;

import java.util.Set;

/**
 * Resolves {@link DocValuesSkipper} implementations by stable {@code getName()} on read.
 * Lucene-style {@link NamedSPILoader} pattern.
 */
public final class SkipperRegistry {

    private static final class Holder {
        static final NamedSPILoader<DocValuesSkipper> LOADER = new NamedSPILoader<>(DocValuesSkipper.class);
    }

    private SkipperRegistry() {}

    public static DocValuesSkipper forName(String name) {
        return Holder.LOADER.lookup(name);
    }

    public static Set<String> availableNames() {
        return Holder.LOADER.availableServices();
    }
}
