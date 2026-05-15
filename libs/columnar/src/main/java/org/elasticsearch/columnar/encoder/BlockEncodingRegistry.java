/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.util.NamedSPILoader;

import java.util.Set;

/**
 * Resolves {@link BlockEncoding} implementations by stable {@code getName()} on read.
 * Lucene-style {@link NamedSPILoader} pattern. Names registered through
 * {@code META-INF/services/org.elasticsearch.columnar.encoder.BlockEncoding}.
 *
 * <p><strong>Once a name is published in a shipped release, the bytes its implementation
 * produces are frozen forever.</strong> New behaviour ships as a new name (and a new
 * class); old names stay readable.
 */
public final class BlockEncodingRegistry {

    private static final class Holder {
        static final NamedSPILoader<BlockEncoding> LOADER = new NamedSPILoader<>(BlockEncoding.class);
    }

    private BlockEncodingRegistry() {}

    public static BlockEncoding forName(String name) {
        return Holder.LOADER.lookup(name);
    }

    public static Set<String> availableNames() {
        return Holder.LOADER.availableServices();
    }
}
