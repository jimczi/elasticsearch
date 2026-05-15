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
 * Resolves {@link BytesBlockEncoder} implementations by stable {@code getName()} on read.
 * Sibling to {@link NumericBlockEncoderRegistry}; name spaces are independent. Same
 * Lucene-style {@link NamedSPILoader} pattern, same backwards-compatibility rules.
 */
public final class BytesBlockEncoderRegistry {

    private static final class Holder {
        static final NamedSPILoader<BytesBlockEncoder> LOADER = new NamedSPILoader<>(BytesBlockEncoder.class);
    }

    private BytesBlockEncoderRegistry() {}

    public static BytesBlockEncoder forName(String name) {
        return Holder.LOADER.lookup(name);
    }

    public static Set<String> availableNames() {
        return Holder.LOADER.availableServices();
    }
}
