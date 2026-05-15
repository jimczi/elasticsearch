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
 * Resolves {@link NumericBlockEncoder} implementations by stable {@code getName()} on read.
 * Wraps Lucene's {@link NamedSPILoader} — the same machinery {@code DocValuesFormat.forName}
 * uses. Implementations register through {@code ServiceLoader} via
 * {@code META-INF/services/org.elasticsearch.columnar.encoder.NumericBlockEncoder}.
 *
 * <p>The first {@link #forName} call triggers eager loading via {@link NamedSPILoader}; the
 * lookup map is then immutable for the lifetime of the JVM. <strong>Once a name is published
 * in a shipped release, the bytes its encoder produces are frozen forever.</strong> New
 * behaviour ships as a new name (and a new class); old names stay readable.
 */
public final class NumericBlockEncoderRegistry {

    private static final class Holder {
        static final NamedSPILoader<NumericBlockEncoder> LOADER = new NamedSPILoader<>(NumericBlockEncoder.class);
    }

    private NumericBlockEncoderRegistry() {}

    /** Lookup the encoder registered under {@code name}. Throws if unknown. */
    public static NumericBlockEncoder forName(String name) {
        return Holder.LOADER.lookup(name);
    }

    /** Names of every registered encoder. */
    public static Set<String> availableNames() {
        return Holder.LOADER.availableServices();
    }
}
