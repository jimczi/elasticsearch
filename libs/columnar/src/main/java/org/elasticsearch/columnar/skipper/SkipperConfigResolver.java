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
 * Per-field skipper-config policy. The format's consumer asks the resolver once per field
 * for a {@link SkipperConfig}; downstream codecs that want different policies per field
 * (e.g. raw-bytes columns get {@link SkipperConfig#DISABLED}, numeric date fields get a
 * 4-level deep skip list) plug in their own resolver.
 *
 * <p>The default implementation returns {@link SkipperConfig#DEFAULT} for every field.
 */
@FunctionalInterface
public interface SkipperConfigResolver {

    /** Resolver that returns {@link SkipperConfig#DEFAULT} for every field. */
    SkipperConfigResolver DEFAULT = fieldName -> SkipperConfig.DEFAULT;

    /** Resolver that disables the skipper for every field. */
    SkipperConfigResolver DISABLED = fieldName -> SkipperConfig.DISABLED;

    /** The skipper config to apply when writing the named field. */
    SkipperConfig resolve(String fieldName);
}
