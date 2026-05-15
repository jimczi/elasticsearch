/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.elasticsearch.columnar.encoder.BytesBlockEncoder;

import java.io.IOException;

/**
 * Replayable source of bytes-valued iterators over a column. The counterpart to
 * {@link LongValuesSupplier} for {@link BytesBlockEncoder}.
 *
 * <p>Each call to {@link #open()} returns a fresh iterator positioned before the first
 * value. Encoders can iterate multiple times to make a specialisation decision (e.g. a
 * dictionary path that wants to count distinct values before committing).
 */
@FunctionalInterface
public interface BytesRefValuesSupplier {

    /** Open a fresh iterator positioned before the first value. May be called multiple times. */
    BytesRefValuesIterator open() throws IOException;
}
