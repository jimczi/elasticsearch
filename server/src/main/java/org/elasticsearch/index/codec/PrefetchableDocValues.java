/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import java.io.IOException;

/**
 * A doc-values column that can announce the reads a batch of documents will need, so that a caller
 * loading many columns can overlap their downloads instead of paying one blocking read at a time.
 *
 * <p>A column is prefetched in stages because some reads depend on others: the location of a value
 * block is only known once its address has been read. The contract that keeps the number of blocking
 * rounds bounded is that stage {@code s} only blocks on bytes that a stage before it asked for. A
 * caller runs stage {@code 0} on every column, then stage {@code 1} on every column that asked for
 * it, and so on, and only then reads the documents.
 *
 * <p>Prefetching does not move the column: it can be interleaved with reads on the same instance.
 *
 * <p>It is the consumer that knows which documents it is about to read, so it is the consumer that calls
 * this; a column never prefetches on its own. What a stage asks for depends on how a format lays a column
 * out, so each doc-values format implements this for itself, and a column of a format that does not is
 * read as before, without prefetching. The TSDB format implements it for numeric, compressed binary and
 * single-valued sorted columns.
 */
public interface PrefetchableDocValues {

    /**
     * Asks for the bytes that stage {@code stage} can locate for {@code docs[from, to)}, which are in
     * ascending order. Stages are run in order, starting at {@code 0}, over the same range.
     *
     * @return whether the column needs the next stage to run before the documents are read
     */
    boolean prefetch(int stage, int[] docs, int from, int to) throws IOException;
}
