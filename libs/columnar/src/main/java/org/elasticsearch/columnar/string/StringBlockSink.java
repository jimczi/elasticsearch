/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;

/**
 * Receives one block of string values, in whichever form the column found cheaper for that block.
 *
 * <p>The two forms carry the same values. Ordinals are <b>block-scoped</b>: they index the dictionary handed
 * to the same call and mean nothing outside it, so a consumer that groups or hashes can work on ints for
 * this block without the column ever exposing a segment-wide ordinal space.
 */
public interface StringBlockSink {

    /**
     * The block as ordinals into a block-local dictionary. Every value in the block appears exactly once in
     * {@code dictionary}, so equal values share an ordinal.
     *
     * @param ordinals       one per requested document, in request order
     * @param count          entries of {@code ordinals} that are set
     * @param dictionary     the distinct values of this block, valid until the call returns
     * @param dictionarySize entries of {@code dictionary} that are set
     */
    void appendOrdinals(int[] ordinals, int count, BytesRef[] dictionary, int dictionarySize);

    /**
     * The block as values, one per requested document. Used when the block holds too few repeats for the
     * ordinal form to save the consumer anything.
     */
    void appendValues(BytesRef[] values, int count);
}
