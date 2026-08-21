/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/** A cursor over a string column's values, in document then written order. */
public abstract class StringColumnValues extends DocIdSetIterator {

    /** Values held by the current document. */
    public abstract int valueCount();

    /** The next value of the current document; the bytes are valid until the following call. */
    public abstract BytesRef nextValue() throws IOException;
}
