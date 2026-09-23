/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.store.ReadAdvice;

import java.util.Optional;

/**
 * What a read asks the kernel to do with the pages it touches, from what the caller said about the file.
 *
 * <p>Both kinds of advice take a mapping out of the recency tracking that decides what is reclaimed first, so a
 * file advised either way is reclaimed ahead of files that are genuinely cold. That trade only pays where the
 * pages are not read again, which is why saying so is what admits a file to any advice at all; the access
 * pattern then only picks which one. A file read at random and read again keeps the default and ages on merit.
 *
 * <p>This says what to ask for, not how to ask. A directory mapping the file itself turns the answer into an
 * {@code madvise} call on that mapping. A directory reading through a shared cache cannot, since one mapping
 * covers regions of many files at once, and instead keeps a mapping per answer and serves the read through the
 * one that matches. Both decide the same way, which is the point of deciding here.
 */
public final class ReadAdvicePolicy {

    private ReadAdvicePolicy() {}

    /**
     * The advice a mapping serving this read should carry, or empty where the read has earned none and the
     * platform default applies.
     */
    public static Optional<ReadAdvice> adviceFor(IOContext context) {
        if (context.hints().contains(NoReuseHint.INSTANCE) == false) {
            return Optional.empty();
        }
        if (context.hints().contains(DataAccessHint.RANDOM)) {
            return Optional.of(ReadAdvice.RANDOM);
        }
        if (context.hints().contains(DataAccessHint.SEQUENTIAL)) {
            return Optional.of(ReadAdvice.SEQUENTIAL);
        }
        return Optional.empty();
    }
}
