/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Prefetches a batch of documents across many doc-values columns before any of them is read, so that
 * the number of blocking rounds depends on how the columns are laid out and not on how many there are.
 *
 * <p>Prefetching saves no read, it only lets reads overlap, so the one limit is on how many are asked for
 * at once: a round covers at most {@link #budget} document and column pairs. Documents are taken a window
 * at a time, sized to that budget, and when there are more columns than the budget a window of one
 * document takes its columns a group at a time. A round runs each {@link PrefetchableDocValues} stage on
 * every column of the group that still needs one, and the caller reads the window once every group is done.
 */
public final class DocValuesBatchPrefetcher {

    /**
     * How many document and column pairs one round may ask for; {@code 0} turns batch prefetching off.
     * A prototype switch, set by benchmarks or with the {@code es.dv.prefetch.budget} system property.
     */
    public static volatile int budget = Integer.getInteger("es.dv.prefetch.budget", 0);

    private final PrefetchableDocValues[] columns;
    private final boolean[] pending;
    private final int windowDocs;
    private final int columnsPerRound;

    private DocValuesBatchPrefetcher(PrefetchableDocValues[] columns, int budget) {
        this.columns = columns;
        this.pending = new boolean[columns.length];
        this.windowDocs = Math.max(1, budget / Math.max(1, columns.length));
        this.columnsPerRound = Math.max(1, budget);
    }

    /**
     * Opens the columns of {@code fields} that can be prefetched. Each is a separate instance from the
     * one the caller reads, so opening them costs no read and disturbs nothing.
     */
    public static DocValuesBatchPrefetcher open(LeafReader reader, Collection<String> fields, int budget) throws IOException {
        List<PrefetchableDocValues> columns = new ArrayList<>(fields.size());
        for (String field : fields) {
            FieldInfo info = reader.getFieldInfos().fieldInfo(field);
            if (info == null) {
                continue;
            }
            Object column = switch (info.getDocValuesType()) {
                case NONE -> null;
                case NUMERIC -> reader.getNumericDocValues(field);
                case BINARY -> reader.getBinaryDocValues(field);
                case SORTED -> reader.getSortedDocValues(field);
                case SORTED_NUMERIC -> {
                    var values = reader.getSortedNumericDocValues(field);
                    var singleton = DocValues.unwrapSingleton(values);
                    yield singleton != null ? singleton : values;
                }
                case SORTED_SET -> {
                    var values = reader.getSortedSetDocValues(field);
                    var singleton = DocValues.unwrapSingleton(values);
                    yield singleton != null ? singleton : values;
                }
            };
            if (column instanceof PrefetchableDocValues prefetchable) {
                columns.add(prefetchable);
            }
        }
        return new DocValuesBatchPrefetcher(columns.toArray(PrefetchableDocValues[]::new), budget);
    }

    /** How many documents one window holds. */
    public int windowDocs() {
        return windowDocs;
    }

    /** Runs every stage of every column over {@code docs[from, to)}, which are in ascending order. */
    public void prefetch(int[] docs, int from, int to) throws IOException {
        for (int first = 0; first < columns.length; first += columnsPerRound) {
            final int last = Math.min(columns.length, first + columnsPerRound);
            boolean more = false;
            for (int c = first; c < last; c++) {
                pending[c] = columns[c].prefetch(0, docs, from, to);
                more |= pending[c];
            }
            for (int stage = 1; more; stage++) {
                more = false;
                for (int c = first; c < last; c++) {
                    if (pending[c]) {
                        pending[c] = columns[c].prefetch(stage, docs, from, to);
                        more |= pending[c];
                    }
                }
            }
        }
    }
}
