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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Prefetches the documents a consumer is about to read across many doc-values columns and many
 * segments, so that the number of blocking rounds depends on how the columns are laid out and not on
 * how many columns or segments there are.
 *
 * <p>It knows nothing of who reads. A consumer that has the documents in hand, such as a fetch phase,
 * a source loader or a block loader, adds each segment with the documents it will read from it, in
 * reading order, and names the columns it will read, either by field or by handing over a column it
 * already holds. Several readers of the same documents share one prefetcher, each adding its own columns
 * to the segment.
 *
 * <p>Reading then proceeds a window at a time. A window starts at the first document not yet covered and
 * runs on into the following segments until it holds {@link #budget} document and column pairs. Each
 * {@link PrefetchableDocValues} stage is run on every column of every segment of the window before the
 * next stage, and the consumer then reads the window.
 *
 * <p>Prefetching saves no read, it only lets reads overlap, so the budget is the one limit: it bounds
 * how much is asked for at once. A document with more columns than the budget takes its columns a
 * group at a time.
 */
public final class DocValuesBatchPrefetcher {

    /**
     * How many document and column pairs one round may ask for; {@code 0} turns batch prefetching off.
     * A prototype switch, set by benchmarks or with the {@code es.dv.prefetch.budget} system property.
     */
    public static volatile int budget = Integer.getInteger("es.dv.prefetch.budget", 0);

    /** The documents to read from one segment and the columns they are read from. */
    public static final class Segment {
        private final LeafReader reader;
        private final int[] docs;
        private final List<PrefetchableDocValues> columns = new ArrayList<>();
        private final Set<String> fields = new HashSet<>();
        private boolean[] pending = new boolean[0];
        /** One past the last position of {@link #docs} that a window has covered. */
        private int covered = 0;
        private int windowFrom;

        private Segment(LeafReader reader, int[] docs) {
            this.reader = reader;
            this.docs = docs;
        }

        /** The documents to read from the segment, in ascending order. */
        public int[] docs() {
            return docs;
        }

        /**
         * Adds the columns of {@code fields} that can be prefetched. Each is opened as an instance of its
         * own, separate from the one the consumer reads, which costs no read and disturbs nothing.
         */
        public void addFields(Collection<String> fields) throws IOException {
            for (String field : fields) {
                if (this.fields.add(field) == false) {
                    continue;
                }
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
        }

        /** Adds a column the consumer already holds. */
        public void addColumn(PrefetchableDocValues column) {
            columns.add(column);
        }
    }

    private final int pairsPerRound;
    private final List<Segment> segments = new ArrayList<>();
    private final List<Segment> window = new ArrayList<>();

    public DocValuesBatchPrefetcher(int budget) {
        this.pairsPerRound = Math.max(1, budget);
    }

    /**
     * The segment of {@code reader} and the documents to read from it. A segment is added after the ones
     * already added, which is the order they are read in; asking again for the same reader and documents
     * gives the same segment back, so that every reader of those documents adds its columns to it.
     */
    public Segment segment(LeafReader reader, int[] docs) {
        for (Segment segment : segments) {
            if (segment.reader == reader && Arrays.equals(segment.docs, docs)) {
                return segment;
            }
        }
        Segment segment = new Segment(reader, docs);
        segments.add(segment);
        return segment;
    }

    /**
     * Makes sure the document at {@code index} of {@code segment} has been prefetched, opening a new
     * window there if no earlier one reached it. The window carries on into the segments added after
     * this one while the budget lasts.
     *
     * @return one past the last position of the segment's documents that has been prefetched
     */
    public int cover(Segment segment, int index) throws IOException {
        if (index < segment.covered) {
            return segment.covered;
        }
        window.clear();
        int remaining = pairsPerRound;
        int from = index;
        for (int s = segments.indexOf(segment); s < segments.size(); s++) {
            Segment next = segments.get(s);
            if (next != segment) {
                from = next.covered;
            }
            int available = next.docs.length - from;
            if (available == 0) {
                continue;
            }
            int perDoc = Math.max(1, next.columns.size());
            int count = Math.min(available, remaining / perDoc);
            if (count == 0 && window.isEmpty()) {
                count = 1;
            }
            if (count == 0) {
                break;
            }
            next.windowFrom = from;
            next.covered = from + count;
            if (next.pending.length < next.columns.size()) {
                next.pending = new boolean[next.columns.size()];
            }
            window.add(next);
            remaining -= count * perDoc;
            if (count < available) {
                break;
            }
        }
        prefetchWindow();
        return segment.covered;
    }

    /** Runs every stage over the window, a group of at most the budget's worth of columns at a time. */
    private void prefetchWindow() throws IOException {
        int groupFirstSegment = 0;
        int groupFirstColumn = 0;
        while (groupFirstSegment < window.size()) {
            // Find where the group ends: after pairsPerRound columns, or at the end of the window.
            int lastSegment = groupFirstSegment;
            int lastColumn = groupFirstColumn;
            int taken = 0;
            while (lastSegment < window.size() && taken < pairsPerRound) {
                int available = window.get(lastSegment).columns.size() - lastColumn;
                int take = Math.min(available, pairsPerRound - taken);
                taken += take;
                lastColumn += take;
                if (lastColumn == window.get(lastSegment).columns.size()) {
                    lastSegment++;
                    lastColumn = 0;
                }
            }
            boolean more = true;
            for (int stage = 0; more; stage++) {
                more = false;
                for (int s = groupFirstSegment; s <= lastSegment && s < window.size(); s++) {
                    Segment segment = window.get(s);
                    int first = s == groupFirstSegment ? groupFirstColumn : 0;
                    int end = s == lastSegment ? lastColumn : segment.columns.size();
                    for (int c = first; c < end; c++) {
                        if (stage == 0 || segment.pending[c]) {
                            segment.pending[c] = segment.columns.get(c).prefetch(stage, segment.docs, segment.windowFrom, segment.covered);
                            more |= segment.pending[c];
                        }
                    }
                }
            }
            groupFirstSegment = lastSegment;
            groupFirstColumn = lastColumn;
        }
    }
}
