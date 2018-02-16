/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import static org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import static org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;

final class CompositeValuesCollectorQueue {
    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final int maxSize;
    private final TreeMap<Integer, Integer> keys;
    private final CompositeValuesSource<?, ?>[] arrays;
    private final int[] docCounts;
    private boolean afterValueSet = false;

    /**
     * Ctr
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     */
    CompositeValuesCollectorQueue(IndexReader reader, CompositeValuesSourceConfig[] sources, int size) {
        this.maxSize = size;
        this.arrays = new CompositeValuesSource<?, ?>[sources.length];
        this.docCounts = new int[size];
        for (int i = 0; i < sources.length; i++) {
            final int reverseMul = sources[i].reverseMul();
            if (sources[i].valuesSource() instanceof WithOrdinals && reader instanceof DirectoryReader) {
                WithOrdinals vs = (WithOrdinals) sources[i].valuesSource();
                arrays[i] = CompositeValuesSource.wrapGlobalOrdinals(vs, size, reverseMul);
            } else if (sources[i].valuesSource() instanceof Bytes) {
                Bytes vs = (Bytes) sources[i].valuesSource();
                arrays[i] = CompositeValuesSource.wrapBinary(vs, size, reverseMul);
            } else if (sources[i].valuesSource() instanceof Numeric) {
                final Numeric vs = (Numeric) sources[i].valuesSource();
                if (vs.isFloatingPoint()) {
                    arrays[i] = CompositeValuesSource.wrapDouble(vs, size, reverseMul);
                } else {
                    arrays[i] = CompositeValuesSource.wrapLong(vs, sources[i].format(), size, reverseMul);
                }
            }
        }
        this.keys = new TreeMap<>(this::compare);
    }

    /**
     * The current size of the queue.
     */
    int size() {
        return keys.size();
    }

    /**
     * Returns a sorted {@link Set} view of the slots contained in this queue.
     */
    Set<Integer> getSortedSlot() {
        return keys.keySet();
    }

    /**
     * Returns the slot of the current candidate or -1 if the candidate is not in the queue.
     */
    Integer getCurrent() {
        return keys.get(CANDIDATE_SLOT);
    }

    /**
     * Returns the document count in <code>slot</code>.
     */
    int getDocCount(int slot) {
        return docCounts[slot];
    }

    /**
     * Copies the current value in <code>slot</code>.
     */
    private void copyCurrent(int slot) {
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].copyCurrent(slot);
        }
        docCounts[slot] = 1;
    }

    /**
     * Compares the values in <code>slot1</code> with <code>slot2</code>.
     */
    int compare(int slot1, int slot2) {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = (slot1 == CANDIDATE_SLOT) ? arrays[i].compareCurrent(slot2) :
                arrays[i].compare(slot1, slot2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Sets the after values for this comparator.
     */
    void setAfter(Comparable<?>[] values) {
        assert values.length == arrays.length;
        afterValueSet = true;
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].setAfter(values[i]);
        }
    }

    /**
     * Compares the after values with the values in <code>slot</code>.
     */
    private int compareWithAfter() {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareCurrentWithAfter();
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Builds the {@link CompositeKey} for <code>slot</code>.
     */
    CompositeKey toCompositeKey(int slot) throws IOException {
        assert slot < maxSize;
        Comparable<?>[] values = new Comparable<?>[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    /**
     * Gets the {@link LeafBucketCollector} that will record the composite buckets of the visited documents.
     */
    CompositeValuesSource.Collector getLeafCollector(LeafReaderContext context, CompositeValuesSource.Collector in) throws IOException {
        int last = arrays.length - 1;
        CompositeValuesSource.Collector next = arrays[last].getLeafCollector(context, in);
        for (int i = last - 1; i >= 0; i--) {
            next = arrays[i].getLeafCollector(context, next);
        }
        return next;
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @param canEarlyTerminate true if the index is sorted by the composite sources.
     * @return The target slot of the candidate or -1 is the candidate is not competitive.
     */
    int addIfCompetitive(boolean canEarlyTerminate) {
        // Checks if the candidate key is competitive
        Integer topSlot = keys.get(CANDIDATE_SLOT);
        if (topSlot != null) {
            // This key is already in the top N, skip it
            docCounts[topSlot] += 1;
            return topSlot;
        }
        if (afterValueSet && compareWithAfter() <= 0) {
            // This key is greater than the top value collected in the previous round
            if (canEarlyTerminate) {
                // The index sort matches the composite sort, we can early terminate this segment
                throw new CollectionTerminatedException();
            }
            // skip it
            return -1;
        }
        if (keys.size() >= maxSize) {
            // The tree map is full, check if the candidate key should be kept
            if (compare(CANDIDATE_SLOT, keys.lastKey()) > 0) {
                // The candidate key is not competitive
                if (canEarlyTerminate) {
                    // The index sort matches the composite sort, we can early terminate this segment
                    throw new CollectionTerminatedException();
                }
                // just skip this key
                return -1;
            }
        }

        // The candidate key is competitive
        final int newSlot;
        if (keys.size() >= maxSize) {
            // the tree map is full, we replace the last key with this candidate
            int slot = keys.pollLastEntry().getKey();
            // and we recycle the deleted slot
            newSlot = slot;
        } else {
            newSlot = keys.size();
            assert newSlot < maxSize;
        }
        // move the candidate key to its new slot
        copyCurrent(newSlot);
        keys.put(newSlot, newSlot);
        return newSlot;
    }
}
