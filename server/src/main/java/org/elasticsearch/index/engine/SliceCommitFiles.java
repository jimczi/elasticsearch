/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DocumentPartitioner;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Groups a Lucene commit's files by the slice (tenant) that owns them, so a slice-aware storage layer (e.g. the
 * stateless object-store commit path) can place each tenant's files in its own blob — independently fetchable,
 * evictable, and encryptable. A slice-sticky buffer produces one segment per slice, recorded in the
 * {@link DocumentPartitioner#PARTITION_ATTRIBUTE} {@code SegmentInfo} attribute. Files not tied to any slice-tagged
 * segment (e.g. {@code segments_N}, or untagged segments) group under the {@code null} key.
 */
public final class SliceCommitFiles {

    private SliceCommitFiles() {}

    /**
     * Returns {@code sliceKey -> files}. The {@code null} key holds files not tied to any slice-tagged
     * segment (shared metadata / untagged segments). Insertion order of files is preserved per slice.
     */
    public static Map<String, Set<String>> groupBySlice(IndexCommit commit) throws IOException {
        final SegmentInfos segmentInfos = Lucene.readSegmentInfos(commit);
        final Map<String, String> segmentToSlice = new HashMap<>();
        for (SegmentCommitInfo sci : segmentInfos) {
            // getAttribute is null for segments written without a partitioner; such segments group under null.
            segmentToSlice.put(sci.info.name, sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE));
        }

        final Map<String, Set<String>> filesBySlice = new HashMap<>();
        for (String file : commit.getFileNames()) {
            final String segmentName = IndexFileNames.parseSegmentName(file);
            // parseSegmentName returns the whole name for non-segment files (e.g. "segments_5"), which won't
            // match any SegmentInfo.name, so those fall through to the null (shared) group.
            final String slice = segmentToSlice.get(segmentName);
            filesBySlice.computeIfAbsent(slice, k -> new LinkedHashSet<>()).add(file);
        }
        return filesBySlice;
    }
}
