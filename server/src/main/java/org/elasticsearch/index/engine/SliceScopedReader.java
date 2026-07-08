/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocumentPartitioner;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.PartitionReaders;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Set;

/**
 * Opens a reader scoped to a single slice (tenant): a view over <b>only</b> that slice's segments in a commit,
 * opening no others. Because a slice-sticky buffer produces one segment per slice (stamped with
 * {@link DocumentPartitioner#PARTITION_ATTRIBUTE}), an inactive tenant is never opened and each tenant has its own
 * doc-id space {@code [0, sliceDocs)}, independent of other tenants' sizes.
 */
public final class SliceScopedReader {

    private SliceScopedReader() {}

    /** Opens a reader over only {@code slice}'s segments in {@code commit}. */
    public static DirectoryReader open(Directory directory, IndexCommit commit, String slice) throws IOException {
        return PartitionReaders.open(
            directory,
            commit,
            sci -> slice.equals(sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE))
        );
    }

    /**
     * Opens a reader over only the {@code allowedSlices}' segments — the basis of leaf-level security: a principal
     * authorized for a set of tenants sees exactly those, no others loaded. O(#segments), no per-document DLS bitset.
     */
    public static DirectoryReader openAllowed(Directory directory, IndexCommit commit, Set<String> allowedSlices) throws IOException {
        return PartitionReaders.open(
            directory,
            commit,
            sci -> allowedSlices.contains(sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE))
        );
    }
}
