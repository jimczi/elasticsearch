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
 * Opens a reader scoped to a single slice (tenant): a view over <b>only</b> that slice's segments in a
 * commit, opening no other slice's segments. Because a slice-sticky indexing buffer produces one segment
 * per slice (stamped with {@link DocumentPartitioner#PARTITION_ATTRIBUTE}), this reader:
 * <ul>
 *   <li>loads only the requested tenant's data — inactive tenants are never opened (the search-side of
 *       "only active tenants consume resources", which pays off most in stateless/object-store);</li>
 *   <li>has its own doc-id space {@code [0, sliceDocs)} independent of other slices, so a tenant is not
 *       constrained by (or counted against) other tenants' sizes.</li>
 * </ul>
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
     * Opens a reader over only the segments of the {@code allowedSlices} — the basis of leaf-level security:
     * a principal authorized for a set of tenants sees exactly those tenants' segments, and no others are
     * loaded or visible. This is O(#segments) (a segment is fully in or out by its slice attribute) and needs
     * no per-document DLS bitset, so it scales to very many tenants.
     */
    public static DirectoryReader openAllowed(Directory directory, IndexCommit commit, Set<String> allowedSlices) throws IOException {
        return PartitionReaders.open(
            directory,
            commit,
            sci -> allowedSlices.contains(sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE))
        );
    }
}
