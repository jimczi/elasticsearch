/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.test.ESTestCase;

public class SliceBlobLayoutTests extends ESTestCase {

    public void testSlicePrefixesAreIsolatedAndReversible() {
        final String idx = "abcUUID";
        final String keyA = SliceBlobLayout.blobKey(idx, 0, "tenantA", "seg_0.blob");
        final String keyB = SliceBlobLayout.blobKey(idx, 0, "tenantB", "seg_0.blob");

        // Each slice lives under its own prefix -> per-prefix SSE-KMS + O(1) prefix delete + cheap listing.
        assertTrue(keyA.startsWith(SliceBlobLayout.slicePrefix(idx, 0, "tenantA")));
        assertTrue(keyB.startsWith(SliceBlobLayout.slicePrefix(idx, 0, "tenantB")));
        assertNotEquals(SliceBlobLayout.slicePrefix(idx, 0, "tenantA"), SliceBlobLayout.slicePrefix(idx, 0, "tenantB"));
        // ...and both under the single index prefix.
        assertTrue(keyA.startsWith(SliceBlobLayout.indexPrefix(idx)));
        assertTrue(keyB.startsWith(SliceBlobLayout.indexPrefix(idx)));

        // Reversible: recover the slice from a key (for GC / listing).
        assertEquals("tenantA", SliceBlobLayout.sliceOf(idx, 0, keyA));
        assertEquals("tenantB", SliceBlobLayout.sliceOf(idx, 0, keyB));
        // Index-level metadata (not under a slice) -> null.
        assertNull(SliceBlobLayout.sliceOf(idx, 0, idx + "/index_meta.blob"));
        // A different shard's key is not attributed to this shard's slices.
        assertNull(SliceBlobLayout.sliceOf(idx, 0, SliceBlobLayout.blobKey(idx, 1, "tenantA", "seg_0.blob")));
    }

    public void testArbitrarySliceNamesSurviveEncoding() {
        final String idx = "idx";
        // Slice names can be arbitrary routing values incl. '/', spaces, unicode — base64 keeps the key well-formed.
        for (String slice : new String[] { "a/b", "with space", "üñîçødé", "sep space test", "", "a".repeat(200) }) {
            final String key = SliceBlobLayout.blobKey(idx, 3, slice, "blob");
            assertEquals(slice, SliceBlobLayout.sliceOf(idx, 3, key));
            // The raw slice value must not leak into the path structure (no stray separators).
            assertEquals(4, key.chars().filter(c -> c == '/').count()); // idx / 3 / s / enc / blob == 4 slashes
        }
    }
}
