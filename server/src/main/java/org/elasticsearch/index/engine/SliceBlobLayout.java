/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Object-store key layout giving every index and slice its own key <b>prefix</b>:
 * <pre>
 *   &lt;index-uuid&gt;/                         index prefix   (index-level metadata / SSE-KMS)
 *   &lt;index-uuid&gt;/&lt;shard&gt;/s/&lt;slice&gt;/       slice prefix   (one tenant's blobs, isolated)
 *   &lt;index-uuid&gt;/&lt;shard&gt;/s/&lt;slice&gt;/&lt;blob&gt;  a unit blob
 * </pre>
 * A per-slice prefix gives per-tenant SSE-KMS encryption, O(1) tenant delete/clone (drop/copy a prefix), and cheap
 * per-tenant listing/GC — all from the layout, no per-blob bookkeeping. Slice names are URL-safe-base64 encoded so
 * arbitrary routing values can't break the path or collide across {@code /} separators; {@link #sliceOf} reverses it.
 */
public final class SliceBlobLayout {

    private static final String SLICE_MARKER = "s";
    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    private SliceBlobLayout() {}

    /** The index-level prefix: {@code <indexUuid>/}. Holds index-shared metadata; index-level SSE-KMS applies here. */
    public static String indexPrefix(String indexUuid) {
        return indexUuid + "/";
    }

    /** The slice-level prefix: {@code <indexUuid>/<shard>/s/<enc(slice)>/}. One tenant's isolated key-space. */
    public static String slicePrefix(String indexUuid, int shard, String slice) {
        return indexUuid + "/" + shard + "/" + SLICE_MARKER + "/" + encode(slice) + "/";
    }

    /** The full object key for one unit blob of a slice. */
    public static String blobKey(String indexUuid, int shard, String slice, String blobName) {
        return slicePrefix(indexUuid, shard, slice) + blobName;
    }

    /**
     * Recovers the slice from a blob key produced by {@link #blobKey}/{@link #slicePrefix}, or {@code null} if the
     * key does not belong to this index+shard's slice layout (e.g. index-level metadata).
     */
    public static String sliceOf(String indexUuid, int shard, String blobKey) {
        final String prefix = indexUuid + "/" + shard + "/" + SLICE_MARKER + "/";
        if (blobKey.startsWith(prefix) == false) {
            return null;
        }
        final int start = prefix.length();
        final int end = blobKey.indexOf('/', start);
        if (end < 0) {
            return null;
        }
        return decode(blobKey.substring(start, end));
    }

    private static String encode(String slice) {
        return ENC.encodeToString(slice.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String token) {
        return new String(DEC.decode(token), StandardCharsets.UTF_8);
    }
}
