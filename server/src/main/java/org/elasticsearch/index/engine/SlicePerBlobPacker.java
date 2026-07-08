/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Packs a commit's files into <b>one blob per slice</b> (tenant), so each tenant's data is an independently
 * fetchable / evictable / encryptable object — the storage-layer counterpart to one-segment-per-slice. Files are
 * grouped by owning slice via {@link SliceCommitFiles}; shared/commit-level files (e.g. {@code segments_N}) go to a
 * single {@code shared} blob. Each blob is self-describing (file bytes, then a file&rarr;offset,length footer and a
 * trailing footer pointer), so a file can be read back from just its tenant's blob.
 * <p>
 * POC of the layout + isolated read; wiring it into {@code StatelessCommitService} is separate.
 */
public final class SlicePerBlobPacker {

    /** Blob name for files not owned by any slice (shared commit metadata). */
    public static final String SHARED_BLOB = "shared.blob";

    private SlicePerBlobPacker() {}

    /** Blob name for a slice's files. Slice keys are constrained to {@code [a-zA-Z0-9._:-]}; ':' is mapped for filenames. */
    public static String blobNameFor(String slice) {
        return slice == null ? SHARED_BLOB : "slice_" + slice.replace(':', '_') + ".blob";
    }

    /**
     * Writes one blob per slice into {@code blobDir} and returns {@code sliceKey -> blobName} (the {@code null}
     * key maps to {@link #SHARED_BLOB}). Source file bytes are read from {@code commitDir}.
     */
    public static Map<String, String> pack(Directory commitDir, IndexCommit commit, Directory blobDir) throws IOException {
        final Map<String, Set<String>> filesBySlice = SliceCommitFiles.groupBySlice(commit);
        final Map<String, String> sliceToBlob = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> group : filesBySlice.entrySet()) {
            final String blobName = blobNameFor(group.getKey());
            writeBlob(commitDir, group.getValue(), blobDir, blobName);
            sliceToBlob.put(group.getKey(), blobName);
        }
        return sliceToBlob;
    }

    private static void writeBlob(Directory commitDir, Set<String> files, Directory blobDir, String blobName) throws IOException {
        try (IndexOutput out = blobDir.createOutput(blobName, IOContext.DEFAULT)) {
            final Map<String, long[]> entries = new LinkedHashMap<>();
            for (String file : files) {
                final long offset = out.getFilePointer();
                try (IndexInput in = commitDir.openInput(file, IOContext.DEFAULT)) {
                    final long length = in.length();
                    out.copyBytes(in, length);
                    entries.put(file, new long[] { offset, length });
                }
            }
            final long footerStart = out.getFilePointer();
            out.writeVInt(entries.size());
            for (Map.Entry<String, long[]> e : entries.entrySet()) {
                out.writeString(e.getKey());
                out.writeVLong(e.getValue()[0]);
                out.writeVLong(e.getValue()[1]);
            }
            out.writeLong(footerStart);
        }
    }

    /**
     * Reads {@code fileName}'s bytes back from just {@code blobName} — opening no other slice's blob. This is
     * the isolated per-tenant fetch: serving one tenant touches only that tenant's blob.
     */
    public static byte[] readFile(Directory blobDir, String blobName, String fileName) throws IOException {
        try (IndexInput blob = blobDir.openInput(blobName, IOContext.DEFAULT)) {
            blob.seek(blob.length() - Long.BYTES);
            final long footerStart = blob.readLong();
            blob.seek(footerStart);
            final int count = blob.readVInt();
            for (int i = 0; i < count; i++) {
                final String name = blob.readString();
                final long offset = blob.readVLong();
                final long length = blob.readVLong();
                if (name.equals(fileName)) {
                    final byte[] bytes = new byte[Math.toIntExact(length)];
                    blob.seek(offset);
                    blob.readBytes(bytes, 0, bytes.length);
                    return bytes;
                }
            }
            throw new FileNotFoundException("file [" + fileName + "] not found in blob [" + blobName + "]");
        }
    }
}
