/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SlicePerBlobPackerTests extends ESTestCase {

    private static final String[] SLICES = { "tenantA", "tenantB", "tenantC" };

    public void testPacksOneBlobPerSliceAndReadsTenantInIsolation() throws Exception {
        final Directory commitDir = new ByteBuffersDirectory();
        final IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        iwc.setDocumentPartitioner(doc -> {
            for (IndexableField f : doc) {
                if (f.name().equals("slice")) {
                    return f.stringValue();
                }
            }
            return null;
        });
        iwc.setMergePolicy(new SlicePartitionedMergePolicy(new TieredMergePolicy()));
        try (IndexWriter w = new IndexWriter(commitDir, iwc)) {
            for (String slice : SLICES) {
                for (int i = 0; i < 5; i++) {
                    final Document d = new Document();
                    d.add(new StringField("slice", slice, Field.Store.YES));
                    d.add(new StringField("id", slice + "-" + i, Field.Store.NO));
                    w.addDocument(d);
                }
            }
            w.forceMerge(1); // one segment per slice
            w.commit();
        }

        try (DirectoryReader reader = DirectoryReader.open(commitDir)) {
            final IndexCommit commit = reader.getIndexCommit();
            final Map<String, Set<String>> filesBySlice = SliceCommitFiles.groupBySlice(commit);

            final RecordingDirectory blobDir = new RecordingDirectory(new ByteBuffersDirectory());
            final Map<String, String> sliceToBlob = SlicePerBlobPacker.pack(commitDir, commit, blobDir);

            // One blob per slice, plus a shared blob for commit-level files (segments_N).
            for (String slice : SLICES) {
                assertEquals("slice_" + slice + ".blob", sliceToBlob.get(slice));
            }
            assertEquals(SlicePerBlobPacker.SHARED_BLOB, sliceToBlob.get(null));

            // Every file reads back byte-identical from its tenant's own blob.
            for (Map.Entry<String, Set<String>> group : filesBySlice.entrySet()) {
                final String blobName = sliceToBlob.get(group.getKey());
                for (String file : group.getValue()) {
                    final byte[] original = readAll(commitDir, file);
                    final byte[] fromBlob = SlicePerBlobPacker.readFile(blobDir, blobName, file);
                    assertArrayEquals("file " + file + " differs after per-slice packing", original, fromBlob);
                }
            }

            // Isolation: reading tenantB's files opens only tenantB's blob — no other tenant's blob is touched.
            blobDir.opened.clear();
            for (String file : filesBySlice.get("tenantB")) {
                SlicePerBlobPacker.readFile(blobDir, "slice_tenantB.blob", file);
            }
            for (String opened : blobDir.opened) {
                assertEquals("reading tenantB touched another tenant's blob: " + opened, "slice_tenantB.blob", opened);
            }
        }
    }

    private static byte[] readAll(Directory dir, String file) throws IOException {
        try (IndexInput in = dir.openInput(file, IOContext.DEFAULT)) {
            final byte[] bytes = new byte[Math.toIntExact(in.length())];
            in.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    private static final class RecordingDirectory extends FilterDirectory {
        final List<String> opened = new ArrayList<>();

        RecordingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            opened.add(name);
            return super.openInput(name, context);
        }
    }
}
