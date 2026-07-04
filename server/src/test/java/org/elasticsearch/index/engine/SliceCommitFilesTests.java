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
import org.apache.lucene.index.DocumentPartitioner;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;

public class SliceCommitFilesTests extends ESTestCase {

    private static final String[] SLICES = { "tenantA", "tenantB", "tenantC" };

    public void testGroupsCommitFilesByOwningSlice() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
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

            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int round = 0; round < 4; round++) {
                    for (String slice : SLICES) {
                        for (int i = 0; i < 3; i++) {
                            final Document d = new Document();
                            d.add(new StringField("slice", slice, Field.Store.YES));
                            d.add(new StringField("id", slice + "-" + round + "-" + i, Field.Store.NO));
                            w.addDocument(d);
                        }
                    }
                    w.flush();
                }
                w.forceMerge(1); // one segment per slice
                w.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final IndexCommit commit = reader.getIndexCommit();
                final Map<String, Set<String>> bySlice = SliceCommitFiles.groupBySlice(commit);

                // Each tenant appears as its own group; commit-level metadata (segments_N) is shared (null key).
                assertThat(bySlice.keySet(), hasItems(SLICES));
                assertTrue("shared files (e.g. segments_N) group under null", bySlice.containsKey(null));
                assertTrue(bySlice.get(null).stream().anyMatch(f -> f.startsWith(IndexFileNames.SEGMENTS)));

                // Groups are disjoint and together cover exactly the commit's files.
                final Set<String> union = new HashSet<>();
                int total = 0;
                for (Set<String> files : bySlice.values()) {
                    union.addAll(files);
                    total += files.size();
                }
                assertEquals("slice groups must be disjoint", total, union.size());
                assertEquals("slice groups must cover every commit file", new HashSet<>(commit.getFileNames()), union);

                // Every file in a slice group truly belongs to a segment tagged with that slice.
                final Map<String, String> segmentToSlice = new HashMap<>();
                final SegmentInfos infos = Lucene.readSegmentInfos(commit);
                for (SegmentCommitInfo sci : infos) {
                    segmentToSlice.put(sci.info.name, sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE));
                }
                for (Map.Entry<String, Set<String>> group : bySlice.entrySet()) {
                    if (group.getKey() != null) {
                        for (String file : group.getValue()) {
                            assertEquals(
                                "file " + file + " grouped under wrong slice",
                                group.getKey(),
                                segmentToSlice.get(IndexFileNames.parseSegmentName(file))
                            );
                        }
                    }
                }
            }
        }
    }
}
