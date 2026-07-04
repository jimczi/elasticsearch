/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perslice;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Wiring proof for the patched Lucene inside Elasticsearch's build/runtime: exercises the new
 * {@code IndexWriterConfig#setDocumentPartitioner} (slice-sticky buffers) together with {@code
 * SlicePartitionedMergePolicy} through a real Lucene {@link IndexWriter}, and asserts that every
 * segment — including after merges and {@code forceMerge(1)} — holds exactly one slice. This is the
 * per-tenant physical isolation running end-to-end in ES, ready for {@code InternalEngine} integration.
 */
public class SlicePartitionedEnginePocTests extends ESTestCase {

    private static final String[] SLICES = { "tenantA", "tenantB", "tenantC" };

    public void testPartitionedWriteAndSliceAwareMergeInEs() throws IOException {
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
                for (int round = 0; round < 6; round++) {
                    for (String slice : SLICES) {
                        for (int i = 0; i < 4; i++) {
                            final Document d = new Document();
                            d.add(new StringField("slice", slice, Field.Store.YES));
                            d.add(new StringField("id", slice + "-" + round + "-" + i, Field.Store.NO));
                            w.addDocument(d);
                        }
                    }
                    w.flush();
                }
                w.forceMerge(1);
            }

            try (DirectoryReader r = DirectoryReader.open(dir)) {
                assertEquals("forceMerge(1) yields one segment per slice", SLICES.length, r.leaves().size());
                final Set<String> slicesSeen = new HashSet<>();
                for (LeafReaderContext ctx : r.leaves()) {
                    final LeafReader lr = ctx.reader();
                    final StoredFields stored = lr.storedFields();
                    final Set<String> inSegment = new HashSet<>();
                    for (int d = 0; d < lr.maxDoc(); d++) {
                        inSegment.add(stored.document(d).get("slice"));
                    }
                    assertEquals("segment mixes tenants: " + inSegment, 1, inSegment.size());
                    slicesSeen.addAll(inSegment);
                }
                assertEquals(Set.of(SLICES), slicesSeen);
            }
        }
    }
}
