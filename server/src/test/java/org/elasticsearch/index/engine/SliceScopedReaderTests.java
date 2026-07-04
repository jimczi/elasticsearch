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
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocumentPartitioner;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SliceScopedReaderTests extends ESTestCase {

    public void testReaderScopedToOneSliceWithIndependentDocIdSpaceAndIsolation() throws Exception {
        final Map<String, Integer> docsPerSlice = new HashMap<>();
        docsPerSlice.put("tenantA", 5);
        docsPerSlice.put("tenantB", 7);
        docsPerSlice.put("tenantC", 3);
        final int totalDocs = 15;

        final RecordingDirectory dir = new RecordingDirectory(new ByteBuffersDirectory());

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
            for (Map.Entry<String, Integer> e : docsPerSlice.entrySet()) {
                for (int i = 0; i < e.getValue(); i++) {
                    final Document d = new Document();
                    d.add(new StringField("slice", e.getKey(), Field.Store.YES));
                    d.add(new StringField("id", e.getKey() + "-" + i, Field.Store.NO));
                    w.addDocument(d);
                }
            }
            w.forceMerge(1); // one segment per slice
            w.commit();
        }

        final List<IndexCommit> commits = DirectoryReader.listCommits(dir);
        final IndexCommit commit = commits.get(commits.size() - 1);

        // Map each slice to its segment name so we can assert other slices' segments are never opened.
        final Map<String, String> sliceToSegment = new HashMap<>();
        for (SegmentCommitInfo sci : Lucene.readSegmentInfos(commit)) {
            sliceToSegment.put(sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE), sci.info.name);
        }

        dir.opened.clear();
        try (CompositeReader reader = SliceScopedReader.open(dir, commit, "tenantB")) {
            // Independent doc-id space: only tenantB's 7 docs, not the shard's 15.
            assertEquals(7, reader.numDocs());
            assertEquals("tenantB has its own [0,7) doc-id space", 7, reader.maxDoc());
            assertNotEquals(totalDocs, reader.maxDoc());

            final IndexSearcher searcher = newSearcher(reader);
            assertEquals(7, searcher.count(new TermQuery(new Term("slice", "tenantB"))));
            // Other tenants' segments are not part of this reader, so they are invisible to it.
            assertEquals(0, searcher.count(new TermQuery(new Term("slice", "tenantA"))));
            assertEquals(0, searcher.count(new TermQuery(new Term("slice", "tenantC"))));
        }

        // Isolation / lazy loading: reading the commit reads every segment's tiny ".si" metadata (how
        // SegmentInfos learns what exists — cheap, like stateless's metadata-only commit open), but the
        // heavy DATA of an inactive slice's segment (.cfs/.cfe/per-field files) is never opened.
        final String aSegment = sliceToSegment.get("tenantA");
        final String cSegment = sliceToSegment.get("tenantC");
        for (String opened : dir.opened) {
            final String segment = IndexFileNames.parseSegmentName(opened);
            if (segment.equals(aSegment) || segment.equals(cSegment)) {
                assertTrue("only an inactive slice's .si metadata may be read, never its data: " + opened, opened.endsWith(".si"));
            }
        }
    }

    public void testEachSliceReaderSeesOnlyItsOwnDocs() throws Exception {
        final RecordingDirectory dir = new RecordingDirectory(new ByteBuffersDirectory());
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
        final Map<String, Integer> counts = new HashMap<>();
        counts.put("tenantA", 5);
        counts.put("tenantB", 7);
        counts.put("tenantC", 3);
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                for (int i = 0; i < e.getValue(); i++) {
                    final Document d = new Document();
                    d.add(new StringField("slice", e.getKey(), Field.Store.YES));
                    w.addDocument(d);
                }
            }
            w.forceMerge(1);
            w.commit();
        }
        final List<IndexCommit> commits = DirectoryReader.listCommits(dir);
        final IndexCommit commit = commits.get(commits.size() - 1);
        for (Map.Entry<String, Integer> e : counts.entrySet()) {
            try (CompositeReader reader = SliceScopedReader.open(dir, commit, e.getKey())) {
                assertEquals("slice " + e.getKey() + " doc count", (int) e.getValue(), reader.numDocs());
            }
        }
    }

    public void testLeafLevelSecurityHidesAndDoesNotLoadUnauthorizedTenants() throws Exception {
        final String[] tenants = { "tenantA", "tenantB", "tenantC", "tenantD" };
        final RecordingDirectory dir = new RecordingDirectory(new ByteBuffersDirectory());
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
            for (String tenant : tenants) {
                for (int i = 0; i < 4; i++) {
                    final Document d = new Document();
                    d.add(new StringField("slice", tenant, Field.Store.YES));
                    w.addDocument(d);
                }
            }
            w.forceMerge(1);
            w.commit();
        }
        final List<IndexCommit> commits = DirectoryReader.listCommits(dir);
        final IndexCommit commit = commits.get(commits.size() - 1);
        final Map<String, String> sliceToSegment = new HashMap<>();
        for (SegmentCommitInfo sci : Lucene.readSegmentInfos(commit)) {
            sliceToSegment.put(sci.info.getAttribute(DocumentPartitioner.PARTITION_ATTRIBUTE), sci.info.name);
        }

        // A principal authorized for {tenantA, tenantC} only.
        final Set<String> allowed = Set.of("tenantA", "tenantC");
        dir.opened.clear();
        try (CompositeReader reader = SliceScopedReader.openAllowed(dir, commit, allowed)) {
            assertEquals("only authorized tenants' docs are visible", 8, reader.numDocs());
            final IndexSearcher searcher = newSearcher(reader);
            assertEquals(4, searcher.count(new TermQuery(new Term("slice", "tenantA"))));
            assertEquals(4, searcher.count(new TermQuery(new Term("slice", "tenantC"))));
            // Unauthorized tenants are invisible — enforced by segment exclusion, no per-doc bitset.
            assertEquals(0, searcher.count(new TermQuery(new Term("slice", "tenantB"))));
            assertEquals(0, searcher.count(new TermQuery(new Term("slice", "tenantD"))));
        }
        // ...and unauthorized tenants' data is never loaded (only their .si metadata is read).
        for (String opened : dir.opened) {
            final String segment = IndexFileNames.parseSegmentName(opened);
            if (segment.equals(sliceToSegment.get("tenantB")) || segment.equals(sliceToSegment.get("tenantD"))) {
                assertTrue("unauthorized tenant data was loaded: " + opened, opened.endsWith(".si"));
            }
        }
    }

    /** Records every {@link #openInput} so we can assert an inactive slice's files are never loaded. */
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
