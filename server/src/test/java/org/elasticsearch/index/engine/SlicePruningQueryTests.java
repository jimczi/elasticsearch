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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SlicePartitionedMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

/**
 * Verifies {@link SlicePruningQuery} restricts a search to one (or a set of) tenant(s) by skipping whole segments,
 * not by post-filtering documents: only the tenant's docs match, and non-matching segments are never scored.
 */
public class SlicePruningQueryTests extends ESTestCase {

    private static IndexWriterConfig slicePartitionedConfig() {
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
        return iwc;
    }

    private static Document doc(String slice, String id) {
        final Document d = new Document();
        d.add(new StringField("slice", slice, Field.Store.NO));
        d.add(new StringField("id", id, Field.Store.NO));
        return d;
    }

    public void testPrunesToOneSliceAndSkipsOtherSegments() throws Exception {
        try (Directory dir = new ByteBuffersDirectory(); IndexWriter w = new IndexWriter(dir, slicePartitionedConfig())) {
            // 10 docs in tenantA, 7 in tenantB, 3 in tenantC -> one segment per tenant (slice-sticky buffer).
            for (int i = 0; i < 10; i++) {
                w.addDocument(doc("tenantA", "a" + i));
            }
            for (int i = 0; i < 7; i++) {
                w.addDocument(doc("tenantB", "b" + i));
            }
            for (int i = 0; i < 3; i++) {
                w.addDocument(doc("tenantC", "c" + i));
            }
            w.flush();

            try (DirectoryReader reader = DirectoryReader.open(w)) {
                final IndexSearcher searcher = newSearcher(reader);

                // Filtering an arbitrary query by tenantB yields exactly tenantB's docs.
                final Query onlyB = new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.MUST)
                    .add(new SlicePruningQuery(Set.of("tenantB")), Occur.FILTER)
                    .build();
                assertEquals(7, searcher.count(onlyB));

                // A doc that exists only in tenantA is invisible when pruned to tenantB.
                final Query aDocUnderB = new BooleanQuery.Builder().add(new TermQuery(new Term("id", "a0")), Occur.MUST)
                    .add(new SlicePruningQuery(Set.of("tenantB")), Occur.FILTER)
                    .build();
                assertEquals(0, searcher.count(aDocUnderB));
                // ...but visible under tenantA.
                final Query aDocUnderA = new BooleanQuery.Builder().add(new TermQuery(new Term("id", "a0")), Occur.MUST)
                    .add(new SlicePruningQuery(Set.of("tenantA")), Occur.FILTER)
                    .build();
                assertEquals(1, searcher.count(aDocUnderA));

                // A multi-slice restriction (A + C) matches both, excludes B.
                assertEquals(
                    13,
                    searcher.count(
                        new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.MUST)
                            .add(new SlicePruningQuery(Set.of("tenantA", "tenantC")), Occur.FILTER)
                            .build()
                    )
                );

                // The pruning is at SEGMENT granularity: for a tenantB search, every leaf that produced a scorer
                // is a tenantB segment (others are skipped, i.e. never traversed).
                final var weight = searcher.createWeight(
                    searcher.rewrite(new SlicePruningQuery(Set.of("tenantB"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );
                for (LeafReaderContext ctx : reader.leaves()) {
                    final String segSlice = Lucene.segmentReader(ctx.reader()).getSegmentInfo().info.getAttribute("lucene.partition.key");
                    if ("tenantB".equals(segSlice)) {
                        assertNotNull("tenantB leaf must be scored", weight.scorerSupplier(ctx));
                    } else {
                        assertNull("non-tenantB leaf must be pruned (skipped)", weight.scorerSupplier(ctx));
                    }
                }
            }
        }
    }

    public void testEqualsAndHashCode() {
        // A Query must honor the equals/hashCode contract so it can be cached; equality is by slice set.
        QueryUtils.checkEqual(new SlicePruningQuery(Set.of("a")), new SlicePruningQuery(Set.of("a")));
        QueryUtils.checkEqual(new SlicePruningQuery(Set.of("a", "b")), new SlicePruningQuery(Set.of("b", "a")));
        QueryUtils.checkUnequal(new SlicePruningQuery(Set.of("a")), new SlicePruningQuery(Set.of("a", "b")));
        QueryUtils.checkUnequal(new SlicePruningQuery(Set.of("a")), new SlicePruningQuery(Set.of("b")));
        QueryUtils.checkHashEquals(new SlicePruningQuery(Set.of("a")));
    }
}
