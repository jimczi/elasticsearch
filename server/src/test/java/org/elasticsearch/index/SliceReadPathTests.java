/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

/**
 * The read paths that do not go through a field type: the term vectors handed back by the term vectors API, and the
 * verifier that fails a query which reached the dictionary without the slice.
 */
public class SliceReadPathTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    private MapperService sliceMapperService(String termVector) throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        return createMapperService(settings, mapping(b -> {
            b.startObject("body").field("type", "text").field("term_vector", termVector).endObject();
            b.startObject("kw").field("type", "keyword").endObject();
        }));
    }

    private static List<String> terms(TermsEnum iterator) throws IOException {
        final List<String> terms = new ArrayList<>();
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            terms.add(term.utf8ToString());
        }
        return terms;
    }

    public void testTermVectorsAreHandedBackWithoutTheSlice() throws IOException {
        final MapperService mapperService = sliceMapperService("with_positions_offsets");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(
                mapperService.documentMapper().parse(source("1", b -> b.field("body", "quick brown fox"), "tenant-a")).rootDoc()
            );
        }, reader -> {
            // What was indexed carries the slice.
            assertThat(
                terms(reader.termVectors().get(0, "body").iterator()),
                contains(
                    SliceIndexing.termPrefix("tenant-a") + "brown",
                    SliceIndexing.termPrefix("tenant-a") + "fox",
                    SliceIndexing.termPrefix("tenant-a") + "quick"
                )
            );
            // What the API hands back does not, and the order survives because a document holds one slice.
            assertThat(
                terms(SliceTermVectors.withoutSlice(reader.termVectors().get(0)).terms("body").iterator()),
                contains("brown", "fox", "quick")
            );
        });
    }

    public void testTermVectorsOfReservedFieldsAreUntouched() throws IOException {
        final MapperService mapperService = sliceMapperService("yes");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("body", "quick"), "tenant-a")).rootDoc());
        }, reader -> {
            // _id carries no slice, so the stripping view must leave it alone; it has no vector, hence null.
            assertNull(SliceTermVectors.withoutSlice(reader.termVectors().get(0)).terms("_id"));
        });
    }

    public void testTheVerifierFailsATermWithoutASlice() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, "tenant-a");
            final TermsEnum iterator = verified.leaves().get(0).reader().terms("kw").iterator();

            // The prefixed term is what the field type would produce, and passes.
            assertTrue(iterator.seekExact(SliceIndexing.prefixTerm("tenant-a", new BytesRef("alpha"))));

            // A plain term means something built the query without the field type.
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves().get(0).reader().terms("kw").iterator().seekExact(new BytesRef("alpha"))
            );
            assertThat(e.getMessage(), containsString("carries no slice"));

            // So does a term carrying the wrong slice.
            IllegalStateException wrong = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves()
                    .get(0)
                    .reader()
                    .terms("kw")
                    .iterator()
                    .seekExact(SliceIndexing.prefixTerm("tenant-b", new BytesRef("alpha")))
            );
            assertThat(wrong.getMessage(), containsString("carries no slice"));
        });
    }

    public void testTheVerifierFailsAnUnconfinedAutomaton() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("kw", "alpha"), "tenant-a")).rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, "tenant-a");
            // A prefix query over the plain value would accept terms with no slice in them at all.
            final var unconfined = new PrefixQuery(new Term("kw", "al")).getAutomaton();
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves()
                    .get(0)
                    .reader()
                    .terms("kw")
                    .intersect(new org.apache.lucene.util.automaton.CompiledAutomaton(unconfined, false, false, true), null)
            );
            assertThat(e.getMessage(), containsString("was not restricted to a slice"));
        });
    }

    public void testTheVerifierLeavesReservedFieldsAlone() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("kw", "alpha"), "tenant-a")).rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, "tenant-a");
            // _id is looked up without a slice and must not be policed: a plain term must not throw here, whatever it
            // resolves to.
            final TermsEnum ids = verified.leaves().get(0).reader().terms("_id").iterator();
            ids.seekExact(new BytesRef("not-an-id"));
        });
    }

    public void testASearchAcrossSlicesAcceptsAnySlicesTerm() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, null);
            assertTrue(
                verified.leaves()
                    .get(0)
                    .reader()
                    .terms("kw")
                    .iterator()
                    .seekExact(SliceIndexing.prefixTerm("tenant-b", new BytesRef("alpha")))
            );
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> verified.leaves().get(0).reader().terms("kw").iterator().seekExact(new BytesRef("alpha"))
            );
            assertThat(e.getMessage(), containsString("carries no slice"));
        });
    }

    public void testAQueryThroughTheFieldTypePassesTheVerifier() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final var context = createSearchExecutionContext(mapperService);
            context.setSliceRouting("tenant-a");
            final DirectoryReader verified = SliceTermsVerifier.wrap(reader, "tenant-a");
            final var searcher = new org.apache.lucene.search.IndexSearcher(verified);
            searcher.setQueryCache(null);
            // Every shape the field type builds must survive the verifier.
            assertEquals(1, searcher.count(context.getFieldType("kw").termQuery("alpha", context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").prefixQuery("al", null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").wildcardQuery("al*", null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").regexpQuery("a.pha", 0, 0, 10000, null, context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").termsQuery(List.of("alpha", "beta"), context)));
            assertEquals(1, searcher.count(context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context)));
            assertEquals(
                1,
                searcher.count(
                    context.getFieldType("kw").fuzzyQuery("alpho", org.elasticsearch.common.unit.Fuzziness.ONE, 0, 50, true, context, null)
                )
            );
        });
    }

    public void testShapesThatSpanSlicesMatchEverySlice() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            for (String slice : List.of("tenant-a", "tenant-b", "tenant-c")) {
                writer.addDocument(mapperService.documentMapper().parse(source(slice, b -> b.field("kw", "alpha"), slice)).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final var context = createSearchExecutionContext(mapperService);
            context.setSliceRouting(null);
            final var searcher = new org.apache.lucene.search.IndexSearcher(reader);
            searcher.setQueryCache(null);
            assertEquals(3, searcher.count(context.getFieldType("kw").termQuery("alpha", context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").termsQuery(List.of("alpha"), context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").regexpQuery("a.pha", 0, 0, 10000, null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").termQueryCaseInsensitive("ALPHA", context)));
            // Every shape spans slices now, none of them error.
            assertEquals(3, searcher.count(context.getFieldType("kw").prefixQuery("al", null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").wildcardQuery("al*", null, context)));
            assertEquals(3, searcher.count(context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context)));
            assertEquals(
                3,
                searcher.count(
                    context.getFieldType("kw").fuzzyQuery("alpho", org.elasticsearch.common.unit.Fuzziness.ONE, 0, 50, true, context, null)
                )
            );
            assertEquals(0, searcher.count(context.getFieldType("kw").termQuery("missing", context)));
        });
    }
}
