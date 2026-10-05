/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Proves that a slice index answers a query exactly as a plain index holding only that slice would.
 *
 * <p>The same documents are indexed twice: once into a slice index, where the terms carry the slice, and once per slice
 * into a plain index holding only that slice's documents. Every query is then built by the field type against both and
 * the matching documents compared. Nothing in the test knows about the prefix, which is the point: if a query shape
 * forgets it, the slice index returns nothing and the duel fails.
 */
public class SliceQueryDuelTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    private static final List<String> SLICES = List.of("tenant-a", "tenant-b", "tenant-c");

    /** name -> slice, keyword, text. Values deliberately overlap across slices. */
    private static final Map<String, String[]> CORPUS = new LinkedHashMap<>();
    static {
        CORPUS.put("a1", new String[] { "tenant-a", "alpha", "the quick brown fox" });
        CORPUS.put("a2", new String[] { "tenant-a", "beta", "a lazy brown dog" });
        CORPUS.put("a3", new String[] { "tenant-a", "alphabet", "quick quick fox" });
        CORPUS.put("b1", new String[] { "tenant-b", "alpha", "the quick red fox" });
        CORPUS.put("b2", new String[] { "tenant-b", "gamma", "a sleepy brown cat" });
        CORPUS.put("b3", new String[] { "tenant-b", "beta", "lazy lazy dog" });
        CORPUS.put("c1", new String[] { "tenant-c", "beta", "the quick brown fox" });
        CORPUS.put("c2", new String[] { "tenant-c", "delta", "a brown bear" });
    }

    private MapperService mapperService(boolean sliced) throws IOException {
        final Settings settings = sliced ? Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build() : Settings.EMPTY;
        return createMapperService(settings, mapping(b -> {
            b.startObject("name").field("type", "keyword").endObject();
            b.startObject("kw").field("type", "keyword").endObject();
            b.startObject("body").field("type", "text").endObject();
        }));
    }

    /** Runs {@code query} against a slice index scoped to {@code slice}, and against a plain index of that slice only. */
    private void duel(String description, CheckedFunction<SearchExecutionContext, Query, IOException> query) throws IOException {
        for (String slice : SLICES) {
            final List<String> sliced = searchSliced(slice, query);
            final List<String> plain = searchPlain(slice, query);
            assertThat(description + " on [" + slice + "]", sliced, equalTo(plain));
        }
    }

    private List<String> searchSliced(String slice, CheckedFunction<SearchExecutionContext, Query, IOException> query) throws IOException {
        final MapperService mapperService = mapperService(true);
        final List<String> names = new ArrayList<>();
        withLuceneIndex(mapperService, writer -> {
            for (var entry : CORPUS.entrySet()) {
                writer.addDocument(parse(mapperService, entry).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final SearchExecutionContext context = createSearchExecutionContext(mapperService);
            context.setSliceRouting(slice);
            names.addAll(matches(reader, query.apply(context)));
        });
        return names;
    }

    private List<String> searchPlain(String slice, CheckedFunction<SearchExecutionContext, Query, IOException> query) throws IOException {
        final MapperService mapperService = mapperService(false);
        final List<String> names = new ArrayList<>();
        withLuceneIndex(mapperService, writer -> {
            for (var entry : CORPUS.entrySet()) {
                if (entry.getValue()[0].equals(slice)) {
                    writer.addDocument(parse(mapperService, entry).rootDoc());
                }
            }
            writer.forceMerge(1);
        }, reader -> names.addAll(matches(reader, query.apply(createSearchExecutionContext(mapperService)))));
        return names;
    }

    private ParsedDocument parse(MapperService mapperService, Map.Entry<String, String[]> entry) throws IOException {
        final String name = entry.getKey();
        final String[] fields = entry.getValue();
        return mapperService.documentMapper().parse(source(name, b -> {
            b.field("name", name);
            b.field("kw", fields[1]);
            b.field("body", fields[2]);
        }, mapperService.getIndexSettings().isSliceEnabled() ? fields[0] : null));
    }

    /** The names of the matching documents, sorted, read from the plain doc values. */
    private static List<String> matches(DirectoryReader reader, Query query) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        final ScoreDoc[] hits = searcher.search(query, 100).scoreDocs;
        Arrays.sort(hits, Comparator.comparingInt(hit -> hit.doc));
        final List<String> names = new ArrayList<>();
        for (ScoreDoc hit : hits) {
            final LeafReaderContext leaf = reader.leaves().get(ReaderUtil.subIndex(hit.doc, reader.leaves()));
            final SortedSetDocValues values = leaf.reader().getSortedSetDocValues("name");
            if (values != null && values.advanceExact(hit.doc - leaf.docBase)) {
                names.add(values.lookupOrd(values.nextOrd()).utf8ToString());
            }
        }
        names.sort(String::compareTo);
        return names;
    }

    public void testTermQuery() throws IOException {
        for (String value : List.of("alpha", "beta", "gamma", "delta", "missing")) {
            duel("term " + value, context -> context.getFieldType("kw").termQuery(value, context));
        }
        for (String value : List.of("brown", "quick", "lazy", "missing")) {
            duel("text term " + value, context -> context.getFieldType("body").termQuery(value, context));
        }
    }

    public void testTermsQuery() throws IOException {
        duel("terms", context -> context.getFieldType("kw").termsQuery(List.of("alpha", "gamma", "missing"), context));
    }

    public void testTermQueryCaseInsensitive() throws IOException {
        duel("term ci", context -> context.getFieldType("kw").termQueryCaseInsensitive("ALPHA", context));
    }

    public void testPrefixQuery() throws IOException {
        for (String value : List.of("al", "alpha", "b", "zzz")) {
            duel("prefix " + value, context -> context.getFieldType("kw").prefixQuery(value, null, context));
        }
    }

    public void testWildcardQuery() throws IOException {
        for (String value : List.of("al*", "*a", "*et*", "?eta", "zzz*")) {
            duel("wildcard " + value, context -> context.getFieldType("kw").wildcardQuery(value, null, context));
        }
    }

    public void testWildcardQueryCaseInsensitive() throws IOException {
        for (String value : List.of("AL*", "*A", "?ETA")) {
            duel(
                "wildcard ci " + value,
                context -> ((StringFieldType) context.getFieldType("kw")).wildcardQuery(value, null, true, context)
            );
        }
    }

    public void testRegexpQuery() throws IOException {
        for (String value : List.of("a.pha", "alpha.*", "(alpha|gamma)", "zzz")) {
            duel("regexp " + value, context -> context.getFieldType("kw").regexpQuery(value, 0, 0, 10000, null, context));
        }
    }

    public void testFuzzyQuery() throws IOException {
        for (String value : List.of("alpho", "betta", "gamma", "zzzzz")) {
            duel(
                "fuzzy " + value,
                context -> context.getFieldType("kw")
                    .fuzzyQuery(value, org.elasticsearch.common.unit.Fuzziness.ONE, 0, 50, true, context, null)
            );
        }
    }

    public void testRangeQuery() throws IOException {
        duel("range a..c", context -> context.getFieldType("kw").rangeQuery("a", "c", true, true, null, null, null, context));
        duel("range null..c", context -> context.getFieldType("kw").rangeQuery(null, "c", true, true, null, null, null, context));
        duel("range a..null", context -> context.getFieldType("kw").rangeQuery("a", null, true, true, null, null, null, context));
        duel("range null..null", context -> context.getFieldType("kw").rangeQuery(null, null, true, true, null, null, null, context));
        duel(
            "range exclusive",
            context -> context.getFieldType("kw").rangeQuery("alpha", "delta", false, false, null, null, null, context)
        );
    }

    private static TextFieldMapper.TextFieldType body(SearchExecutionContext context) {
        return (TextFieldMapper.TextFieldType) context.getFieldType("body");
    }

    private static Query intervalQuery(org.apache.lucene.queries.intervals.IntervalsSource source) {
        return new org.apache.lucene.queries.intervals.IntervalQuery("body", source);
    }

    public void testTermIntervals() throws IOException {
        for (String term : List.of("brown", "quick", "lazy", "missing")) {
            duel("term intervals " + term, context -> intervalQuery(body(context).termIntervals(new BytesRef(term), context)));
        }
    }

    public void testPrefixIntervals() throws IOException {
        for (String term : List.of("br", "qu", "zzz")) {
            duel("prefix intervals " + term, context -> intervalQuery(body(context).prefixIntervals(new BytesRef(term), context)));
        }
    }

    public void testWildcardIntervals() throws IOException {
        for (String pattern : List.of("bro*", "*own", "?uick", "zzz*")) {
            duel(
                "wildcard intervals " + pattern,
                context -> intervalQuery(body(context).wildcardIntervals(new BytesRef(pattern), context))
            );
        }
    }

    public void testRegexpIntervals() throws IOException {
        for (String pattern : List.of("b.own", "qu.*", "(brown|quick)", "zzz")) {
            duel("regexp intervals " + pattern, context -> intervalQuery(body(context).regexpIntervals(new BytesRef(pattern), context)));
        }
    }

    public void testFuzzyIntervals() throws IOException {
        for (String term : List.of("brwon", "quik", "brown", "zzzzz")) {
            duel("fuzzy intervals " + term, context -> intervalQuery(body(context).fuzzyIntervals(term, 1, 0, true, context)));
        }
    }

    public void testRangeIntervals() throws IOException {
        duel(
            "range intervals b..d",
            context -> intervalQuery(body(context).rangeIntervals(new BytesRef("b"), new BytesRef("d"), true, true, context))
        );
        duel(
            "range intervals open upper",
            context -> intervalQuery(body(context).rangeIntervals(new BytesRef("b"), null, true, true, context))
        );
        duel(
            "range intervals open lower",
            context -> intervalQuery(body(context).rangeIntervals(null, new BytesRef("d"), true, true, context))
        );
    }

    /** A phrase has to line up positions, so it fails if the per-slice sources are stitched together wrongly. */
    public void testPhraseIntervals() throws IOException {
        duel(
            "phrase intervals",
            context -> intervalQuery(
                org.apache.lucene.queries.intervals.Intervals.ordered(
                    body(context).termIntervals(new BytesRef("quick"), context),
                    body(context).termIntervals(new BytesRef("brown"), context)
                )
            )
        );
        duel(
            "phrase intervals reversed",
            context -> intervalQuery(
                org.apache.lucene.queries.intervals.Intervals.ordered(
                    body(context).termIntervals(new BytesRef("brown"), context),
                    body(context).termIntervals(new BytesRef("quick"), context)
                )
            )
        );
    }

    /**
     * The duel would also pass if nothing matched anywhere, so this checks the corpus actually distinguishes the slices:
     * a term every slice holds must match different documents in each.
     */
    public void testTheCorpusWouldCatchALeak() throws IOException {
        final List<String> a = searchSliced("tenant-a", context -> context.getFieldType("kw").termQuery("alpha", context));
        final List<String> b = searchSliced("tenant-b", context -> context.getFieldType("kw").termQuery("alpha", context));
        assertThat(a, equalTo(List.of("a1")));
        assertThat(b, equalTo(List.of("b1")));
        assertThat(a, not(equalTo(b)));
    }
}
