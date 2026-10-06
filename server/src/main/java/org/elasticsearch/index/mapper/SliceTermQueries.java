/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Search-time counterpart of {@link SliceTermPrefix}: a slice index stores {@code slice|term} in its terms dictionary,
 * so a query has to ask for the same thing.
 *
 * <p>Field types build their own queries, so applying the prefix there means every query shape goes through it, the way
 * {@code _id} is handled for slice indices. It also keeps the resulting query honest about what it matches, which
 * matters because the query cache keys on the query and a plain term would be the same in every slice.
 */
public final class SliceTermQueries {

    private SliceTermQueries() {}

    /** How a search sees a field's terms. */
    public enum Scope {
        /** Terms carry no slice, either because the index has none or because the field is not prefixed. */
        PLAIN,
        /** Terms carry a slice and the search targets specific ones. */
        SLICES,
        /** Terms carry a slice and the search spans all of them. */
        ALL_SLICES
    }

    public static Scope scope(String field, @Nullable SearchExecutionContext context) {
        if (context == null || SliceIndexing.prefixesTerms(field) == false) {
            return Scope.PLAIN;
        }
        final var settings = context.getIndexSettings();
        if (settings == null || settings.isSliceEnabled() == false) {
            return Scope.PLAIN;
        }
        return context.getSliceRouting() == null ? Scope.ALL_SLICES : Scope.SLICES;
    }

    /** The slices a search targets. Never empty, because {@link Scope#SLICES} means routing was given. */
    public static String[] slices(SearchExecutionContext context) {
        return Strings.splitStringByCommaToArray(context.getSliceRouting());
    }

    /**
     * The forms {@code value} takes in the dictionaries a search targets: one per slice, or the value unchanged when the
     * field carries no slice. Fails for a search spanning every slice, which has no single term to ask for.
     */
    public static List<BytesRef> searchTerms(String field, BytesRef value, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> List.of(value);
            case SLICES -> {
                final List<BytesRef> terms = new ArrayList<>();
                for (String slice : slices(context)) {
                    terms.add(term(slice, value));
                }
                yield terms;
            }
            case ALL_SLICES -> throw new IllegalArgumentException(
                "["
                    + SliceIndexing.PARAM_NAME
                    + "="
                    + SliceIndexing.SLICE_ALL
                    + "] is not supported for this query on field ["
                    + field
                    + "]; the terms of a slice index carry the slice they belong to, so a single slice must be named"
            );
        };
    }

    /**
     * The one way a query shape becomes slice-aware. A shape says how it looks on a plain index and what automaton it
     * accepts; on a slice index the automaton is confined to the slices the search targets, and on a plain one the
     * shape is built as it always was.
     *
     * <p>Every shape in {@link StringFieldType} and {@link TermBasedFieldType} goes through here, so slices are one
     * line in each rather than a branch to remember. {@code SliceTermsVerifier} fails any shape that forgets.
     */
    public static Query shape(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<Query> onPlainIndex,
        Supplier<Automaton> accepts,
        @Nullable MultiTermQuery.RewriteMethod method
    ) {
        return shape(field, context, onPlainIndex, null, accepts, method);
    }

    /**
     * As above, for a shape that has a native form once it knows which slice it is asking about: {@code onSlice} is given
     * the slice and returns the shape's own query over the prefixed term, keeping whatever that query type does better
     * than a bare automaton. {@link org.apache.lucene.search.FuzzyQuery} is the case in point, where pinning the slice
     * with {@code prefixLength} keeps its Levenshtein enumeration and its scoring.
     *
     * <p>Searching every slice still goes through the automaton, since no single term can say "any slice".
     */
    public static Query shape(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<Query> onPlainIndex,
        @Nullable Function<String, Query> onSlice,
        Supplier<Automaton> accepts,
        @Nullable MultiTermQuery.RewriteMethod method
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> onPlainIndex.get();
            case SLICES -> {
                if (onSlice == null) {
                    yield automatonQuery(field, accepts.get(), method, context);
                }
                final String[] slices = slices(context);
                if (slices.length == 1) {
                    yield onSlice.apply(slices[0]);
                }
                final BooleanQuery.Builder any = new BooleanQuery.Builder();
                for (String slice : slices) {
                    any.add(onSlice.apply(slice), BooleanClause.Occur.SHOULD);
                }
                yield any.build();
            }
            case ALL_SLICES -> automatonQuery(field, accepts.get(), method, context);
        };
    }

    /**
     * Whether the search reaches past a single slice, which a query shape built around one term cannot express. Those
     * shapes hand their automaton to {@link #automatonQuery} instead.
     */
    public static boolean spansSlices(String field, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> false;
            case SLICES -> slices(context).length > 1;
            case ALL_SLICES -> true;
        };
    }

    /**
     * The one term to search for, carrying the slice when the index prefixes its terms. Only valid when the search does
     * not span slices; {@link #spansSlices} says when it does.
     */
    public static BytesRef searchTerm(String field, BytesRef value, @Nullable SearchExecutionContext context) {
        final List<BytesRef> terms = searchTerms(field, value, context);
        assert terms.size() == 1 : "a search spanning slices has no single term; check spansSlices first";
        return terms.get(0);
    }

    /** The exclusive end of a slice's range in the dictionary, for a range left open at the top. */
    public static BytesRef sliceEnd(String slice) {
        final byte[] end = SliceIndexing.termPrefixBytes(slice);
        end[end.length - 1]++;
        return new BytesRef(end);
    }

    /** The form {@code value} takes in the dictionary of {@code slice}. */
    public static BytesRef term(String slice, BytesRef value) {
        return SliceIndexing.prefixTerm(slice, value);
    }

    /** A query for one term, asking each targeted slice for its own copy. */
    public static Query termQuery(String field, BytesRef value, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> new TermQuery(new Term(field, value));
            case SLICES -> {
                final String[] slices = slices(context);
                yield slices.length == 1
                    ? new TermQuery(new Term(field, term(slices[0], value)))
                    : termsQuery(field, List.of(value), context);
            }
            case ALL_SLICES -> anySlice(field, Automata.makeBinary(value));
        };
    }

    /** A query for several terms, asking each targeted slice for its own copy of each. */
    public static Query termsQuery(String field, Collection<BytesRef> values, @Nullable SearchExecutionContext context) {
        return switch (scope(field, context)) {
            case PLAIN -> new TermInSetQuery(field, values);
            case SLICES -> {
                final List<BytesRef> terms = new ArrayList<>(values.size() * slices(context).length);
                for (String slice : slices(context)) {
                    for (BytesRef value : values) {
                        terms.add(term(slice, value));
                    }
                }
                yield new TermInSetQuery(field, terms);
            }
            case ALL_SLICES -> {
                final List<BytesRef> sorted = new ArrayList<>(values);
                sorted.sort(BytesRef::compareTo);
                yield anySlice(field, Automata.makeBinaryStringUnion(sorted));
            }
        };
    }

    /**
     * Confines an automaton built over plain terms to the slices a search targets. Every multi-term shape goes through
     * here when it cannot express itself as a single term.
     */
    public static Query automatonQuery(
        String field,
        Automaton plain,
        @Nullable MultiTermQuery.RewriteMethod method,
        @Nullable SearchExecutionContext context
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> automaton(field, plain, method);
            case SLICES -> {
                final String[] slices = slices(context);
                if (slices.length == 1) {
                    yield automaton(field, confineTo(slices[0], plain), method);
                }
                final BooleanQuery.Builder any = new BooleanQuery.Builder();
                for (String slice : slices) {
                    any.add(automaton(field, confineTo(slice, plain), method), BooleanClause.Occur.SHOULD);
                }
                yield any.build();
            }
            case ALL_SLICES -> automaton(field, confineToAny(plain), method);
        };
    }

    /**
     * The intervals counterpart of {@link #shape}: a shape says how it looks on a plain index and what it accepts, and
     * on a slice index it is confined to the slices the search targets. Every interval shape in a text field goes
     * through here.
     */
    public static IntervalsSource intervals(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<IntervalsSource> onPlainIndex,
        Supplier<Automaton> accepts,
        String label
    ) {
        return intervals(field, context, onPlainIndex, null, accepts, label);
    }

    /**
     * As above, for a shape with a native source once it knows the slice: {@code onSlice} returns the shape's own source
     * over the prefixed term, which for a plain term is a direct lookup rather than a term enumeration.
     */
    public static IntervalsSource intervals(
        String field,
        @Nullable SearchExecutionContext context,
        Supplier<IntervalsSource> onPlainIndex,
        @Nullable Function<String, IntervalsSource> onSlice,
        Supplier<Automaton> accepts,
        String label
    ) {
        return switch (scope(field, context)) {
            case PLAIN -> onPlainIndex.get();
            case SLICES -> {
                final String[] slices = slices(context);
                if (onSlice == null) {
                    if (slices.length == 1) {
                        yield confined(confineTo(slices[0], accepts.get()), label);
                    }
                    final List<IntervalsSource> sources = new ArrayList<>(slices.length);
                    for (String slice : slices) {
                        sources.add(confined(confineTo(slice, accepts.get()), label));
                    }
                    yield Intervals.or(sources);
                }
                if (slices.length == 1) {
                    yield onSlice.apply(slices[0]);
                }
                final List<IntervalsSource> sources = new ArrayList<>(slices.length);
                for (String slice : slices) {
                    sources.add(onSlice.apply(slice));
                }
                yield Intervals.or(sources);
            }
            case ALL_SLICES -> confined(confineToAny(accepts.get()), label);
        };
    }

    private static IntervalsSource confined(Automaton automaton, String label) {
        return Intervals.multiterm(compile(automaton), IndexSearcher.getMaxClauseCount(), label);
    }

    private static CompiledAutomaton compile(Automaton automaton) {
        return new CompiledAutomaton(automaton, false, false, true);
    }

    /** {@code slice|<plain>}. */
    private static Automaton confineTo(String slice, Automaton plain) {
        return determinize(Operations.concatenate(Automata.makeBinary(new BytesRef(SliceIndexing.termPrefixBytes(slice))), plain));
    }

    /** {@code <any slice>|<plain>}, where the slice part holds no separator. */
    /** Any slice's prefix followed by {@code plain}: {@code ANY^TERM_PREFIX_LENGTH plain}. */
    private static Automaton confineToAny(Automaton plain) {
        return determinize(Operations.concatenate(anySlicePrefix(), plain));
    }

    /** Exactly {@link SliceIndexing#TERM_PREFIX_LENGTH} arbitrary bytes, which is what any slice's prefix looks like. */
    public static Automaton anySlicePrefix() {
        final Automaton any = new Automaton();
        int from = any.createState();
        for (int i = 0; i < SliceIndexing.TERM_PREFIX_LENGTH; i++) {
            final int to = any.createState();
            any.addTransition(from, to, 0, 0xFF);
            from = to;
        }
        any.setAccept(from, true);
        any.finishState();
        return any;
    }

    private static Automaton determinize(Automaton automaton) {
        // Concatenation yields a non-deterministic automaton, and a query may not run one.
        return Operations.determinize(automaton, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    private static Query anySlice(String field, Automaton plain) {
        return automaton(field, confineToAny(plain), null);
    }

    private static AutomatonQuery automaton(String field, Automaton automaton, @Nullable MultiTermQuery.RewriteMethod method) {
        return method == null
            ? new AutomatonQuery(new Term(field), automaton, true)
            : new AutomatonQuery(new Term(field), automaton, true, method);
    }
}
