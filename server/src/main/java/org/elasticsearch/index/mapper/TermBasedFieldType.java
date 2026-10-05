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
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.UTF32ToUTF8;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Base {@link MappedFieldType} implementation for a field that is indexed
 *  with the inverted index. */
public abstract class TermBasedFieldType extends SimpleMappedFieldType {

    protected final TextSearchInfo textSearchInfo;

    public TermBasedFieldType(String name, IndexType indexType, boolean isStored, TextSearchInfo textSearchInfo, Map<String, String> meta) {
        super(name, indexType, isStored, meta);
        this.textSearchInfo = textSearchInfo;
    }

    @Override
    public TextSearchInfo getTextSearchInfo() {
        return this.textSearchInfo;
    }

    /** Returns the indexed value used to construct search "values".
     *  This method is used for the default implementations of most
     *  query factory methods such as {@link #termQuery}. */
    protected BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    /**
     * The term to look up for {@code value}, carrying the slice when the index this search targets prefixes its terms.
     * Query shapes that build a term go through here, which is what keeps the prefix off the query builders.
     */
    protected final BytesRef slicedTerm(Object value, SearchExecutionContext context) {
        return SliceTermQueries.searchTerm(name(), indexedValueForSearch(value), context);
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, SearchExecutionContext context) {
        failIfNotIndexed();
        // The slice is matched exactly: folding its case could reach a slice whose name differs only in case.
        final BytesRef valueForSearch = indexedValueForSearch(value);
        if (SliceTermQueries.scope(name(), context) != SliceTermQueries.Scope.PLAIN) {
            // Lucene's case folding works on code points, so it is converted to bytes before the slice, which is
            // matched byte for byte, is put in front of it.
            return SliceTermQueries.automatonQuery(
                name(),
                new UTF32ToUTF8().convert(Automata.makeCaseInsensitiveString(valueForSearch.utf8ToString())),
                null,
                context
            );
        }
        // check if valueForSearch is the same as an empty string
        // if we have a length of zero, just do a regular term query
        if (valueForSearch.length == 0) {
            return termQuery(value, context);
        }
        return AutomatonQueries.caseInsensitiveTermQuery(new Term(name(), valueForSearch));
    }

    @Override
    public boolean mayExistInIndex(SearchExecutionContext context) {
        return context.fieldExistsInIndex(name());
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        failIfNotIndexed();
        return SliceTermQueries.termQuery(name(), indexedValueForSearch(value), context);
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        failIfNotIndexed();
        List<BytesRef> bytesRefs = values.stream().map(this::indexedValueForSearch).toList();
        return SliceTermQueries.termsQuery(name(), bytesRefs, context);
    }

}
