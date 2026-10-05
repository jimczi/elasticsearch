/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Iterator;

/**
 * The stored term vectors of a slice index carry {@code slice|term}, because that is what was indexed. The term vectors
 * API and the highlighters built on it hand terms back to the user, so the slice is removed on the way out. A document
 * belongs to one slice, so all of its terms share a prefix and removing it keeps them in order.
 */
public final class SliceTermVectors {

    private SliceTermVectors() {}

    /** The same fields with the slice removed from the terms of every field that carries one. */
    public static Fields withoutSlice(Fields fields) {
        return fields == null ? null : new Fields() {
            @Override
            public Iterator<String> iterator() {
                return fields.iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                final Terms terms = fields.terms(field);
                if (terms == null || SliceIndexing.prefixesTerms(field) == false) {
                    return terms;
                }
                return new StrippedTerms(terms);
            }

            @Override
            public int size() {
                return fields.size();
            }
        };
    }

    private static final class StrippedTerms extends Terms {

        private final Terms in;

        StrippedTerms(Terms in) {
            this.in = in;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new StrippedTermsEnum(in.iterator());
        }

        @Override
        public long size() throws IOException {
            return in.size();
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return in.getSumTotalTermFreq();
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return in.getSumDocFreq();
        }

        @Override
        public int getDocCount() throws IOException {
            return in.getDocCount();
        }

        @Override
        public boolean hasFreqs() {
            return in.hasFreqs();
        }

        @Override
        public boolean hasOffsets() {
            return in.hasOffsets();
        }

        @Override
        public boolean hasPositions() {
            return in.hasPositions();
        }

        @Override
        public boolean hasPayloads() {
            return in.hasPayloads();
        }
    }

    private static final class StrippedTermsEnum extends BaseTermsEnum {

        private final TermsEnum in;

        StrippedTermsEnum(TermsEnum in) {
            this.in = in;
        }

        @Override
        public BytesRef next() throws IOException {
            final BytesRef term = in.next();
            return term == null ? null : SliceIndexing.stripTermPrefix(term);
        }

        @Override
        public BytesRef term() throws IOException {
            return SliceIndexing.stripTermPrefix(in.term());
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            // A document's vector holds one slice, so a lookup by the plain term has to go through the prefixed form,
            // which only the owning slice knows. Callers of a vector enumerate it instead.
            throw new UnsupportedOperationException("term vectors of a slice index cannot be sought by a plain term");
        }

        @Override
        public void seekExact(long ord) throws IOException {
            in.seekExact(ord);
        }

        @Override
        public long ord() throws IOException {
            return in.ord();
        }

        @Override
        public int docFreq() throws IOException {
            return in.docFreq();
        }

        @Override
        public long totalTermFreq() throws IOException {
            return in.totalTermFreq();
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            return in.postings(reuse, flags);
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            return in.impacts(flags);
        }

        @Override
        public TermState termState() throws IOException {
            return in.termState();
        }
    }
}
