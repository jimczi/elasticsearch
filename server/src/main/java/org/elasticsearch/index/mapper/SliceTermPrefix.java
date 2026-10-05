/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.SliceIndexing;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

/**
 * Rewrites the Lucene fields of a document so that every term they index starts with the slice the document belongs to.
 * A slice's terms then sort together in the terms dictionary, which makes its postings contiguous on disk: reading a block
 * of postings brings back terms of that slice rather than a slice of every term.
 *
 * <p>This sits on {@link LuceneDocument#add}, the one point every mapper goes through, so a field type that indexes terms
 * is covered without knowing about slices. The rewrite is driven by the Lucene field alone: a field indexes terms when its
 * {@link IndexableFieldType#indexOptions()} is set, and {@link SliceIndexing#prefixesTerms} decides whether its name takes
 * part.
 *
 * <p>The prefix reaches the terms dictionary only. Doc values, points and stored values keep the plain value, which matters
 * because a single Lucene field often feeds several of them from the same accessor: such a field is split into an
 * index-only view carrying the prefix and a plain view carrying everything else.
 */
public final class SliceTermPrefix {

    private final String slice;
    private final byte[] bytePrefix;
    private final char[] charPrefix;

    public SliceTermPrefix(String slice) {
        this.slice = slice;
        this.bytePrefix = SliceIndexing.termPrefix(slice);
        this.charPrefix = (slice + (char) SliceIndexing.SLICE_TERM_SEPARATOR).toCharArray();
        // Slice values are restricted to ASCII, so prefixing a token stream character by character and prefixing a BytesRef
        // byte by byte produce the same term.
        assert charPrefix.length == bytePrefix.length : slice;
    }

    public String slice() {
        return slice;
    }

    /**
     * Adds {@code field} to {@code out}, prefixed when it indexes terms. A field that feeds doc values, points or stored
     * values from the same value as its terms contributes two entries.
     */
    void add(List<IndexableField> out, IndexableField field) {
        final IndexableFieldType type = field.fieldType();
        if (type.indexOptions() == IndexOptions.NONE || SliceIndexing.prefixesTerms(field.name()) == false) {
            out.add(field);
            return;
        }
        switch (field.invertableType()) {
            case TOKEN_STREAM ->
                // The prefix goes on the token stream, so the value the field also stores stays plain and no split is needed.
                out.add(new PrefixedTokenStreamField(field));
            case BINARY -> {
                final boolean split = type.stored() || type.docValuesType() != DocValuesType.NONE || type.pointDimensionCount() > 0;
                out.add(new PrefixedBinaryField(field, split ? new IndexOnlyType(type) : type));
                if (split) {
                    out.add(new PlainValueField(field));
                }
            }
        }
    }

    /**
     * The terms of a field whose value Lucene reads as bytes, prefixed. Reads through to the wrapped field so that a mapper
     * holding on to it, as the accumulating doc-values fields do, still decides the value.
     */
    private final class PrefixedBinaryField implements IndexableField {

        private final IndexableField delegate;
        private final IndexableFieldType type;

        PrefixedBinaryField(IndexableField delegate, IndexableFieldType type) {
            this.delegate = delegate;
            this.type = type;
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public IndexableFieldType fieldType() {
            return type;
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }

        @Override
        public BytesRef binaryValue() {
            return SliceIndexing.prefixTerm(bytePrefix, delegate.binaryValue());
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            return null;
        }

        @Override
        public String stringValue() {
            return null;
        }

        @Override
        public Reader readerValue() {
            return null;
        }

        @Override
        public Number numericValue() {
            return null;
        }

        @Override
        public StoredValue storedValue() {
            return null;
        }
    }

    /**
     * The terms of a field Lucene inverts through an analyzer, prefixed. Everything else is the wrapped field unchanged,
     * including the value it stores.
     */
    private final class PrefixedTokenStreamField implements IndexableField {

        private final IndexableField delegate;

        PrefixedTokenStreamField(IndexableField delegate) {
            this.delegate = delegate;
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public IndexableFieldType fieldType() {
            return delegate.fieldType();
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.TOKEN_STREAM;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            if (reuse instanceof PrefixingTokenFilter filter) {
                final TokenStream inner = delegate.tokenStream(analyzer, filter.input());
                if (inner == filter.input()) {
                    // Lucene reuses one token stream per field across the documents of a buffer, and consecutive
                    // documents belong to different slices, so the prefix has to be reset rather than inherited.
                    filter.setPrefix(charPrefix);
                    return filter;
                }
                return new PrefixingTokenFilter(inner, charPrefix);
            }
            return new PrefixingTokenFilter(delegate.tokenStream(analyzer, reuse), charPrefix);
        }

        @Override
        public BytesRef binaryValue() {
            return delegate.binaryValue();
        }

        @Override
        public String stringValue() {
            return delegate.stringValue();
        }

        @Override
        public Reader readerValue() {
            return delegate.readerValue();
        }

        @Override
        public Number numericValue() {
            return delegate.numericValue();
        }

        @Override
        public StoredValue storedValue() {
            return delegate.storedValue();
        }
    }

    /**
     * The doc values, points and stored value of a field, with its terms removed. Pairs with {@link PrefixedBinaryField},
     * which carries the terms.
     */
    private static final class PlainValueField implements IndexableField {

        private final IndexableField delegate;
        private final IndexableFieldType type;

        PlainValueField(IndexableField delegate) {
            this.delegate = delegate;
            this.type = new NotIndexedType(delegate.fieldType());
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public IndexableFieldType fieldType() {
            return type;
        }

        @Override
        public InvertableType invertableType() {
            return delegate.invertableType();
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
            return null;
        }

        @Override
        public BytesRef binaryValue() {
            return delegate.binaryValue();
        }

        @Override
        public String stringValue() {
            return delegate.stringValue();
        }

        @Override
        public Reader readerValue() {
            return delegate.readerValue();
        }

        @Override
        public Number numericValue() {
            return delegate.numericValue();
        }

        @Override
        public StoredValue storedValue() {
            return delegate.storedValue();
        }
    }

    /**
     * Prepends the slice to every token. Slice values are ASCII, so the characters this writes are the bytes
     * {@link SliceIndexing#prefixTerm} would have written.
     */
    static final class PrefixingTokenFilter extends TokenFilter {

        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private char[] prefix;

        PrefixingTokenFilter(TokenStream input, char[] prefix) {
            super(input);
            this.prefix = prefix;
        }

        TokenStream input() {
            return input;
        }

        void setPrefix(char[] prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean incrementToken() throws IOException {
            if (input.incrementToken() == false) {
                return false;
            }
            final int length = termAttribute.length();
            final char[] buffer = termAttribute.resizeBuffer(prefix.length + length);
            System.arraycopy(buffer, 0, buffer, prefix.length, length);
            System.arraycopy(prefix, 0, buffer, 0, prefix.length);
            termAttribute.setLength(prefix.length + length);
            return true;
        }
    }

    /**
     * Everything an {@link IndexableFieldType} reports, taken from another one. The two views below override only what
     * they change, so each reads as a short list of differences.
     */
    private abstract static class DelegatingType implements IndexableFieldType {

        final IndexableFieldType in;

        DelegatingType(IndexableFieldType in) {
            this.in = in;
        }

        @Override
        public boolean stored() {
            return in.stored();
        }

        @Override
        public boolean tokenized() {
            return in.tokenized();
        }

        @Override
        public boolean storeTermVectors() {
            return in.storeTermVectors();
        }

        @Override
        public boolean storeTermVectorOffsets() {
            return in.storeTermVectorOffsets();
        }

        @Override
        public boolean storeTermVectorPositions() {
            return in.storeTermVectorPositions();
        }

        @Override
        public boolean storeTermVectorPayloads() {
            return in.storeTermVectorPayloads();
        }

        @Override
        public boolean omitNorms() {
            return in.omitNorms();
        }

        @Override
        public IndexOptions indexOptions() {
            return in.indexOptions();
        }

        @Override
        public DocValuesType docValuesType() {
            return in.docValuesType();
        }

        @Override
        public DocValuesSkipIndexType docValuesSkipIndexType() {
            return in.docValuesSkipIndexType();
        }

        @Override
        public int pointDimensionCount() {
            return in.pointDimensionCount();
        }

        @Override
        public int pointIndexDimensionCount() {
            return in.pointIndexDimensionCount();
        }

        @Override
        public int pointNumBytes() {
            return in.pointNumBytes();
        }

        @Override
        public int vectorDimension() {
            return in.vectorDimension();
        }

        @Override
        public VectorEncoding vectorEncoding() {
            return in.vectorEncoding();
        }

        @Override
        public VectorSimilarityFunction vectorSimilarityFunction() {
            return in.vectorSimilarityFunction();
        }

        @Override
        public Map<String, String> getAttributes() {
            return in.getAttributes();
        }
    }

    /** A field type reduced to what it contributes to the terms dictionary. */
    private static final class IndexOnlyType extends DelegatingType {

        IndexOnlyType(IndexableFieldType in) {
            super(in);
        }

        @Override
        public boolean stored() {
            return false;
        }

        @Override
        public DocValuesType docValuesType() {
            return DocValuesType.NONE;
        }

        @Override
        public DocValuesSkipIndexType docValuesSkipIndexType() {
            return DocValuesSkipIndexType.NONE;
        }

        @Override
        public int pointDimensionCount() {
            return 0;
        }

        @Override
        public int pointIndexDimensionCount() {
            return 0;
        }

        @Override
        public int pointNumBytes() {
            return 0;
        }
    }

    /** A field type with its contribution to the terms dictionary removed. */
    private static final class NotIndexedType extends DelegatingType {

        NotIndexedType(IndexableFieldType in) {
            super(in);
        }

        @Override
        public boolean tokenized() {
            return false;
        }

        @Override
        public boolean storeTermVectors() {
            return false;
        }

        @Override
        public boolean storeTermVectorOffsets() {
            return false;
        }

        @Override
        public boolean storeTermVectorPositions() {
            return false;
        }

        @Override
        public boolean storeTermVectorPayloads() {
            return false;
        }

        @Override
        public boolean omitNorms() {
            return true;
        }

        @Override
        public IndexOptions indexOptions() {
            return IndexOptions.NONE;
        }
    }
}
