/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.string.DictionaryPolicy;

import java.io.IOException;

/**
 * A binary Lucene {@link DocValuesFormat}: every field is a {@code BinaryDocValues} column tagged with a
 * {@link ColumnarFieldType} ({@link #TYPE_ATTRIBUTE}), served through this library's own range-query and
 * block-loader APIs. The typed doc-values shapes are rejected.
 *
 * <p>Pipeline selection is delegated to the injected {@link NumericPipelineSelector}. Callers that
 * need per-field encoding (e.g. ALP for doubles, SplitDelta for counters) supply a concrete
 * implementation via the two-arg constructor. The no-arg SPI constructor uses the default pipeline for
 * every field.
 */
public class ColumNARDocValuesFormat extends DocValuesFormat {

    /** {@link org.apache.lucene.index.FieldInfo} attribute naming a field's {@link ColumnarFieldType}. The mapper sets it. */
    public static final String TYPE_ATTRIBUTE = "columnar.type";

    /** Smallest allowed block size. Must be a power of 2. */
    public static final int MIN_BLOCK_SIZE = 128;

    /** Largest allowed block size. Caps O(blockSize) per-field allocations in the encoder. */
    public static final int MAX_BLOCK_SIZE = 8192;

    /** Default block size used when none is specified. */
    public static final int DEFAULT_BLOCK_SIZE = MIN_BLOCK_SIZE;

    static final String DATA_CODEC = "ColumNARData";
    static final String DATA_EXTENSION = "cnd";
    static final String META_CODEC = "ColumNARMeta";
    static final String META_EXTENSION = "cnm";

    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;
    private final int targetChunkBytes;
    private final DictionaryPolicy dictionaryPolicy;

    /** SPI constructor. Uses the default pipeline for every field. */
    public ColumNARDocValuesFormat() {
        this((fieldName, type) -> NumericPipeline::defaultPipeline, DEFAULT_BLOCK_SIZE, DEFAULT_DICTIONARY_POLICY);
    }

    /**
     * The dictionary bounds a string column is written under when none is given. A dictionary is kept only
     * when it accounts for most of the column's values and stays small against the bytes it stands in for.
     *
     * <p>The byte bound is what decides whether a column with a few thousand distinct values can hold all of
     * them: a column of host names needs a quarter of a megabyte for its vocabulary, and one that cannot
     * hold it falls back to storing every value. Beyond half a megabyte the bound stops admitting whole
     * vocabularies and starts admitting the tails of large ones, where terms seen once add almost nothing to
     * what the dictionary covers and widen the ordinal every value pays for.
     */
    public static final DictionaryPolicy DEFAULT_DICTIONARY_POLICY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /**
     * The bytes a chunk of a byte stream holds before it is closed and compressed. A chunk is what the
     * compressor sees at once, so a column whose vocabulary is larger than one repeats its terms across
     * chunks with the compressor learning them afresh in each.
     */
    public static final int DEFAULT_TARGET_CHUNK_BYTES = 64 * 1024;

    /**
     * Constructs a format with a custom pipeline selector and block size.
     * {@code blockSize} must be a power of 2 in [{@value #MIN_BLOCK_SIZE}, {@value #MAX_BLOCK_SIZE}].
     */
    public ColumNARDocValuesFormat(final NumericPipelineSelector pipelineSelector, int blockSize) {
        this(pipelineSelector, blockSize, DEFAULT_DICTIONARY_POLICY);
    }

    /** Constructs a format whose string columns are written under {@code dictionaryPolicy}. */
    public ColumNARDocValuesFormat(final NumericPipelineSelector pipelineSelector, int blockSize, DictionaryPolicy dictionaryPolicy) {
        this(pipelineSelector, blockSize, DEFAULT_TARGET_CHUNK_BYTES, dictionaryPolicy);
    }

    /** Constructs a format whose byte streams are cut into chunks of about {@code targetChunkBytes}. */
    public ColumNARDocValuesFormat(
        final NumericPipelineSelector pipelineSelector,
        int blockSize,
        int targetChunkBytes,
        DictionaryPolicy dictionaryPolicy
    ) {
        super(ColumnarFormat.NAME);
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException(
                "blockSize must be a power of 2 in [" + MIN_BLOCK_SIZE + ", " + MAX_BLOCK_SIZE + "], got: " + blockSize
            );
        }
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
        this.targetChunkBytes = targetChunkBytes;
        this.dictionaryPolicy = dictionaryPolicy;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(state, pipelineSelector, blockSize, targetChunkBytes, dictionaryPolicy);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }
}
