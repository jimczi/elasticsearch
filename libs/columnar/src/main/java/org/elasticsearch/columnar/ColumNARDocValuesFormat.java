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
    /**
     * The bytes a chunk of a byte stream holds before it is closed and compressed. A chunk is what the
     * compressor sees at once, so a column whose vocabulary is larger than one repeats its terms across
     * chunks with the compressor learning them afresh in each.
     */
    public static final int DEFAULT_TARGET_CHUNK_BYTES = 64 * 1024;

    /** SPI constructor. Uses the default pipeline for every field. */
    public ColumNARDocValuesFormat() {
        this((fieldName, type) -> NumericPipeline::defaultPipeline, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Constructs a format with a custom pipeline selector and block size.
     * {@code blockSize} must be a power of 2 in [{@value #MIN_BLOCK_SIZE}, {@value #MAX_BLOCK_SIZE}].
     */
    public ColumNARDocValuesFormat(final NumericPipelineSelector pipelineSelector, int blockSize) {
        this(pipelineSelector, blockSize, DEFAULT_TARGET_CHUNK_BYTES);
    }

    /** Constructs a format whose byte streams are cut into chunks of about {@code targetChunkBytes}. */
    public ColumNARDocValuesFormat(final NumericPipelineSelector pipelineSelector, int blockSize, int targetChunkBytes) {
        super(ColumnarFormat.NAME);
        if (blockSize < MIN_BLOCK_SIZE || blockSize > MAX_BLOCK_SIZE || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException(
                "blockSize must be a power of 2 in [" + MIN_BLOCK_SIZE + ", " + MAX_BLOCK_SIZE + "], got: " + blockSize
            );
        }
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
        this.targetChunkBytes = targetChunkBytes;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(state, pipelineSelector, blockSize, targetChunkBytes);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }
}
