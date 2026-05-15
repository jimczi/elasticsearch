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
import org.elasticsearch.columnar.encoder.BlockEncoding;
import org.elasticsearch.columnar.encoder.BytesBlockEncoder;
import org.elasticsearch.columnar.encoder.Lz4BlockEncoding;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericMinMaxSkipIndex;
import org.elasticsearch.columnar.encoder.RawBytesBlockEncoder;
import org.elasticsearch.columnar.encoder.SkipIndex;
import org.elasticsearch.columnar.encoder.SkipIndexParams;

import java.io.IOException;
import java.util.Objects;

/**
 * Elasticsearch columnar doc-values format. Binary-only at the Lucene API surface;
 * typed views over the binary substrate live in
 * {@link org.elasticsearch.columnar.bridge}.
 *
 * <p><b>Per-field instance.</b> Each {@code ColumNARDocValuesFormat} instance is
 * conceptually a per-field configuration: one encoder choice, one outer encoding, one
 * skip index, one block-size target. Routing several fields with different
 * configurations is the job of Lucene's {@code PerFieldDocValuesFormat} above this layer
 * — this format doesn't carry per-field branching itself.
 *
 * <p><b>Self-describing bytes.</b> Every choice that affects the on-disk bytes — encoder
 * id, encoding id, skip-index id, target encoded bytes per block, max values per block —
 * is persisted in the field's metadata. The producer reconstructs everything from those
 * ids and ints alone; it never consults the writer's format instance. Encoder, encoding,
 * and skip-index ids are resolved through Lucene-style {@code NamedSPILoader}-like
 * registries discovered via {@link java.util.ServiceLoader}.
 *
 * <p><b>Block close conditions.</b> Two ints govern when a block closes:
 * {@code targetEncodedBytesPerBlock} (the consumer tracks a running compression ratio and
 * resizes the row budget block-by-block to land near this target) and
 * {@code maxValuesPerBlock} (a row-count safety net). Whichever fires first wins.
 *
 * <p><b>Versioning.</b> Files carry {@code CodecUtil.writeIndexHeader} with
 * {@link #VERSION_CURRENT}; the reader validates the {@code [VERSION_START,
 * VERSION_CURRENT]} range. Encoder, encoding, and skip-index ids carry the long-term
 * backwards-compatibility contract — once published, the bytes an id produces are frozen
 * forever. Non-additive framing changes ship as a fresh format class with a fresh SPI
 * name (Lucene's {@code Lucene90DocValuesFormat} → {@code Lucene104DocValuesFormat}
 * precedent).
 */
public class ColumNARDocValuesFormat extends DocValuesFormat {

    public static final String NAME = "ColumNARDocValuesFormat";
    public static final String DATA_EXTENSION = "cdv";
    public static final String META_EXTENSION = "cdvm";
    public static final String DATA_CODEC = "ColumnarDocValuesData";
    public static final String META_CODEC = "ColumnarDocValuesMetadata";
    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    static final byte FIELD_TYPE_BINARY = 1;
    /**
     * Binary field encoded as a per-segment dictionary plus per-block ordinal payloads.
     * The ordinals are encoded through the numeric pipeline (NumericBlockEncoder for longs +
     * BlockEncoding) so all the numeric primitives (BitPack, DeltaPack auto-pick, LZ4)
     * apply automatically. The dictionary is small (≤ 256 entries) and sits at the end of
     * the field's metadata, read once on open.
     */
    static final byte FIELD_TYPE_DICT_BINARY = 2;
    /**
     * Single-valued field whose payloads were written through the bridge's long packer
     * (every doc carries a {@code [byte 'L'][vint 1][LE long]} payload). The consumer
     * unpacks the longs, runs them through the numeric encoder + encoding pipeline, and
     * persists the choice. The producer re-packs the same {@code 'L'} shape on read so
     * the bridge sees identical bytes. Detected automatically — callers don't pick this
     * field type; the consumer does on the basis of the per-doc payload shape.
     */
    static final byte FIELD_TYPE_PACKED_LONG = 3;
    /**
     * Multi-valued sibling of {@link #FIELD_TYPE_PACKED_LONG}: at least one doc carries
     * count != 1. Same {@code 'L'}-shape payloads are unpacked into a count stream + a
     * flat value stream, each persisted through its own numeric encoder. The producer
     * walks the count stream to map doc id → value-stream offset and re-packs the
     * {@code 'L'} payload on read.
     */
    static final byte FIELD_TYPE_PACKED_LONGS_MV = 4;
    /** Per-segment dictionary cap. Picked so ordinals fit in 8 bits with bit-packing. */
    static final int DICT_BINARY_MAX_DICT_SIZE = 256;
    static final int META_FIELD_END_SENTINEL = -1;

    /** Production-default target encoded (compressed) bytes per block. */
    public static final int DEFAULT_TARGET_ENCODED_BYTES_PER_BLOCK = 1 << 20; // 1 MB
    /** Safety-net row cap per block. Whichever fires first closes the block. */
    public static final int DEFAULT_MAX_VALUES_PER_BLOCK = 1 << 16; // 65 536

    private final NumericBlockEncoder longEncoder;
    private final BytesBlockEncoder bytesRefEncoder;
    private final BlockEncoding encoding;
    private final SkipIndex numericSkipIndex;
    private final SkipIndexParams skipIndexParams;
    private final int targetEncodedBytesPerBlock;
    private final int maxValuesPerBlock;
    /**
     * When true, binary fields with ≤ {@link #DICT_BINARY_MAX_DICT_SIZE} distinct values
     * (estimated from a sample) write through the per-segment dictionary path: dictionary
     * at field level, ordinals encoded per block via the numeric encoder + encoding. When
     * false, the field uses the raw {@code [vint length][bytes]} path. The default
     * constructor sets this to {@code true} so low-cardinality keyword fields get the
     * compression win automatically.
     */
    private final boolean preferDictionaryForBinary;

    /**
     * Default constructor wired for SPI. Production defaults across the four seams:
     * {@link org.elasticsearch.columnar.numericpipeline.NumericPipelineEncoder} for numerics,
     * {@link RawBytesBlockEncoder} for binary, {@link Lz4BlockEncoding}
     * ({@link Lz4BlockEncoding.Mode#FAST}) as the outer encoding,
     * {@link NumericMinMaxSkipIndex} with {@link SkipIndexParams#DEFAULTS}, 1 MB target
     * encoded bytes per block, 65 536 row cap. Override via the parameterised constructor
     * — instances are intended to be one-per-field, with {@code PerFieldDocValuesFormat}
     * routing the right instance to each field.
     */
    public ColumNARDocValuesFormat() {
        this(
            org.elasticsearch.columnar.numericpipeline.NumericPipelineEncoder.INSTANCE,
            RawBytesBlockEncoder.INSTANCE,
            Lz4BlockEncoding.INSTANCE,
            NumericMinMaxSkipIndex.INSTANCE,
            SkipIndexParams.DEFAULTS,
            DEFAULT_TARGET_ENCODED_BYTES_PER_BLOCK,
            DEFAULT_MAX_VALUES_PER_BLOCK,
            true
        );
    }

    /**
     * Per-field constructor. Every argument is the choice that applies to the single
     * field this format instance is configured for; {@code PerFieldDocValuesFormat} owns
     * the multi-field routing layer above. All choices are persisted on disk as ids
     * (encoder / encoding / skip-index) or as int values (target / cap), so the producer
     * reconstructs everything without consulting the writer's format instance.
     */
    public ColumNARDocValuesFormat(
        NumericBlockEncoder longEncoder,
        BytesBlockEncoder bytesRefEncoder,
        BlockEncoding encoding,
        SkipIndex numericSkipIndex,
        SkipIndexParams skipIndexParams,
        int targetEncodedBytesPerBlock,
        int maxValuesPerBlock,
        boolean preferDictionaryForBinary
    ) {
        super(NAME);
        this.longEncoder = Objects.requireNonNull(longEncoder, "longEncoder");
        this.bytesRefEncoder = Objects.requireNonNull(bytesRefEncoder, "bytesRefEncoder");
        this.encoding = Objects.requireNonNull(encoding, "encoding");
        this.numericSkipIndex = Objects.requireNonNull(numericSkipIndex, "numericSkipIndex");
        if (numericSkipIndex.kind() != SkipIndex.Kind.NUMERIC) {
            throw new IllegalArgumentException(
                "numericSkipIndex must have kind=NUMERIC, got " + numericSkipIndex.kind() + " from " + numericSkipIndex.getClass().getName()
            );
        }
        this.skipIndexParams = Objects.requireNonNull(skipIndexParams, "skipIndexParams");
        if (targetEncodedBytesPerBlock <= 0) {
            throw new IllegalArgumentException("targetEncodedBytesPerBlock must be positive, got " + targetEncodedBytesPerBlock);
        }
        if (maxValuesPerBlock <= 0) {
            throw new IllegalArgumentException("maxValuesPerBlock must be positive, got " + maxValuesPerBlock);
        }
        this.targetEncodedBytesPerBlock = targetEncodedBytesPerBlock;
        this.maxValuesPerBlock = maxValuesPerBlock;
        this.preferDictionaryForBinary = preferDictionaryForBinary;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ColumNARDocValuesConsumer(
            state,
            longEncoder,
            bytesRefEncoder,
            encoding,
            numericSkipIndex,
            skipIndexParams,
            targetEncodedBytesPerBlock,
            maxValuesPerBlock,
            preferDictionaryForBinary
        );
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ColumNARDocValuesProducer(state);
    }
}
