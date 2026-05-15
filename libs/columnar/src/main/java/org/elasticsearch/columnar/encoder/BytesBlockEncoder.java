/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.NamedSPILoader;
import org.elasticsearch.columnar.BytesRefValuesSupplier;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;

import java.io.IOException;

/**
 * Extension-point interface for turning a block of <strong>variable-length byte
 * sequences</strong> (one per doc) into a byte sequence (the block's "payload"), and back.
 * This is the bytes-typed face of the format's binary substrate — every binary field
 * (keyword / ip / binary today; building block for sorted / sorted-set later) eventually
 * flows through a {@code BytesBlockEncoder} implementation to produce the bytes that
 * {@link ColumNARDocValuesFormat} stores.
 *
 * <p>Sibling interface to {@link NumericBlockEncoder} (long-typed); neither extends the
 * other — encoders stay monomorphic on the hot path. <b>No numeric-typed methods leak into
 * this interface</b>: implementations work strictly on byte sequences via the flat-buffer
 * shape below.
 *
 * <p><b>Flat-buffer block layout in memory.</b> The encoder consumes (and produces, on
 * decode) a pair of caller-owned scratches:
 * <ul>
 *   <li>{@code valueBytes}: a flat {@code byte[]} holding the concatenated value bytes —
 *       value {@code i} occupies
 *       {@code valueBytes[valueOffsets[i] .. valueOffsets[i+1])}.</li>
 *   <li>{@code valueOffsets}: an {@code int[]} of length {@code valuesLen + 1} where entry
 *       {@code valuesLen} marks the end of the last value in {@code valueBytes}.</li>
 * </ul>
 * This shape avoids per-value heap allocations on the hot path; the consumer fills it by
 * appending each {@code BytesRef} into the flat buffer as it iterates Lucene's
 * {@code BinaryDocValues} source.
 *
 * <p><b>Specialise on the segment, not on a sample.</b> Before encoding, the consumer hands
 * the encoder a {@link BytesRefValuesSupplier} from which it can open as many fresh
 * iterators as it likes to inspect the column's values — typically to count distinct values
 * (for a dictionary path), measure shared-prefix length, or sample length distribution.
 * Encoders should not buffer values on heap; the supplier provides off-heap, replayable
 * access.
 *
 * <p><b>Identity and BWC contract.</b> Each implementation declares a stable, unique
 * {@link #getName()} persisted in per-field metadata; once shipped, the encoder's wire format is
 * frozen forever, and new behavior arrives under a fresh id. The id space is independent
 * from {@link NumericBlockEncoder} ids; metadata records which type-specific registry to
 * consult based on the field type byte.
 *
 * <p>Reserved built-in ids:
 * <ul>
 *   <li>{@code 0} — {@link RawBytesBlockEncoder}, {@code [vint len][bytes]} per value,
 *       baseline.</li>
 * </ul>
 */
public interface BytesBlockEncoder extends NamedSPILoader.NamedSPI {

    /**
     * Stable identifier persisted in metadata to look up this encoder on read. See the
     * interface Javadoc for the immutability contract and the list of reserved built-in ids.
     */
    // String getName() inherited from NamedSPILoader.NamedSPI.
    /**
     * Optionally return a different (more specific) encoder picked once per segment based on
     * iterating the column's values. Default returns {@code this}; "auto" encoders override
     * to choose between prefix-compressed, block-dictionary, etc. based on observed
     * cardinality / shared prefix length / size distribution.
     *
     * <p>The {@link BytesRefValuesSupplier} can be
     * {@linkplain BytesRefValuesSupplier#open() opened} multiple times for multi-pass
     * analysis (e.g. a first pass to count distinct values, a second pass to build the
     * dictionary).
     *
     * <p><b>Invariant.</b> The returned encoder is recorded once in per-field metadata and
     * applied to every block of the field.
     */
    default BytesBlockEncoder specializeForSegment(BytesRefValuesSupplier values) throws IOException {
        return this;
    }

    /**
     * Upper bound on the number of bytes {@link #encode} will write for a block of
     * {@code valuesLen} values whose total payload byte count is {@code totalValueBytes}.
     */
    int maxEncodedSize(int valuesLen, int totalValueBytes);

    /**
     * Encode {@code valuesLen} values whose bytes live in
     * {@code valueBytes[valueOffsets[0] .. valueOffsets[valuesLen])} into {@code dest}
     * starting at {@code destOffset}. Returns the number of bytes written.
     *
     * <p>Declared {@code throws IOException} because typical implementations write via
     * Lucene's variable-int helpers, which propagate IOException from
     * {@link org.apache.lucene.store.DataOutput}; concrete in-memory implementations (e.g.
     * {@link RawBytesBlockEncoder}) won't actually throw.
     */
    int encode(byte[] valueBytes, int[] valueOffsets, int valuesLen, byte[] dest, int destOffset) throws IOException;

    /**
     * Decode a previously encoded block by reading sequentially from {@code in}, then fill
     * the caller-provided flat layout:
     * <ul>
     *   <li>{@code valueBytes[valueBytesOffset .. valueBytesOffset + totalValueBytes)}
     *       receives the concatenated value bytes. The caller sized this buffer using
     *       {@code totalValueBytes} read from the per-block metadata.</li>
     *   <li>{@code valueOffsets[valueOffsetsOffset + i]} is set to the start of value
     *       {@code i} in {@code valueBytes}, for {@code 0 <= i <= valuesLen} — entry
     *       {@code valuesLen} is the past-the-end marker.</li>
     * </ul>
     *
     * @param formatVersion the {@link ColumNARDocValuesFormat#VERSION_CURRENT}-style value
     *                      the segment was written with. Encoders that never changed their
     *                      wire format can ignore it; encoders that opted into the same-id
     *                      evolution path MUST branch on this value.
     */
    void decode(
        int formatVersion,
        DataInput in,
        byte[] valueBytes,
        int valueBytesOffset,
        int[] valueOffsets,
        int valueOffsetsOffset,
        int valuesLen
    ) throws IOException;
}
