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
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.LongValuesSupplier;
import org.elasticsearch.columnar.NumericType;

import java.io.IOException;

/**
 * Extension-point interface for turning a block of <strong>long values</strong> into a byte
 * sequence (the block's "payload"), and back. This is the long-typed face of the format's
 * binary substrate — every numeric field (long, int, float, double — see {@link NumericType})
 * eventually flows through a {@code NumericBlockEncoder} implementation to produce the bytes
 * that {@link ColumNARDocValuesFormat} stores. Sibling to {@link BytesBlockEncoder} (the
 * bytes-typed face); neither extends the other so encoders and callers stay monomorphic on
 * the hot path.
 *
 * <p>{@link ColumNARDocValuesFormat} writes one block of {@code blockSize} long values at a
 * time by calling {@link #encode} — the encoder's output bytes are the "payload". The
 * payload is then passed through a {@link BlockEncoding} (which may be identity, compress,
 * encrypt, etc.). On read the outer encoding is reversed first (see
 * {@link BlockEncoding#decode}) and the resulting {@link DataInput} positioned at the
 * payload bytes is handed to {@link #decode}, which streams values into a caller-provided
 * {@code long[]}. Reading is sequential and zero-copy when the outer encoding is identity
 * over an {@code IndexInput}.
 *
 * <p><b>Numeric type.</b> Each implementation declares the {@link NumericType} it is
 * designed for. Generic encoders (bit-pack, delta + bit-pack, …) that treat the value as a
 * raw 64-bit bit pattern declare {@link NumericType#LONG} — the universal type. Specialised
 * encoders (e.g. a future double-histogram encoder) may declare a narrower type to make the
 * intent explicit and so the format can validate the field's persisted numeric type matches.
 *
 * <p><b>Specialise on the segment, not on a sample.</b> Before encoding any block the
 * consumer hands the encoder a {@link LongValuesSupplier} from which it can open as many
 * fresh iterators as it likes to inspect the segment's full value sequence. Encoders return
 * the most specific concrete encoder they can prove correct from the iteration — typically
 * by computing range, GCD, monotonicity, or cardinality. The iterator-supplier shape lets
 * the encoder process the values off-heap and re-walk them across passes; no representative
 * sample is loaded into a {@code long[]}.
 *
 * <p><b>Off-heap and caller-provided buffers.</b> Decode never allocates on the hot path —
 * callers provide both the output {@code long[]} and any per-encoder scratch buffer (see
 * {@link #scratchLongs}). When {@link IdentityBlockEncoding} is in use the {@code DataInput}
 * handed to {@link #decode} is the underlying {@code IndexInput} itself, so reads pull
 * straight from mmap'd memory without an intermediate heap copy.
 *
 * <p><b>Identity and BWC contract.</b> Each implementation declares a stable, unique
 * {@link #getName()} that is persisted in the per-field metadata. Two evolution paths exist:
 * <ol>
 *   <li><b>Preferred — new encoder under a new id.</b> Significant changes (new compression
 *       primitive, different framing) ship as a fresh class with a fresh id. Old ids stay
 *       readable verbatim; old segments load via the old class, new segments via the new
 *       one.</li>
 *   <li><b>Same id, branch on format version.</b> Small non-additive tweaks MAY reuse the
 *       same id if the implementation accepts the {@code formatVersion} hand-off into
 *       {@link #decode} and routes to the right code path. In that case the global
 *       {@link ColumNARDocValuesFormat#VERSION_CURRENT} is incremented.</li>
 * </ol>
 * Either way: bytes published in a release stay decodable forever.
 *
 * <p><b>Extensibility.</b> Downstream modules add additional encoders by implementing this
 * interface and registering the implementation via
 * {@code META-INF/services/org.elasticsearch.columnar.NumericBlockEncoder}.
 * {@link NumericBlockEncoderRegistry} discovers them at class-init time and resolves ids on
 * read.
 *
 * <p>Reserved built-in ids:
 * <ul>
 *   <li>{@code 0} — {@link RawBlockEncoder}, 8 bytes per long, baseline for measurement.</li>
 *   <li>{@code 1} — {@link BitPackBlockEncoder}, min-delta + bit-pack, production default.</li>
 *   <li>{@code 2} — {@link DeltaPackedBlockEncoder}, delta + zigzag + bit-pack (monotonic).</li>
 *   <li>{@code 3} — {@link GcdDeltaPackedBlockEncoder}, GCD divide + delta + bit-pack
 *       (monotonic with non-trivial GCD).</li>
 *   <li>{@code 4} — {@link GcdBitPackBlockEncoder}, GCD divide + min-subtract + bit-pack
 *       (non-monotonic with non-trivial GCD).</li>
 * </ul>
 */
public interface NumericBlockEncoder extends NamedSPILoader.NamedSPI {

    /**
     * Stable identifier persisted in metadata to look up this encoder on read. See the
     * interface Javadoc for the immutability contract and the list of reserved built-in ids.
     */
    // String getName() inherited from NamedSPILoader.NamedSPI.
    /**
     * Semantic numeric type the encoder is designed for. Generic encoders that work on raw
     * 64-bit bit patterns return {@link NumericType#LONG}; specialised encoders may declare a
     * narrower type. The format records this in per-field metadata so the producer can
     * validate that a column written as one numeric type is not read back as another.
     *
     * <p>Default returns {@link NumericType#LONG} — fits every generic encoder shipped today.
     */
    default NumericType numericType() {
        return NumericType.LONG;
    }

    /**
     * Optionally return a different (more specific) encoder picked once per segment based on
     * iterating the column's values. The default implementation returns {@code this}.
     *
     * <p>Concrete "auto" encoders override this to inspect the value sequence (range, GCD,
     * cardinality, monotonicity, …) and hand back the best concrete encoder for the segment.
     * The {@link LongValuesSupplier} can be {@linkplain LongValuesSupplier#open() opened}
     * multiple times to re-walk the values for multi-pass analysis — encoders should not
     * buffer the values on heap.
     *
     * <p><b>Invariant.</b> The returned encoder is recorded once in per-field metadata and
     * used for every block in the field; the format does not permit per-block encoder
     * changes. This keeps the reader simple (one encoder lookup per field) and makes the
     * on-disk format self-describing without per-block discriminators.
     */
    default NumericBlockEncoder specializeForSegment(LongValuesSupplier values) throws IOException {
        return this;
    }

    /**
     * Upper bound on the number of bytes {@link #encode} will write for a block of
     * {@code valuesLen} values. The consumer uses this to size its scratch buffer.
     */
    int maxEncodedSize(int valuesLen);

    /**
     * Number of {@code long} scratch slots {@link #decode} requires for a block of
     * {@code valuesLen} values. Callers allocate one buffer of (at least) this size per
     * {@code NumericDocValues} instance and reuse it across every block. Encoders that do
     * not need scratch return {@code 0}.
     */
    default int scratchLongs(int valuesLen) {
        return 0;
    }

    /**
     * Encode {@code values[valuesOffset, valuesOffset + valuesLen)} into {@code dest}
     * starting at {@code destOffset}. Returns the number of bytes written.
     */
    int encode(long[] values, int valuesOffset, int valuesLen, byte[] dest, int destOffset);

    /**
     * Decode a previously encoded block by reading sequentially from {@code in}. The encoder
     * reads exactly the number of bytes it wrote during {@link #encode}, then writes the
     * decoded {@code valuesLen} long values into
     * {@code dest[destOffset, destOffset + valuesLen)}.
     *
     * <p>The {@code scratch} buffer is provided by the caller and must contain at least
     * {@link #scratchLongs} slots; encoders that don't need scratch may receive {@code null}.
     *
     * @param formatVersion the {@link ColumNARDocValuesFormat#VERSION_CURRENT}-style value
     *                      the segment was written with. Encoders that never changed their
     *                      wire format can ignore it. Encoders that opted into the same-id
     *                      evolution path (see the interface BWC contract) MUST branch on
     *                      this value to stay readable across published format versions.
     */
    void decode(int formatVersion, DataInput in, long[] dest, int destOffset, int valuesLen, long[] scratch) throws IOException;
}
