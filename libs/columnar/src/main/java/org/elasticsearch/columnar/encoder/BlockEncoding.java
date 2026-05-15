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
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.NamedSPILoader;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;

import java.io.IOException;

/**
 * Outer encoding layer applied to the bytes that a {@link NumericBlockEncoder} produced for one block.
 * {@link ColumNARDocValuesFormat} runs this as the last step on write and the first step on read.
 *
 * <p>An implementation can be identity (no transformation — {@link IdentityBlockEncoding}), a
 * compression layer (a future LZ4 / Zstd implementation), or any other byte-level transform such
 * as encryption. The interface is intentionally neutral: compression is one feature, not the
 * contract — the no-op variant is just as legitimate as a compressing one.
 *
 * <p>On write the implementation reads from a caller-provided {@code byte[]} buffer and writes
 * the transformed payload to the {@link DataOutput}; the caller records the written length in
 * the per-field metadata. On read the implementation takes the file's {@link DataInput} and
 * returns a {@link DataInput} exposing the decoded bytes — the identity implementation returns
 * the input directly so reads stay off-heap when the underlying {@code IndexInput} is mmap'd.
 * Real transforms decode into a caller-provided scratch buffer and return a
 * {@code ByteArrayDataInput} wrapping it.
 *
 * <p><b>Identity and BWC contract.</b> Each implementation declares a stable, unique
 * {@link #getName()} that is persisted in the per-field metadata. Once an id is published in a
 * shipped Elasticsearch version, <strong>the encoding's wire format is frozen forever</strong>;
 * future changes ship as a new implementation under a new id. Old ids stay readable.
 *
 * <p><b>BlockEncoding is a layer, not a filter.</b> Encodings transform bytes without
 * participating in block-level filtering. Filter pushdown lives in {@link NumericBlockEncoder} (the
 * encoder may peek stats, or an outer wrapping encoder such as a future bloom filter may
 * short-circuit). Keeping concerns separated lets the byte-level transform be swapped without
 * touching filter semantics.
 *
 * <p><b>Wire format vs runtime configuration.</b> Only the {@link #getName()} is persisted in
 * per-field metadata; the bytes on disk are completely described by it. Configuration that
 * does not change the wire format — for example an LZ4 or Zstd compression level chosen by the
 * writer — must stay encoder-only state and is never recorded. Compression-level decoders are
 * level-agnostic by design, so a reader that reconstructs the encoding via {@link
 * BlockEncodingRegistry#forName(String)} can decode any segment regardless of the writer's chosen
 * level. Implementations MUST guarantee that for the same {@link #getName()}, every byte the
 * encoder writes can be decoded back identically by every other instance with the same id.
 *
 * <p><b>Extensibility.</b> Downstream modules register additional encodings by adding a line to
 * {@code META-INF/services/org.elasticsearch.columnar.BlockEncoding} pointing at their
 * implementation class. {@link BlockEncodingRegistry} discovers them at class-init time and
 * resolves ids on read.
 *
 * <p><b>Stateful writers, stateless readers.</b> The interface itself is a registry-friendly
 * singleton: implementations are looked up by {@link #getName()} via {@link BlockEncodingRegistry}
 * (potentially shared across many segment writers / readers). Per-writer scratch — LZ4 hash
 * tables, Zstd contexts, anything that can't safely be shared across concurrent segment
 * writes — lives on {@link Encoder} instances handed out by {@link #newEncoder()}. The
 * {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer} calls {@code newEncoder()} once when constructed and reuses
 * the returned {@code Encoder} across every block it writes. Decoding stays on the interface
 * because Zstd / LZ4 / identity all decode without per-reader scratch beyond a caller-owned
 * byte buffer.
 *
 * <p>Reserved built-in ids:
 * <ul>
 *   <li>{@code 0} — {@link IdentityBlockEncoding}, identity pass-through.</li>
 *   <li>{@code 1} — {@link Lz4BlockEncoding}, LZ4 via Lucene's pure-Java codec
 *       (compression level configurable; level NOT persisted).</li>
 *   <li>{@code 2} — reserved for a future Zstd implementation backed by the native binding
 *       at {@code org.elasticsearch.nativeaccess.Zstd}; needs a module-info on this library
 *       plus a qualified export from {@code libs/native}, deferred to a follow-up.</li>
 * </ul>
 */
public interface BlockEncoding extends NamedSPILoader.NamedSPI {

    /**
     * Stable identifier persisted in metadata to look up this encoding on read. See the interface
     * Javadoc for the immutability contract and the list of reserved built-in ids.
     */
    // String getName() inherited from NamedSPILoader.NamedSPI.
    /**
     * Construct a per-consumer {@link Encoder}. The consumer calls this once at construction
     * time and reuses the returned encoder for every block it writes. Implementations may hold
     * scratch in the returned instance (e.g. an LZ4 hash table, a Zstd context) without worrying
     * about concurrent segment writes — each consumer gets its own.
     *
     * <p>Identity encodings return a stateless singleton; real transforms allocate per-call.
     */
    Encoder newEncoder();

    /**
     * Read {@code encodedLen} bytes from {@code in}, decode them (if needed), and return a
     * {@link DataInput} positioned at the start of the decoded bytes.
     *
     * <p>Identity implementations should return {@code in} directly so the caller's
     * {@link NumericBlockEncoder} reads straight from mmap'd memory; real transforms decode into the
     * caller-provided {@code scratch} buffer and return a wrapper such as
     * {@link org.apache.lucene.store.ByteArrayDataInput}. The caller's encoder is responsible
     * for consuming exactly {@code decodedLen} bytes from the returned reader.
     *
     * @param formatVersion the {@link ColumNARDocValuesFormat#VERSION_CURRENT}-style value the
     *                      segment was written with. Encodings that never changed their wire
     *                      format can ignore it; encodings that opted into the same-id evolution
     *                      path MUST branch on it to stay readable across published versions.
     * @param in the segment file's data input, positioned at the start of this block's
     *           encoded bytes
     * @param encodedLen the number of encoded bytes for this block
     * @param scratch caller-owned scratch buffer of length {@code >= decodedLen}; unused by
     *                identity encodings but always non-null
     * @param decodedLen the length of the decoded payload (must match what
     *                   {@link Encoder#encode} produced on the write side before being passed
     *                   to this layer)
     */
    DataInput decode(int formatVersion, DataInput in, int encodedLen, byte[] scratch, int decodedLen) throws IOException;

    /**
     * Per-consumer write-side context. Held for the lifetime of one
     * {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer} and called once per block.
     */
    interface Encoder {
        /**
         * Encode {@code src[srcOffset, srcOffset + srcLen)} and write the result to {@code out}.
         * Returns the number of bytes written, which the caller records as the "encoded length"
         * for the block.
         */
        int encode(byte[] src, int srcOffset, int srcLen, DataOutput out) throws IOException;
    }
}
