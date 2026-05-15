/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.encoder;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.compress.LZ4;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;

import java.io.IOException;

/**
 * LZ4 {@link BlockEncoding} backed by {@link org.apache.lucene.util.compress.LZ4} (pure Java; no
 * native code). Production-default outer encoding for {@link ColumNARDocValuesFormat}.
 *
 * <p><b>Level is encoder-only state, never persisted.</b> The writer picks one of two LZ4
 * speed/ratio modes — {@link Mode#FAST} (the LZ4 default, fastest) or {@link Mode#HIGH} (better
 * ratio at noticeably higher CPU cost). The decoder is level-agnostic: Lucene's
 * {@link LZ4#decompress} reads the same bit stream regardless of which hash-table flavor
 * produced it, so a reader instantiated by the {@link BlockEncodingRegistry} with the default
 * mode decodes any segment we ever wrote.
 *
 * <p><b>Per-consumer state.</b> Each {@link org.elasticsearch.columnar.ColumNARDocValuesConsumer} calls {@link #newEncoder}
 * once and reuses the returned {@link Encoder}. That encoder holds a single hash table
 * ({@link LZ4.FastCompressionHashTable} for {@link Mode#FAST}, {@link LZ4.HighCompressionHashTable}
 * for {@link Mode#HIGH}) and a reusable {@link ByteBuffersDataOutput} scratch, so block-by-block
 * compression incurs no per-call allocations on the hot path. Singletons are fine on the
 * registry side because we never share the {@code Encoder} across segment writes.
 *
 * <p><b>Wire format.</b> Each encoded block is a vint length prefix followed by raw LZ4 bytes.
 * The vint lets the decoder reconstruct one block at a time inside a potentially shared
 * {@link DataInput}.
 */
public final class Lz4BlockEncoding implements BlockEncoding {

    public static final String NAME = "Lz4";

    /** LZ4 speed/ratio mode. Does not appear on disk. */
    public enum Mode {
        /** LZ4 default — fastest compression, lowest ratio. Production default. */
        FAST,
        /** LZ4 HC — better ratio, noticeably slower. */
        HIGH
    }

    /**
     * SPI-discoverable default ({@link Mode#FAST}). The public no-arg constructor is required
     * by {@link java.util.ServiceLoader}.
     */
    public static final Lz4BlockEncoding INSTANCE = new Lz4BlockEncoding(Mode.FAST);

    /** {@link Mode#HIGH} variant; same {@link #getName()}, trades CPU for a smaller payload. */
    public static final Lz4BlockEncoding HIGH = new Lz4BlockEncoding(Mode.HIGH);

    private final Mode mode;

    /** Required for ServiceLoader. Defaults to {@link Mode#FAST}. */
    public Lz4BlockEncoding() {
        this(Mode.FAST);
    }

    public Lz4BlockEncoding(Mode mode) {
        this.mode = mode;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Encoder newEncoder() {
        // LZ4.HashTable is package-private in Lucene so we cannot declare it as a field type;
        // each Encoder holds its concrete subtype instead.
        return switch (mode) {
            case FAST -> new FastEncoder();
            case HIGH -> new HighEncoder();
        };
    }

    @Override
    public DataInput decode(int formatVersion, DataInput in, int encodedLen, byte[] scratch, int decodedLen) throws IOException {
        // encodedLen is the total on-disk byte count for this block (vint prefix + compressed
        // payload). We read the vint to confirm the framing then hand the input straight to
        // LZ4.decompress, which reads exactly the bytes it needs to reconstruct decodedLen
        // bytes into scratch.
        final int compressedLen = in.readVInt();
        if (compressedLen < 0) {
            throw new IOException("invalid LZ4 compressed length " + compressedLen);
        }
        LZ4.decompress(in, decodedLen, scratch, 0);
        return new ByteArrayDataInput(scratch, 0, decodedLen);
    }

    private static final class FastEncoder implements Encoder {
        private final LZ4.FastCompressionHashTable hashTable = new LZ4.FastCompressionHashTable();
        // Per-Encoder scratch — sized lazily on first call, reused thereafter. Safe because one
        // Encoder belongs to exactly one ColumNARDocValuesConsumer (a single segment write).
        private final ByteBuffersDataOutput scratch = ByteBuffersDataOutput.newResettableInstance();

        @Override
        public int encode(byte[] src, int srcOffset, int srcLen, DataOutput out) throws IOException {
            scratch.reset();
            LZ4.compress(src, srcOffset, srcLen, scratch, hashTable);
            return writeLengthAndPayload(scratch, out);
        }
    }

    private static final class HighEncoder implements Encoder {
        private final LZ4.HighCompressionHashTable hashTable = new LZ4.HighCompressionHashTable();
        private final ByteBuffersDataOutput scratch = ByteBuffersDataOutput.newResettableInstance();

        @Override
        public int encode(byte[] src, int srcOffset, int srcLen, DataOutput out) throws IOException {
            scratch.reset();
            LZ4.compress(src, srcOffset, srcLen, scratch, hashTable);
            return writeLengthAndPayload(scratch, out);
        }
    }

    private static int writeLengthAndPayload(ByteBuffersDataOutput scratch, DataOutput out) throws IOException {
        final int compressedLen = Math.toIntExact(scratch.size());
        out.writeVInt(compressedLen);
        scratch.copyTo(out);
        return vintLength(compressedLen) + compressedLen;
    }

    private static int vintLength(int value) {
        int bytes = 1;
        int v = value;
        while ((v & ~0x7F) != 0) {
            v >>>= 7;
            bytes++;
        }
        return bytes;
    }
}
