/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numericpipeline;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.LongValuesSupplier;
import org.elasticsearch.columnar.NumericType;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;

import java.io.IOException;

/**
 * Numeric {@link NumericBlockEncoder} that runs a per-block stage pipeline:
 * {@link DeltaStage delta} → {@link OffsetStage offset} → {@link GcdStage gcd}
 * → {@link BitPackStage bit-pack}. Each transform stage decides at encode time
 * whether running it shrinks the encoded size; a per-block bitmap records which
 * transforms fired so decode reverses only those. The terminal bit-pack stage always
 * runs and writes the value payload.
 *
 * <p>Different blocks of the same field can therefore pick different stage combinations —
 * a monotonic-with-day-granularity field gets delta+gcd+bit-pack on smooth blocks and
 * falls back to bit-pack-only on blocks where ingestion jitter breaks monotonicity. The
 * encoder commits to nothing at the segment level; the decision lives per block.
 *
 * <p><b>Per-block layout.</b>
 * <pre>
 *   [Byte stageBitmap]            // bit i set ⇔ stage with id i applied (LSB = stage 0)
 *   [Bit-pack payload]            // VInt(bitsPerValue) + packed longs
 *   [Stage metadata, written in REVERSE pipeline order for applied stages only]
 * </pre>
 * The metadata is written back-to-front so decode peels stages in the order each layer
 * needs its metadata: bit-pack first, then gcd, then offset, then delta.
 *
 * <p><b>Dispatch.</b> The encoder uses a switch on {@link StageId} to keep each stage's
 * encode / decode call site monomorphic — the JIT inlines the concrete stage body without
 * paying the cost of a megamorphic interface dispatch over four implementations.
 *
 * <p><b>Reserved id.</b> {@code 5}. Once published in a release, the wire format produced
 * by this encoder is frozen forever.
 */
public final class NumericPipelineEncoder implements NumericBlockEncoder {

    public static final String NAME = "Pipeline";
    public static final NumericPipelineEncoder INSTANCE = new NumericPipelineEncoder();

    /**
     * Transform stages in pipeline order. Order is part of the wire format: delta first
     * produces a sequence whose minimum may be non-zero (offset's job) and whose entries
     * may share a common factor (gcd's job). Reordering would change the encoded bytes.
     */
    private static final NumericStage[] TRANSFORMS = new NumericStage[] { DeltaStage.INSTANCE, OffsetStage.INSTANCE, GcdStage.INSTANCE };

    private static final PayloadStage PAYLOAD = BitPackStage.INSTANCE;

    public NumericPipelineEncoder() {}

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public NumericType numericType() {
        return NumericType.LONG;
    }

    /**
     * No segment-level specialisation — the pipeline adapts per block, so the encoder
     * remains {@code this} for every segment.
     */
    @Override
    public NumericBlockEncoder specializeForSegment(LongValuesSupplier values) {
        return this;
    }

    @Override
    public int maxEncodedSize(int valuesLen) {
        // 1-byte stage bitmap + bit-pack header (VInt up to 5 bytes) + bit-pack body
        // (worst case at bpv=64 raw mode) + per-stage metadata (max 10 bytes per stage
        // for VLong/ZLong).
        final int bitmapBytes = 1;
        final int payloadHeaderBytes = 5;
        final int payloadBodyBytes = Math.multiplyExact(BitPackStage.maxPackedLongs(valuesLen) + 1, Long.BYTES);
        final int metadataBytes = TRANSFORMS.length * 10;
        return Math.addExact(Math.addExact(bitmapBytes, payloadHeaderBytes), Math.addExact(payloadBodyBytes, metadataBytes));
    }

    @Override
    public int scratchLongs(int valuesLen) {
        // Decode reads the packed words into scratch then unpacks them straight into the
        // caller's destination — the worst-case packed-words count at bpv=63 is what we
        // need.
        return BitPackStage.maxPackedLongs(valuesLen);
    }

    @Override
    public int encode(long[] values, int valuesOffset, int valuesLen, byte[] dest, int destOffset) {
        if (valuesLen == 0) {
            return 0;
        }
        // Stages mutate in place; copy so the caller's array stays untouched.
        final long[] work = new long[valuesLen];
        System.arraycopy(values, valuesOffset, work, 0, valuesLen);

        final byte[][] stageMeta = new byte[TRANSFORMS.length][];
        byte bitmap = 0;
        try {
            for (int i = 0; i < TRANSFORMS.length; i++) {
                final ByteBuffersDataOutput buf = new ByteBuffersDataOutput();
                final boolean applied = encodeStage(TRANSFORMS[i], work, valuesLen, buf);
                if (applied) {
                    bitmap |= (byte) (1 << TRANSFORMS[i].stageId().id);
                    stageMeta[i] = buf.toArrayCopy();
                }
            }
            final ByteBuffersDataOutput payloadBuf = new ByteBuffersDataOutput();
            encodePayload(PAYLOAD, work, valuesLen, payloadBuf);
            final byte[] payloadBytes = payloadBuf.toArrayCopy();

            final ByteArrayDataOutput out = new ByteArrayDataOutput(dest, destOffset, dest.length - destOffset);
            out.writeByte(bitmap);
            out.writeBytes(payloadBytes, 0, payloadBytes.length);
            // Reverse pipeline order so decode unrolls back-to-front.
            for (int i = TRANSFORMS.length - 1; i >= 0; i--) {
                if (stageMeta[i] != null) {
                    out.writeBytes(stageMeta[i], 0, stageMeta[i].length);
                }
            }
            return out.getPosition() - destOffset;
        } catch (IOException e) {
            // In-memory DataOutput implementations don't actually throw.
            throw new AssertionError("in-memory DataOutput should not throw", e);
        }
    }

    @Override
    public void decode(int formatVersion, DataInput in, long[] dest, int destOffset, int valuesLen, long[] scratch) throws IOException {
        if (valuesLen == 0) {
            return;
        }
        final int bitmap = in.readByte() & 0xff;
        decodePayload(PAYLOAD, dest, destOffset, valuesLen, in, scratch);
        for (int i = TRANSFORMS.length - 1; i >= 0; i--) {
            final NumericStage stage = TRANSFORMS[i];
            if ((bitmap & (1 << stage.stageId().id)) != 0) {
                decodeStage(stage, dest, destOffset, valuesLen, in);
            }
        }
    }

    // Static-dispatch wrappers: switching on the stage id at each call site lets the JIT
    // inline the concrete stage body (a single iface call per id has only one observed
    // receiver type — monomorphic — instead of three).

    private static boolean encodeStage(NumericStage stage, long[] values, int valueCount, DataOutput metaOut) throws IOException {
        return switch (stage.stageId()) {
            case DELTA_STAGE -> ((DeltaStage) stage).encode(values, valueCount, metaOut);
            case OFFSET_STAGE -> ((OffsetStage) stage).encode(values, valueCount, metaOut);
            case GCD_STAGE -> ((GcdStage) stage).encode(values, valueCount, metaOut);
            case BITPACK_PAYLOAD -> throw new AssertionError("payload stage routed through transform path");
        };
    }

    private static void decodeStage(NumericStage stage, long[] values, int valuesOffset, int valueCount, DataInput metaIn)
        throws IOException {
        switch (stage.stageId()) {
            case DELTA_STAGE -> ((DeltaStage) stage).decode(values, valuesOffset, valueCount, metaIn);
            case OFFSET_STAGE -> ((OffsetStage) stage).decode(values, valuesOffset, valueCount, metaIn);
            case GCD_STAGE -> ((GcdStage) stage).decode(values, valuesOffset, valueCount, metaIn);
            case BITPACK_PAYLOAD -> throw new AssertionError("payload stage routed through transform path");
        }
    }

    private static void encodePayload(PayloadStage stage, long[] values, int valueCount, DataOutput dataOut) throws IOException {
        switch (stage.stageId()) {
            case BITPACK_PAYLOAD -> ((BitPackStage) stage).encode(values, valueCount, dataOut);
            case DELTA_STAGE, OFFSET_STAGE, GCD_STAGE -> throw new AssertionError("transform stage routed through payload path");
        }
    }

    private static void decodePayload(PayloadStage stage, long[] values, int valuesOffset, int valueCount, DataInput dataIn, long[] scratch)
        throws IOException {
        switch (stage.stageId()) {
            case BITPACK_PAYLOAD -> ((BitPackStage) stage).decode(values, valuesOffset, valueCount, dataIn, scratch);
            case DELTA_STAGE, OFFSET_STAGE, GCD_STAGE -> throw new AssertionError("transform stage routed through payload path");
        }
    }

    /** Helper for tests: round-trip a block by encoding then decoding. */
    static long[] roundTrip(long[] values) throws IOException {
        final byte[] buf = new byte[INSTANCE.maxEncodedSize(values.length)];
        final int written = INSTANCE.encode(values, 0, values.length, buf, 0);
        final long[] dest = new long[values.length];
        final long[] scratch = new long[INSTANCE.scratchLongs(values.length)];
        INSTANCE.decode(ColumNARDocValuesFormat.VERSION_CURRENT, new ByteArrayDataInput(buf, 0, written), dest, 0, values.length, scratch);
        return dest;
    }
}
