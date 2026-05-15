/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.encoder.BlockEncoding;
import org.elasticsearch.columnar.encoder.BytesBlockEncoder;
import org.elasticsearch.columnar.encoder.DeltaPackedBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.SkipIndex;
import org.elasticsearch.columnar.encoder.SkipIndexParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writer half of {@link ColumNARDocValuesFormat}.
 *
 * <p><b>Binary substrate, type adapters above it.</b> The on-disk format stores byte payloads
 * per block — every field type funnels through {@link #appendBlockPayload}, which wraps a block
 * of bytes with the configured {@link BlockEncoding} and appends it to the data file. The
 * {@code add*Field} methods are thin type adapters: each one buffers values in its native shape,
 * invokes a type-specific encoder ({@link NumericBlockEncoder} for longs, {@link BytesBlockEncoder}
 * for binary), then records type-specific per-block metadata. This keeps the binary substrate
 * independent from any value type, and lets future iterations plug in {@code addSortedField}
 * etc. without touching the substrate-level writer.
 *
 * <p><b>Metadata layout.</b> The {@code .cdvm} file records the codec-level block size once at
 * the top, then per field: a fixed summary (encoder id, encoding id, value count, block count,
 * precomputed global stats, scratch sizing hints) followed by a <strong>fixed-size block
 * table</strong>. Block table records are uniform width so the producer can compute any block's
 * record offset by arithmetic and seek directly — no per-block arrays are loaded into memory at
 * open time, the table stays in mmap.
 *
 * <p>v0 supports dense single-valued numeric and binary fields. Sparse, multi-valued, sorted,
 * and sorted-set fields throw {@link UnsupportedOperationException} until later iterations.
 */
final class ColumNARDocValuesConsumer extends DocValuesConsumer {

    private final IndexOutput data;
    private final IndexOutput meta;
    private final NumericBlockEncoder longEncoder;
    private final BytesBlockEncoder bytesRefEncoder;
    private final BlockEncoding encoding;
    // Per-consumer encoder context (LZ4 hash table / Zstd scratch / identity lambda). One per
    // ColumNARDocValuesConsumer; reused across every block this consumer writes.
    private final BlockEncoding.Encoder blockWriter;
    private final SkipIndex numericSkipIndex;
    private final SkipIndexParams skipIndexParams;
    private final int targetEncodedBytesPerBlock;
    private final int maxValuesPerBlock;
    private final boolean preferDictionaryForBinary;
    private final int maxDoc;
    private boolean closed = false;

    ColumNARDocValuesConsumer(
        SegmentWriteState state,
        NumericBlockEncoder longEncoder,
        BytesBlockEncoder bytesRefEncoder,
        BlockEncoding encoding,
        SkipIndex numericSkipIndex,
        SkipIndexParams skipIndexParams,
        int targetEncodedBytesPerBlock,
        int maxValuesPerBlock,
        boolean preferDictionaryForBinary
    ) throws IOException {
        this.longEncoder = longEncoder;
        this.bytesRefEncoder = bytesRefEncoder;
        this.encoding = encoding;
        this.blockWriter = encoding.newEncoder();
        this.numericSkipIndex = numericSkipIndex;
        this.skipIndexParams = skipIndexParams;
        this.targetEncodedBytesPerBlock = targetEncodedBytesPerBlock;
        this.maxValuesPerBlock = maxValuesPerBlock;
        this.preferDictionaryForBinary = preferDictionaryForBinary;
        this.maxDoc = state.segmentInfo.maxDoc();
        boolean success = false;
        IndexOutput dataOut = null;
        IndexOutput metaOut = null;
        try {
            final String dataName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.DATA_EXTENSION
            );
            dataOut = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(
                dataOut,
                ColumNARDocValuesFormat.DATA_CODEC,
                ColumNARDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            final String metaName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.META_EXTENSION
            );
            metaOut = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(
                metaOut,
                ColumNARDocValuesFormat.META_CODEC,
                ColumNARDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            // Block size moved to per-field metadata — each field's resolver picks its own
            // row cap and byte target. Nothing block-related at the segment level.
            this.data = dataOut;
            this.meta = metaOut;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(dataOut, metaOut);
            }
        }
    }

    /**
     * Numerics are NOT supported at the Lucene doc-values level. The format's lineage is
     * binary doc values; the mapper packs longs into a {@link BinaryDocValues} payload
     * using {@code org.elasticsearch.columnar.bridge.PackedLongBinaryPacker} (or one of the
     * convenience field wrappers in that package) and routes the write through
     * {@link #addBinaryField}. The bridge exposes the same bytes back as a long iterator on
     * the read side. Keeps the format's API one-shape; numerics live in the bridge layer,
     * never inside the codec.
     */
    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw new UnsupportedOperationException(
            "ColumNARDocValuesFormat is binary-only. Numeric values must be packed into a "
                + "BinaryDocValuesField via org.elasticsearch.columnar.bridge.PackedLongBinaryPacker "
                + "(or the ColumNARLongField / ColumNARIntField / ColumNARFloatField / "
                + "ColumNARDoubleField wrappers) and routed through addBinaryField. The bridge "
                + "exposes them as long iterators via ColumNARLongValues on the read side."
        );
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        // Pre-pass: buffer all values + (if dict path enabled) count distinct values into a
        // dictionary. We need both the values AND the cardinality before we can decide which
        // path to write — and the source DocValues iterator is single-pass, so buffering is
        // the only option that's correct on cross-segment merges.
        final BinaryDocValues values = valuesProducer.getBinary(field);
        final java.util.List<byte[]> rawValues = new java.util.ArrayList<>(maxDoc);
        final java.util.LinkedHashMap<ByteWrap, Integer> dict = preferDictionaryForBinary ? new java.util.LinkedHashMap<>() : null;
        boolean dictOverflowed = false;
        int lastDoc = -1;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            if (doc != lastDoc + 1) {
                throw new UnsupportedOperationException(
                    "ColumNARDocValuesFormat v0 supports only dense binary fields; saw gap before doc " + doc
                );
            }
            lastDoc = doc;
            final BytesRef v = values.binaryValue();
            final byte[] copy = new byte[v.length];
            System.arraycopy(v.bytes, v.offset, copy, 0, v.length);
            rawValues.add(copy);
            if (dict != null && dictOverflowed == false) {
                final ByteWrap key = new ByteWrap(copy);
                if (dict.containsKey(key) == false) {
                    if (dict.size() >= ColumNARDocValuesFormat.DICT_BINARY_MAX_DICT_SIZE) {
                        dictOverflowed = true;
                    } else {
                        dict.put(key, dict.size());
                    }
                }
            }
        }
        if (rawValues.size() != maxDoc) {
            throw new UnsupportedOperationException(
                "ColumNARDocValuesFormat v0 supports only dense binary fields; got "
                    + rawValues.size()
                    + " values for "
                    + maxDoc
                    + " docs in segment"
            );
        }
        // Dispatch order:
        // 1. dict-binary — wins when ≤ DICT_BINARY_MAX_DICT_SIZE distinct payloads, since
        // the per-segment dictionary + bit-packed ordinals beats any encoder running
        // on the raw payload bytes. This catches both low-cardinality keyword AND
        // low-cardinality long fields (e.g. an enum-style long with 8 distinct values).
        // 2. packed-long — every doc carries [byte 'L'][vint 1][LE long]. We unpack the
        // longs and route through the numeric pipeline (delta / offset / GCD / bit-pack),
        // which is the path TSDB uses for typed numeric DV — closes the storage gap
        // on monotonic timestamps and other patterned numeric distributions. Multi-valued
        // 'L' payloads (count != 1) skip this path and fall through to raw bytes.
        // 3. raw bytes — last-resort path for high-cardinality binary that doesn't fit the
        // two above.
        if (dict != null && dictOverflowed == false && dict.isEmpty() == false) {
            writeDictBinaryField(field, rawValues, dict);
        } else if (looksLikePackedLong(rawValues)) {
            writePackedLongField(field, rawValues);
        } else {
            writeRawBinaryField(field, rawValues);
        }
    }

    /**
     * True if every value carries the bridge's long shape: starts with the {@code 'L'}
     * marker, has a well-formed vint count, and exactly {@code 8 * count} value bytes
     * after the count. Catches both single-valued (count = 1) and multi-valued
     * (count > 1) bridge payloads. Empty collection is false — nothing to pack.
     */
    private static boolean looksLikePackedLong(java.util.List<byte[]> rawValues) {
        if (rawValues.isEmpty()) {
            return false;
        }
        for (byte[] v : rawValues) {
            if (v.length < 2 || v[0] != (byte) 'L') return false;
            int pos = 1;
            int count = v[pos++] & 0xFF;
            if ((count & 0x80) != 0) {
                count &= 0x7F;
                int shift = 7;
                while (true) {
                    if (pos >= v.length) return false;
                    final byte b = v[pos++];
                    count |= (b & 0x7F) << shift;
                    shift += 7;
                    if ((b & 0x80) == 0) break;
                    if (shift > 28) return false;
                }
            }
            if (count < 0) return false;
            if (v.length - pos != count * 8L) return false;
        }
        return true;
    }

    /**
     * Numeric-pipeline write path for bridge-written long fields. Decodes every payload's
     * count + values into flat arrays, then dispatches to the single-valued or multi-valued
     * sub-path based on whether any doc carries more than one value. The single-valued path
     * persists no count stream (every doc has count = 1, recovered implicitly); the
     * multi-valued path writes a count stream alongside the value stream, both through the
     * numeric encoder pipeline.
     */
    private void writePackedLongField(FieldInfo field, java.util.List<byte[]> rawValues) throws IOException {
        final int n = rawValues.size();
        final int[] counts = new int[n];
        long totalValues = 0;
        boolean allSingleValued = true;
        int maxValuesPerDoc = 1;
        for (int i = 0; i < n; i++) {
            final byte[] v = rawValues.get(i);
            int pos = 1;
            int count = v[pos++] & 0xFF;
            if ((count & 0x80) != 0) {
                count &= 0x7F;
                int shift = 7;
                while (true) {
                    final byte b = v[pos++];
                    count |= (b & 0x7F) << shift;
                    shift += 7;
                    if ((b & 0x80) == 0) break;
                }
            }
            counts[i] = count;
            totalValues += count;
            if (count != 1) allSingleValued = false;
            if (count > maxValuesPerDoc) maxValuesPerDoc = count;
        }
        if (totalValues > Integer.MAX_VALUE) {
            // Pathological case — fall back to bytes path rather than overflow the int buffers
            // the numeric encoder pipeline uses.
            writeRawBinaryField(field, rawValues);
            return;
        }
        final long[] values = new long[(int) totalValues];
        int vCursor = 0;
        for (int i = 0; i < n; i++) {
            final byte[] v = rawValues.get(i);
            int pos = 1;
            // Skip the vint count.
            while ((v[pos++] & 0x80) != 0) {
                // intentional empty body
            }
            for (int j = 0; j < counts[i]; j++) {
                values[vCursor++] = readLongLE(v, pos);
                pos += 8;
            }
        }
        if (allSingleValued) {
            writePackedSingleValuedLongField(field, values);
        } else {
            writePackedMultiValuedLongField(field, counts, values, maxValuesPerDoc);
        }
    }

    /**
     * Single-valued long write path. Every doc carries one value; no count stream is
     * persisted (the reader infers count = 1 from the field type).
     */
    private void writePackedSingleValuedLongField(FieldInfo field, long[] values) throws IOException {
        final int n = values.length;
        final NumericBlockEncoder fieldEncoder = longEncoder.specializeForSegment(LongValuesSupplier.fromArray(values, 0, n));
        final int fieldBlockSize = maxValuesPerBlock;
        final byte[] payloadBuf = new byte[fieldEncoder.maxEncodedSize(fieldBlockSize)];
        final java.util.List<NumericBlockMeta> blockMeta = new java.util.ArrayList<>();
        int maxPayloadLen = 0;
        int writeCursor = 0;
        while (writeCursor < n) {
            final int valuesInBlock = Math.min(fieldBlockSize, n - writeCursor);
            final int payloadLen = fieldEncoder.encode(values, writeCursor, valuesInBlock, payloadBuf, 0);
            final long offset = data.getFilePointer();
            final int encodedLen = appendBlockPayload(payloadBuf, payloadLen);
            blockMeta.add(new NumericBlockMeta(offset, payloadLen, encodedLen, valuesInBlock));
            if (payloadLen > maxPayloadLen) maxPayloadLen = payloadLen;
            writeCursor += valuesInBlock;
        }

        // Per-field metadata layout (packed-long, mirrors the dict-binary numeric-block shape):
        // [Int fieldNumber][Byte FIELD_TYPE_PACKED_LONG]
        // [String encoderName][String encodingName][VInt blockSize]
        // [VLong valueCount][VInt blockCount]
        // [Int maxPayloadLen]
        // [blockCount × NUMERIC_BLOCK_RECORD_SIZE bytes block table]
        meta.writeInt(field.number);
        meta.writeByte(ColumNARDocValuesFormat.FIELD_TYPE_PACKED_LONG);
        meta.writeString(fieldEncoder.getName());
        meta.writeString(encoding.getName());
        meta.writeVInt(fieldBlockSize);
        meta.writeVLong(n);
        meta.writeVInt(blockMeta.size());
        meta.writeInt(maxPayloadLen);
        for (NumericBlockMeta b : blockMeta) {
            meta.writeLong(b.offset());
            meta.writeInt(b.payloadLen());
            meta.writeInt(b.encodedLen());
            meta.writeInt(b.valuesInBlock());
        }
    }

    private static long readLongLE(byte[] arr, int offset) {
        return (long) (arr[offset] & 0xFF) | (long) (arr[offset + 1] & 0xFF) << 8 | (long) (arr[offset + 2] & 0xFF) << 16
            | (long) (arr[offset + 3] & 0xFF) << 24 | (long) (arr[offset + 4] & 0xFF) << 32 | (long) (arr[offset + 5] & 0xFF) << 40
            | (long) (arr[offset + 6] & 0xFF) << 48 | (long) (arr[offset + 7] & 0xFF) << 56;
    }

    /**
     * Multi-valued long write path. Counts and values get their own numeric-encoder pipelines
     * (each auto-picked from its own sample) but share the same outer {@link BlockEncoding}.
     * On read the producer walks the count stream to map doc id → value-stream offset, then
     * re-packs the {@code 'L'}-shape payload the bridge expects.
     */
    private void writePackedMultiValuedLongField(FieldInfo field, int[] counts, long[] values, int maxValuesPerDoc) throws IOException {
        final int docCount = counts.length;
        final int totalValues = values.length;
        final int blockSize = maxValuesPerBlock;

        // Value stream — auto-pick encoder, encode block-by-block.
        final NumericBlockEncoder valueEncoder = longEncoder.specializeForSegment(LongValuesSupplier.fromArray(values, 0, totalValues));
        final byte[] valuePayloadBuf = new byte[valueEncoder.maxEncodedSize(blockSize)];
        final java.util.List<NumericBlockMeta> valueBlocks = new java.util.ArrayList<>();
        int maxValuePayload = 0;
        int wc = 0;
        while (wc < totalValues) {
            final int vib = Math.min(blockSize, totalValues - wc);
            final int pl = valueEncoder.encode(values, wc, vib, valuePayloadBuf, 0);
            final long off = data.getFilePointer();
            final int el = appendBlockPayload(valuePayloadBuf, pl);
            valueBlocks.add(new NumericBlockMeta(off, pl, el, vib));
            if (pl > maxValuePayload) maxValuePayload = pl;
            wc += vib;
        }

        // Count stream — per-doc value counts as longs. Auto-pick its own encoder; tiny
        // value range means bit-pack lands at ceil(log2(maxValuesPerDoc + 1)) bits per doc.
        final long[] countLongs = new long[docCount];
        for (int i = 0; i < docCount; i++) {
            countLongs[i] = counts[i];
        }
        final NumericBlockEncoder countEncoder = longEncoder.specializeForSegment(LongValuesSupplier.fromArray(countLongs, 0, docCount));
        final byte[] countPayloadBuf = new byte[countEncoder.maxEncodedSize(blockSize)];
        final java.util.List<NumericBlockMeta> countBlocks = new java.util.ArrayList<>();
        int maxCountPayload = 0;
        wc = 0;
        while (wc < docCount) {
            final int vib = Math.min(blockSize, docCount - wc);
            final int pl = countEncoder.encode(countLongs, wc, vib, countPayloadBuf, 0);
            final long off = data.getFilePointer();
            final int el = appendBlockPayload(countPayloadBuf, pl);
            countBlocks.add(new NumericBlockMeta(off, pl, el, vib));
            if (pl > maxCountPayload) maxCountPayload = pl;
            wc += vib;
        }

        // Per-field metadata layout (packed-longs-mv):
        // [Int fieldNumber][Byte FIELD_TYPE_PACKED_LONGS_MV]
        // [String valueEncoderName][String countEncoderName][String encodingName][VInt blockSize]
        // [VInt docCount][VLong totalValueCount][Int maxValuesPerDoc]
        // [VInt valueBlockCount][Int maxValuePayloadLen]
        // [VInt countBlockCount][Int maxCountPayloadLen]
        // [valueBlockCount × NUMERIC_BLOCK_RECORD_SIZE value block table]
        // [countBlockCount × NUMERIC_BLOCK_RECORD_SIZE count block table]
        meta.writeInt(field.number);
        meta.writeByte(ColumNARDocValuesFormat.FIELD_TYPE_PACKED_LONGS_MV);
        meta.writeString(valueEncoder.getName());
        meta.writeString(countEncoder.getName());
        meta.writeString(encoding.getName());
        meta.writeVInt(blockSize);
        meta.writeVInt(docCount);
        meta.writeVLong(totalValues);
        meta.writeInt(maxValuesPerDoc);
        meta.writeVInt(valueBlocks.size());
        meta.writeInt(maxValuePayload);
        meta.writeVInt(countBlocks.size());
        meta.writeInt(maxCountPayload);
        for (NumericBlockMeta b : valueBlocks) {
            meta.writeLong(b.offset());
            meta.writeInt(b.payloadLen());
            meta.writeInt(b.encodedLen());
            meta.writeInt(b.valuesInBlock());
        }
        for (NumericBlockMeta b : countBlocks) {
            meta.writeLong(b.offset());
            meta.writeInt(b.payloadLen());
            meta.writeInt(b.encodedLen());
            meta.writeInt(b.valuesInBlock());
        }
    }

    /**
     * Dictionary-binary write path: one per-segment dictionary holding distinct values,
     * per-block ordinal stream encoded through the numeric pipeline ({@link NumericBlockEncoder}
     * for longs + {@link BlockEncoding} for the outer wrap). The auto-pick on
     * {@code longEncoder.specializeForSegment} runs over the first block of ordinals so
     * sorted-ordinal blocks get {@link DeltaPackedBlockEncoder} for free.
     */
    private void writeDictBinaryField(FieldInfo field, java.util.List<byte[]> rawValues, java.util.LinkedHashMap<ByteWrap, Integer> dict)
        throws IOException {
        final int n = rawValues.size();
        // Build the ordinal stream.
        final long[] ordinals = new long[n];
        for (int i = 0; i < n; i++) {
            ordinals[i] = dict.get(new ByteWrap(rawValues.get(i)));
        }
        // Auto-pick the numeric encoder by giving it a supplier backed by the already-built
        // ordinal array. The encoder can iterate as many times as it likes over the in-memory
        // ordinal stream — same off-heap iterator contract the per-field numeric path uses,
        // just sourced from a long[] rather than a NumericDocValues.
        final NumericBlockEncoder fieldEncoder = longEncoder.specializeForSegment(LongValuesSupplier.fromArray(ordinals, 0, n));
        final int fieldBlockSize = maxValuesPerBlock;
        final byte[] payloadBuf = new byte[fieldEncoder.maxEncodedSize(fieldBlockSize)];
        // Encode ordinal blocks via the numeric pipeline; the same code path numeric fields use.
        final java.util.List<NumericBlockMeta> blockMeta = new java.util.ArrayList<>();
        int maxPayloadLen = 0;
        int writeCursor = 0;
        while (writeCursor < n) {
            final int valuesInBlock = Math.min(fieldBlockSize, n - writeCursor);
            final int payloadLen = fieldEncoder.encode(ordinals, writeCursor, valuesInBlock, payloadBuf, 0);
            final long offset = data.getFilePointer();
            final int encodedLen = appendBlockPayload(payloadBuf, payloadLen);
            blockMeta.add(new NumericBlockMeta(offset, payloadLen, encodedLen, valuesInBlock));
            if (payloadLen > maxPayloadLen) maxPayloadLen = payloadLen;
            writeCursor += valuesInBlock;
        }

        // Per-field metadata layout (dict-binary):
        // [Int fieldNumber][Byte FIELD_TYPE_DICT_BINARY]
        // [String encoderName][String encodingName]
        // [VInt blockSize]
        // [VLong valueCount][VInt blockCount]
        // [Int maxPayloadLen]
        // [VInt dictSize]
        // [for each dict entry: VInt length, bytes]
        // [blockCount × NUMERIC_BLOCK_RECORD_SIZE bytes block table]
        meta.writeInt(field.number);
        meta.writeByte(ColumNARDocValuesFormat.FIELD_TYPE_DICT_BINARY);
        meta.writeString(fieldEncoder.getName());
        meta.writeString(encoding.getName());
        meta.writeVInt(fieldBlockSize);
        meta.writeVLong(n);
        meta.writeVInt(blockMeta.size());
        meta.writeInt(maxPayloadLen);
        // Dictionary (small, ≤ DICT_BINARY_MAX_DICT_SIZE entries). Written here so the
        // reader's sequential walk through .cdvm picks it up before the block table; on read
        // the dict is cached on heap (its size is bounded).
        meta.writeVInt(dict.size());
        for (ByteWrap key : dict.keySet()) {
            meta.writeVInt(key.bytes.length);
            meta.writeBytes(key.bytes, 0, key.bytes.length);
        }
        // Numeric-shaped block table (20 bytes per record — same as numeric fields).
        for (NumericBlockMeta b : blockMeta) {
            meta.writeLong(b.offset());
            meta.writeInt(b.payloadLen());
            meta.writeInt(b.encodedLen());
            meta.writeInt(b.valuesInBlock());
        }
    }

    /** Raw binary write path — original behavior, accepting pre-buffered values. */
    private void writeRawBinaryField(FieldInfo field, java.util.List<byte[]> rawValues) throws IOException {
        // Block-size policy. Binary fields close on EITHER the row cap OR a byte budget —
        // the byte budget is the format's target encoded bytes interpreted as a raw-payload
        // cap (LZ4 typically halves the payload, so this targets ≈ the configured
        // compressed budget after encoding).
        final int fieldBlockSize = maxValuesPerBlock;
        final int fieldMaxBlockBytes = targetEncodedBytesPerBlock;
        byte[] valueBytes = new byte[Math.max(64, Math.min(fieldBlockSize, 1024) * 8)];
        final int[] valueOffsets = new int[fieldBlockSize + 1];
        int valueBytesLen = 0;
        valueOffsets[0] = 0;
        final List<BinaryBlockMeta> blockMeta = new ArrayList<>();
        byte[] payloadBuf = new byte[bytesRefEncoder.maxEncodedSize(fieldBlockSize, valueBytes.length)];
        long valueCount = 0;
        int posInBlock = 0;
        int maxPayloadLen = 0;
        int maxTotalValueBytes = 0;
        for (byte[] v : rawValues) {
            valueBytes = ArrayUtil.grow(valueBytes, valueBytesLen + v.length);
            System.arraycopy(v, 0, valueBytes, valueBytesLen, v.length);
            valueBytesLen += v.length;
            valueOffsets[++posInBlock] = valueBytesLen;
            valueCount++;
            // Byte-bounded blocks: close on EITHER the row-count cap (safety net) OR the byte
            // budget — whichever fires first. Keeps wide-payload binary columns at predictable
            // per-block decode cost; tightly-packed dict-binary columns close on row count.
            if (posInBlock == fieldBlockSize || valueBytesLen >= fieldMaxBlockBytes) {
                payloadBuf = ensurePayloadCapacity(payloadBuf, posInBlock, valueBytesLen);
                final BinaryBlockMeta block = flushBinaryBlock(valueBytes, valueOffsets, posInBlock, valueBytesLen, payloadBuf);
                blockMeta.add(block);
                if (block.payloadLen() > maxPayloadLen) maxPayloadLen = block.payloadLen();
                if (block.totalValueBytes() > maxTotalValueBytes) maxTotalValueBytes = block.totalValueBytes();
                posInBlock = 0;
                valueBytesLen = 0;
                valueOffsets[0] = 0;
            }
        }
        if (posInBlock > 0) {
            payloadBuf = ensurePayloadCapacity(payloadBuf, posInBlock, valueBytesLen);
            final BinaryBlockMeta block = flushBinaryBlock(valueBytes, valueOffsets, posInBlock, valueBytesLen, payloadBuf);
            blockMeta.add(block);
            if (block.payloadLen() > maxPayloadLen) maxPayloadLen = block.payloadLen();
            if (block.totalValueBytes() > maxTotalValueBytes) maxTotalValueBytes = block.totalValueBytes();
        }
        meta.writeInt(field.number);
        meta.writeByte(ColumNARDocValuesFormat.FIELD_TYPE_BINARY);
        meta.writeString(bytesRefEncoder.getName());
        meta.writeString(encoding.getName());
        meta.writeVInt(fieldBlockSize);
        meta.writeVLong(valueCount);
        meta.writeVInt(blockMeta.size());
        meta.writeInt(maxPayloadLen);
        meta.writeInt(maxTotalValueBytes);
        for (BinaryBlockMeta b : blockMeta) {
            meta.writeLong(b.offset());
            meta.writeInt(b.payloadLen());
            meta.writeInt(b.encodedLen());
            meta.writeInt(b.valuesInBlock());
            meta.writeInt(b.totalValueBytes());
        }
    }

    /** Byte-array wrapper that hashes/equals by content. Used to deduplicate keyword values. */
    private static final class ByteWrap {
        final byte[] bytes;
        final int hash;

        ByteWrap(byte[] bytes) {
            this.bytes = bytes;
            this.hash = java.util.Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof ByteWrap other) {
                return java.util.Arrays.equals(bytes, other.bytes);
            }
            return false;
        }
    }

    /**
     * Long-typed adapter: encode {@code valuesInBlock} longs into the caller-owned
     * {@code payloadBuf} via the field's specialised {@link NumericBlockEncoder}, then write the
     * payload through the {@link BlockEncoding} via {@link #appendBlockPayload}.
     */
    private NumericBlockMeta flushNumericBlock(NumericBlockEncoder fieldEncoder, long[] buffer, int valuesInBlock, byte[] payloadBuf)
        throws IOException {
        final int payloadLen = fieldEncoder.encode(buffer, 0, valuesInBlock, payloadBuf, 0);
        final long offset = data.getFilePointer();
        final int encodedLen = appendBlockPayload(payloadBuf, payloadLen);
        return new NumericBlockMeta(offset, payloadLen, encodedLen, valuesInBlock);
    }

    /**
     * Binary-typed adapter: encode {@code valuesInBlock} byte sequences (flat layout) into
     * {@code payloadBuf} via the {@link BytesBlockEncoder}, then write the payload through
     * the {@link BlockEncoding} via {@link #appendBlockPayload}.
     */
    private BinaryBlockMeta flushBinaryBlock(
        byte[] valueBytes,
        int[] valueOffsets,
        int valuesInBlock,
        int totalValueBytes,
        byte[] payloadBuf
    ) throws IOException {
        final int payloadLen = bytesRefEncoder.encode(valueBytes, valueOffsets, valuesInBlock, payloadBuf, 0);
        final long offset = data.getFilePointer();
        final int encodedLen = appendBlockPayload(payloadBuf, payloadLen);
        return new BinaryBlockMeta(offset, payloadLen, encodedLen, valuesInBlock, totalValueBytes);
    }

    /**
     * Binary substrate write path. Wraps a block payload with the configured {@link BlockEncoding}
     * and appends it to the data file, returning the number of bytes written (the on-disk
     * "encoded" length). This is the one place block bytes hit disk; every typed adapter routes
     * through here.
     */
    private int appendBlockPayload(byte[] payload, int payloadLen) throws IOException {
        return blockWriter.encode(payload, 0, payloadLen, data);
    }

    /**
     * Grow {@code payloadBuf} if needed so it can hold the encoded form of a binary block of
     * {@code valuesInBlock} entries totaling {@code totalValueBytes} payload bytes.
     */
    private byte[] ensurePayloadCapacity(byte[] payloadBuf, int valuesInBlock, int totalValueBytes) {
        final int needed = bytesRefEncoder.maxEncodedSize(valuesInBlock, totalValueBytes);
        return payloadBuf.length < needed ? new byte[ArrayUtil.oversize(needed, Byte.BYTES)] : payloadBuf;
    }

    private record NumericBlockMeta(long offset, int payloadLen, int encodedLen, int valuesInBlock) {}

    private record BinaryBlockMeta(long offset, int payloadLen, int encodedLen, int valuesInBlock, int totalValueBytes) {}

    /**
     * Build a replayable {@link LongValuesSupplier} backed by the source
     * {@link DocValuesProducer}'s {@code NumericDocValues} stream. Each call to {@code open()}
     * returns a fresh iterator that walks the column's values in doc order — encoders can
     * call it once, twice, or any number of times during specialisation without buffering
     * anything on heap.
     */
    private static LongValuesSupplier numericValuesSupplier(FieldInfo field, DocValuesProducer valuesProducer) {
        return () -> {
            final NumericDocValues dv = valuesProducer.getNumeric(field);
            return new LongValuesIterator() {
                private long current;

                @Override
                public boolean next() throws IOException {
                    final int doc = dv.nextDoc();
                    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                        return false;
                    }
                    current = dv.longValue();
                    return true;
                }

                @Override
                public long longValue() {
                    return current;
                }
            };
        };
    }

    /**
     * Build a replayable {@link BytesRefValuesSupplier} backed by the source
     * {@link DocValuesProducer}'s {@code BinaryDocValues} stream.
     */
    @SuppressWarnings("unused")
    private static BytesRefValuesSupplier binaryValuesSupplier(FieldInfo field, DocValuesProducer valuesProducer) {
        return () -> {
            final BinaryDocValues dv = valuesProducer.getBinary(field);
            return new BytesRefValuesIterator() {
                private BytesRef current;

                @Override
                public boolean next() throws IOException {
                    final int doc = dv.nextDoc();
                    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                        return false;
                    }
                    current = dv.binaryValue();
                    return true;
                }

                @Override
                public BytesRef bytesValue() {
                    return current;
                }
            };
        };
    }

    /**
     * Sorted (string) doc values surface ordinals through {@code SortedDocValues.ordValue()};
     * this format treats ordinals as an encoder-internal compression trick (see the
     * dictionary-binary path) and never exposes them on the read API. Mappers route
     * single-valued string fields through {@link #addBinaryField}; this codec stores the
     * bytes faithfully and the low-cardinality dictionary encoder picks itself up at
     * encode time when the value cardinality warrants it.
     */
    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw new UnsupportedOperationException(
            "ColumNARDocValuesFormat does not accept SortedDocValues — ordinals are an "
                + "encoder-internal detail of this format. Route string fields through "
                + "addBinaryField; the dictionary-binary encoder kicks in for low-cardinality "
                + "values."
        );
    }

    /**
     * Multi-valued numerics are NOT handled via Lucene's {@code SortedNumericDocValues}
     * (which sorts values within a doc) because this format's lineage is binary doc values
     * and must preserve insertion order. Mappers that need multi-valued numerics encode
     * the values into a binary payload (per-doc length-prefixed long sequence) and route
     * through {@link #addBinaryField}; this codec stores the bytes faithfully. The
     * {@code _search}-side bridge that translates back to a {@code SortedNumericDocValues}
     * view for legacy aggregation code lives downstream of this codec, not inside it.
     */
    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw new UnsupportedOperationException(
            "ColumNARDocValuesFormat does not accept SortedNumericDocValues — its insertion "
                + "order is not preserved. Pack multi-valued numerics into a BinaryDocValuesField "
                + "and route through addBinaryField."
        );
    }

    /**
     * Sorted-set doc values combine ordinals and sort semantics; this format treats both
     * as off-limits for the public read API (ordinals are encoder-internal, insertion
     * order is the format's lineage). Mappers route multi-valued string fields through
     * {@link #addBinaryField} with the values packed into one per-doc payload preserving
     * insertion order; the codec stores the bytes faithfully.
     */
    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw new UnsupportedOperationException(
            "ColumNARDocValuesFormat does not accept SortedSetDocValues — neither sort "
                + "semantics nor ordinal exposure fit this format's lineage. Pack multi-valued "
                + "string fields into a BinaryDocValuesField (insertion-order preserving) and "
                + "route through addBinaryField."
        );
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(ColumNARDocValuesFormat.META_FIELD_END_SENTINEL);
                CodecUtil.writeFooter(meta);
            }
            if (data != null) {
                CodecUtil.writeFooter(data);
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
        }
    }
}
