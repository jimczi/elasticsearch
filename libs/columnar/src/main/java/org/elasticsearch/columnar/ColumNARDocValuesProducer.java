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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.columnar.encoder.BlockEncoding;
import org.elasticsearch.columnar.encoder.BlockEncodingRegistry;
import org.elasticsearch.columnar.encoder.BytesBlockEncoder;
import org.elasticsearch.columnar.encoder.BytesBlockEncoderRegistry;
import org.elasticsearch.columnar.encoder.NumericBlockEncoder;
import org.elasticsearch.columnar.encoder.NumericBlockEncoderRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reader half of {@link ColumNARDocValuesFormat}. Serves dense binary fields via
 * {@link #getBinary} — direct ({@code FIELD_TYPE_BINARY}) or dictionary-encoded
 * ({@code FIELD_TYPE_DICT_BINARY}). All other Lucene doc-values shapes (numeric / sorted /
 * sorted-numeric / sorted-set) return {@code null} or throw — the format is binary-only at
 * the Lucene API surface; typed views over the binary substrate live in
 * {@code org.elasticsearch.columnar.bridge}.
 *
 * <p><b>Off-heap block tables.</b> Per-field on-heap state is just a small summary record
 * (encoder/encoding ids, value count, block count, precomputed global stats, scratch sizing
 * hints, and the absolute file offset of the field's block table). Every per-block field —
 * data-file offset, payload length, encoded length, value count — lives in the mmap'd
 * metadata file and is read on demand. Sequential reads pay one fixed-size mmap read per
 * block boundary; random access pays the same. Heap use for block metadata is constant
 * regardless of segment size.
 */
final class ColumNARDocValuesProducer extends DocValuesProducer implements Accountable {

    /**
     * Bytes per record in a numeric block table: {@code dataOffset (long) + payloadLen (int)
     * + encodedLen (int) + valuesInBlock (int)}. Used by the dict-binary read path for its
     * ordinal blocks (which run through the numeric encoder pipeline).
     */
    static final int NUMERIC_BLOCK_RECORD_SIZE = 8 + 4 + 4 + 4;

    /**
     * Bytes per record in a binary field's block table: {@code dataOffset (long) +
     * payloadLen (int) + encodedLen (int) + valuesInBlock (int) + totalValueBytes (int)}.
     */
    static final int BINARY_BLOCK_RECORD_SIZE = 8 + 4 + 4 + 4 + 4;

    /** Bytes per binary per-field summary record: {@code maxPayloadLen (int) + maxTotalValueBytes (int)}. */
    static final int BINARY_SUMMARY_RECORD_SIZE = 4 + 4;

    private final IndexInput data;
    // Second handle on the .cdvm file kept open for the producer's lifetime — used for random
    // access reads of fixed-size block-table records. Cloned per NumericDocValues / BinaryDocValues
    // / DocValuesSkipper instance so each reader has its own seek position.
    private final IndexInput metaData;
    // The format version this segment was written with. Threaded into every NumericBlockEncoder /
    // BlockEncoding / BytesBlockEncoder decode call so an encoder that opted into the
    // same-id evolution path can branch on it.
    private final int formatVersion;
    private final Map<Integer, BinaryFieldMeta> binaryFields;
    private final Map<Integer, DictBinaryFieldMeta> dictBinaryFields;
    private final Map<Integer, PackedLongFieldMeta> packedLongFields;
    private final Map<Integer, PackedLongsMVFieldMeta> packedLongsMVFields;
    private final int maxDoc;

    ColumNARDocValuesProducer(SegmentReadState state) throws IOException {
        this.maxDoc = state.segmentInfo.maxDoc();
        boolean success = false;
        IndexInput dataIn = null;
        IndexInput metaInRandom = null;
        try {
            final String metaName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.META_EXTENSION
            );
            binaryFields = new HashMap<>();
            dictBinaryFields = new HashMap<>();
            packedLongFields = new HashMap<>();
            packedLongsMVFields = new HashMap<>();
            int seenFormatVersion = -1;
            try (ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaName)) {
                Throwable priorE = null;
                try {
                    seenFormatVersion = CodecUtil.checkIndexHeader(
                        metaIn,
                        ColumNARDocValuesFormat.META_CODEC,
                        ColumNARDocValuesFormat.VERSION_START,
                        ColumNARDocValuesFormat.VERSION_CURRENT,
                        state.segmentInfo.getId(),
                        state.segmentSuffix
                    );
                    // Block size is a per-field knob — read by readBinaryFieldMeta /
                    // readDictBinaryFieldMeta from each field's header.
                    readFields(metaIn);
                } catch (Throwable t) {
                    priorE = t;
                } finally {
                    CodecUtil.checkFooter(metaIn, priorE);
                }
            }
            this.formatVersion = seenFormatVersion;

            // Second handle on .cdvm for on-demand block-table reads. Stays open with the producer.
            metaInRandom = state.directory.openInput(metaName, state.context);

            final String dataName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.DATA_EXTENSION
            );
            dataIn = state.directory.openInput(dataName, state.context);
            CodecUtil.checkIndexHeader(
                dataIn,
                ColumNARDocValuesFormat.DATA_CODEC,
                ColumNARDocValuesFormat.VERSION_START,
                ColumNARDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(dataIn);

            this.data = dataIn;
            this.metaData = metaInRandom;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(dataIn, metaInRandom);
            }
        }
    }

    private void readFields(ChecksumIndexInput in) throws IOException {
        while (true) {
            final int fieldNumber = in.readInt();
            if (fieldNumber == ColumNARDocValuesFormat.META_FIELD_END_SENTINEL) {
                return;
            }
            final byte fieldType = in.readByte();
            switch (fieldType) {
                case ColumNARDocValuesFormat.FIELD_TYPE_BINARY -> binaryFields.put(fieldNumber, readBinaryFieldMeta(in));
                case ColumNARDocValuesFormat.FIELD_TYPE_DICT_BINARY -> dictBinaryFields.put(fieldNumber, readDictBinaryFieldMeta(in));
                case ColumNARDocValuesFormat.FIELD_TYPE_PACKED_LONG -> packedLongFields.put(fieldNumber, readPackedLongFieldMeta(in));
                case ColumNARDocValuesFormat.FIELD_TYPE_PACKED_LONGS_MV -> packedLongsMVFields.put(
                    fieldNumber,
                    readPackedLongsMVFieldMeta(in)
                );
                default -> throw new IOException("unknown field type " + fieldType + " for field " + fieldNumber);
            }
        }
    }

    private static BinaryFieldMeta readBinaryFieldMeta(ChecksumIndexInput in) throws IOException {
        final String encoderName = in.readString();
        final String encodingName = in.readString();
        final int blockSize = in.readVInt();
        if (blockSize <= 0) {
            throw new IOException("invalid blockSize " + blockSize);
        }
        final long valueCount = in.readVLong();
        final int blockCount = in.readVInt();
        final long summaryOffset = in.getFilePointer();
        in.skipBytes(BINARY_SUMMARY_RECORD_SIZE);
        final long blockTableOffset = in.getFilePointer();
        in.skipBytes((long) blockCount * BINARY_BLOCK_RECORD_SIZE);
        return new BinaryFieldMeta(encoderName, encodingName, blockSize, valueCount, blockCount, summaryOffset, blockTableOffset);
    }

    private static PackedLongFieldMeta readPackedLongFieldMeta(ChecksumIndexInput in) throws IOException {
        final String encoderName = in.readString();
        final String encodingName = in.readString();
        final int blockSize = in.readVInt();
        if (blockSize <= 0) {
            throw new IOException("invalid blockSize " + blockSize);
        }
        final long valueCount = in.readVLong();
        final int blockCount = in.readVInt();
        final int maxPayloadLen = in.readInt();
        final long blockTableOffset = in.getFilePointer();
        in.skipBytes((long) blockCount * NUMERIC_BLOCK_RECORD_SIZE);
        return new PackedLongFieldMeta(encoderName, encodingName, blockSize, valueCount, blockCount, maxPayloadLen, blockTableOffset);
    }

    private static PackedLongsMVFieldMeta readPackedLongsMVFieldMeta(ChecksumIndexInput in) throws IOException {
        final String valueEncoderName = in.readString();
        final String countEncoderName = in.readString();
        final String encodingName = in.readString();
        final int blockSize = in.readVInt();
        if (blockSize <= 0) {
            throw new IOException("invalid blockSize " + blockSize);
        }
        final int docCount = in.readVInt();
        final long totalValueCount = in.readVLong();
        final int maxValuesPerDoc = in.readInt();
        final int valueBlockCount = in.readVInt();
        final int maxValuePayloadLen = in.readInt();
        final int countBlockCount = in.readVInt();
        final int maxCountPayloadLen = in.readInt();
        final long valueBlockTableOffset = in.getFilePointer();
        in.skipBytes((long) valueBlockCount * NUMERIC_BLOCK_RECORD_SIZE);
        final long countBlockTableOffset = in.getFilePointer();
        in.skipBytes((long) countBlockCount * NUMERIC_BLOCK_RECORD_SIZE);
        return new PackedLongsMVFieldMeta(
            valueEncoderName,
            countEncoderName,
            encodingName,
            blockSize,
            docCount,
            totalValueCount,
            maxValuesPerDoc,
            valueBlockCount,
            maxValuePayloadLen,
            countBlockCount,
            maxCountPayloadLen,
            valueBlockTableOffset,
            countBlockTableOffset
        );
    }

    private static DictBinaryFieldMeta readDictBinaryFieldMeta(ChecksumIndexInput in) throws IOException {
        final String encoderName = in.readString();
        final String encodingName = in.readString();
        final int blockSize = in.readVInt();
        if (blockSize <= 0) {
            throw new IOException("invalid blockSize " + blockSize);
        }
        final long valueCount = in.readVLong();
        final int blockCount = in.readVInt();
        final int maxPayloadLen = in.readInt();
        // Dictionary — small (≤ DICT_BINARY_MAX_DICT_SIZE entries). Loaded eagerly because
        // its on-heap footprint is bounded; everything else stays in mmap.
        final int dictSize = in.readVInt();
        final byte[][] dict = new byte[dictSize][];
        for (int i = 0; i < dictSize; i++) {
            final int len = in.readVInt();
            dict[i] = new byte[len];
            in.readBytes(dict[i], 0, len);
        }
        final long blockTableOffset = in.getFilePointer();
        in.skipBytes((long) blockCount * NUMERIC_BLOCK_RECORD_SIZE);
        return new DictBinaryFieldMeta(encoderName, encodingName, blockSize, valueCount, blockCount, maxPayloadLen, blockTableOffset, dict);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) {
        // The format is binary-only at the Lucene API surface; numeric writes throw UOE on
        // the consumer side, so no field is ever a numeric DV. Typed long views over the
        // binary substrate are served via the bridge (ColumNARLongValues + the typed Field
        // wrappers in org.elasticsearch.columnar.bridge).
        return null;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        // Bridge-written single-valued long fields first: payloads were stored as longs and
        // run through the numeric pipeline; we re-pack the 'L' shape on read so callers
        // (including PackedLongsFromBinaryDocValues) see the same bytes the bridge wrote.
        final PackedLongFieldMeta longMeta = packedLongFields.get(field.number);
        if (longMeta != null) {
            final NumericBlockEncoder encoder = NumericBlockEncoderRegistry.forName(longMeta.encoderName);
            if (encoder == null) {
                throw new IOException(
                    "unknown NumericBlockEncoder id "
                        + longMeta.encoderName
                        + " for packed-long field "
                        + field.name
                        + "; downstream modules must register their encoder via ServiceLoader"
                );
            }
            final BlockEncoding encoding = resolveEncoding(longMeta.encodingName, field.name);
            return new ColumnarPackedLongDocValues(formatVersion, data.clone(), metaData.clone(), longMeta, encoder, encoding);
        }
        final PackedLongsMVFieldMeta mvMeta = packedLongsMVFields.get(field.number);
        if (mvMeta != null) {
            final NumericBlockEncoder valueEnc = NumericBlockEncoderRegistry.forName(mvMeta.valueEncoderName);
            final NumericBlockEncoder countEnc = NumericBlockEncoderRegistry.forName(mvMeta.countEncoderName);
            if (valueEnc == null || countEnc == null) {
                throw new IOException(
                    "unknown NumericBlockEncoder id (value="
                        + mvMeta.valueEncoderName
                        + ", count="
                        + mvMeta.countEncoderName
                        + ") for packed-longs-mv field "
                        + field.name
                        + "; downstream modules must register their encoder via ServiceLoader"
                );
            }
            final BlockEncoding encoding = resolveEncoding(mvMeta.encodingName, field.name);
            return new ColumnarPackedLongsMVDocValues(formatVersion, data.clone(), metaData.clone(), mvMeta, valueEnc, countEnc, encoding);
        }
        // Try the dict-binary path; falls through to raw if the field wasn't written
        // through the dictionary path.
        final DictBinaryFieldMeta dictMeta = dictBinaryFields.get(field.number);
        if (dictMeta != null) {
            final NumericBlockEncoder ordEncoder = NumericBlockEncoderRegistry.forName(dictMeta.encoderName);
            if (ordEncoder == null) {
                throw new IOException(
                    "unknown NumericBlockEncoder id "
                        + dictMeta.encoderName
                        + " for dict-binary field "
                        + field.name
                        + "; downstream modules must register their encoder via ServiceLoader"
                );
            }
            final BlockEncoding encoding = resolveEncoding(dictMeta.encodingName, field.name);
            return new ColumnarDictBinaryDocValues(
                formatVersion,
                data.clone(),
                metaData.clone(),
                dictMeta,
                dictMeta.blockSize,
                ordEncoder,
                encoding
            );
        }
        final BinaryFieldMeta meta = binaryFields.get(field.number);
        if (meta == null) {
            return null;
        }
        final BytesBlockEncoder encoder = BytesBlockEncoderRegistry.forName(meta.encoderName);
        if (encoder == null) {
            throw new IOException(
                "unknown BytesBlockEncoder id "
                    + meta.encoderName
                    + " for binary field "
                    + field.name
                    + "; downstream modules must register their encoder via ServiceLoader"
            );
        }
        final BlockEncoding encoding = resolveEncoding(meta.encodingName, field.name);
        return new ColumnarBinaryDocValues(formatVersion, data.clone(), metaData.clone(), meta, meta.blockSize, encoder, encoding);
    }

    private static BlockEncoding resolveEncoding(String name, String fieldName) throws IOException {
        final BlockEncoding encoding = BlockEncodingRegistry.forName(name);
        if (encoding == null) {
            throw new IOException(
                "unknown BlockEncoding name "
                    + name
                    + " for field "
                    + fieldName
                    + "; downstream modules must register their encoding via ServiceLoader"
            );
        }
        return encoding;
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw new UnsupportedOperationException("ColumNARDocValuesFormat v0 does not support sorted doc values yet");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw new UnsupportedOperationException("ColumNARDocValuesFormat v0 does not support sorted numeric doc values yet");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw new UnsupportedOperationException("ColumNARDocValuesFormat v0 does not support sorted set doc values yet");
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        // Skip indexes are wired only on the numeric DV path, which is no longer reachable
        // (binary-only writes). Bridge-side skippers over the binary substrate are tracked
        // outside this method.
        return null;
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(data, metaData);
    }

    /** Constant on-heap footprint per field map entry — set once at class init via RamUsageEstimator. */
    private static final long BINARY_FIELD_META_BYTES = RamUsageEstimator.shallowSizeOfInstance(BinaryFieldMeta.class);
    private static final long BASE_BYTES = RamUsageEstimator.shallowSizeOfInstance(ColumNARDocValuesProducer.class);

    /**
     * On-heap bytes held by this producer. Per-block metadata stays <strong>off-heap</strong>
     * (the {@code .cdvm} block table is read on demand from mmap) and per-field summary stats
     * stay <strong>off-heap</strong> (lazy-read from the fixed-size summary record). What
     * stays on heap is the small per-field handle ({@code encoderName} / {@code encodingName} /
     * {@code valueCount} / {@code blockCount} / {@code summaryOffset} / {@code blockTableOffset})
     * plus the HashMap entries.
     */
    @Override
    public long ramBytesUsed() {
        long bytes = BASE_BYTES;
        final long binaryEntry = BINARY_FIELD_META_BYTES + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;
        bytes += (long) binaryFields.size() * binaryEntry;
        return bytes;
    }

    private static final class BinaryFieldMeta {
        final String encoderName;
        final String encodingName;
        final int blockSize;
        final long valueCount;
        final int blockCount;
        final long summaryOffset;
        final long blockTableOffset;

        BinaryFieldMeta(
            String encoderName,
            String encodingName,
            int blockSize,
            long valueCount,
            int blockCount,
            long summaryOffset,
            long blockTableOffset
        ) {
            this.encoderName = encoderName;
            this.encodingName = encodingName;
            this.blockSize = blockSize;
            this.valueCount = valueCount;
            this.blockCount = blockCount;
            this.summaryOffset = summaryOffset;
            this.blockTableOffset = blockTableOffset;
        }
    }

    /**
     * Holds the fields of one numeric block-table record — used inline by the dict-binary
     * read path for its ordinal blocks (the ordinals run through the numeric encoder
     * pipeline, so they share the {@link #NUMERIC_BLOCK_RECORD_SIZE} layout).
     */
    private static final class NumericBlockRecord {
        long dataOffset;
        int payloadLen;
        int encodedLen;
        int valuesInBlock;
    }

    private static final class BinaryBlockRecord {
        long dataOffset;
        int payloadLen;
        int encodedLen;
        int valuesInBlock;
        int totalValueBytes;
    }

    private static void readBinaryBlockRecord(IndexInput metaData, BinaryFieldMeta meta, int blockIdx, BinaryBlockRecord out)
        throws IOException {
        metaData.seek(meta.blockTableOffset + (long) blockIdx * BINARY_BLOCK_RECORD_SIZE);
        out.dataOffset = metaData.readLong();
        out.payloadLen = metaData.readInt();
        out.encodedLen = metaData.readInt();
        out.valuesInBlock = metaData.readInt();
        out.totalValueBytes = metaData.readInt();
    }

    private static final class ColumnarBinaryDocValues extends BinaryDocValues {
        private final int formatVersion;
        private final IndexInput data;
        private final IndexInput metaData;
        private final BinaryFieldMeta meta;
        private final int blockSize;
        private final BytesBlockEncoder encoder;
        private final BlockEncoding encoding;
        // Caller-owned scratch buffers, allocated once and reused. payloadScratch holds the
        // BlockEncoding's decoded output (ignored when the encoding is identity); currentBytes
        // and currentOffsets hold the flat layout of the current block's decoded values.
        private final byte[] payloadScratch;
        private byte[] currentBytes;
        private final int[] currentOffsets;
        private final BytesRef ref;
        private final BinaryBlockRecord blockRecord = new BinaryBlockRecord();

        private int currentBlockIndex = -1;
        private int doc = -1;

        ColumnarBinaryDocValues(
            int formatVersion,
            IndexInput data,
            IndexInput metaData,
            BinaryFieldMeta meta,
            int blockSize,
            BytesBlockEncoder encoder,
            BlockEncoding encoding
        ) throws IOException {
            this.formatVersion = formatVersion;
            this.data = data;
            this.metaData = metaData;
            this.meta = meta;
            this.blockSize = blockSize;
            this.encoder = encoder;
            this.encoding = encoding;
            // maxPayloadLen / maxTotalValueBytes live off-heap in the per-field summary record.
            // Read once on construction; the mmap'd page is hot so this is essentially free.
            metaData.seek(meta.summaryOffset);
            final int maxPayloadLen = metaData.readInt();
            final int maxTotalValueBytes = metaData.readInt();
            this.payloadScratch = new byte[Math.max(64, maxPayloadLen)];
            this.currentBytes = new byte[ArrayUtil.oversize(Math.max(64, maxTotalValueBytes), Byte.BYTES)];
            this.currentOffsets = new int[blockSize + 1];
            this.ref = new BytesRef();
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            final int blockIdx = doc / blockSize;
            if (blockIdx != currentBlockIndex) {
                loadBlock(blockIdx);
            }
            final int posInBlock = doc % blockSize;
            ref.bytes = currentBytes;
            ref.offset = currentOffsets[posInBlock];
            ref.length = currentOffsets[posInBlock + 1] - currentOffsets[posInBlock];
            return ref;
        }

        private void loadBlock(int blockIdx) throws IOException {
            readBinaryBlockRecord(metaData, meta, blockIdx, blockRecord);
            data.seek(blockRecord.dataOffset);
            if (currentBytes.length < blockRecord.totalValueBytes) {
                currentBytes = new byte[ArrayUtil.oversize(blockRecord.totalValueBytes, Byte.BYTES)];
            }
            final DataInput payload = encoding.decode(formatVersion, data, blockRecord.encodedLen, payloadScratch, blockRecord.payloadLen);
            encoder.decode(formatVersion, payload, currentBytes, 0, currentOffsets, 0, blockRecord.valuesInBlock);
            currentBlockIndex = blockIdx;
        }

        @Override
        public boolean advanceExact(int target) {
            if (target < 0 || target >= meta.valueCount) {
                doc = (int) meta.valueCount;
                return false;
            }
            doc = target;
            return true;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            if (doc >= meta.valueCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public int advance(int target) {
            if (target >= meta.valueCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            doc = target;
            return doc;
        }

        @Override
        public long cost() {
            return meta.valueCount;
        }
    }

    /**
     * On-heap state for a dictionary-binary field. The dictionary is loaded eagerly on
     * open (its size is bounded by {@link ColumNARDocValuesFormat#DICT_BINARY_MAX_DICT_SIZE});
     * the per-block ordinals stay in the mmap'd data file and decode lazily on advance.
     */
    private static final class DictBinaryFieldMeta {
        final String encoderName;
        final String encodingName;
        final int blockSize;
        final long valueCount;
        final int blockCount;
        final int maxPayloadLen;
        final long blockTableOffset;
        final byte[][] dict;

        DictBinaryFieldMeta(
            String encoderName,
            String encodingName,
            int blockSize,
            long valueCount,
            int blockCount,
            int maxPayloadLen,
            long blockTableOffset,
            byte[][] dict
        ) {
            this.encoderName = encoderName;
            this.encodingName = encodingName;
            this.blockSize = blockSize;
            this.valueCount = valueCount;
            this.blockCount = blockCount;
            this.maxPayloadLen = maxPayloadLen;
            this.blockTableOffset = blockTableOffset;
            this.dict = dict;
        }
    }

    /**
     * Read path for {@link ColumNARDocValuesFormat#FIELD_TYPE_DICT_BINARY}. Decodes per-block
     * ordinals via the numeric pipeline (NumericBlockEncoder + BlockEncoding) and materialises bytes
     * per doc through the per-segment dictionary.
     */
    private static final class ColumnarDictBinaryDocValues extends BinaryDocValues {
        private final int formatVersion;
        private final IndexInput data;
        private final IndexInput metaData;
        private final DictBinaryFieldMeta meta;
        private final int blockSize;
        private final NumericBlockEncoder encoder;
        private final BlockEncoding encoding;
        private final long[] currentOrdinals;
        private final byte[] payloadScratch;
        private final long[] decoderScratch;
        private final NumericBlockRecord blockRecord = new NumericBlockRecord();
        private final BytesRef ref = new BytesRef();
        private int currentBlockIndex = -1;
        private int doc = -1;

        ColumnarDictBinaryDocValues(
            int formatVersion,
            IndexInput data,
            IndexInput metaData,
            DictBinaryFieldMeta meta,
            int blockSize,
            NumericBlockEncoder encoder,
            BlockEncoding encoding
        ) {
            this.formatVersion = formatVersion;
            this.data = data;
            this.metaData = metaData;
            this.meta = meta;
            this.blockSize = blockSize;
            this.encoder = encoder;
            this.encoding = encoding;
            this.currentOrdinals = new long[blockSize];
            this.payloadScratch = new byte[Math.max(encoder.maxEncodedSize(blockSize), meta.maxPayloadLen)];
            final int scratchLongs = encoder.scratchLongs(blockSize);
            this.decoderScratch = scratchLongs > 0 ? new long[scratchLongs] : null;
        }

        private void loadBlock(int blockIdx) throws IOException {
            metaData.seek(meta.blockTableOffset + (long) blockIdx * NUMERIC_BLOCK_RECORD_SIZE);
            blockRecord.dataOffset = metaData.readLong();
            blockRecord.payloadLen = metaData.readInt();
            blockRecord.encodedLen = metaData.readInt();
            blockRecord.valuesInBlock = metaData.readInt();
            data.seek(blockRecord.dataOffset);
            final DataInput payload = encoding.decode(formatVersion, data, blockRecord.encodedLen, payloadScratch, blockRecord.payloadLen);
            encoder.decode(formatVersion, payload, currentOrdinals, 0, blockRecord.valuesInBlock, decoderScratch);
            currentBlockIndex = blockIdx;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            final int blockIdx = doc / blockSize;
            if (blockIdx != currentBlockIndex) {
                loadBlock(blockIdx);
            }
            final int posInBlock = doc - blockIdx * blockSize;
            final int ord = (int) currentOrdinals[posInBlock];
            ref.bytes = meta.dict[ord];
            ref.offset = 0;
            ref.length = meta.dict[ord].length;
            return ref;
        }

        @Override
        public boolean advanceExact(int target) {
            if (target < 0 || target >= meta.valueCount) {
                doc = (int) meta.valueCount;
                return false;
            }
            doc = target;
            return true;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            if (doc >= meta.valueCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public int advance(int target) {
            if (target >= meta.valueCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            doc = target;
            return doc;
        }

        @Override
        public long cost() {
            return meta.valueCount;
        }
    }

    private static final class PackedLongFieldMeta {
        final String encoderName;
        final String encodingName;
        final int blockSize;
        final long valueCount;
        final int blockCount;
        final int maxPayloadLen;
        final long blockTableOffset;

        PackedLongFieldMeta(
            String encoderName,
            String encodingName,
            int blockSize,
            long valueCount,
            int blockCount,
            int maxPayloadLen,
            long blockTableOffset
        ) {
            this.encoderName = encoderName;
            this.encodingName = encodingName;
            this.blockSize = blockSize;
            this.valueCount = valueCount;
            this.blockCount = blockCount;
            this.maxPayloadLen = maxPayloadLen;
            this.blockTableOffset = blockTableOffset;
        }
    }

    /**
     * Read path for {@link ColumNARDocValuesFormat#FIELD_TYPE_PACKED_LONG}. Decodes per-block
     * longs through the numeric pipeline, then re-packs each doc's long into the
     * {@code [byte 'L'][vint 1][LE long]} payload the bridge expects. The 10-byte output
     * buffer is allocated once and reused.
     */
    private static final class ColumnarPackedLongDocValues extends BinaryDocValues {
        private final int formatVersion;
        private final IndexInput data;
        private final IndexInput metaData;
        private final PackedLongFieldMeta meta;
        private final int blockSize;
        private final NumericBlockEncoder encoder;
        private final BlockEncoding encoding;
        private final long[] currentValues;
        private final byte[] payloadScratch;
        private final long[] decoderScratch;
        private final NumericBlockRecord blockRecord = new NumericBlockRecord();
        private final byte[] outBuf = new byte[10];
        private final BytesRef ref = new BytesRef();
        private int currentBlockIndex = -1;
        private int doc = -1;

        ColumnarPackedLongDocValues(
            int formatVersion,
            IndexInput data,
            IndexInput metaData,
            PackedLongFieldMeta meta,
            NumericBlockEncoder encoder,
            BlockEncoding encoding
        ) {
            this.formatVersion = formatVersion;
            this.data = data;
            this.metaData = metaData;
            this.meta = meta;
            this.blockSize = meta.blockSize;
            this.encoder = encoder;
            this.encoding = encoding;
            this.currentValues = new long[blockSize];
            this.payloadScratch = new byte[Math.max(encoder.maxEncodedSize(blockSize), meta.maxPayloadLen)];
            final int scratchLongs = encoder.scratchLongs(blockSize);
            this.decoderScratch = scratchLongs > 0 ? new long[scratchLongs] : null;
            // The 'L' marker and vint(1) count are constant per doc; only the long bytes
            // change. Pre-write the shape header once.
            outBuf[0] = (byte) 'L';
            outBuf[1] = 0x01;
            ref.bytes = outBuf;
            ref.offset = 0;
            ref.length = 10;
        }

        private void loadBlock(int blockIdx) throws IOException {
            metaData.seek(meta.blockTableOffset + (long) blockIdx * NUMERIC_BLOCK_RECORD_SIZE);
            blockRecord.dataOffset = metaData.readLong();
            blockRecord.payloadLen = metaData.readInt();
            blockRecord.encodedLen = metaData.readInt();
            blockRecord.valuesInBlock = metaData.readInt();
            data.seek(blockRecord.dataOffset);
            final DataInput payload = encoding.decode(formatVersion, data, blockRecord.encodedLen, payloadScratch, blockRecord.payloadLen);
            encoder.decode(formatVersion, payload, currentValues, 0, blockRecord.valuesInBlock, decoderScratch);
            currentBlockIndex = blockIdx;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            final int blockIdx = doc / blockSize;
            if (blockIdx != currentBlockIndex) {
                loadBlock(blockIdx);
            }
            writeLongLE(outBuf, 2, currentValues[doc - blockIdx * blockSize]);
            return ref;
        }

        private static void writeLongLE(byte[] arr, int off, long v) {
            arr[off] = (byte) v;
            arr[off + 1] = (byte) (v >>> 8);
            arr[off + 2] = (byte) (v >>> 16);
            arr[off + 3] = (byte) (v >>> 24);
            arr[off + 4] = (byte) (v >>> 32);
            arr[off + 5] = (byte) (v >>> 40);
            arr[off + 6] = (byte) (v >>> 48);
            arr[off + 7] = (byte) (v >>> 56);
        }

        @Override
        public boolean advanceExact(int target) {
            if (target < 0 || target >= meta.valueCount) {
                doc = (int) meta.valueCount;
                return false;
            }
            doc = target;
            return true;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            if (doc >= meta.valueCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public int advance(int target) {
            if (target >= meta.valueCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            doc = target;
            return doc;
        }

        @Override
        public long cost() {
            return meta.valueCount;
        }
    }

    private static final class PackedLongsMVFieldMeta {
        final String valueEncoderName;
        final String countEncoderName;
        final String encodingName;
        final int blockSize;
        final int docCount;
        final long totalValueCount;
        final int maxValuesPerDoc;
        final int valueBlockCount;
        final int maxValuePayloadLen;
        final int countBlockCount;
        final int maxCountPayloadLen;
        final long valueBlockTableOffset;
        final long countBlockTableOffset;

        PackedLongsMVFieldMeta(
            String valueEncoderName,
            String countEncoderName,
            String encodingName,
            int blockSize,
            int docCount,
            long totalValueCount,
            int maxValuesPerDoc,
            int valueBlockCount,
            int maxValuePayloadLen,
            int countBlockCount,
            int maxCountPayloadLen,
            long valueBlockTableOffset,
            long countBlockTableOffset
        ) {
            this.valueEncoderName = valueEncoderName;
            this.countEncoderName = countEncoderName;
            this.encodingName = encodingName;
            this.blockSize = blockSize;
            this.docCount = docCount;
            this.totalValueCount = totalValueCount;
            this.maxValuesPerDoc = maxValuesPerDoc;
            this.valueBlockCount = valueBlockCount;
            this.maxValuePayloadLen = maxValuePayloadLen;
            this.countBlockCount = countBlockCount;
            this.maxCountPayloadLen = maxCountPayloadLen;
            this.valueBlockTableOffset = valueBlockTableOffset;
            this.countBlockTableOffset = countBlockTableOffset;
        }
    }

    /**
     * Read path for {@link ColumNARDocValuesFormat#FIELD_TYPE_PACKED_LONGS_MV}. Decodes
     * count and value blocks on demand from the numeric pipeline, walks the count stream
     * forward (matching Lucene's forward-only DocValuesIterator contract) to map doc id →
     * value-stream offset, and re-packs the {@code [byte 'L'][vint count][LE long]*count}
     * payload the bridge expects on every {@code binaryValue()} call.
     */
    private static final class ColumnarPackedLongsMVDocValues extends BinaryDocValues {
        private final int formatVersion;
        private final IndexInput data;
        private final IndexInput metaData;
        private final PackedLongsMVFieldMeta meta;
        private final int blockSize;
        private final NumericBlockEncoder valueEncoder;
        private final NumericBlockEncoder countEncoder;
        private final BlockEncoding encoding;

        // Cached blocks. valueCursor/doc invariants are maintained by advanceExact/nextDoc.
        private final long[] currentValues;
        private final long[] currentCounts;
        private final byte[] payloadScratch;
        private final long[] valueDecoderScratch;
        private final long[] countDecoderScratch;
        private final NumericBlockRecord blockRecord = new NumericBlockRecord();
        private int currentValueBlock = -1;
        private int currentCountBlock = -1;

        // Re-pack scratch for the 'L'-shape payload. Sized for the largest doc.
        private final byte[] outBuf;
        private final BytesRef ref = new BytesRef();

        private int doc = -1;
        // sum of counts[0..doc-1] — start position of the current doc's values in the
        // global value stream. After advanceExact(target) returns true, valueCursor points
        // at target's values.
        private long valueCursor = 0;

        ColumnarPackedLongsMVDocValues(
            int formatVersion,
            IndexInput data,
            IndexInput metaData,
            PackedLongsMVFieldMeta meta,
            NumericBlockEncoder valueEncoder,
            NumericBlockEncoder countEncoder,
            BlockEncoding encoding
        ) {
            this.formatVersion = formatVersion;
            this.data = data;
            this.metaData = metaData;
            this.meta = meta;
            this.blockSize = meta.blockSize;
            this.valueEncoder = valueEncoder;
            this.countEncoder = countEncoder;
            this.encoding = encoding;
            this.currentValues = new long[blockSize];
            this.currentCounts = new long[blockSize];
            final int scratchBytes = Math.max(
                Math.max(valueEncoder.maxEncodedSize(blockSize), countEncoder.maxEncodedSize(blockSize)),
                Math.max(meta.maxValuePayloadLen, meta.maxCountPayloadLen)
            );
            this.payloadScratch = new byte[scratchBytes];
            final int vScratch = valueEncoder.scratchLongs(blockSize);
            this.valueDecoderScratch = vScratch > 0 ? new long[vScratch] : null;
            final int cScratch = countEncoder.scratchLongs(blockSize);
            this.countDecoderScratch = cScratch > 0 ? new long[cScratch] : null;
            // Output buffer sized for the largest doc: marker (1) + vint count (≤5) + 8*max values.
            this.outBuf = new byte[1 + 5 + 8 * meta.maxValuesPerDoc];
            outBuf[0] = (byte) 'L';
            ref.bytes = outBuf;
            ref.offset = 0;
        }

        private void loadValueBlock(int blockIdx) throws IOException {
            metaData.seek(meta.valueBlockTableOffset + (long) blockIdx * NUMERIC_BLOCK_RECORD_SIZE);
            blockRecord.dataOffset = metaData.readLong();
            blockRecord.payloadLen = metaData.readInt();
            blockRecord.encodedLen = metaData.readInt();
            blockRecord.valuesInBlock = metaData.readInt();
            data.seek(blockRecord.dataOffset);
            final DataInput payload = encoding.decode(formatVersion, data, blockRecord.encodedLen, payloadScratch, blockRecord.payloadLen);
            valueEncoder.decode(formatVersion, payload, currentValues, 0, blockRecord.valuesInBlock, valueDecoderScratch);
            currentValueBlock = blockIdx;
        }

        private void loadCountBlock(int blockIdx) throws IOException {
            metaData.seek(meta.countBlockTableOffset + (long) blockIdx * NUMERIC_BLOCK_RECORD_SIZE);
            blockRecord.dataOffset = metaData.readLong();
            blockRecord.payloadLen = metaData.readInt();
            blockRecord.encodedLen = metaData.readInt();
            blockRecord.valuesInBlock = metaData.readInt();
            data.seek(blockRecord.dataOffset);
            final DataInput payload = encoding.decode(formatVersion, data, blockRecord.encodedLen, payloadScratch, blockRecord.payloadLen);
            countEncoder.decode(formatVersion, payload, currentCounts, 0, blockRecord.valuesInBlock, countDecoderScratch);
            currentCountBlock = blockIdx;
        }

        private int countAt(int d) throws IOException {
            final int blockIdx = d / blockSize;
            if (blockIdx != currentCountBlock) {
                loadCountBlock(blockIdx);
            }
            return (int) currentCounts[d - blockIdx * blockSize];
        }

        private long valueAt(long valueIdx) throws IOException {
            final int blockIdx = (int) (valueIdx / blockSize);
            if (blockIdx != currentValueBlock) {
                loadValueBlock(blockIdx);
            }
            return currentValues[(int) (valueIdx - (long) blockIdx * blockSize)];
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            final int count = countAt(doc);
            int pos = writeVInt(outBuf, 1, count);
            for (int i = 0; i < count; i++) {
                writeLongLE(outBuf, pos, valueAt(valueCursor + i));
                pos += 8;
            }
            ref.length = pos;
            return ref;
        }

        private static int writeVInt(byte[] arr, int pos, int v) {
            while ((v & ~0x7F) != 0) {
                arr[pos++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            arr[pos++] = (byte) v;
            return pos;
        }

        private static void writeLongLE(byte[] arr, int off, long v) {
            arr[off] = (byte) v;
            arr[off + 1] = (byte) (v >>> 8);
            arr[off + 2] = (byte) (v >>> 16);
            arr[off + 3] = (byte) (v >>> 24);
            arr[off + 4] = (byte) (v >>> 32);
            arr[off + 5] = (byte) (v >>> 40);
            arr[off + 6] = (byte) (v >>> 48);
            arr[off + 7] = (byte) (v >>> 56);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            if (target < 0 || target >= meta.docCount) {
                doc = meta.docCount;
                return false;
            }
            if (target < doc) {
                // Forward-only contract; if a caller breaks it, reset and re-walk from 0.
                doc = -1;
                valueCursor = 0;
            }
            while (doc < target) {
                if (doc >= 0) {
                    valueCursor += countAt(doc);
                }
                doc++;
            }
            return true;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            if (doc >= 0 && doc < meta.docCount) {
                valueCursor += countAt(doc);
            }
            doc++;
            if (doc >= meta.docCount) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            if (advanceExact(target) == false) {
                doc = DocIdSetIterator.NO_MORE_DOCS;
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            return doc;
        }

        @Override
        public long cost() {
            return meta.docCount;
        }
    }
}
