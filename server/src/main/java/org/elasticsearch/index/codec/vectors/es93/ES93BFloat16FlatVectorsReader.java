/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NoReuseHint;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.store.VectorFieldHint;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;
import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;

public final class ES93BFloat16FlatVectorsReader extends FlatVectorsReader {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ES93BFloat16FlatVectorsReader.class);

    private final IntObjectHashMap<FieldEntry> fields;
    private final FlatVectorsScorer vectorScorer;
    private final IndexInput vectorData;
    private final FieldInfos fieldInfos;
    private final IOContext dataContext;
    // what a merge needs to map the vectors for itself
    private final Directory directory;
    private final String vectorDataFN;
    // the reader this one was cloned from, which owns the mapping merges read
    private final ES93BFloat16FlatVectorsReader original;
    private IndexInput mergeVectorData;
    private boolean closed;

    /** Shares everything the merge instance can reuse, over a mapping of its own. */
    private ES93BFloat16FlatVectorsReader(ES93BFloat16FlatVectorsReader reader, IndexInput vectorData) {
        this.fields = reader.fields;
        this.vectorScorer = reader.vectorScorer;
        this.vectorData = vectorData;
        this.fieldInfos = reader.fieldInfos;
        this.dataContext = reader.dataContext;
        this.directory = reader.directory;
        this.vectorDataFN = reader.vectorDataFN;
        this.original = reader.original;
    }

    public ES93BFloat16FlatVectorsReader(SegmentReadState state, FlatVectorsScorer scorer) throws IOException {
        this.fields = new IntObjectHashMap<>();
        int versionMeta = readMetadata(state);
        this.directory = state.directory;
        this.vectorDataFN = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION
        );
        this.original = this;
        this.fieldInfos = state.fieldInfos;
        this.vectorScorer = scorer;
        // Flat formats are used to randomly access vectors from their node ID that is stored
        // in the HNSW graph.
        // the field these vectors belong to, so a directory can look it up in the mapping
        VectorFieldHint field = VectorFieldHint.forSuffix(state.fieldInfos, state.segmentSuffix);
        dataContext = field == null
            ? state.context.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM)
            : state.context.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM, field);
        try {
            vectorData = openDataInput(
                state,
                versionMeta,
                ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION,
                ES93BFloat16FlatVectorsFormat.VECTOR_DATA_CODEC_NAME,
                dataContext
            );
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    private int readMetadata(SegmentReadState state) throws IOException {
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES93BFloat16FlatVectorsFormat.META_EXTENSION
        );
        int versionMeta = -1;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    ES93BFloat16FlatVectorsFormat.META_CODEC_NAME,
                    ES93BFloat16FlatVectorsFormat.VERSION_START,
                    ES93BFloat16FlatVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(meta, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
        }
        return versionMeta;
    }

    private static IndexInput openDataInput(
        SegmentReadState state,
        int versionMeta,
        String fileExtension,
        String codecName,
        IOContext context
    ) throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        IndexInput in = state.directory.openInput(fileName, context);
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                ES93BFloat16FlatVectorsFormat.VERSION_START,
                ES93BFloat16FlatVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (versionMeta != versionVectorData) {
                throw new CorruptIndexException(
                    "Format versions mismatch: meta=" + versionMeta + ", " + codecName + "=" + versionVectorData,
                    in
                );
            }
            CodecUtil.retrieveChecksum(in);
            return in;
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(in);
            throw t;
        }
    }

    private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            FieldEntry fieldEntry = FieldEntry.create(meta, info);
            fields.put(info.number, fieldEntry);
        }
    }

    @Override
    public long ramBytesUsed() {
        return ES93BFloat16FlatVectorsReader.SHALLOW_SIZE + fields.ramBytesUsed();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        final FieldEntry entry = getFieldEntryOrThrow(fieldInfo.name);
        return Map.of(ES93BFloat16FlatVectorsFormat.VECTOR_DATA_EXTENSION, entry.vectorDataLength());
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(vectorData);
    }

    /**
     * A merge copies these vectors front to back. Read advice applies to a whole mapping, so it maps
     * them again rather than re-advising the one searches are reading, and a directory can tell the
     * two opens apart. Closed by {@link #finishMerge()}.
     */
    @Override
    public FlatVectorsReader getMergeInstance() throws IOException {
        return new ES93BFloat16FlatVectorsReader(this, original.mergeVectorData().clone());
    }

    /**
     * The vectors as a merge reads them, front to back. Read advice applies to a whole mapping, so
     * a merge maps them again rather than re-advising the one searches are reading at random, and a
     * directory can tell the two opens apart. Mapped on the first merge and closed with this reader,
     * since merge instances are never closed.
     */
    private synchronized IndexInput mergeVectorData() throws IOException {
        assert original == this;
        ensureOpen();
        if (mergeVectorData == null) {
            if (dataContext.hints(DataAccessHint.class).findFirst().orElse(null) != DataAccessHint.RANDOM) {
                mergeVectorData = vectorData;
            } else {
                try {
                    // withHints replaces the set, so carry over what the reader was opened with
                    var field = dataContext.hints(VectorFieldHint.class).findFirst().orElse(null);
                    mergeVectorData = directory.openInput(
                        vectorDataFN,
                        field == null
                            ? dataContext.withHints(
                                FileTypeHint.DATA,
                                FileDataHint.KNN_VECTORS,
                                DataAccessHint.SEQUENTIAL,
                                NoReuseHint.INSTANCE
                            )
                            : dataContext.withHints(
                                FileTypeHint.DATA,
                                FileDataHint.KNN_VECTORS,
                                DataAccessHint.SEQUENTIAL,
                                NoReuseHint.INSTANCE,
                                field
                            )
                    );
                } catch (FileNotFoundException | NoSuchFileException e) {
                    // an open reader outlives its files, so fall back to the mapping it already holds
                    mergeVectorData = vectorData;
                }
            }
        }
        return mergeVectorData;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new org.apache.lucene.store.AlreadyClosedException("this reader is closed");
        }
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        scoreAndCollectAll(knnCollector, acceptDocs, getFloatVectorValues(field).scorer(target));
    }

    private FieldEntry getFieldEntryOrThrow(String field) {
        final FieldInfo info = fieldInfos.fieldInfo(field);
        final FieldEntry entry;
        if (info == null || (entry = fields.get(info.number)) == null) {
            throw new IllegalArgumentException("field=\"" + field + "\" not found");
        }
        return entry;
    }

    private FieldEntry getFieldEntry(String field, VectorEncoding expectedEncoding) {
        final FieldEntry fieldEntry = getFieldEntryOrThrow(field);
        if (fieldEntry.vectorEncoding != expectedEncoding) {
            throw new IllegalArgumentException(
                "field=\"" + field + "\" is encoded as: " + fieldEntry.vectorEncoding + " expected: " + expectedEncoding
            );
        }
        return fieldEntry;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
        return OffHeapBFloat16VectorValues.load(
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.ordToDoc,
            fieldEntry.vectorEncoding,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            vectorData
        );
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        throw new IllegalStateException(field + " only supports float vectors");
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
        return vectorScorer;
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field, VectorEncoding.FLOAT32);
        return vectorScorer.getRandomVectorScorer(
            fieldEntry.similarityFunction,
            OffHeapBFloat16VectorValues.load(
                fieldEntry.similarityFunction,
                vectorScorer,
                fieldEntry.ordToDoc,
                fieldEntry.vectorEncoding,
                fieldEntry.dimension,
                fieldEntry.size,
                fieldEntry.vectorDataOffset,
                fieldEntry.vectorDataLength,
                vectorData
            ),
            target
        );
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        throw new UnsupportedOperationException(field + " only supports float vectors");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed == false) {
            closed = true;
            IOUtils.close(vectorData, original == this && mergeVectorData != vectorData ? mergeVectorData : null);
        }
    }

    private record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        long vectorDataOffset,
        long vectorDataLength,
        int dimension,
        int size,
        OrdToDocDISIReaderConfiguration ordToDoc,
        FieldInfo info
    ) {

        FieldEntry {
            if (vectorEncoding == VectorEncoding.BYTE) {
                throw new IllegalStateException(
                    "Incorrect vector encoding for field=\"" + info.name + "\"; " + vectorEncoding + " not supported"
                );
            }

            if (similarityFunction != info.getVectorSimilarityFunction()) {
                throw new IllegalStateException(
                    "Inconsistent vector similarity function for field=\""
                        + info.name
                        + "\"; "
                        + similarityFunction
                        + " != "
                        + info.getVectorSimilarityFunction()
                );
            }
            int infoVectorDimension = info.getVectorDimension();
            if (infoVectorDimension != dimension) {
                throw new IllegalStateException(
                    "Inconsistent vector dimension for field=\"" + info.name + "\"; " + infoVectorDimension + " != " + dimension
                );
            }

            int byteSize = BFloat16.BYTES;
            long vectorBytes = Math.multiplyExact((long) infoVectorDimension, byteSize);
            long numBytes = Math.multiplyExact(vectorBytes, size);
            if (numBytes != vectorDataLength) {
                throw new IllegalStateException(
                    "Vector data length "
                        + vectorDataLength
                        + " not matching size="
                        + size
                        + " * dim="
                        + dimension
                        + " * byteSize="
                        + byteSize
                        + " = "
                        + numBytes
                );
            }
        }

        static FieldEntry create(IndexInput input, FieldInfo info) throws IOException {
            final VectorEncoding vectorEncoding = readVectorEncoding(input);
            final VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
            final var vectorDataOffset = input.readVLong();
            final var vectorDataLength = input.readVLong();
            final var dimension = input.readVInt();
            final var size = input.readInt();
            final var ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
            return new FieldEntry(similarityFunction, vectorEncoding, vectorDataOffset, vectorDataLength, dimension, size, ordToDoc, info);
        }
    }
}
