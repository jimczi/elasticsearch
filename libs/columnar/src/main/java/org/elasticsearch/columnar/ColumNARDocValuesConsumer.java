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
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericColumnWriter;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.numeric.NumericPipelineSelector;
import org.elasticsearch.columnar.numeric.SkipIndexCodec;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnValues;
import org.elasticsearch.columnar.string.StringColumnWriter;
import org.elasticsearch.columnar.string.ValueStream;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Writes tagged columns onto the binary substrate; numeric types decode their {@code NumericBinaryPayload}
 * into the long column. Field metadata is flushed on {@link #close()}.
 *
 * <p><b>Merge contract.</b> {@link #mergeBinaryField} re-encodes all source segments through the
 * current writer's pipeline. There is no version-preserving merge and no mixed-version output
 * segment: a force-merge is a silent format upgrade.
 */
final class ColumNARDocValuesConsumer extends DocValuesConsumer {

    private final int maxDoc;
    private final Directory directory;
    private final IOContext context;
    private final IndexOutput data;
    private final IndexOutput meta;
    private final List<FieldEntry> fields = new ArrayList<>();
    private final NumericPipelineSelector pipelineSelector;
    private final int blockSize;

    private final int targetChunkBytes;
    private final DictionaryPolicy dictionaryPolicy;
    private boolean closed = false;

    /** One column's metadata; exactly one of the two shapes is set, chosen by the field's type. */
    private record FieldEntry(int fieldNumber, byte fieldTypeId, NumericColumnMetadata numeric, StringColumnMetadata string) {}

    ColumNARDocValuesConsumer(
        SegmentWriteState state,
        NumericPipelineSelector pipelineSelector,
        int blockSize,
        int targetChunkBytes,
        DictionaryPolicy dictionaryPolicy
    ) throws IOException {
        this.pipelineSelector = pipelineSelector;
        this.blockSize = blockSize;
        this.targetChunkBytes = targetChunkBytes;
        this.dictionaryPolicy = dictionaryPolicy;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.directory = state.directory;
        this.context = state.context;
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.DATA_EXTENSION
            );
            data = state.directory.createOutput(dataName, state.context);
            ColumnarCodecUtil.writeHeader(
                data,
                ColumNARDocValuesFormat.DATA_CODEC,
                FormatVersion.CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            String metaName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.META_EXTENSION
            );
            meta = state.directory.createOutput(metaName, state.context);
            ColumnarCodecUtil.writeHeader(
                meta,
                ColumNARDocValuesFormat.META_CODEC,
                FormatVersion.CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        ColumnarFieldType type = ColumnarFieldType.fromField(field);
        if (type == ColumnarFieldType.STRING) {
            writeStringColumn(field, () -> stringCursor(valuesProducer.getBinary(field)));
        } else if (type.isNumeric()) {
            writeNumericColumn(field, type, () -> ColumnarNumericBinaryDocValues.decodePayloads(valuesProducer.getBinary(field)));
        } else {
            throw new UnsupportedOperationException("ColumNAR field type [" + type + "] is not implemented yet");
        }
    }

    /**
     * Merge: re-runs the encoder pipeline over the source segments, reading their values in bulk off
     * disk via {@link ColumnarNumericBinaryDocValues#directValues}. A fresh merge cursor
     * ({@link DocIDMerger} in merged doc order) is built per pass — count, iterator, values (the skip
     * index is built inline while the values are encoded, so it needs no pass of its own).
     */
    @Override
    public void mergeBinaryField(FieldInfo field, MergeState mergeState) throws IOException {
        ColumnarFieldType type = ColumnarFieldType.fromField(field);
        if (type == ColumnarFieldType.STRING) {
            // When every segment holds a dictionary that let nothing escape, their union is every value the
            // merged column has, so its vocabulary is known without reading the values to find out.
            StringColumnWriter.Vocabulary known = unionOfDictionaries(field, mergeState);
            if (known == null) {
                // No dictionaries to union, but the segments may have kept a summary of what they hold.
                known = combinedSummaries(field, mergeState);
            }
            final StringColumnWriter.Vocabulary vocabulary = known;
            writeStringColumn(field, () -> stringMergeCursor(field, mergeState, vocabulary), known);
            return;
        }
        writeNumericColumn(field, type, () -> mergeCursor(field, mergeState));
    }

    /**
     * The union of the segments' dictionaries, or null when it cannot stand for the merged column: a
     * segment without a dictionary, or one that let values escape, holds values the union would not name.
     * The union is bounded by the same policy as a surveyed vocabulary, and abandoned once it exceeds it.
     */
    private StringColumnWriter.Vocabulary unionOfDictionaries(FieldInfo field, MergeState mergeState) throws IOException {
        if (dictionaryPolicy.enabled() == false) {
            return null;
        }
        final TreeSet<BytesRef> union = new TreeSet<>();
        long unionBytes = 0;
        long columnBytes = 0;
        final BytesRef term = new BytesRef();
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            final DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            final FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            final BinaryDocValues binary = producer.getBinary(readerField);
            if ((binary instanceof ColumnarStringBinaryDocValues) == false) {
                return null;
            }
            final StringColumnReader reader = ((ColumnarStringBinaryDocValues) binary).reader();
            if (reader.numValues() == 0) {
                // A segment the field never appeared in has nothing to contribute and nothing to disagree
                // with; it must not decide the shape of the merged column.
                continue;
            }
            if (reader.hasDictionary() == false || reader.exceptionCount() > 0) {
                return null;
            }
            for (int ordinal = 0; ordinal < reader.dictionarySize(); ordinal++) {
                reader.termAt(ordinal, term);
                if (union.add(BytesRef.deepCopyOf(term))) {
                    unionBytes += term.length;
                    if (unionBytes > dictionaryPolicy.maxBytes()) {
                        return null;
                    }
                }
            }
            columnBytes += reader.valueBytes();
        }
        if (union.isEmpty()) {
            return null;
        }
        // How often each term is used is not recorded anywhere in a dictionary column, so the union carries
        // no counts. It does not need them: it holds every value, and a merge of these segments always
        // prefers it to a survey.
        return StringColumnWriter.knownVocabulary(new ArrayList<>(union), columnBytes, 1.0, null);
    }

    /**
     * A vocabulary combined from the segments' summaries, which every string column keeps. Counts are summed
     * across segments and the result trimmed to the policy's bound the same way a survey trims, so the
     * guarantee carries: a term the merged column holds often enough survives, and the coverage worked out
     * from the summed counts is an under-estimate because each of them was.
     */
    private StringColumnWriter.Vocabulary combinedSummaries(FieldInfo field, MergeState mergeState) throws IOException {
        if (dictionaryPolicy.enabled() == false) {
            return null;
        }
        final Map<BytesRef, Long> combined = new HashMap<>();
        // What the combined summaries may hold while they are being combined. Without it the map would grow
        // with the number of segments merged rather than with the dictionary any one of them can describe.
        // Trimming the least frequent keeps the guarantee the summaries carry: a term the merged column
        // holds often enough is in every summary that saw it, so it outlives the terms that are not.
        final long combinedBound = 4L * dictionaryPolicy.maxBytes();
        long combinedBytes = 0;
        long numValues = 0;
        long columnBytes = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            final DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            final FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            final BinaryDocValues binary = producer.getBinary(readerField);
            if ((binary instanceof ColumnarStringBinaryDocValues) == false) {
                return null;
            }
            final StringColumnReader reader = ((ColumnarStringBinaryDocValues) binary).reader();
            if (reader.numValues() == 0) {
                continue;
            }
            if (reader.hasSummary() == false) {
                return null;
            }
            final List<BytesRef> terms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            reader.readSummary(terms, counts);
            for (int t = 0; t < terms.size(); t++) {
                if (combined.merge(terms.get(t), counts.get(t), Long::sum) == counts.get(t)) {
                    combinedBytes += terms.get(t).length;
                }
            }
            numValues += reader.summaryValues();
            columnBytes += reader.valueBytes();
            if (combinedBytes > combinedBound) {
                combinedBytes = trimToBound(combined, combinedBound);
            }
        }
        if (combined.isEmpty() || numValues == 0) {
            return null;
        }
        // Trim to the bound, keeping the terms seen most; the rest are what a merged column lets escape.
        // Terms seen equally often are ordered by term, so the same inputs always yield the same column.
        final List<Map.Entry<BytesRef, Long>> ranked = new ArrayList<>(combined.entrySet());
        ranked.sort(Map.Entry.<BytesRef, Long>comparingByValue().reversed().thenComparing(Map.Entry::getKey));
        final TreeSet<BytesRef> kept = new TreeSet<>();
        long bytes = 0;
        long covered = 0;
        final long budget = dictionaryPolicy.budgetFor(columnBytes);
        for (Map.Entry<BytesRef, Long> entry : ranked) {
            // As at flush: a term the merged column holds once does not repay a dictionary entry.
            if (entry.getValue() <= 1) {
                break;
            }
            if (bytes + entry.getKey().length > budget) {
                break;
            }
            kept.add(entry.getKey());
            bytes += entry.getKey().length;
            covered += entry.getValue();
        }
        if (kept.isEmpty()) {
            return null;
        }
        // Whether this is worth a dictionary is left to the same gate a surveyed vocabulary passes. A
        // vocabulary that does not clear it still leaves the merged column its summary, so the segment
        // after this one is spared the survey too.
        final List<BytesRef> sorted = new ArrayList<>(kept);
        final long[] countsPerTerm = new long[sorted.size()];
        for (int t = 0; t < sorted.size(); t++) {
            countsPerTerm[t] = combined.get(sorted.get(t));
        }
        return StringColumnWriter.knownVocabulary(sorted, columnBytes, (double) covered / numValues, countsPerTerm);
    }

    /** Drops the least frequent terms until the terms held fit {@code bound}, and returns what they weigh. */
    private static long trimToBound(Map<BytesRef, Long> combined, long bound) {
        final List<Map.Entry<BytesRef, Long>> ranked = new ArrayList<>(combined.entrySet());
        ranked.sort(Map.Entry.<BytesRef, Long>comparingByValue().reversed().thenComparing(Map.Entry::getKey));
        long bytes = 0;
        int kept = 0;
        while (kept < ranked.size() && bytes + ranked.get(kept).getKey().length <= bound) {
            bytes += ranked.get(kept).getKey().length;
            kept++;
        }
        for (int i = kept; i < ranked.size(); i++) {
            combined.remove(ranked.get(i).getKey());
        }
        return bytes;
    }

    /** The source segments' values in merged document order, for a string column. */
    /**
     * @param vocabulary the terms the merged column will be written with, when they are already known, so a
     *                   sub whose own dictionary they were taken from can hand over ordinals rather than
     *                   values; null when the values have to be read
     */
    private static StringColumnValues stringMergeCursor(FieldInfo field, MergeState mergeState, StringColumnWriter.Vocabulary vocabulary)
        throws IOException {
        final List<StringMergeSub> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            final DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            final FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            final BinaryDocValues binary = producer.getBinary(readerField);
            if (binary == null) {
                continue;
            }
            cost += binary.cost();
            subs.add(new StringMergeSub(mergeState.docMaps[i], binary, ordinalMap(binary, vocabulary)));
        }
        final DocIDMerger<StringMergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        final long finalCost = cost;
        return new StringColumnValues() {
            private StringMergeSub current;
            private int docID = -1;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = merger.next();
                docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return current.values.binaryValue();
            }

            @Override
            public int nextOrdinal() throws IOException {
                if (current.ordinalMap == null) {
                    return -1;
                }
                final int ordinal = ((ColumnarStringBinaryDocValues) current.values).ordinal();
                // Past the dictionary is the escape marker: the value is in the segment's exception stream
                // and has to be read.
                return ordinal < current.ordinalMap.length ? current.ordinalMap[ordinal] : -1;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }
        };
    }

    /**
     * What each of a segment's dictionary ordinals becomes in the merged column, or null when the segment
     * has no dictionary to read them from. Built once per segment, so it costs the dictionary rather than
     * the values — which is the whole point, since resolving a value's bytes only to look them up again is
     * most of what merging a dictionary column costs.
     *
     * <p>A term the merged column does not hold maps to {@code -1}, and so does anything the segment let
     * escape: those values are read as values, and the rest are carried over as ordinals.
     */
    private static int[] ordinalMap(BinaryDocValues values, StringColumnWriter.Vocabulary vocabulary) throws IOException {
        if (vocabulary == null || values instanceof ColumnarStringBinaryDocValues == false) {
            return null;
        }
        final StringColumnReader reader = ((ColumnarStringBinaryDocValues) values).reader();
        if (reader.hasDictionary() == false) {
            return null;
        }
        final int[] map = new int[reader.dictionarySize()];
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < map.length; ordinal++) {
            reader.termAt(ordinal, term);
            final int id = vocabulary.terms().find(term);
            map[ordinal] = id < 0 ? -1 : vocabulary.ordinalOfId()[id];
        }
        return map;
    }

    private static final class StringMergeSub extends DocIDMerger.Sub {
        private final BinaryDocValues values;
        private final int[] ordinalMap;

        StringMergeSub(MergeState.DocMap docMap, BinaryDocValues values, int[] ordinalMap) {
            super(docMap);
            this.values = values;
            this.ordinalMap = ordinalMap;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    private static NumericColumnValues mergeCursor(FieldInfo field, MergeState mergeState) throws IOException {
        List<MergeSub> subs = new ArrayList<>();
        long cost = 0;
        for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
            DocValuesProducer producer = mergeState.docValuesProducers[i];
            if (producer == null) {
                continue;
            }
            FieldInfo readerField = mergeState.fieldInfos[i].fieldInfo(field.name);
            if (readerField == null || readerField.getDocValuesType() != DocValuesType.BINARY) {
                continue;
            }
            BinaryDocValues binary = producer.getBinary(readerField);
            if (binary == null) {
                continue;
            }
            // Read decoded longs directly for our own columns; fall back to the payload for anything else.
            NumericColumnValues values = binary instanceof ColumnarNumericBinaryDocValues columnar
                ? columnar.directValues()
                : ColumnarNumericBinaryDocValues.decodePayloads(binary);
            cost += values.cost();
            subs.add(new MergeSub(mergeState.docMaps[i], values));
        }

        DocIDMerger<MergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        long finalCost = cost;
        return new NumericColumnValues() {
            private MergeSub current;
            private int docID = -1;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() throws IOException {
                current = merger.next();
                docID = current == null ? DocIdSetIterator.NO_MORE_DOCS : current.mappedDocID;
                return docID;
            }

            @Override
            public int valueCount() {
                return current.values.valueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return current.values.nextValue();
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return finalCost;
            }
        };
    }

    private static final class MergeSub extends DocIDMerger.Sub {
        private final NumericColumnValues values;

        MergeSub(MergeState.DocMap docMap, NumericColumnValues values) {
            super(docMap);
            this.values = values;
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }
    }

    /** A string column is written from the same binary surface, one value per document. */
    private void writeStringColumn(FieldInfo field, IOSupplier<StringColumnValues> cursors) throws IOException {
        writeStringColumn(field, cursors, null);
    }

    /** @param known a vocabulary the values are already known to be covered by, or null to survey them */
    private void writeStringColumn(FieldInfo field, IOSupplier<StringColumnValues> cursors, StringColumnWriter.Vocabulary known)
        throws IOException {
        int numDocsWithField = 0;
        long numValues = 0;
        final StringColumnValues counter = cursors.get();
        for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
            numDocsWithField++;
            numValues += counter.valueCount();
        }
        final StringColumnMetadata metadata = StringColumnWriter.write(
            maxDoc,
            numDocsWithField,
            numValues,
            cursors,
            ChunkCodec.ZSTD,
            targetChunkBytes,
            ValueStream.VALUES_PER_BLOCK,
            dictionaryPolicy,
            known,
            directory,
            context,
            data
        );
        fields.add(new FieldEntry(field.number, ColumnarFieldType.STRING.id(), null, metadata));
    }

    /** Presents a binary field's values as a string cursor; one value per document at this surface. */
    private static StringColumnValues stringCursor(BinaryDocValues values) {
        return new StringColumnValues() {
            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return values.binaryValue();
            }

            @Override
            public int docID() {
                return values.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return values.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }

            @Override
            public long cost() {
                return values.cost();
            }
        };
    }

    private void writeNumericColumn(FieldInfo field, ColumnarFieldType type, IOSupplier<NumericColumnValues> cursors) throws IOException {
        // Count in one pass, then stream the values block by block from fresh cursors — never buffer
        // the whole field on-heap, so a large merge stays memory-bounded.
        int numDocsWithField = 0;
        long numValues = 0;
        NumericColumnValues counter = cursors.get();
        for (int doc = counter.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = counter.nextDoc()) {
            numDocsWithField++;
            numValues += counter.valueCount();
        }

        // A BINARY field can't carry a skipper, so the column builds its own skip index inline
        // during the value-encode pass — no extra cursor over the data.
        final NumericPipeline pipeline = pipelineSelector.select(field.name, type).build(blockSize);
        assert pipeline.blockSize() == blockSize
            : "template ignored blockSize argument: built " + pipeline.blockSize() + ", expected " + blockSize;
        NumericColumnMetadata metadata = NumericColumnWriter.write(
            maxDoc,
            numDocsWithField,
            numValues,
            cursors,
            pipeline,
            BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
            SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
            directory,
            context,
            data
        );
        fields.add(new FieldEntry(field.number, type.id(), metadata, null));
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("numeric");
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted-numeric");
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted");
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw typedNotSupported("sorted-set");
    }

    private static UnsupportedOperationException typedNotSupported(String shape) {
        return new UnsupportedOperationException(
            "ColumNAR is a binary doc-values format and does not handle "
                + shape
                + " doc values; store the field as a binary doc-values field carrying the '"
                + ColumNARDocValuesFormat.TYPE_ATTRIBUTE
                + "' attribute"
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
            for (FieldEntry entry : fields) {
                meta.writeInt(entry.fieldNumber());
                meta.writeByte(entry.fieldTypeId());
                if (entry.string() != null) {
                    entry.string().writeTo(meta);
                } else {
                    entry.numeric().writeTo(meta);
                }
            }
            meta.writeInt(-1);
            CodecUtil.writeFooter(meta);
            CodecUtil.writeFooter(data);
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
