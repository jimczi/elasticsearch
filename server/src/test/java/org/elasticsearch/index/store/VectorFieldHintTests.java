/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class VectorFieldHintTests extends ESTestCase {

    /** Two fields written by distinct format instances take separate suffixes, so each resolves. */
    public void testResolvesTheFieldOfEachSuffix() throws Exception {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(new PerFieldPerInstanceCodec());
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("first", new float[] { 1, 0 }, VectorSimilarityFunction.DOT_PRODUCT));
                doc.add(new KnnFloatVectorField("second", new float[] { 0, 1 }, VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                FieldInfos fieldInfos = leaf.reader().getFieldInfos();
                for (String field : List.of("first", "second")) {
                    String suffix = suffixOf(fieldInfos, field);
                    assertEquals(new VectorFieldHint(field), VectorFieldHint.forSuffix(fieldInfos, suffix));
                }
            }
        }
    }

    /** A suffix covering several fields describes none of them, so it resolves to nothing. */
    public void testSharedSuffixResolvesToNothing() throws Exception {
        try (Directory dir = newDirectory()) {
            // the default codec shares one format instance, so both fields land on one suffix
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("first", new float[] { 1, 0 }, VectorSimilarityFunction.DOT_PRODUCT));
                doc.add(new KnnFloatVectorField("second", new float[] { 0, 1 }, VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                FieldInfos fieldInfos = reader.leaves().get(0).reader().getFieldInfos();
                String shared = suffixOf(fieldInfos, "first");
                assertEquals(shared, suffixOf(fieldInfos, "second"));
                assertNull(VectorFieldHint.forSuffix(fieldInfos, shared));
            }
        }
    }

    public void testUnknownSuffixResolvesToNothing() throws Exception {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("first", new float[] { 1, 0 }, VectorSimilarityFunction.DOT_PRODUCT));
                w.addDocument(doc);
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                FieldInfos fieldInfos = reader.leaves().get(0).reader().getFieldInfos();
                assertNull(VectorFieldHint.forSuffix(fieldInfos, "NoSuchFormat_7"));
            }
        }
    }

    private static String suffixOf(FieldInfos fieldInfos, String field) {
        var fi = fieldInfos.fieldInfo(field);
        return fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY)
            + "_"
            + fi.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_SUFFIX_KEY);
    }

    /**
     * Hands every field its own format instance, the way Elasticsearch does for dense vectors. It
     * keeps the default codec's name so the segment still resolves to a registered codec on read;
     * the per-field suffixes it wrote are recorded in the field attributes either way.
     */
    private static class PerFieldPerInstanceCodec extends FilterCodec {
        PerFieldPerInstanceCodec() {
            super(Codec.getDefault().getName(), Codec.getDefault());
        }

        @Override
        public KnnVectorsFormat knnVectorsFormat() {
            return new PerFieldKnnVectorsFormat() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    // a new instance per field, which is what gives each its own files
                    return new Lucene99HnswVectorsFormat();
                }
            };
        }
    }
}
