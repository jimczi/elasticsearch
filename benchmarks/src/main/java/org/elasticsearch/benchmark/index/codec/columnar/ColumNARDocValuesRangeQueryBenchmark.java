/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.benchmark.stateless.AbstractStatelessQueryBenchmark;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Random;

/**
 * Bulk-scorer range-query bench measuring how each format's {@code DocValuesSkipper} (or absence
 * of one) affects {@link org.apache.lucene.search.IndexSearcher#count(Query)} on a numeric range
 * filter.
 *
 * <p>The bench reuses {@link AbstractStatelessQueryBenchmark} which builds the index once and
 * runs each query invocation through a freshly-constructed
 * {@link org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory} wrapper — so we
 * measure the same read path stateless clusters take in production, with cold / hot cache
 * variants exposed as a JMH parameter.
 *
 * <p>The numeric field is written via {@link NumericDocValuesField#indexedField} which attaches a
 * skipper to the field at index time. Lucene's {@code DocValuesRewriteMethod} then uses
 * {@code LeafReader.getDocValuesSkipper} during query execution; our format implements that hook
 * via the per-block min/max metadata.
 */
public class ColumNARDocValuesRangeQueryBenchmark extends AbstractStatelessQueryBenchmark {

    private static final String FIELD = "value";
    private static final int N_DOCS = 1_000_000;

    /** Doc-values format under test. */
    @Param({ "lucene90", "es87tsdb", "es819v3tsdb", "es95tsdb", "columnar" })
    public String format;

    /** Workload shape — controls how often the range query's bounds intersect each block. */
    @Param({ "monotonic", "gauge_like" })
    public String workload;

    /** Fraction of the value range covered by the filter; small fractions exercise skipping the most. */
    @Param({ "0.01", "0.2" })
    public double rangeFraction;

    private long lowerBound;
    private long upperBound;

    @Override
    protected IndexWriterConfig indexWriterConfig() {
        final DocValuesFormat dvFormat = newDocValuesFormat(format);
        return new IndexWriterConfig().setCodec(new Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String fieldName) {
                return dvFormat;
            }
        });
    }

    @Override
    protected void buildIndex(IndexWriter writer) throws IOException {
        final Random random = new Random(42);
        long minSeen = Long.MAX_VALUE;
        long maxSeen = Long.MIN_VALUE;
        for (int i = 0; i < N_DOCS; i++) {
            final long v = nextValue(workload, i, random);
            if (v < minSeen) {
                minSeen = v;
            }
            if (v > maxSeen) {
                maxSeen = v;
            }
            final Document doc = new Document();
            doc.add(NumericDocValuesField.indexedField(FIELD, v));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        // Compute filter bounds from the observed value range so the requested fraction is
        // honored regardless of workload shape.
        final long span = maxSeen - minSeen;
        final long width = (long) (span * rangeFraction);
        lowerBound = minSeen;
        upperBound = lowerBound + width;
    }

    @Override
    protected Object runQuery(IndexSearcher searcher) throws IOException {
        final Query rangeQuery = NumericDocValuesField.newSlowRangeQuery(FIELD, lowerBound, upperBound);
        return searcher.count(rangeQuery);
    }

    private static DocValuesFormat newDocValuesFormat(String name) {
        return switch (name) {
            case "lucene90" -> new Lucene90DocValuesFormat();
            case "es87tsdb" -> new ES87TSDBDocValuesFormat();
            case "es819v3tsdb" -> new ES819Version3TSDBDocValuesFormat();
            case "es95tsdb" -> new ES95TSDBDocValuesFormat();
            case "columnar" -> new ColumNARDocValuesFormat();
            default -> throw new IllegalArgumentException("unknown format: " + name);
        };
    }

    private static long nextValue(String workload, int i, Random random) {
        return switch (workload) {
            case "monotonic" -> 1_700_000_000_000L + i * 1000L;
            case "gauge_like" -> 5000L + random.nextInt(101) - 50;
            default -> throw new IllegalArgumentException("unknown workload: " + workload);
        };
    }

    /**
     * Per-(format, workload) on-disk size reported as a JMH auxiliary counter so it appears
     * alongside throughput in the bench output. Use as a non-regression check on encoder size.
     *
     * <p>Note: {@link AbstractStatelessQueryBenchmark} already exposes cache stats via its own
     * {@code CacheCounters}; this is additive and reports the size of the on-disk index built
     * once per trial.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class StorageCounters {
        public long onDiskBytes;
    }
}
