/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.engine;

import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.LuceneChangesSnapshot;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Fork(value = 1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class LuceneChangesSnapshotBenchmark {
  @Param({ "10000" })
  int numDocs;

  @Param({ "1000" })
  int numFields;

  @Param({ "10" })
  int numFieldsPerDoc;

  @Param({"true", "false"})
  boolean isSynthetic;

  @Param({ "12345" })
  long seed;

  static final int NUM_OPS = 1000;

  static Random rand = new Random(1234L);

  Path path;
  MapperService mapperService;
  FSDirectory dir;
  IndexReader reader;
  Engine.Searcher searcher;
  LuceneChangesSnapshot snapshot;

  static int MAX_TERMS = 10000;
  static String[] randomStrings = new String[MAX_TERMS];

  static {
    LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    for (int i = 0; i < MAX_TERMS; i++) {
      randomStrings[i] = randomString(rand, 10, 50);
    }
  }

  @Setup
  public void setup() throws IOException {
    this.path = Files.createTempDirectory("snapshot_changes");

    Map<String, String> fields = randomFields();
    String mapping = createMapping(fields, isSynthetic);
    this.mapperService = MapperServiceFactory.create(mapping);
    System.out.println("synthetic=" + mapperService.mappingLookup().isSourceSynthetic());
    IndexWriterConfig config = new IndexWriterConfig();
    //config.setIndexSort(new Sort(new SortField[] { new SortedNumericSortField("random", SortField.Type.INT) }));
    try (
            FSDirectory dir = FSDirectory.open(path);
            IndexWriter writer = new IndexWriter(dir, config);
    ) {
      for (int i = 0; i < numDocs; i++) {
        SourceToParse sourceToParse = randomSource(UUID.randomUUID().toString(), fields);
        ParsedDocument doc = mapperService.documentMapper().parse(sourceToParse);
        doc.updateSeqID(i, 0);
        doc.version().setLongValue(0);
        writer.addDocuments(doc.docs());
      }
    }
    this.dir = FSDirectory.open(path);
    this.reader = DirectoryReader.open(dir);
    long size = 0;
    for (var file : dir.listAll()) {
      size += dir.fileLength(file);
    }
    this.searcher = new Engine.Searcher("snapshot", reader, new BM25Similarity(), null, new QueryCachingPolicy() {
      @Override
      public void onUse(Query query) {}

      @Override
      public boolean shouldCache(Query query) throws IOException {
        return false;
      }
    }, () -> {
    });
    this.snapshot = new LuceneChangesSnapshot(searcher, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, 0, numDocs-1, true, true, true, IndexVersion.current());
    System.out.println("size=" + size);
  }

  @TearDown
  public void tearDown() {
    try {
      for (var file : dir.listAll()) {
        dir.deleteFile(file);
      }
      Files.delete(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    IOUtils.closeWhileHandlingException(searcher, reader, dir);
  }


  @Benchmark
  @OperationsPerInvocation(NUM_OPS)
  public long recover() throws IOException {
    long totalSize = 0;
    try (var snapshot = new LuceneChangesSnapshot(isSynthetic ? mapperService.mappingLookup() : null, searcher, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, 0, NUM_OPS-1, true, true, true, IndexVersion.current())) {
      Translog.Operation op;
      while ((op = snapshot.next()) != null) {
        totalSize += op.estimateSize();
      }
    }
    return totalSize;
  }

  final String[] types = new String[] {"keyword", "text", "long", "double"};
  private String createMapping(Map<String, String> fields, boolean syntheticSource) throws IOException {
    XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
    builder.startObject();
    if (syntheticSource) {
      builder.startObject("_source");
      builder.field("mode", "synthetic");
      builder.endObject();
    }
    builder.startObject("properties");
    builder.startObject("random").field("type", "integer").endObject();
    for (var entry : fields.entrySet()) {
      builder.startObject(entry.getKey());
      builder.field("type", entry.getValue());
      if (syntheticSource && entry.getValue().equals("text")) {
        builder.field("store", true);
      }
      builder.endObject();
    }
    builder.endObject().endObject();
    return BytesReference.bytes(builder).utf8ToString();
  }

  private Map<String, String> randomFields() {
    Map<String, String> fields = new HashMap<>(numFields);
    for (int i = 0; i < numFields; i++) {
      String type = types[rand.nextInt(4)];
      fields.put(randomString(rand, 5, 20), type);
    }
    return fields;
  }
  
  private SourceToParse randomSource(String id, Map<String, String> fields) {
    Map<String, Object> source = new HashMap<>(numFieldsPerDoc);
    source.put("random", rand.nextInt());
    var fieldList = new ArrayList<>(fields.keySet());
    for (int i = 0; i < numFieldsPerDoc; i++) {
      int fieldIndex = rand.nextInt(0, fields.size());
      if (source.containsKey(fieldList.get(fieldIndex))) {
        continue;
      }
      String type = fields.get(fieldList.get(fieldIndex));
      Object value = switch (type) {
        case "text":
          yield randomString(rand, 1024, 4096);

        case "keyword":
          // cardinality=10000
          yield randomStrings[rand.nextInt(0, 10000)];

        case "long":
          yield rand.nextLong();

        case "double":
          yield rand.nextDouble();

        default:
          throw new AssertionError("Unkwnown type: " + type);
      };
      source.put(fieldList.get(fieldIndex), value);
    }
    try {
      XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
      builder.map(source);
      builder.flush();
      return new SourceToParse(id, BytesReference.bytes(builder), XContentType.JSON);
    } catch (IOException e) {
      throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
    }
  }

  static String randomString(Random rand, int min, int max) {
    var length = rand.nextInt(min, max);
    var builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append((byte) (32 + rand.nextInt(94)));
    }
    return builder.toString();
  }
}
