/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.benchmark;

import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.columnar.BitPacking;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Throughput comparison of {@link BitPacking} against Lucene's {@link PackedInts} decoder on
 * fixed-width long blocks. The bench measures encode and decode separately so we can see which
 * side dominates.
 *
 * <p>Reuses the JMH annotation conventions from the top-level {@code benchmarks/} module
 * ({@link Mode#AverageTime}, microsecond reporting, single fork, single thread, three warmup
 * iterations, five measurement iterations).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class BitPackingBenchmark {

    @Param({ "8", "12", "16", "24", "32" })
    private int bitsPerValue;

    @Param({ "128", "1024" })
    private int blockSize;

    @Param("42")
    private int seed;

    private long[] values;
    private long[] packed;
    private long[] decodeOut;

    private PackedInts.Decoder luceneDecoder;
    private PackedInts.Encoder luceneEncoder;
    private long[] luceneBlocks;
    private int luceneIterations;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(BitPackingBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        final Random random = new Random(seed);
        final long max = (1L << bitsPerValue) - 1L;
        values = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            values[i] = (random.nextLong() & Long.MAX_VALUE) % (max + 1L);
        }
        packed = new long[BitPacking.requiredLongs(blockSize, bitsPerValue)];
        BitPacking.pack(values, 0, blockSize, bitsPerValue, packed);

        decodeOut = new long[blockSize];

        luceneDecoder = PackedInts.getDecoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsPerValue);
        luceneEncoder = PackedInts.getEncoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsPerValue);
        // longBlockCount() values are produced per iteration of the bulk encoder/decoder
        luceneIterations = blockSize / luceneDecoder.longValueCount();
        luceneBlocks = new long[luceneDecoder.longBlockCount() * luceneIterations];
        luceneEncoder.encode(values, 0, luceneBlocks, 0, luceneIterations);
    }

    @Benchmark
    public void packOurs(Blackhole bh) {
        final int written = BitPacking.pack(values, 0, blockSize, bitsPerValue, packed);
        bh.consume(written);
        bh.consume(packed[0]);
    }

    @Benchmark
    public void unpackOurs(Blackhole bh) {
        BitPacking.unpack(packed, 0, blockSize, bitsPerValue, decodeOut, 0);
        bh.consume(decodeOut[0]);
        bh.consume(decodeOut[decodeOut.length - 1]);
    }

    @Benchmark
    public void unpackLucene(Blackhole bh) {
        luceneDecoder.decode(luceneBlocks, 0, decodeOut, 0, luceneIterations);
        bh.consume(decodeOut[0]);
        bh.consume(decodeOut[decodeOut.length - 1]);
    }
}
