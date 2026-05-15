/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.benchmark;

import org.elasticsearch.columnar.Dictionary;
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
 * Throughput of {@link Dictionary} encode and decode at varying block cardinality.
 *
 * <p>Encode includes sort + dedupe + per-value binary search and scales with block size and
 * cardinality. Decode is a pure {@code indices[i] -> dict[indices[i]]} fan-out and is the
 * dominant cost in any read-heavy pipeline.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class DictionaryBenchmark {

    @Param({ "256", "1024" })
    private int blockSize;

    @Param({ "4", "16", "64", "256" })
    private int cardinality;

    @Param("42")
    private int seed;

    private long[] raw;
    private int[] encodedIndices;
    private long[] encodedDict;
    private int dictSize;

    private int[] scratchIndices;
    private long[] scratchDict;
    private long[] scratchOut;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(DictionaryBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        final Random random = new Random(seed);
        raw = generate(blockSize, cardinality, random);
        scratchIndices = new int[blockSize];
        scratchDict = new long[blockSize];
        scratchOut = new long[blockSize];

        encodedIndices = new int[blockSize];
        encodedDict = new long[blockSize];
        dictSize = Dictionary.encode(raw, 0, blockSize, encodedIndices, 0, encodedDict, 0);
    }

    @Benchmark
    public void encode(Blackhole bh) {
        final int produced = Dictionary.encode(raw, 0, blockSize, scratchIndices, 0, scratchDict, 0);
        bh.consume(produced);
        bh.consume(scratchDict[0]);
        bh.consume(scratchIndices[scratchIndices.length - 1]);
    }

    @Benchmark
    public void decode(Blackhole bh) {
        Dictionary.decode(encodedIndices, 0, blockSize, encodedDict, 0, scratchOut, 0);
        bh.consume(scratchOut[0]);
        bh.consume(scratchOut[scratchOut.length - 1]);
        bh.consume(dictSize);
    }

    private static long[] generate(int blockSize, int cardinality, Random random) {
        final long[] palette = new long[cardinality];
        for (int i = 0; i < cardinality; i++) {
            palette[i] = random.nextLong();
        }
        final long[] out = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            out[i] = palette[random.nextInt(cardinality)];
        }
        return out;
    }
}
