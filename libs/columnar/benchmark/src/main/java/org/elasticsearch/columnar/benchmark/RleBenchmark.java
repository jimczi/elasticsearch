/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.benchmark;

import org.elasticsearch.columnar.Rle;
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
 * Throughput of {@link Rle} encode and decode. Sweeps block size and average run length.
 *
 * <p>Workloads use a controlled run-length distribution: every value is repeated for a run of
 * geometrically-distributed length around {@code avgRunLength}. {@code avgRunLength = 1} means no
 * adjacent duplicates (worst case for RLE — output runs ≈ input size); higher values mean longer
 * compressible runs.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class RleBenchmark {

    @Param({ "256", "1024" })
    private int blockSize;

    @Param({ "1", "4", "16", "64" })
    private int avgRunLength;

    @Param("42")
    private int seed;

    private long[] raw;
    private long[] encodedValues;
    private int[] encodedCounts;
    private int encodedRuns;

    private long[] scratchValues;
    private int[] scratchCounts;
    private long[] scratchOut;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(RleBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        final Random random = new Random(seed);
        raw = generate(blockSize, avgRunLength, random);
        scratchValues = new long[blockSize];
        scratchCounts = new int[blockSize];
        scratchOut = new long[blockSize];

        encodedValues = new long[blockSize];
        encodedCounts = new int[blockSize];
        encodedRuns = Rle.encode(raw, 0, blockSize, encodedValues, 0, encodedCounts, 0);
    }

    @Benchmark
    public void encode(Blackhole bh) {
        final int runs = Rle.encode(raw, 0, blockSize, scratchValues, 0, scratchCounts, 0);
        bh.consume(runs);
        bh.consume(scratchValues[0]);
    }

    @Benchmark
    public void decode(Blackhole bh) {
        final int written = Rle.decode(encodedValues, 0, encodedCounts, 0, encodedRuns, scratchOut, 0);
        bh.consume(written);
        bh.consume(scratchOut[0]);
        bh.consume(scratchOut[scratchOut.length - 1]);
    }

    private static long[] generate(int len, int avgRunLength, Random random) {
        final long[] out = new long[len];
        int i = 0;
        long currentValue = random.nextLong();
        while (i < len) {
            // Geometric run length with mean ~ avgRunLength, minimum 1.
            final int runLen = avgRunLength <= 1 ? 1 : 1 + (int) (-Math.log(1.0 - random.nextDouble()) * (avgRunLength - 1));
            final int end = Math.min(len, i + runLen);
            for (int k = i; k < end; k++) {
                out[k] = currentValue;
            }
            i = end;
            currentValue = random.nextLong();
        }
        return out;
    }
}
