/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.benchmark;

import org.elasticsearch.columnar.Delta;
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
 * Throughput of {@link Delta} encode / decode at first and second order, across three
 * representative numeric workloads:
 * <ul>
 *   <li>{@code timestamp_like} — monotonically increasing with small jitter (constant stride
 *       around 1000 ms).</li>
 *   <li>{@code gauge_like} — bounded random walk around a baseline (cpu / temperature style).</li>
 *   <li>{@code counter_with_resets} — monotonically increasing with occasional resets to zero
 *       (process metric style).</li>
 * </ul>
 *
 * <p>Uses the same JMH annotation conventions as the top-level {@code benchmarks/} module.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class DeltaBenchmark {

    @Param({ "256", "1024" })
    private int blockSize;

    @Param({ "timestamp_like", "gauge_like", "counter_with_resets" })
    private String shape;

    @Param({ "1", "2" })
    private int order;

    @Param("42")
    private int seed;

    private long[] raw;
    private long[] encoded;
    private long[] scratch;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder().include(DeltaBenchmark.class.getSimpleName()).build();
        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setup() {
        final Random random = new Random(seed);
        raw = generate(shape, blockSize, random);
        encoded = new long[blockSize];
        scratch = new long[blockSize];
        if (order == 1) {
            Delta.encode(raw, 0, blockSize, encoded, 0);
        } else {
            Delta.encode2(raw, 0, blockSize, encoded, 0);
        }
    }

    @Benchmark
    public void encode(Blackhole bh) {
        if (order == 1) {
            Delta.encode(raw, 0, blockSize, scratch, 0);
        } else {
            Delta.encode2(raw, 0, blockSize, scratch, 0);
        }
        bh.consume(scratch[0]);
        bh.consume(scratch[scratch.length - 1]);
    }

    @Benchmark
    public void decode(Blackhole bh) {
        if (order == 1) {
            Delta.decode(encoded, 0, blockSize, scratch, 0);
        } else {
            Delta.decode2(encoded, 0, blockSize, scratch, 0);
        }
        bh.consume(scratch[0]);
        bh.consume(scratch[scratch.length - 1]);
    }

    private static long[] generate(String shape, int n, Random random) {
        return switch (shape) {
            case "timestamp_like" -> timestampLike(n, random);
            case "gauge_like" -> gaugeLike(n, random);
            case "counter_with_resets" -> counterWithResets(n, random);
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    private static long[] timestampLike(int n, Random random) {
        final long[] out = new long[n];
        long t = 1_700_000_000_000L;
        for (int i = 0; i < n; i++) {
            t += 1000L + random.nextInt(11) - 5;
            out[i] = t;
        }
        return out;
    }

    private static long[] gaugeLike(int n, Random random) {
        final long[] out = new long[n];
        final long baseline = 5000L;
        final long maxStep = 50L;
        long current = baseline;
        for (int i = 0; i < n; i++) {
            current += random.nextLong(-maxStep, maxStep + 1);
            if (current < 0) {
                current = 0;
            }
            out[i] = current;
        }
        return out;
    }

    private static long[] counterWithResets(int n, Random random) {
        final long[] out = new long[n];
        long counter = 0;
        for (int i = 0; i < n; i++) {
            counter += 1 + random.nextInt(100);
            if (random.nextInt(256) == 0) {
                counter = 0;
            }
            out[i] = counter;
        }
        return out;
    }
}
