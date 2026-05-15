# Benchmarks

JMH benches comparing the columnar format against Lucene's default `DocValuesFormat` and
Elasticsearch's TSDB format on representative workloads. They live in
`benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/columnar/`. Results are
not published in the library docs — they're working numbers that move with the encoders.

## Why the benches exist

Each bench targets one specific read or write path so a regression on that path shows up
as a single signal:

- **`ColumnarDocValuesIndexingBenchmark`** — ingestion speed. Each format runs through
  its native write API (TSDB through `NumericDocValuesField` / `SortedNumericDocValuesField`,
  columnar through `ColumNARLongField` over `BinaryDocValuesField`) so each format pays
  only what its production path costs. This is the *apples-to-best* comparison.
- **`ColumnarDocValuesIterationBenchmark`** — sequential scan
  (`nextDoc + longValue`/`binaryValue`) and random advance (`advanceExact` on
  pre-generated probes). Plus on-disk byte count, surfaced as a JMH `AuxCounters` event
  so storage moves alongside throughput.
- **`ColumnarDocValuesBlockLoadingBenchmark`** — fill caller-owned `long[]` pages of
  varying sizes by walking the bridge (or the native numeric DV for the other formats).
  Models what `AbstractLongsFromDocValuesBlockLoader` does inside ES|QL. This is the
  read path the format is primarily tuned for.
- **`ColumnarDocValuesRangeQueryBenchmark`** — `NumericDocValuesField.newSlowRangeQuery`
  through `IndexSearcher.count`, with the field's `DocValuesSkipper` engaged. Measures
  the filter-pushdown payoff.
- **`ColumnarDocValuesScanBenchmark`** / **`ColumnarDocValuesKeywordScanBenchmark`** —
  per-format baselines for single-valued long and keyword fields with cardinality sweeps.

Each bench parameterises by `format` (lucene / es87tsdb / es819v3tsdb / es95tsdb /
columnar), workload (`monotonic`, `gauge_like`, `lowcard`, `random`, `floats`, `doubles`,
`keyword_short`, `keyword_lowcard`), and an axis specific to the bench (page size, range
fraction, value cardinality).

## Running

A single bench:

```
./gradlew :benchmarks:jmh -PjmhInclude=ColumnarDocValuesIterationBenchmark
```

Sweep a parameter:

```
./gradlew :benchmarks:jmh -PjmhInclude=ColumnarDocValuesBlockLoadingBenchmark \
                          -PjmhArgs='-p pageSize=256,1024,8192'
```

Sweeping all benches takes hours; prefer running the relevant bench while iterating on
its read path. JMH's `-prof gc` and `-prof perfasm` (Linux) are useful when investigating
cache misses or allocation pressure.

## Extending

Add a new bench by copying the closest existing one and:

1. Replacing the read or write loop with the path you want to measure.
2. Keeping the same `@Param` axes for `format` and workload so results compare
   side-by-side with the existing suite.
3. Surfacing on-disk bytes via `StorageCounters` (the JMH `@AuxCounters` pattern already
   used in the iteration bench).
4. Adding the bench class to the runner block in `main(...)` if it should be part of the
   matrix run.

Add a new workload by extending the workload enum in the bench (or the helper that
generates the source array). Workloads are intentionally small and self-contained —
~30 lines per workload — so a new one shouldn't bring in new dependencies.

## What the benches do not test

- **Correctness.** Correctness gates are in `libs/columnar/src/test/java/`. Benches
  measure speed and storage on already-correct code.
- **Multi-segment merges.** All benches build single-segment indices. Merge cost is a
  separate concern.
- **Distributed read paths.** Benches run a single JVM against `FSDirectory`. Blob-store
  prefetch behaviour is exercised through Elasticsearch's higher-level test suites.
