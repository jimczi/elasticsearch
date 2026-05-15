# ColumNAR report — storage & compute

Snapshot of ColumNAR's on-disk size and sequential-scan latency against Lucene 10.4's
default `DocValuesFormat` and the Elasticsearch TSDB format. Working report — meant to
be regenerated periodically (nightly CI is a good cadence). Open issues for follow-ups;
don't list pending work here. Maintainers should read this as "current state across the
full workload matrix" and pick what's worth investigating.

Single-segment, force-merged, **1 000 000 docs per (workload, format) tuple**. Each
workload is sized so the resulting field crosses multiple internal blocks; numbers below
are total on-disk bytes (storage) and median elapsed time over 3 runs after 1 warmup
(compute). Sequential-scan touches every doc through each format's native read path.

Run with:

```
./gradlew :libs:columnar:test --tests org.elasticsearch.columnar.StorageReportTests
```

Each format writes through its native indexing path and is read back through the
matching native API:

- `lucene104` / `es95tsdb` — `NumericDocValuesField` (SV num) /
  `SortedNumericDocValuesField` (MV num) / `BinaryDocValuesField` (SV bytes) /
  `SortedSetDocValuesField` (MV bytes).
- `columnar` (ColumNAR) — `ColumNARLongField` (numeric) / `ColumNARKeywordField`
  (keyword) over `BinaryDocValuesField`. ColumNAR runs at production defaults:
  `Pipeline` numeric encoder (delta → offset → GCD → bit-pack, per-block adaptive),
  `RawBytes`, `Lz4` (`Mode.FAST`), 1 MB target encoded bytes per block, 65 536 row cap.

**Fairness caveat for the bytes-MV scan.** The test scans *values*, not ordinals.
ColumNAR walks `BinaryDocValues` (one materialised value per `binaryValue()` call);
the Lucene-keyword baselines walk `SortedSetDocValues`, which pays an
`ord` lookup + `lookupOrd` dictionary call per value. That's an inherent cost of
ord-based storage for value-iteration workloads — not a ColumNAR optimisation. The
storage numbers stay apples-to-apples; the bytes-MV compute numbers should be read
as "what value-iteration costs in each format's native API", not as a head-to-head
benchmark of identical work.

## Storage — numeric, single-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `sv_ts_seconds` | 1.9 MB | 322.2 KB | 262.0 KB | **0.13x** | **0.81x** |
| `sv_ts_millis_jitter` | 2.9 MB | 1.0 MB | 997.3 KB | **0.34x** | **0.93x** |
| `sv_gauge_5000pm50` | 993.0 KB | 901.7 KB | 875.7 KB | **0.88x** | **0.97x** |
| `sv_lowcard_8` | 504.7 KB | 7.7 MB | 384.6 KB | **0.76x** | **0.05x** |
| `sv_lowcard_64` | 993.5 KB | 7.7 MB | 752.8 KB | **0.76x** | **0.10x** |
| `sv_rand_uniform` | 7.6 MB | 7.7 MB | 7.7 MB | 1.00x | 1.00x |
| `sv_counter_steady` | 2.4 MB | 550.8 KB | 2.4 MB | 1.00x | 4.48x |
| `sv_floats_random` | 3.4 MB | 3.9 MB | 3.3 MB | **0.99x** | **0.86x** |
| `sv_doubles_random` | 7.6 MB | 6.8 MB | 6.8 MB | **0.89x** | 1.00x |

## Storage — numeric, multi-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `mv_long_1to4` | 20.5 MB | 20.5 MB | 19.4 MB | **0.95x** | **0.95x** |
| `mv_long_1to16` | 66.3 MB | 66.4 MB | 65.7 MB | 0.99x | 0.99x |
| `mv_long_doc_ids` | 40.8 MB | 41.2 MB | 40.2 MB | **0.99x** | **0.98x** |

## Storage — bytes, single-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `sv_kw_short_random` | 15.3 MB | 13.3 MB | 18.3 MB | 1.20x | 1.38x |
| `sv_kw_lowcard_16` | 9.5 MB | 1.6 MB | 507.2 KB | **0.05x** | **0.31x** |
| `sv_kw_lowcard_512` | 9.5 MB | 2.4 MB | 3.3 MB | **0.35x** | 1.36x |
| `sv_kw_prefix_repeat` | 45.7 MB | 11.8 MB | 21.3 MB | **0.47x** | 1.81x |
| `sv_kw_long_repeated_subs` | 82.5 MB | 19.3 MB | 38.4 MB | **0.47x** | 1.98x |
| `sv_kw_ip_v4` | 3.8 MB | 4.1 MB | 5.8 MB | 1.52x | 1.43x |

## Storage — bytes, multi-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `mv_kw_1to4` | 14.5 MB | 14.4 MB | 17.7 MB | 1.22x | 1.23x |
| `mv_kw_1to16_tags` | 45.9 MB | 45.8 MB | 58.4 MB | 1.27x | 1.28x |

## Compute — sequential scan, all values touched (median of 3 after 1 warmup)

### Numeric — single-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `sv_ts_seconds` | 2.5 ms | 4.0 ms | 5.8 ms | 2.30x | 1.45x |
| `sv_ts_millis_jitter` | 4.5 ms | 2.8 ms | 5.6 ms | 1.23x | 2.00x |
| `sv_gauge_5000pm50` | 2.7 ms | 2.5 ms | 8.9 ms | 3.34x | 3.55x |
| `sv_lowcard_8` | 2.6 ms | 2.5 ms | 6.1 ms | 2.33x | 2.43x |
| `sv_lowcard_64` | 3.1 ms | 2.5 ms | 6.1 ms | 1.99x | 2.42x |
| `sv_rand_uniform` | 3.4 ms | 2.2 ms | 5.5 ms | 1.60x | 2.46x |
| `sv_counter_steady` | 2.6 ms | 2.6 ms | 5.3 ms | 2.07x | 2.00x |
| `sv_floats_random` | 3.3 ms | 2.7 ms | 5.4 ms | 1.63x | 1.97x |
| `sv_doubles_random` | 3.0 ms | 3.0 ms | 6.1 ms | 2.05x | 2.05x |

### Numeric — multi-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `mv_long_1to4` | 12.3 ms | 14.2 ms | 15.5 ms | 1.27x | 1.10x |
| `mv_long_1to16` | 5.5 ms | 7.6 ms | 10.4 ms | 1.90x | 1.38x |
| `mv_long_doc_ids` | 11.2 ms | 19.7 ms | 9.4 ms | **0.84x** | **0.48x** |

### Bytes — single-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `sv_kw_short_random` | 5.7 ms | 16.1 ms | 9.4 ms | 1.66x | **0.58x** |
| `sv_kw_lowcard_16` | 10.9 ms | 10.7 ms | 3.5 ms | **0.32x** | **0.33x** |
| `sv_kw_lowcard_512` | 10.6 ms | 11.6 ms | 9.8 ms | **0.92x** | **0.84x** |
| `sv_kw_prefix_repeat` | 3.9 ms | 9.8 ms | 4.5 ms | 1.17x | **0.46x** |
| `sv_kw_long_repeated_subs` | 4.9 ms | 10.4 ms | 2.6 ms | **0.52x** | **0.25x** |
| `sv_kw_ip_v4` | 4.4 ms | 6.8 ms | 11.1 ms | 2.55x | 1.64x |

### Bytes — multi-valued

| workload | lucene104 | es95tsdb | columnar | col/lucene | col/tsdb |
|---|---:|---:|---:|---:|---:|
| `mv_kw_1to4` | 96.7 ms | 94.0 ms | 13.2 ms | **0.14x** | **0.14x** |
| `mv_kw_1to16_tags` | 83.7 ms | 87.6 ms | 4.8 ms | **0.06x** | **0.05x** |

## Where ColumNAR wins on both dimensions

- **Low-cardinality keyword** (`sv_kw_lowcard_16`) — 19× smaller, ~3× faster scan. The
  dict-binary path holds the dictionary in heap; reads materialise bytes via direct
  array lookup instead of the term-dict walk that `lucene104` / `es95tsdb` do.
- **Multi-valued keyword** (`mv_kw_1to4`, `mv_kw_1to16_tags`) — storage 1.2-1.3×
  larger than baselines. Compute looks dramatically faster (7-20×), but per the
  fairness caveat above this is the cost of `SortedSetDocValues` ord-iteration vs
  value-iteration, not a like-for-like compute win.
- **Monotonic / patterned numerics** — `sv_ts_seconds` is 7.6× smaller than `lucene104`
  and 19% smaller than TSDB. Scan latency is higher (the bridge re-packs an `'L'`-shape
  payload per doc) but the storage win dominates for bulk-load and skip-pushdown.
- **Multi-valued ascending IDs** (`mv_long_doc_ids`) — slight storage win **and** 2×
  faster scan than TSDB. The packed-long MV pipeline beats `SortedNumericDocValues` at
  this access pattern.

## Where ColumNAR loses

### Storage
- **`sv_counter_steady`** — 4.48× larger than TSDB. TSDB's offset stage catches this
  distribution; our pipeline's offset gate is more conservative.
- **`sv_kw_short_random`**, **`sv_kw_ip_v4`** — random short bytes, no repetition for
  LZ4 to find. Per-block headers and the dict-binary cap (256 entries) work against us.
- **Multi-valued bytes** (`mv_kw_1to4`, `mv_kw_1to16_tags`) — 22-28% larger than
  baselines. The `'B'`-shape per-doc header isn't fully recovered by LZ4 across short
  random values.

### Compute
- **All single-valued numeric workloads** — ColumNAR scan is 1.5-3.5× slower than the
  native numeric DV path. The bridge has to decode the per-doc `'L'`-shape payload
  before exposing the long; the native `NumericDocValues` path is two array lookups.
  This cost is currently unavoidable on the bridge read path; the format-native bulk
  seam (`readValues(long[], int)`) sidesteps it when the consumer takes an ES|QL-style
  bulk block, but the sequential scan benchmarked here goes value-by-value.
- **`sv_kw_ip_v4`**, some short-bytes random workloads — bridge overhead dominates
  when there's no encoding win to recover it.

## How to read this

ColumNAR's design centre — block-adaptive numeric pipeline, dict-binary auto-pick,
LZ4-over-block, bridge-typed access — pays off where the data has *structure* (low
cardinality, monotonicity, prefix repetition, multi-valued sets). On uniformly random
or fixed-width content it pays a small per-block tax and a per-doc bridge tax that
shows up in single-value scans. The regressions above are the things worth opening
issues on; the wins justify owning a per-column-tuned format alongside Lucene's
default.
