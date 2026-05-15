# PLAN — open work items

Snapshot of what's still on the table after this session. Companion to `REPORTS.md`
(numbers) and `CONTRIBUTING.md` (how to land changes). When something here gets done,
move it to the changelog of the PR and delete the entry — this file is the live
backlog, not a history.

## High-priority — surfaced by REPORTS.md regressions

- **`sv_counter_steady` storage regression (4.48× vs TSDB).** Investigate the offset
  stage gate in `NumericPipelineEncoder` — TSDB catches stepper-with-resets,
  ColumNAR's predicate misses it. Likely a small tweak in `OffsetStage.shouldApply`.
- **Numeric SV scan latency (1.5–3.5× the native path).** The bridge re-packs an
  `'L'`-shape payload per doc on read, which the per-doc scan benchmark counts. Two
  approaches: (a) tighten the bridge decode hot path with a smaller per-doc payload
  scratch + cached `BytesRef` reuse, (b) push consumers toward the bulk seam
  `ColumNARLongValues.readValues(long[], int)` which sidesteps the re-pack entirely.
- **`sv_kw_lowcard_512` and `sv_kw_short_random` storage / scan.** The 256-entry
  dict-binary cap is too conservative for fields that have a few thousand distinct
  values. A larger cap (and an ord-stream that bit-packs more aggressively) likely
  closes both gaps.
- **Multi-valued bytes storage (1.22–1.28×).** The `'B'`-shape per-doc header adds a
  marker + vint count we don't recover. A per-block count-stream variant (mirror of
  the multi-valued packed-long path) would let the marker live once per block instead
  of once per doc.

## Format extensions

- **Base test cases for new encoder / encoding / skip-index.** Reusable
  `Base*TestCase` classes under `org.elasticsearch.columnar.testing` (or a
  `testFixtures` source set if the build supports it) so downstream module authors
  validate a new implementation by extending the right base and getting a standard
  battery of round-trip / edge-case assertions. Lucene's `BaseDocValuesFormatTestCase`
  is the model.
- **Zstd `BlockEncoding`.** Drafted; needs `module-info.java` on `libs/columnar` plus
  a qualified export of `org.elasticsearch.nativeaccess` from `libs/native`. Same
  module surgery that unblocks the MemorySegment seam.
- **MemorySegment read path end-to-end.** `tryReadBlockAsSegment` hook on
  `BlockEncoder`, identity encoding hands back the mmap slice verbatim, ES|QL
  consumes the segment without heap copy. Gated on `module-info.java`.
- **Multi-level numeric skipper as default.** The `skipper` package's
  `MultiLevelNumericSkipper` is registered but not the default. Switch on for
  segments above some doc-count threshold (or unconditionally — level-0 layout
  matches `NumericMinMaxSkipIndex` so no overhead on small segments).
- **Bytes skip index** for range filters on keyword / IP. Per-interval lexicographic
  min/max BytesRef. New `SkipIndex` impl plus a Lucene-side bridge.

## Producer / consumer cleanups

- **Skip-index consolidation.** Two skip-index abstractions live in parallel today
  (`SkipIndex` in `encoder/` and `DocValuesSkipper` in `skipper/`). Pick one as the
  load-bearing one, migrate the other. Done as a sync-up — invasive.
- **Sparse numeric / sparse binary.** Producer / consumer currently assert dense
  fields. A presence stream (bitmap or doc-id deltas) on the substrate would unlock
  sparse columns; rarely needed in our target workloads, file under "later".
- **Map / object field type** (workload #23 in the original plan). Native columnar
  shape for structured nested data; deferred until a downstream consumer asks.

## Consumer-side (downstream of `libs/columnar`)

- **ES|QL `OptionalColumnAtATimeReader` adapter** that recognises ColumNAR and goes
  through the bridge's `readValues(long[], int)` bulk seam. Lives in `server/`, not
  here.
- **Server-side legacy aggregation adapters** that synthesise
  `SortedSetDocValues` over the bridge's bytes when an aggregation explicitly needs
  ord-based bucketing.
- **Per-field-type integration** for the mappers that should switch to ColumNAR
  through `PerFieldDocValuesFormat`. One mapper at a time, behind feature flags.

## Tooling

- **Nightly job that runs `StorageReportTests`** and posts `REPORTS.md` diffs as a
  GitHub check. Lets regressions show up the morning after a merge.
- **JMH benchmark coverage** at the same level as the report — ingestion, range
  query, block-load — for every (workload, format) tuple, so CI can catch compute
  regressions too.

## Out-of-scope for this library

These belong somewhere else; flagged here so they don't get re-proposed.

- Mapping plumbing, `MappedFieldType` per-field knobs — `server/`.
- Synthetic-source layer changes — `server/`.
- Distributed-store integration (S3 / GCS prefetch tuning) — exercised through
  `IndexInput.prefetch` hooks; tuning lives in stateless directory factories, not
  here.

---

When this session resumes, start from this list (after the colleague-review feedback
is in). The `CONTRIBUTING.md` rules apply: small items can be picked up directly;
anything that touches stable contracts (constructor, SPI surface, persisted names,
wire format) gets a sync first.
