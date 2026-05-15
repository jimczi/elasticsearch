# libs/columnar

Lucene `DocValuesFormat` for Elasticsearch columnar workloads. Single-binary substrate,
typed bridge layer, pluggable encoders / encodings / skip indexes. Sits upstream of
`server/`; only depends on `lucene-core`.

## Entry points for contributors

- `README.md` — what the format stores, how multi-valued works, the four pluggable
  seams, how to configure the format.
- `docs/ENCODERS.md` — the built-in encoders, encodings, and skip indexes; when to pick
  which.
- `docs/BENCHMARKS.md` — the JMH benches, what each measures, how to run, how to extend.

Granular on-disk layout lives in each subpackage's `package-info.java` and on the
Javadoc of each encoder / encoding / skip-index class.

## Rules of the road

- Encoder, encoding, and skip-index ids are **frozen forever** once shipped. New
  behaviour ships as a new id, not a silent change.
- The format is **binary-only at the Lucene API surface**. `addNumericField` /
  `addSortedField` / `addSortedNumericField` / `addSortedSetField` all throw UOE. Typed
  views are bridge-side.
- **Ordinals are an encoder-internal detail.** They never reach the read API.
- Imports follow Elasticsearch convention — no wildcards, formatter in `:libs:columnar:spotlessApply`.
- New encoder / encoding / skip-index implementations register through `ServiceLoader`.
