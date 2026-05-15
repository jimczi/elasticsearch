# ColumNAR — columnar doc-values format

ColumNAR (Native Adaptive Representation) is a Lucene `DocValuesFormat` tuned for
columnar workloads. It lives at `libs/columnar/` and depends only on `lucene-core`. The
library ships:

- `ColumNARDocValuesFormat` — the Lucene format, registered via SPI under the name
  `ColumNARDocValuesFormat`.
- A bridge layer (`org.elasticsearch.columnar.bridge`) that gives indexer and reader code
  typed entry points over the format's single on-disk substrate.
- Composable encoder, encoding, and skip-index extension points
  (`org.elasticsearch.columnar.encoder`) resolved through Lucene-style `NamedSPILoader`
  registries.
- A small set of low-level primitives (`org.elasticsearch.columnar.primitive`) — bit-pack,
  delta, GCD, RLE, dictionary — usable from any encoder.

## First principles

**Lucene's default `DocValuesFormat` is one format among many.** Lucene as a library
can't be optimised for every workload; that's by design, and it's why
`DocValuesFormat` is an extension point. ColumNAR is an additional format alongside
the Lucene default — picked through `PerFieldDocValuesFormat` per field, never
replacing the default everywhere.

Owning a format here lets the library:

- **Move at our own pace** on the things that matter for columnar reads — block-level
  encoder adaptation, dictionary auto-pick, bridge-level typed access, prefetch hooks
  for blob-storage directories — without coordinating every change with the upstream
  release cycle.
- **Cover workloads the default isn't tuned for** — bridge-routed long fields with
  block-adaptive pipelines, low-cardinality keyword with per-segment dictionaries,
  prefix-rich bytes with block-wide LZ4, multi-valued numerics with count streams.
- **Stay Lucene-shaped.** ColumNAR plugs into Lucene through standard SPI; on read it
  exposes plain `BinaryDocValues`; on write it accepts `BinaryDocValuesField`. Every
  encoder / encoding / skip-index extension uses Lucene's `NamedSPILoader` pattern.
  When a refinement we built here makes sense upstream, the path is clear — port it
  into Lucene. The work we do on the consumer side (ES|QL block loaders, search
  aggregations) still applies to Lucene's formats too.

In short: Lucene first for the substrate (file format mechanics, `IndexInput`,
`DocValuesFormat` contract, `NamedSPILoader`), ColumNAR for the per-column choices we
need to evolve quickly.

## What it stores

**Binary doc values, and only binary doc values.** Every value that Lucene knows about —
numeric, keyword, IP, multi-valued, anything — becomes a byte payload on disk. The
format's Lucene-facing surface accepts `addBinaryField` and rejects every typed write
(`addNumericField`, `addSortedField`, `addSortedNumericField`, `addSortedSetField`) with
`UnsupportedOperationException`. There is one storage substrate, not five.

Typed access happens **above** the format, in the bridge. To index a long field you use
`ColumNARLongField` (a `BinaryDocValuesField` subclass that packs the long into the
payload format the readers expect). To read it back you wrap the standard Lucene
`BinaryDocValues` in `PackedLongsFromBinaryDocValues`, which exposes `ColumNARLongValues`:
a `DocIdSetIterator` with typed accessors (`longAt`, `intAt`, `floatAt`, `doubleAt`) and a
bulk seam (`readValues(long[], int)`) shaped for ES|QL block-loader builders. Keyword
fields use the same pattern with `ColumNARKeywordField` and
`PackedBytesFromBinaryDocValues`.

## Multi-valued is the same shape as single-valued

A document with `[1, 2, 3]` and a document with `42` write to the same field through the
same bridge entry point. The packer writes a one-byte shape marker, a varint value count,
then the values. Single-valued docs cost one extra varint (`0x01`); multi-valued docs cost
one varint plus per-value bytes.

**Insertion order is preserved.** The bridge never sorts within a document, never dedups,
never exposes ordinals. Lucene's `SortedNumericDocValues` (sorts within doc) and
`SortedSetDocValues` (sorts + dedups, exposes ords) are explicitly **not** the model —
they're rejected on write. Downstream code that needs sorted or ord-indexed views builds
those adapters over the bridge's bytes, not over a typed Lucene API.

## The four pluggable seams

The format wires together four things, each with a stable integer id persisted in
per-field metadata and resolved at read time via `ServiceLoader`-discovered registries:

1. **Numeric block encoder** (`NumericBlockEncoder`) — turns a block of longs into bytes.
   The dict-binary write path also runs ordinal blocks through a numeric encoder.
2. **Bytes block encoder** (`BytesBlockEncoder`) — turns a block of byte-sequence values
   into bytes. Used directly for binary fields.
3. **Block encoding** (`BlockEncoding`) — wraps the encoder's output with an optional
   outer layer (LZ4 today, identity pass-through for measurement; Zstd reserved). Decode
   is level-agnostic so writers can pick fast/high without affecting readers.
4. **Skip index** (`SkipIndex`) — per-column doc-id-range index. Lucene's
   `DocValuesSkipper` surfaces level-0 today.

Each id is wire-format-frozen: once an encoder/encoding/skip-index id ships in a release
the bytes it produces stay readable forever. New behaviour ships as a new id, never as a
silent change to an existing one. Framing changes ship as a new format class with a new
SPI name (Lucene's `Lucene90DocValuesFormat` → `Lucene104DocValuesFormat` precedent).

## Configuring the format

Each `ColumNARDocValuesFormat` instance is conceptually configured for **one field**.
Multi-field routing is the job of Lucene's `PerFieldDocValuesFormat` above this layer —
hand it the right instance for each field. The format itself doesn't carry per-field
branching.

The no-arg constructor (what Lucene SPI uses) wires the production defaults. To override:

```java
DocValuesFormat fmt = new ColumNARDocValuesFormat(
    BitPackBlockEncoder.INSTANCE,       // NumericBlockEncoder
    RawBytesBlockEncoder.INSTANCE,      // BytesBlockEncoder
    Lz4BlockEncoding.INSTANCE,          // BlockEncoding
    NumericMinMaxSkipIndex.INSTANCE,    // SkipIndex
    SkipIndexParams.DEFAULTS,           // SkipIndexParams
    1 << 20,                            // targetEncodedBytesPerBlock (1 MB)
    1 << 16,                            // maxValuesPerBlock (65 536 row cap)
    true                                // preferDictionaryForBinary
);
```

Every argument that affects the on-disk bytes is persisted in the field's metadata as a
registry id or an int, so the reader reconstructs everything without consulting the
writer's format instance. The two ints (`targetEncodedBytesPerBlock` and
`maxValuesPerBlock`) control when a block closes — whichever fires first.

See `docs/ENCODERS.md` for which encoder fits which workload, and the Javadoc on each
implementation for what it does to the bytes.

## Module layout

- `org.elasticsearch.columnar` — public format entry points (`ColumNARDocValuesFormat`),
  the bridge-facing iterator + supplier APIs (`LongValuesIterator`, `LongValuesSupplier`,
  `BytesRefValuesIterator`, `BytesRefValuesSupplier`, `NumericType`), plus the
  package-private consumer + producer.
- `org.elasticsearch.columnar.bridge` — typed indexable fields (`ColumNARLongField`,
  `ColumNARKeywordField`, …) and typed read iterators (`ColumNARLongValues`,
  `ColumNARBytesValues`) over the binary substrate.
- `org.elasticsearch.columnar.encoder` — block-encoder / block-encoding / skip-index SPI
  contracts, registries, and concrete implementations.
- `org.elasticsearch.columnar.primitive` — bit-packing, delta, GCD, RLE, dictionary.
  No Lucene dependency; usable from any encoder.
- `org.elasticsearch.columnar.numericpipeline` — TSDB-style numeric pipeline encoder
  (delta → offset → GCD → bit-pack stages, decided per block).
- `org.elasticsearch.columnar.skipper` — multi-level skipper with configurable stats.

Granular on-disk details live in each subpackage's `package-info.java` and on the Javadoc
of each encoder / encoding / skip-index class. The format's wire shape is described where
the bytes are written, not in user-facing docs.

## Benchmarks

JMH benches live in `benchmarks/` under
`org.elasticsearch.benchmark.index.codec.columnar.*`. They compare the columnar format
against Lucene's default `DocValuesFormat` and the ES TSDB format on representative
workloads. See `docs/BENCHMARKS.md` for the benchmark design — what each bench measures,
how to run a single bench, and how to extend the matrix.
