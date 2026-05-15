# Encoders, encodings, and skip indexes

Four independent plug-in seams, each registered through Lucene's
[`NamedSPILoader`](https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/NamedSPILoader.java).
A new implementation registers exactly like a Lucene `DocValuesFormat`: implement the
interface, provide a stable `String getName()`, ship a `public` no-arg constructor, and
add the fully-qualified class name to
`META-INF/services/<interface-FQN>`. The format's reader resolves the name on read and
asks the registry for the implementation; the framework does the rest.

## NumericBlockEncoder — `long[]` → bytes

| name | implementation | when to pick |
|---|---|---|
| `Raw` | `RawBlockEncoder` | 8 bytes per long. Measurement baseline. |
| `BitPack` | `BitPackBlockEncoder` | Production default for raw long fields. Min-subtract + bit-pack with per-segment auto-pick to `DeltaPack` on monotonic samples. |
| `DeltaPack` | `DeltaPackedBlockEncoder` | First-order delta + zig-zag + bit-pack. Right for monotonic data. |
| `GcdBitPack` | `GcdBitPackBlockEncoder` | Min-subtract + GCD divide + bit-pack. Right for divisor-aligned data (timestamps rounded to seconds, fixed-step counters). |
| `GcdDeltaPack` | `GcdDeltaPackedBlockEncoder` | Delta + GCD + bit-pack. Right for monotonic + divisor-aligned data. |
| `Pipeline` | `NumericPipelineEncoder` | Per-block adaptive pipeline (delta → offset → GCD → bit-pack), each stage gated by per-block stats. Use when the distribution varies block-by-block. **Production default for bridge-written long fields.** |

## BytesBlockEncoder — `byte[][]` → bytes

| name | implementation | when to pick |
|---|---|---|
| `RawBytes` | `RawBytesBlockEncoder` | `[vint len][bytes]` per value. Default for high-cardinality keyword / text / IP / binary. Inter-value compression comes from the outer `BlockEncoding`. |

Low-cardinality keyword takes a different on-disk shape: the consumer auto-detects (≤ 256
distinct payloads), builds a per-segment dictionary, and writes ordinal blocks through the
**numeric** encoder pipeline. Ordinals are an encoder-internal detail; they never reach
the read API.

## BlockEncoding — outer wrap

| name | implementation | when to pick |
|---|---|---|
| `Identity` | `IdentityBlockEncoding` | Zero-copy pass-through over `IndexInput`. Use for measurement and for fields where the encoder already produces dense bytes. |
| `Lz4` | `Lz4BlockEncoding` | Production default. Uses Lucene's pure-Java LZ4. `Mode.FAST` and `Mode.HIGH` are encoder-only state (the decoder is level-agnostic), so different writers can pick different levels without touching the wire format. |

## SkipIndex — per-column doc-id range index

Surfaces through Lucene's `DocValuesSkipper`.

| name | implementation | when to pick |
|---|---|---|
| `NumericMinMax` | `NumericMinMaxSkipIndex` | Per-interval min/max long. Production default for numeric. |
| `MultiLevelNumericSkipper` | `MultiLevelNumericSkipper` (in `skipper` package) | Multi-level numeric skipper with configurable stats (MIN_MAX / SUM / NULL_COUNT). |

`SkipIndexParams` / `SkipperConfig` control the interval thresholds (max docs, max source
bytes per interval); whichever fires first wins.

## Adding a new implementation

```java
package com.example.columnar;

public final class MyEncoder implements NumericBlockEncoder {
    public static final String NAME = "MyEncoder";

    public MyEncoder() {} // public no-arg ctor — required for ServiceLoader

    @Override public String getName() { return NAME; }
    @Override public int encode(...) { ... }
    @Override public void decode(int formatVersion, ...) {
        // formatVersion is the segment-wide version (see "Versioning" below).
        // Branch on it for backwards-compatible reads of bytes you wrote earlier.
    }
    // ... other interface methods
}
```

Register it:

```
# src/main/resources/META-INF/services/org.elasticsearch.columnar.encoder.NumericBlockEncoder
com.example.columnar.MyEncoder
```

The registry picks it up at first lookup. The name must be ASCII alphanumeric and < 128
characters — same constraint Lucene's `NamedSPILoader` enforces on `DocValuesFormat` names.

Validate the implementation by extending the corresponding base test case (see
`org.elasticsearch.columnar.testing.*`); the base runs the standard round-trip / edge-case
battery, so a new encoder is correct once the base passes.

## Versioning

A **single segment-wide version** lives on the format —
`ColumNARDocValuesFormat.VERSION_CURRENT`. The writer stamps it into the file header; the
reader reads it once and threads it into every `decode(int formatVersion, ...)` call.

Each encoder / encoding / skip-index is responsible for its own backwards compatibility.
The contract is:

- **Names are frozen.** A name published in a shipped release stays in the registry. Old
  segments referencing that name keep loading.
- **Bytes under that name evolve via the version.** When you need to change the wire
  format of an existing implementation, bump `VERSION_CURRENT` once for the whole library
  and add a `if (formatVersion < N)` branch to your `decode`. The writer always produces
  the latest format; the reader branches.
- **BWC is forever, unless an implementation is removed.** Dropping an implementation
  from the registry is the *only* way to stop supporting old segments. Doing so makes
  every segment that referenced that name fail to open with a clear error — by design.

This puts the BWC burden on each implementation, not on the format scaffolding. New
encoders ship as new names. Tweaks to existing encoders ship as version-branching code.
Removals are an explicit, deliberate break.
