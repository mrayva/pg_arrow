# pg_arrow

A PostgreSQL extension that encodes a batch of rows into an
[Apache Arrow](https://arrow.apache.org/) IPC stream (one RecordBatch), and
decodes one back into columnar jsonb for inspection/verification. Built on
the real `libarrow` C++ library (`libarrow-dev`, apt.arrow.apache.org), not
a hand-rolled implementation of the IPC spec.

```sql
CREATE TYPE trade AS (symbol text, price numeric(10,2), qty bigint);

SELECT rows_to_arrow(ARRAY[
    ROW('AAPL', 227.15, 100)::trade,
    ROW('MSFT', 412.30, 50)::trade
]);
-- bytea: one Arrow IPC stream, one RecordBatch, 2 rows x 3 columns

SELECT arrow_to_jsonb(rows_to_arrow(ARRAY[
    ROW('AAPL', 227.15, 100)::trade,
    ROW('MSFT', 412.30, 50)::trade
]));
-- {"symbol": ["AAPL","MSFT"], "price": [227.15,412.30], "qty": [100,50]}
```

## Why a separate extension, not part of pg_zerialize

[pg_zerialize](https://github.com/mrayva/pg_zerialize) already covers 7
binary wire formats (msgpack/cbor/zera/flexbuffers/ion/bson/beve) via the
[zerialize](https://github.com/mrayva/zerialize) library's generic
Reader/Writer interface. Arrow doesn't fit that interface: its physical
layout (validity bitmaps, typed column buffers, FlatBuffers-encoded IPC
framing) is fundamentally different from the per-value
`begin_map`/`key`/`int64` streaming model zerialize's Writer concept is
built around. Apache's own builder APIs (`arrow::Int64Builder`,
`arrow::ipc::MakeStreamWriter`, etc.) are far more complete and correct
than reimplementing a meaningful slice of the IPC spec by hand would be -
so this is a standalone extension linking `libarrow` directly, not a
zerialize protocol.

## Columnar-batch-only: no `row_to_arrow(record)`

Unlike pg_zerialize's `row_to_X`/`rows_to_X`/`rows_to_X_columnar` trio,
`pg_arrow` has exactly one encoder: `rows_to_arrow(anyarray)`. There's no
sensible "single-row Arrow document" - Arrow's RecordBatch format carries
real fixed overhead (a schema message, buffer alignment/padding) that only
amortizes across many rows; a one-row RecordBatch would be almost entirely
that overhead. This matches exactly the columnar-batching work done in
pg_zerialize (`rows_to_<fmt>_columnar`) - Arrow's wire format *is* what
that pattern produces, natively, at the physical layout level.

## Type support (v1)

Flat scalar columns only - no nested composite, array, uuid, json/jsonb,
enum, or network-address columns (same scope pg_zerialize's own columnar
batch path uses). Supported column types:

| PostgreSQL type | Arrow type |
|---|---|
| `int2` | `int16` |
| `int4` | `int32` |
| `int8` | `int64` |
| `float4` | `float32` |
| `float8` | `float64` |
| `bool` | `boolean` |
| `text`/`varchar`/`bpchar` | `utf8` |
| `bytea` | `binary` |
| `date` | `date32` (Unix epoch) |
| `timestamp` | `timestamp[us]` (naive) |
| `timestamptz` | `timestamp[us, tz=UTC]` |
| `numeric(p,s)` | `decimal128(p,s)` - see below |
| `numeric` (unconstrained) | `utf8` - see below |

### NUMERIC: the one place this differs from pg_zerialize

Arrow has a real fixed-point decimal type (`decimal128`) that PostgreSQL's
`NUMERIC` actually maps to precisely - pg_zerialize's own formats don't
have an equivalent, so it converts NUMERIC to float8 or text. `pg_arrow`
does better where it can:

- **`numeric(p,s)`** (a declared precision/scale): mapped exactly to
  `decimal128(p, s)`. Lossless.
- **Unconstrained `numeric`** (no declared precision/scale, `typmod ==
  -1`): different rows can have arbitrary/differing scale, but Arrow's
  `decimal128` needs one fixed `(precision, scale)` for the whole column.
  There's no single fixed decimal type guaranteed to fit every row
  losslessly, so this falls back to a `utf8` column carrying the exact
  `numeric_out()` text - lossless, but **not** a decimal type. If you need
  a real `decimal128` column, declare the source column's precision/scale
  explicitly.
- A declared `numeric(p,s)` outside what `decimal128` can represent
  (`p > 38`, or `s < 0` - PostgreSQL 15+ allows negative-scale NUMERIC)
  also falls back to the same `utf8` text path.

### Dates and timestamps: Unix epoch, not PostgreSQL's

PostgreSQL's `date`/`timestamp(tz)` are internally relative to
2000-01-01. Arrow's `date32`/`timestamp` types are relative to the Unix
epoch (1970-01-01) by definition, and real Arrow tooling (pandas, polars,
DuckDB, ...) interprets them that way unconditionally - so `pg_arrow`
converts, unlike pg_zerialize's wire formats (which just pass through the
raw PostgreSQL-epoch integer, fine there since only pg_zerialize's own
decoder ever reads it back). Verified independently against `pyarrow`
(not just this extension's own decoder) during development.

## Empty input

`rows_to_arrow(ARRAY[]::sometype[])` produces a zero-row RecordBatch **with
the real column schema still attached** - Arrow's format always carries a
schema, so this is the natural behavior, not pg_zerialize's schema-less
`{}`. This only works for a concretely-typed composite array; an empty
array of anonymous `record` has no schema to fall back on (there's no
element to introspect a runtime typmod from) and raises an error instead.

## Building

Requires `libarrow-dev` (apt.arrow.apache.org has current packages; this
was built and tested against 19.0.1):

```sh
make
sudo make install
psql -c "CREATE EXTENSION pg_arrow;"
make installcheck   # pg_regress suite
```

## Verification

Beyond the pg_regress suite (`sql/`/`expected/`), this was cross-checked
against 20,000 real rows from an existing NYSE trade fixture table already
used by pg_zerialize's own test/benchmark work: `arrow_to_jsonb(
rows_to_arrow(batch))` compared (structural jsonb equality, not string
comparison) against `msgpack_to_jsonb(rows_to_msgpack_columnar(batch))` on
the identical 100-row batches - 200/200 batches matched exactly, an
independent confirmation against pg_zerialize's already-verified reference
output.

## Performance vs pg_zerialize's msgpack columnar path

Measured server-side (`clock_timestamp()`, with an untimed warmup call
before each timed loop, 5-100 reps depending on batch size) on the same
NYSE trade fixture used above, comparing `rows_to_arrow`/`arrow_to_jsonb`
against `rows_to_msgpack_columnar`/`msgpack_to_jsonb`.

All 15 columns (mostly `text`):

| Batch | Arrow encode | msgpack encode | Arrow decode | msgpack decode | Arrow size | msgpack size |
|---|---|---|---|---|---|---|
| 500 rows | 0.099 ms | 0.056 ms | 0.204 ms | 0.395 ms | 66 KB | 42 KB |
| 5,000 rows | 1.36 ms | 0.62 ms | 2.35 ms | 3.86 ms | 645 KB | 418 KB |
| 50,000 rows | 16.2 ms | 6.35 ms | 26.1 ms | 38.4 ms | 6.41 MB | 4.16 MB |

A narrower, more numeric-heavy 5-column projection of the same rows
(`Exchange`, `Symbol`, `Trade Volume` (int8), `Trade Price` (float8),
`Sequence Number` (int8)):

| Batch | Arrow encode | msgpack encode | Arrow decode | msgpack decode | Arrow size | msgpack size |
|---|---|---|---|---|---|---|
| 100 rows | 0.0093 ms | 0.0039 ms | 0.0224 ms | 0.0337 ms | 4.4 KB | 2.2 KB |
| 500 rows | 0.028 ms | 0.018 ms | 0.097 ms | 0.160 ms | 18.8 KB | 10.9 KB |
| 5,000 rows | 0.257 ms | 0.176 ms | 0.973 ms | 1.607 ms | 181 KB | 108 KB |
| 50,000 rows | 2.55 ms | 1.81 ms | 9.72 ms | 15.76 ms | 1.83 MB | 1.10 MB |

Two patterns hold across both schemas: msgpack encodes faster (Arrow pays
fixed per-column builder setup - narrowing from 15 to 5 columns shrinks
that gap from ~2.2-2.6x down to ~1.4-1.6x, since there's proportionally
less of it to pay), and Arrow decodes ~1.5-1.65x *faster* regardless of
column count or type (typed columnar buffers read back more cheaply than
msgpack's tag-by-tag parsing).

Size is the one place the schema comparison overturns the intuitive guess:
Arrow ran ~54% larger on the wide/text schema, but ~67-72% larger on the
narrower, more-numeric one - *worse*, not better. Each Arrow column
carries a fixed validity bitmap + alignment padding regardless of value
width, and that fixed cost is a larger fraction of an 8-byte int64/float8
value than of msgpack's compact fixed-width encoding of the same value -
so narrowing toward numeric columns doesn't shrink Arrow's size overhead
the way narrowing toward numeric columns might seem like it should.

Pick msgpack when encode-bound (e.g. a hot publish path) or size-sensitive,
Arrow when decode-bound or when the consumer is real Arrow-ecosystem
tooling (pandas/polars/DuckDB) that would otherwise need a conversion
step.

## Consuming from nats_tool

[nats_asio](https://github.com/mrayva/nats_asio)'s `nats_tool` can decode
`rows_to_arrow()` output published over NATS directly, the same way it
already handles pg_zerialize's wire formats:

```sh
nats_tool grub --topic trades.arrow --json --format arrow
nats_tool grub --topic trades.arrow --json --format arrow --expand_columnar
```

Built with `NATS_ASIO_ENABLE_ARROW` (default `ON`, requires `libarrow-dev`
at build time). Decode-only - there's no `--format arrow` for `pub` mode,
since `rows_to_arrow()` is a SQL-side batch construct, not something
buildable from arbitrary JSON input. Verified end-to-end against real rows:
`rows_to_arrow()` here -> `nats_publish_binary()` (pgnats) -> `nats_tool
grub --format arrow --json`, cross-checked structurally against
`msgpack_to_jsonb(rows_to_msgpack_columnar(...))` on the same rows.

## Not (yet) built

- Nested/composite/array columns.
- An `arrow_populate_record`/`arrow_populate_recordset` decode-to-typed-
  composite path (pg_zerialize's `X_populate_record(set)` equivalent) -
  `arrow_to_jsonb` covers verification/inspection for now.

## See Also

- [`pg_zerialize`](https://github.com/mrayva/pg_zerialize): sister
  extension covering 7 other binary wire formats (msgpack/cbor/zera/
  flexbuffers/ion/bson/beve) via the zerialize library - richer feature
  surface (nested composites, `X_populate_record(set)`, JSONB builders)
  and generally faster to encode, but without Arrow's typed columnar
  layout or Arrow-ecosystem interop.
