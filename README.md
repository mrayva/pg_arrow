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

## Not (yet) built

- Nested/composite/array columns.
- `nats_tool --format arrow` decode support in nats_asio - Arrow isn't a
  zerialize protocol, so this would need `libarrow` linked into nats_asio
  directly; a separate, later effort.
- An `arrow_populate_record`/`arrow_populate_recordset` decode-to-typed-
  composite path (pg_zerialize's `X_populate_record(set)` equivalent) -
  `arrow_to_jsonb` covers verification/inspection for now.
