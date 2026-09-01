/*
 * pg_arrow.cpp
 * PostgreSQL extension: encode a batch of rows (array of composite records)
 * into an Apache Arrow IPC stream (one RecordBatch), and decode one back
 * into columnar jsonb for verification/inspection.
 *
 * Separate from pg_zerialize deliberately: Arrow's physical layout
 * (validity bitmaps, typed column buffers, FlatBuffers-encoded IPC framing)
 * doesn't fit zerialize's per-value Writer interface, and Apache's own
 * builder APIs are far more complete/correct than hand-rolling the IPC
 * spec. Uses the system libarrow-dev package (already apt-installable
 * here), not a vendored copy.
 *
 * Columnar-batch-only: there's no sensible "single-row Arrow document" (a
 * one-row RecordBatch is almost entirely fixed overhead - schema message,
 * buffer alignment/padding) - so unlike pg_zerialize's row_to_X/rows_to_X/
 * rows_to_X_columnar trio, this has exactly one encoder: rows_to_arrow().
 */

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/jsonb.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"
#include "utils/varbit.h"
#include "access/htup_details.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/inval.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum rows_to_arrow(PG_FUNCTION_ARGS);
Datum arrow_to_jsonb(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(rows_to_arrow);
PG_FUNCTION_INFO_V1(arrow_to_jsonb);
}

// utils/datetime.h (pulled in above) #defines bare-word macros for
// EXTRACT()'s field constants (DAY, SECOND, MONTH, YEAR, HOUR, MINUTE,
// WEEK) that collide with Arrow's own enum member names (e.g.
// arrow::DateUnit::DAY, arrow::TimeUnit::type::SECOND in type_fwd.h) -
// same class of problem pg_zerialize hit with utils/datetime.h's INVALID
// macro clobbering an unrelated glaze declaration. This file never uses
// PostgreSQL's own EXTRACT-field macros, so dropping them is safe.
#undef MONTH
#undef YEAR
#undef DAY
#undef HOUR
#undef MINUTE
#undef SECOND
#undef WEEK

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/endian.h>

#include <vector>
#include <string>
#include <string_view>
#include <memory>
#include <unordered_map>
#include <stdexcept>
#include <cstring>
#include <array>
#include <charconv>
#include <limits>

namespace {

// ===== Schema introspection (pg_arrow's own, scoped-down equivalent of
// pg_zerialize's CachedColumn/CachedSchema/get_cached_schema - deliberately
// not shared code, see the "separate extension" design decision) =====

enum class ArrowKind {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Utf8,
    Binary,
    // A constrained NUMERIC(p,s) maps to the NARROWEST of these four that can
    // represent its declared precision (Decimal32: p<=9, Decimal64: p<=18,
    // Decimal128: p<=38, Decimal256: p<=76) - not always the widest regardless
    // of how small p actually is. Picking the narrowest fitting width matters
    // downstream: nats_sidecar's own Arrow reader natively supports all four
    // widths (widened internally to its own Int256), so a table declaring
    // e.g. numeric(4,2) is what makes that reader's decimal32 path reachable
    // at all from a real Postgres source.
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    // A NUMERIC column that can't map to a fixed DecimalN(precision,scale)
    // for the whole column - either unconstrained (typmod == -1, so
    // different rows may have different scales) or the typmod's declared
    // precision/scale falls outside what Decimal256 (the widest of the four
    // above) can represent (precision > 76 or scale < 0). Postgres itself
    // allows NUMERIC precision up to 1000, so a very wide declared column
    // still falls back here even though Decimal256 covers most real-world
    // cases. Falls back to the exact numeric_out() text, as a Utf8 column -
    // lossless, unlike forcing a fixed decimal that might not fit every row.
    // See README for why.
    NumericText,
    // BIT(n) for n in {8,16,32,64} - the widths where a fixed-length bit string packs into a
    // whole number of bytes with no partial-byte padding (VarBit's own padding rule: only the
    // LAST byte can be partially used, and BIT(n) at these four widths never has one). Postgres
    // stores the bytes most-significant-byte first (utils/varbit.h's own doc comment, confirmed
    // independently against a live instance: B'1000000000000000'::bit(16)::int4 = 32768) -
    // append_value() converts via arrow::bit_util::FromBigEndian. Any other BIT(n) length, and
    // BIT VARYING entirely (its length is a maximum, not an actual per-row length - a real,
    // separate design problem), stay Unsupported. See README for the full rationale.
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Date32,
    // Both TIMESTAMP and TIMESTAMPTZ map here: both are stored internally
    // as int64 microseconds since the Postgres epoch (2000-01-01). Only the
    // Arrow field's timezone metadata differs (see build_arrow_type()) -
    // "UTC" for TIMESTAMPTZ (which Postgres always stores normalized to
    // UTC), unset/naive for TIMESTAMP.
    TimestampMicros,
    TimestampMicrosTz,
    Unsupported
};

struct TypeCacheKey {
    Oid tupType;
    int32 tupTypmod;
    bool operator==(const TypeCacheKey& other) const {
        return tupType == other.tupType && tupTypmod == other.tupTypmod;
    }
};

} // namespace

namespace std {
    template<>
    struct hash<TypeCacheKey> {
        size_t operator()(const TypeCacheKey& k) const {
            return hash<Oid>()(k.tupType) ^ (hash<int32>()(k.tupTypmod) << 1);
        }
    };
}

namespace {

struct CachedColumn {
    int attnum;
    std::string name;
    Oid typid;
    int32 typmod;
    ArrowKind kind;
    // Only meaningful when kind is one of Decimal32/64/128/256.
    int32_t decimal_precision;
    int32_t decimal_scale;
    std::shared_ptr<arrow::Field> field;
};

struct CachedSchema {
    TupleDesc tupdesc;
    std::vector<CachedColumn> columns;
    std::shared_ptr<arrow::Schema> arrow_schema;
    bool has_unsupported_columns;
};

static std::unordered_map<TypeCacheKey, CachedSchema> schema_cache;

static inline void clear_schema_cache()
{
    for (auto& entry : schema_cache) {
        FreeTupleDesc(entry.second.tupdesc);
    }
    schema_cache.clear();
}

static void schema_syscache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    (void)arg; (void)cacheid; (void)hashvalue;
    clear_schema_cache();
}

static void schema_relcache_callback(Datum arg, Oid relid)
{
    (void)arg; (void)relid;
    clear_schema_cache();
}

// Postgres's numeric typmod encoding: ((precision << 16) | scale) + VARHDRSZ,
// or -1 for an unconstrained column. Standard, stable convention (same one
// numeric_send/numeric_recv and \d use).
static inline void decode_numeric_typmod(int32 typmod, int32_t* precision, int32_t* scale)
{
    int32 tmp = typmod - VARHDRSZ;
    *precision = (tmp >> 16) & 0xFFFF;
    *scale = tmp & 0xFFFF;
}

static ArrowKind classify_column(Oid typid, int32 typmod, int32_t* out_precision, int32_t* out_scale)
{
    *out_precision = 0;
    *out_scale = 0;
    switch (typid) {
        case INT2OID: return ArrowKind::Int16;
        case INT4OID: return ArrowKind::Int32;
        case INT8OID: return ArrowKind::Int64;
        case FLOAT4OID: return ArrowKind::Float32;
        case FLOAT8OID: return ArrowKind::Float64;
        case BOOLOID: return ArrowKind::Boolean;
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            return ArrowKind::Utf8;
        case BYTEAOID:
            return ArrowKind::Binary;
        case DATEOID:
            return ArrowKind::Date32;
        case TIMESTAMPOID:
            return ArrowKind::TimestampMicros;
        case TIMESTAMPTZOID:
            return ArrowKind::TimestampMicrosTz;
        case NUMERICOID: {
            if (typmod == -1) {
                return ArrowKind::NumericText;
            }
            int32_t precision, scale;
            decode_numeric_typmod(typmod, &precision, &scale);
            // Decimal256 (the widest of the four we emit) supports precision
            // in [1,76] and requires scale >= 0 (Postgres 15+ allows
            // NUMERIC(p, negative_scale), which none of Decimal32/64/128/256
            // can represent) - fall back to text for anything outside that
            // range, same reasoning as unconstrained. Postgres itself allows
            // precision up to 1000, so precision in (76,1000] still falls
            // back here too - a known, deliberately-unclosed gap (see
            // NumericText's own comment).
            if (precision < 1 || precision > 76 || scale < 0 || scale > precision) {
                return ArrowKind::NumericText;
            }
            *out_precision = precision;
            *out_scale = scale;
            // Narrowest decimal width that fits the DECLARED precision, not
            // always the widest - matches Arrow's own kMaxPrecision per width
            // (Decimal32Type::kMaxPrecision=9, Decimal64Type::kMaxPrecision=18,
            // Decimal128Type::kMaxPrecision=38, Decimal256Type::kMaxPrecision=76).
            // A value that's individually smaller than its column's declared
            // precision still gets the column's own (wider) width - width is a
            // schema-level property of the whole column, not chosen per row.
            if (precision <= arrow::Decimal32Type::kMaxPrecision) {
                return ArrowKind::Decimal32;
            }
            if (precision <= arrow::Decimal64Type::kMaxPrecision) {
                return ArrowKind::Decimal64;
            }
            if (precision <= arrow::Decimal128Type::kMaxPrecision) {
                return ArrowKind::Decimal128;
            }
            return ArrowKind::Decimal256;
        }
        case BITOID:
            // typmod is the bit count directly for BIT(n) - not packed like NUMERIC's
            // precision/scale (verified against a live instance: bit(8)/16/32/64 -> atttypmod
            // 8/16/32/64 exactly). No VARBITOID (BIT VARYING) case here at all - its length is a
            // maximum, not an actual per-row length, so it falls to Unsupported below like any
            // other unhandled type, same as any other BIT(n) width.
            switch (typmod) {
                case 8:  return ArrowKind::UInt8;
                case 16: return ArrowKind::UInt16;
                case 32: return ArrowKind::UInt32;
                case 64: return ArrowKind::UInt64;
                default: return ArrowKind::Unsupported;
            }
        default:
            return ArrowKind::Unsupported;
    }
}

static std::shared_ptr<arrow::DataType> build_arrow_type(const CachedColumn& col)
{
    switch (col.kind) {
        case ArrowKind::Int16: return arrow::int16();
        case ArrowKind::Int32: return arrow::int32();
        case ArrowKind::Int64: return arrow::int64();
        case ArrowKind::Float32: return arrow::float32();
        case ArrowKind::Float64: return arrow::float64();
        case ArrowKind::Boolean: return arrow::boolean();
        case ArrowKind::Utf8: return arrow::utf8();
        case ArrowKind::Binary: return arrow::binary();
        case ArrowKind::NumericText: return arrow::utf8();
        case ArrowKind::Decimal32: return arrow::decimal32(col.decimal_precision, col.decimal_scale);
        case ArrowKind::Decimal64: return arrow::decimal64(col.decimal_precision, col.decimal_scale);
        case ArrowKind::Decimal128: return arrow::decimal128(col.decimal_precision, col.decimal_scale);
        case ArrowKind::Decimal256: return arrow::decimal256(col.decimal_precision, col.decimal_scale);
        case ArrowKind::UInt8: return arrow::uint8();
        case ArrowKind::UInt16: return arrow::uint16();
        case ArrowKind::UInt32: return arrow::uint32();
        case ArrowKind::UInt64: return arrow::uint64();
        case ArrowKind::Date32: return arrow::date32();
        case ArrowKind::TimestampMicros: return arrow::timestamp(arrow::TimeUnit::MICRO);
        case ArrowKind::TimestampMicrosTz: return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
        case ArrowKind::Unsupported: break;
    }
    return nullptr;
}

static const CachedSchema& get_cached_schema(Oid tupType, int32 tupTypmod)
{
    TypeCacheKey key{tupType, tupTypmod};
    auto it = schema_cache.find(key);
    if (it != schema_cache.end()) {
        return it->second;
    }

    TupleDesc tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
    MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
    TupleDesc cached_tupdesc = CreateTupleDescCopy(tupdesc);
    MemoryContextSwitchTo(old_context);
    ReleaseTupleDesc(tupdesc);

    CachedSchema schema;
    schema.tupdesc = cached_tupdesc;
    schema.has_unsupported_columns = false;
    schema.columns.reserve(cached_tupdesc->natts);

    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int i = 0; i < cached_tupdesc->natts; i++) {
        Form_pg_attribute att = TupleDescAttr(cached_tupdesc, i);
        if (att->attisdropped) {
            continue;
        }

        CachedColumn col;
        col.attnum = i + 1;
        col.name = std::string(NameStr(att->attname));
        col.typid = att->atttypid;
        col.typmod = att->atttypmod;
        col.kind = classify_column(col.typid, col.typmod, &col.decimal_precision, &col.decimal_scale);
        if (col.kind == ArrowKind::Unsupported) {
            schema.has_unsupported_columns = true;
        }
        auto arrow_type = build_arrow_type(col);
        // A null arrow_type only happens for Unsupported; schema.columns
        // still gets an entry (for error messages naming the column) but
        // has_unsupported_columns already gates rows_to_arrow() from ever
        // building a RecordBatch with it, so `field` is left null here.
        if (arrow_type) {
            col.field = arrow::field(col.name, arrow_type, /*nullable=*/true);
            fields.push_back(col.field);
        }
        schema.columns.push_back(std::move(col));
    }

    schema.arrow_schema = arrow::schema(fields);

    auto [inserted_it, inserted] = schema_cache.emplace(key, std::move(schema));
    (void)inserted;
    return inserted_it->second;
}

// ===== Batch schema resolution (mirrors pg_zerialize's
// columnar_batch_schema()) =====

static const CachedSchema* columnar_batch_schema(Datum* elements, bool* nulls, int nitems)
{
    const CachedSchema* schema = nullptr;
    Oid schema_type = InvalidOid;
    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) {
            continue;
        }
        HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
        Oid tupType = HeapTupleHeaderGetTypeId(rec);
        if (!schema) {
            int32 tupTypmod = HeapTupleHeaderGetTypMod(rec);
            schema = &get_cached_schema(tupType, tupTypmod);
            schema_type = tupType;
        } else if (tupType != schema_type) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("rows_to_arrow requires all rows to share the same composite type"),
                     errdetail("Row %d has a different type OID than earlier rows.", i)));
        }
    }
    if (!schema) {
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("rows_to_arrow requires at least one non-null row to determine the column schema")));
    }
    if (schema->has_unsupported_columns) {
        std::string bad_cols;
        for (const auto& col : schema->columns) {
            if (col.kind == ArrowKind::Unsupported) {
                if (!bad_cols.empty()) bad_cols += ", ";
                bad_cols += col.name;
            }
        }
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("rows_to_arrow does not support this row's column types"),
                 errdetail("Unsupported column(s): %s. Only flat scalar columns are "
                          "supported (no nested composite, array, uuid, json/jsonb, "
                          "enum, or network-address columns).", bad_cols.c_str())));
    }
    return schema;
}

// ===== Row -> RecordBatch =====

// PostgreSQL epoch (2000-01-01) to Unix epoch (1970-01-01) offset, in days -
// standard, stable constants from datatype/timestamp.h. Needed because
// Arrow's Date32/Timestamp types are defined relative to the Unix epoch,
// while Postgres's DateADT/Timestamp(Tz) internal values are relative to
// its own epoch - getting this right matters here (unlike pg_zerialize's
// wire formats) because real Arrow tooling (pandas/polars/DuckDB) will
// interpret these types with strict Unix-epoch semantics.
static constexpr int64_t kPgToUnixEpochDays = POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
static constexpr int64_t kPgToUnixEpochMicros = kPgToUnixEpochDays * 86400LL * 1000000LL;

// Decimal32/Decimal64/Decimal128/Decimal256 all expose the identical FromString(text, &out,
// &precision, &scale)/Rescale(from, to) shape (confirmed directly against arrow/util/decimal.h
// before templating this - not assumed from Decimal128's own API alone), so one function serves
// all four DecimalN widths append_value() actually emits, instead of near-duplicate copies.
template <typename DecimalT>
static void decimalN_from_numeric(Datum value, int32_t target_precision, int32_t target_scale,
                                   DecimalT* out)
{
    char* text = DatumGetCString(DirectFunctionCall1(numeric_out, value));
    DecimalT parsed;
    int32_t parsed_precision = 0, parsed_scale = 0;
    auto status = DecimalT::FromString(text, &parsed, &parsed_precision, &parsed_scale);
    if (!status.ok()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("failed to parse numeric value \"%s\" as a decimal: %s",
                        text, status.ToString().c_str())));
    }
    if (parsed_scale == target_scale) {
        *out = parsed;
        return;
    }
    auto rescaled = parsed.Rescale(parsed_scale, target_scale);
    if (!rescaled.ok()) {
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                 errmsg("numeric value \"%s\" does not fit in decimal(%d,%d)",
                        text, target_precision, target_scale)));
    }
    *out = rescaled.ValueOrDie();
}

static void append_value(arrow::ArrayBuilder* builder, const CachedColumn& col, Datum value, bool isnull)
{
    arrow::Status status;
    if (isnull) {
        status = builder->AppendNull();
    } else {
        switch (col.kind) {
            case ArrowKind::Int16:
                status = static_cast<arrow::Int16Builder*>(builder)->Append(DatumGetInt16(value));
                break;
            case ArrowKind::Int32:
                status = static_cast<arrow::Int32Builder*>(builder)->Append(DatumGetInt32(value));
                break;
            case ArrowKind::Int64:
                status = static_cast<arrow::Int64Builder*>(builder)->Append(DatumGetInt64(value));
                break;
            case ArrowKind::Float32:
                status = static_cast<arrow::FloatBuilder*>(builder)->Append(DatumGetFloat4(value));
                break;
            case ArrowKind::Float64:
                status = static_cast<arrow::DoubleBuilder*>(builder)->Append(DatumGetFloat8(value));
                break;
            case ArrowKind::Boolean:
                status = static_cast<arrow::BooleanBuilder*>(builder)->Append(DatumGetBool(value));
                break;
            case ArrowKind::Utf8: {
                text* t = DatumGetTextPP(value);
                status = static_cast<arrow::StringBuilder*>(builder)->Append(
                    VARDATA_ANY(t), static_cast<int32_t>(VARSIZE_ANY_EXHDR(t)));
                break;
            }
            case ArrowKind::Binary: {
                bytea* b = DatumGetByteaPP(value);
                status = static_cast<arrow::BinaryBuilder*>(builder)->Append(
                    reinterpret_cast<const uint8_t*>(VARDATA_ANY(b)),
                    static_cast<int32_t>(VARSIZE_ANY_EXHDR(b)));
                break;
            }
            case ArrowKind::NumericText: {
                char* text_val = DatumGetCString(DirectFunctionCall1(numeric_out, value));
                status = static_cast<arrow::StringBuilder*>(builder)->Append(text_val);
                break;
            }
            case ArrowKind::Decimal32: {
                arrow::Decimal32 dec;
                decimalN_from_numeric(value, col.decimal_precision, col.decimal_scale, &dec);
                status = static_cast<arrow::Decimal32Builder*>(builder)->Append(dec);
                break;
            }
            case ArrowKind::Decimal64: {
                arrow::Decimal64 dec;
                decimalN_from_numeric(value, col.decimal_precision, col.decimal_scale, &dec);
                status = static_cast<arrow::Decimal64Builder*>(builder)->Append(dec);
                break;
            }
            case ArrowKind::Decimal128: {
                arrow::Decimal128 dec;
                decimalN_from_numeric(value, col.decimal_precision, col.decimal_scale, &dec);
                status = static_cast<arrow::Decimal128Builder*>(builder)->Append(dec);
                break;
            }
            case ArrowKind::Decimal256: {
                arrow::Decimal256 dec;
                decimalN_from_numeric(value, col.decimal_precision, col.decimal_scale, &dec);
                status = static_cast<arrow::Decimal256Builder*>(builder)->Append(dec);
                break;
            }
            // classify_column() only ever picks these four kinds when the column's own typmod
            // (bit count) exactly matches the width, so VARBITBYTES(vb) below is guaranteed to
            // equal sizeof(the target type) - trusted from that column-level classification, not
            // re-validated per row (same convention decimal precision/scale already relies on).
            case ArrowKind::UInt8: {
                VarBit* vb = DatumGetVarBitP(value);
                uint8_t raw;
                std::memcpy(&raw, VARBITS(vb), sizeof(raw)); // single byte - no endian conversion
                status = static_cast<arrow::UInt8Builder*>(builder)->Append(raw);
                break;
            }
            case ArrowKind::UInt16: {
                VarBit* vb = DatumGetVarBitP(value);
                uint16_t raw;
                std::memcpy(&raw, VARBITS(vb), sizeof(raw));
                status = static_cast<arrow::UInt16Builder*>(builder)->Append(
                    arrow::bit_util::FromBigEndian(raw));
                break;
            }
            case ArrowKind::UInt32: {
                VarBit* vb = DatumGetVarBitP(value);
                uint32_t raw;
                std::memcpy(&raw, VARBITS(vb), sizeof(raw));
                status = static_cast<arrow::UInt32Builder*>(builder)->Append(
                    arrow::bit_util::FromBigEndian(raw));
                break;
            }
            case ArrowKind::UInt64: {
                VarBit* vb = DatumGetVarBitP(value);
                uint64_t raw;
                std::memcpy(&raw, VARBITS(vb), sizeof(raw));
                status = static_cast<arrow::UInt64Builder*>(builder)->Append(
                    arrow::bit_util::FromBigEndian(raw));
                break;
            }
            case ArrowKind::Date32: {
                int32_t pg_days = DatumGetDateADT(value);
                status = static_cast<arrow::Date32Builder*>(builder)->Append(
                    static_cast<int32_t>(pg_days + kPgToUnixEpochDays));
                break;
            }
            case ArrowKind::TimestampMicros:
            case ArrowKind::TimestampMicrosTz: {
                int64_t pg_micros = DatumGetTimestamp(value);
                status = static_cast<arrow::TimestampBuilder*>(builder)->Append(
                    pg_micros + kPgToUnixEpochMicros);
                break;
            }
            case ArrowKind::Unsupported:
                // columnar_batch_schema() already rejects any schema
                // containing an Unsupported column before this is reached.
                throw std::logic_error("append_value: unreachable Unsupported column");
        }
    }
    if (!status.ok()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to append value for column \"%s\": %s",
                        col.name.c_str(), status.ToString().c_str())));
    }
}

static std::shared_ptr<arrow::RecordBatch> build_record_batch(
    Datum* elements, bool* nulls, int nitems, const CachedSchema& schema)
{
    const size_t nattrs = static_cast<size_t>(schema.tupdesc->natts);
    std::vector<Datum> all_values(static_cast<size_t>(nitems) * nattrs);
    std::unique_ptr<bool[]> all_nulls = std::make_unique<bool[]>(static_cast<size_t>(nitems) * nattrs);
    std::fill(all_nulls.get(), all_nulls.get() + static_cast<size_t>(nitems) * nattrs, true);

    for (int i = 0; i < nitems; i++) {
        if (nulls[i]) continue;
        HeapTupleHeader rec = DatumGetHeapTupleHeader(elements[i]);
        HeapTupleData tuple;
        tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
        tuple.t_data = rec;
        heap_deform_tuple(&tuple, schema.tupdesc,
                          &all_values[static_cast<size_t>(i) * nattrs],
                          &all_nulls[static_cast<size_t>(i) * nattrs]);
    }

    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(schema.columns.size());

    for (const CachedColumn& col : schema.columns) {
        const int idx = col.attnum - 1;
        std::unique_ptr<arrow::ArrayBuilder> builder;
        arrow::Status make_status = arrow::MakeBuilder(arrow::default_memory_pool(), col.field->type(), &builder);
        if (!make_status.ok()) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to create Arrow builder for column \"%s\": %s",
                            col.name.c_str(), make_status.ToString().c_str())));
        }
        if (!builder->Reserve(nitems).ok()) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to reserve builder capacity for column \"%s\"", col.name.c_str())));
        }
        for (int i = 0; i < nitems; i++) {
            const size_t off = static_cast<size_t>(i) * nattrs + static_cast<size_t>(idx);
            append_value(builder.get(), col, all_values[off], all_nulls[off]);
        }
        std::shared_ptr<arrow::Array> array;
        auto finish_status = builder->Finish(&array);
        if (!finish_status.ok()) {
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to finish Arrow array for column \"%s\": %s",
                            col.name.c_str(), finish_status.ToString().c_str())));
        }
        columns.push_back(std::move(array));
    }

    return arrow::RecordBatch::Make(schema.arrow_schema, nitems, std::move(columns));
}

static bytea* record_batch_to_bytea(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::shared_ptr<arrow::Schema>& schema)
{
    auto stream_result = arrow::io::BufferOutputStream::Create();
    if (!stream_result.ok()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to create Arrow output stream: %s", stream_result.status().ToString().c_str())));
    }
    auto stream = stream_result.ValueOrDie();

    auto writer_result = arrow::ipc::MakeStreamWriter(stream, schema);
    if (!writer_result.ok()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to create Arrow IPC stream writer: %s", writer_result.status().ToString().c_str())));
    }
    auto writer = writer_result.ValueOrDie();

    auto write_status = writer->WriteRecordBatch(*batch);
    if (!write_status.ok()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to write Arrow RecordBatch: %s", write_status.ToString().c_str())));
    }
    if (!writer->Close().ok()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to close Arrow IPC stream writer")));
    }

    auto buffer_result = stream->Finish();
    if (!buffer_result.ok()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("failed to finish Arrow output buffer: %s", buffer_result.status().ToString().c_str())));
    }
    auto buffer = buffer_result.ValueOrDie();

    size_t len = static_cast<size_t>(buffer->size());
    bytea* result = (bytea*) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);
    memcpy(VARDATA(result), buffer->data(), len);
    return result;
}

} // namespace

extern "C" Datum
rows_to_arrow(PG_FUNCTION_ARGS)
{
    ArrayType* arr = PG_GETARG_ARRAYTYPE_P(0);
    Oid element_type = ARR_ELEMTYPE(arr);
    int ndim = ARR_NDIM(arr);

    if (ndim > 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("multidimensional arrays are not supported by rows_to_arrow")));
    }
    if (element_type != RECORDOID && get_typtype(element_type) != TYPTYPE_COMPOSITE) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("rows_to_arrow requires an array of composite records"),
                 errdetail("Got array element type OID %u.", element_type)));
    }

    if (ndim == 0 || ArrayGetNItems(ndim, ARR_DIMS(arr)) == 0) {
        // Empty input -> an empty (zero-row) RecordBatch, but *with* a real
        // schema attached, matching Arrow's own always-has-a-schema
        // convention (unlike pg_zerialize's schema-less {} for an empty
        // columnar batch - see the design notes in the plan/README for
        // why that's the more natural choice here). This is possible for
        // a concretely-typed composite array (e.g. mytype[]) since
        // ARR_ELEMTYPE() gives the declared element type regardless of
        // how many elements are actually present - no row needs
        // inspecting. Only an array of anonymous `record` truly has no
        // schema to fall back on with zero elements (record's structure is
        // only known from an actual value's runtime typmod), so that one
        // case still errors.
        if (element_type == RECORDOID) {
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("rows_to_arrow requires a non-empty array to determine the column schema "
                            "for an array of anonymous record type"),
                     errdetail("An empty array of a concretely-typed composite (e.g. mytype[]) works "
                              "fine; only anonymous record[] needs at least one element.")));
        }
        const CachedSchema& schema = get_cached_schema(element_type, -1);
        if (schema.has_unsupported_columns) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("rows_to_arrow does not support this row's column types")));
        }
        auto batch = build_record_batch(nullptr, nullptr, 0, schema);
        PG_RETURN_BYTEA_P(record_batch_to_bytea(batch, schema.arrow_schema));
    }

    int16 typlen; bool typbyval; char typalign;
    get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
    Datum* elements; bool* nulls; int nitems;
    deconstruct_array(arr, element_type, typlen, typbyval, typalign, &elements, &nulls, &nitems);

    const CachedSchema& schema = *columnar_batch_schema(elements, nulls, nitems);

    bytea* result;
    try {
        auto batch = build_record_batch(elements, nulls, nitems, schema);
        result = record_batch_to_bytea(batch, schema.arrow_schema);
    } catch (const std::exception& ex) {
        pfree(elements);
        pfree(nulls);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("rows_to_arrow failed"), errdetail("%s", ex.what())));
    }

    pfree(elements);
    pfree(nulls);
    PG_RETURN_BYTEA_P(result);
}

// ===== Arrow -> jsonb (decode/verification path) =====

namespace {

// PostgreSQL's own float8_numeric formats internally via "%.*g" with
// DBL_DIG (15 significant digits) - not guaranteed round-trip-exact for
// every double. Same fix pg_zerialize's own decoders already apply for the
// identical reason: format via std::to_chars(..., max_digits10) (17
// digits, round-trip-exact) first, then numeric_in on that text.
static Numeric numeric_from_double_exact(double v)
{
    std::array<char, 64> buffer;
    auto converted = std::to_chars(buffer.data(), buffer.data() + buffer.size(), v,
                                    std::chars_format::general,
                                    std::numeric_limits<double>::max_digits10);
    if (converted.ec != std::errc()) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to format double value")));
    }
    *converted.ptr = '\0';
    return DatumGetNumeric(DirectFunctionCall3(
        numeric_in, CStringGetDatum(buffer.data()), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
}

static JsonbValue* array_column_to_jsonb(const std::shared_ptr<arrow::Array>& array, JsonbParseState** pstate)
{
    pushJsonbValue(pstate, WJB_BEGIN_ARRAY, nullptr);
    for (int64_t i = 0; i < array->length(); i++) {
        JsonbValue jv;
        if (array->IsNull(i)) {
            jv.type = jbvNull;
            pushJsonbValue(pstate, WJB_ELEM, &jv);
            continue;
        }
        switch (array->type_id()) {
            case arrow::Type::INT16:
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(
                    static_cast<const arrow::Int16Array&>(*array).Value(i));
                break;
            case arrow::Type::INT32:
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(
                    static_cast<const arrow::Int32Array&>(*array).Value(i));
                break;
            case arrow::Type::INT64:
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(
                    static_cast<const arrow::Int64Array&>(*array).Value(i));
                break;
            case arrow::Type::FLOAT: {
                double v = static_cast<double>(static_cast<const arrow::FloatArray&>(*array).Value(i));
                jv.type = jbvNumeric;
                jv.val.numeric = numeric_from_double_exact(v);
                break;
            }
            case arrow::Type::DOUBLE: {
                double v = static_cast<const arrow::DoubleArray&>(*array).Value(i);
                jv.type = jbvNumeric;
                jv.val.numeric = numeric_from_double_exact(v);
                break;
            }
            case arrow::Type::BOOL:
                jv.type = jbvBool;
                jv.val.boolean = static_cast<const arrow::BooleanArray&>(*array).Value(i);
                break;
            case arrow::Type::STRING: {
                auto sv = static_cast<const arrow::StringArray&>(*array).GetView(i);
                jv.type = jbvString;
                jv.val.string.val = const_cast<char*>(sv.data());
                jv.val.string.len = static_cast<int>(sv.size());
                break;
            }
            case arrow::Type::BINARY: {
                // Same ["~b", base64, "base64"] tag convention pg_zerialize's
                // decoders use for blobs, for cross-extension consistency.
                auto sv = static_cast<const arrow::BinaryArray&>(*array).GetView(i);
                bytea* b = (bytea*) palloc(sv.size() + VARHDRSZ);
                SET_VARSIZE(b, sv.size() + VARHDRSZ);
                memcpy(VARDATA(b), sv.data(), sv.size());
                text* encoded = DatumGetTextPP(DirectFunctionCall2(
                    binary_encode, PointerGetDatum(b), CStringGetTextDatum("base64")));
                pushJsonbValue(pstate, WJB_BEGIN_ARRAY, nullptr);
                JsonbValue tag; tag.type = jbvString;
                tag.val.string.val = const_cast<char*>("~b"); tag.val.string.len = 2;
                pushJsonbValue(pstate, WJB_ELEM, &tag);
                JsonbValue encv; encv.type = jbvString;
                encv.val.string.val = VARDATA_ANY(encoded);
                encv.val.string.len = static_cast<int>(VARSIZE_ANY_EXHDR(encoded));
                pushJsonbValue(pstate, WJB_ELEM, &encv);
                JsonbValue fmt; fmt.type = jbvString;
                fmt.val.string.val = const_cast<char*>("base64"); fmt.val.string.len = 6;
                pushJsonbValue(pstate, WJB_ELEM, &fmt);
                pushJsonbValue(pstate, WJB_END_ARRAY, nullptr);
                continue;
            }
            case arrow::Type::DECIMAL32: {
                auto text_val = static_cast<const arrow::Decimal32Array&>(*array).FormatValue(i);
                jv.type = jbvNumeric;
                jv.val.numeric = DatumGetNumeric(DirectFunctionCall3(
                    numeric_in, CStringGetDatum(text_val.c_str()), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
                break;
            }
            case arrow::Type::DECIMAL64: {
                auto text_val = static_cast<const arrow::Decimal64Array&>(*array).FormatValue(i);
                jv.type = jbvNumeric;
                jv.val.numeric = DatumGetNumeric(DirectFunctionCall3(
                    numeric_in, CStringGetDatum(text_val.c_str()), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
                break;
            }
            case arrow::Type::DECIMAL128: {
                auto text_val = static_cast<const arrow::Decimal128Array&>(*array).FormatValue(i);
                jv.type = jbvNumeric;
                jv.val.numeric = DatumGetNumeric(DirectFunctionCall3(
                    numeric_in, CStringGetDatum(text_val.c_str()), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
                break;
            }
            case arrow::Type::DECIMAL256: {
                auto text_val = static_cast<const arrow::Decimal256Array&>(*array).FormatValue(i);
                jv.type = jbvNumeric;
                jv.val.numeric = DatumGetNumeric(DirectFunctionCall3(
                    numeric_in, CStringGetDatum(text_val.c_str()), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
                break;
            }
            case arrow::Type::UINT8:
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(
                    static_cast<int64_t>(static_cast<const arrow::UInt8Array&>(*array).Value(i)));
                break;
            case arrow::Type::UINT16:
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(
                    static_cast<int64_t>(static_cast<const arrow::UInt16Array&>(*array).Value(i)));
                break;
            case arrow::Type::UINT32:
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(
                    static_cast<int64_t>(static_cast<const arrow::UInt32Array&>(*array).Value(i)));
                break;
            case arrow::Type::UINT64: {
                // Unlike UINT8/16/32, a uint64 value can exceed int64_to_numeric()'s signed
                // domain (its own top half, 2^63 to 2^64-1) - go through text + numeric_in()
                // instead, the same round trip the DECIMAL32/64/128/256 cases above already use.
                uint64_t v = static_cast<const arrow::UInt64Array&>(*array).Value(i);
                std::string text_val = std::to_string(v);
                jv.type = jbvNumeric;
                jv.val.numeric = DatumGetNumeric(DirectFunctionCall3(
                    numeric_in, CStringGetDatum(text_val.c_str()), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
                break;
            }
            case arrow::Type::DATE32: {
                int32_t unix_days = static_cast<const arrow::Date32Array&>(*array).Value(i);
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(static_cast<int64_t>(unix_days) - kPgToUnixEpochDays);
                break;
            }
            case arrow::Type::TIMESTAMP: {
                int64_t unix_micros = static_cast<const arrow::TimestampArray&>(*array).Value(i);
                jv.type = jbvNumeric;
                jv.val.numeric = int64_to_numeric(unix_micros - kPgToUnixEpochMicros);
                break;
            }
            default:
                jv.type = jbvString;
                jv.val.string.val = const_cast<char*>("<unsupported Arrow type>");
                jv.val.string.len = static_cast<int>(strlen("<unsupported Arrow type>"));
                break;
        }
        pushJsonbValue(pstate, WJB_ELEM, &jv);
    }
    return pushJsonbValue(pstate, WJB_END_ARRAY, nullptr);
}

} // namespace

extern "C" Datum
arrow_to_jsonb(PG_FUNCTION_ARGS)
{
    bytea* data = PG_GETARG_BYTEA_PP(0);
    auto buffer = std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t*>(VARDATA_ANY(data)), VARSIZE_ANY_EXHDR(data));
    auto reader_stream = std::make_shared<arrow::io::BufferReader>(buffer);

    auto stream_reader_result = arrow::ipc::RecordBatchStreamReader::Open(reader_stream);
    if (!stream_reader_result.ok()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("failed to open Arrow IPC stream: %s", stream_reader_result.status().ToString().c_str())));
    }
    auto stream_reader = stream_reader_result.ValueOrDie();

    std::shared_ptr<arrow::RecordBatch> batch;
    auto read_status = stream_reader->ReadNext(&batch);
    if (!read_status.ok()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("failed to read Arrow RecordBatch: %s", read_status.ToString().c_str())));
    }

    JsonbParseState* pstate = nullptr;
    JsonbValue* result;
    if (!batch) {
        // A zero-field schema (rows_to_arrow's "no columns" case, if it
        // ever arises) yields no RecordBatch at all from ReadNext.
        pushJsonbValue(&pstate, WJB_BEGIN_OBJECT, nullptr);
        result = pushJsonbValue(&pstate, WJB_END_OBJECT, nullptr);
    } else {
        pushJsonbValue(&pstate, WJB_BEGIN_OBJECT, nullptr);
        for (int i = 0; i < batch->num_columns(); i++) {
            JsonbValue keyv;
            keyv.type = jbvString;
            const std::string& name = batch->column_name(i);
            keyv.val.string.val = const_cast<char*>(name.data());
            keyv.val.string.len = static_cast<int>(name.size());
            pushJsonbValue(&pstate, WJB_KEY, &keyv);
            array_column_to_jsonb(batch->column(i), &pstate);
        }
        result = pushJsonbValue(&pstate, WJB_END_OBJECT, nullptr);
    }

    PG_RETURN_JSONB_P(JsonbValueToJsonb(result));
}

extern "C" void
_PG_init(void)
{
    CacheRegisterSyscacheCallback(TYPEOID, schema_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(RELOID, schema_syscache_callback, (Datum) 0);
    CacheRegisterSyscacheCallback(ATTNUM, schema_syscache_callback, (Datum) 0);
    CacheRegisterRelcacheCallback(schema_relcache_callback, (Datum) 0);
}
