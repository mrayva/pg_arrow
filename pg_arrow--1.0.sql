-- pg_arrow extension SQL definitions, version 1.0
--
-- rows_to_arrow(anyarray) is the only encoder - Arrow's physical format is
-- inherently columnar (a one-row RecordBatch would be almost entirely fixed
-- overhead), so unlike pg_zerialize there's no row_to_arrow(record).

CREATE OR REPLACE FUNCTION rows_to_arrow(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_arrow'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_arrow(anyarray) IS
'Encode an array of PostgreSQL rows/records into a single Apache Arrow IPC stream (one RecordBatch). Requires a homogeneous, flat scalar composite type across all non-null elements. NUMERIC columns with a declared precision/scale map exactly to the narrowest Arrow decimal width that fits (Decimal32/64/128/256); unconstrained or overly-wide NUMERIC columns fall back to a Utf8 column of exact numeric_out() text. BIT(8)/BIT(16)/BIT(32)/BIT(64) map exactly to Arrow UInt8/UInt16/UInt32/UInt64; any other BIT(n) length, and BIT VARYING entirely, are unsupported.';

CREATE OR REPLACE FUNCTION arrow_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'arrow_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION arrow_to_jsonb(bytea) IS
'Decode one Arrow IPC stream (as produced by rows_to_arrow) into columnar jsonb: {"col1":[v,v,...],"col2":[v,v,...],...}. Binary columns use the ["~b", base64, "base64"] tag convention, matching pg_zerialize''s decoders.';
