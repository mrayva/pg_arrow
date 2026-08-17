SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_arrow CASCADE;
CREATE EXTENSION pg_arrow;

-- Constrained NUMERIC(p,s) -> exact Arrow Decimal128(p,s).
CREATE TYPE pgarrow_decimal_row AS (price numeric(12,4));

SELECT arrow_to_jsonb(rows_to_arrow(ARRAY[
    ROW(123.4500)::pgarrow_decimal_row,
    ROW(999999.9999)::pgarrow_decimal_row,
    ROW(-42.1)::pgarrow_decimal_row,
    ROW(NULL)::pgarrow_decimal_row
])) = jsonb_build_object('price', jsonb_build_array(123.4500, 999999.9999, -42.1000, NULL))
    AS decimal128_exact_roundtrip;

DROP TYPE pgarrow_decimal_row;

-- Unconstrained NUMERIC (typmod == -1) -> Utf8 text fallback, exact
-- numeric_out() text, not a fixed decimal - values here deliberately have
-- different scales, which a single fixed Decimal128(p,s) for the whole
-- column couldn't represent losslessly.
CREATE TYPE pgarrow_loose_numeric_row AS (v numeric);

SELECT arrow_to_jsonb(rows_to_arrow(ARRAY[
    ROW(1)::pgarrow_loose_numeric_row,
    ROW(1.5)::pgarrow_loose_numeric_row,
    ROW(3.14159265358979)::pgarrow_loose_numeric_row,
    ROW(-100.001)::pgarrow_loose_numeric_row
])) = jsonb_build_object('v', jsonb_build_array('1', '1.5', '3.14159265358979', '-100.001'))
    AS unconstrained_numeric_text_fallback;

DROP TYPE pgarrow_loose_numeric_row;

DROP EXTENSION pg_arrow;
