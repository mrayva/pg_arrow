SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_arrow CASCADE;
CREATE EXTENSION pg_arrow;

-- Nested composite column -> rejected (out of scope, flat scalar schemas only).
CREATE TYPE pgarrow_inner AS (x int);
CREATE TYPE pgarrow_outer AS (id int, inner_val pgarrow_inner);
SELECT rows_to_arrow(ARRAY[ROW(1, ROW(2)::pgarrow_inner)::pgarrow_outer]);
DROP TYPE pgarrow_outer;
DROP TYPE pgarrow_inner;

-- Non-composite array element.
SELECT rows_to_arrow(ARRAY[1, 2, 3]);

-- All-NULL-rows input: columnar_batch_schema() requires at least one
-- non-null element to resolve a schema from, same restriction
-- pg_zerialize's own columnar_batch_schema() has for the identical case -
-- even for a concretely-named type, since this path resolves the schema
-- from an actual element's runtime type/typmod, not the array's declared
-- element type (contrast with the empty-array case above, which *can* use
-- the declared element type since there's no per-element runtime typmod to
-- prefer over it).
CREATE TYPE pgarrow_t AS (x int);
SELECT rows_to_arrow(ARRAY[NULL::pgarrow_t, NULL::pgarrow_t]);

-- Mismatched composite types within one array isn't directly reachable via
-- plain ARRAY[...] syntax (Postgres enforces one element type at parse
-- time), so that branch of columnar_batch_schema() isn't exercised here -
-- same limitation noted in pg_zerialize's own columnar tests.

-- Empty array of anonymous record[] -> no schema to fall back on.
SELECT rows_to_arrow(ARRAY[]::record[]);

DROP TYPE pgarrow_t;
DROP EXTENSION pg_arrow;
