SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_arrow CASCADE;
CREATE EXTENSION pg_arrow;

CREATE TYPE pgarrow_row AS (
    id int,
    name text,
    score double precision,
    active boolean,
    raw bytea
);

-- Basic round-trip: several rows, a NULL field within a row, and a wholly-
-- NULL row, all in one batch.
SELECT arrow_to_jsonb(rows_to_arrow(ARRAY[
    ROW(1, 'alice', 9.5, true, '\xDEADBEEF'::bytea)::pgarrow_row,
    ROW(2, 'bob', NULL, false, NULL)::pgarrow_row,
    NULL::pgarrow_row
]));

-- Structural comparison against jsonb_build_object-based construction, the
-- same style of check used throughout pg_zerialize's own columnar tests.
SELECT arrow_to_jsonb(rows_to_arrow(ARRAY[
    ROW(1, 'a', 1.5, true, NULL)::pgarrow_row,
    ROW(2, 'b', 2.5, false, NULL)::pgarrow_row
])) = jsonb_build_object(
    'id', jsonb_build_array(1, 2),
    'name', jsonb_build_array('a', 'b'),
    'score', jsonb_build_array(1.5, 2.5),
    'active', jsonb_build_array(true, false),
    'raw', jsonb_build_array(NULL, NULL)
) AS matches_jsonb_construction;

-- Empty array of a concretely-typed composite -> empty (zero-row) columns,
-- not a schema-less {} - Arrow's format always carries a schema.
SELECT arrow_to_jsonb(rows_to_arrow(ARRAY[]::pgarrow_row[]));

DROP TYPE pgarrow_row;
DROP EXTENSION pg_arrow;
