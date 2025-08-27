-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION jsonb_stats" to load this file. \quit

--------------------------------------------------------------------------------
-- C Implementation
--------------------------------------------------------------------------------

CREATE FUNCTION stat(anyelement)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION stats(jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Overloaded helper function to create a single-key `stats` object
CREATE FUNCTION stats(code text, val anyelement)
RETURNS jsonb
AS $$ SELECT stats(jsonb_build_object(code, stat(val))) $$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jsonb_stats_sfunc(jsonb, text, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_agg(text, jsonb) (
    sfunc = jsonb_stats_sfunc,
    stype = jsonb,
    initcond = '{}'
);

CREATE FUNCTION jsonb_stats_accum(jsonb, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jsonb_stats_final(jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_agg(jsonb) (
    sfunc = jsonb_stats_accum,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_final
);

CREATE FUNCTION jsonb_stats_merge(jsonb, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_merge_agg(jsonb) (
    sfunc = jsonb_stats_merge,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_final
);
