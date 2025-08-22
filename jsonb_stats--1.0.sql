-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION jsonb_stats" to load this file. \quit

--------------------------------------------------------------------------------
-- C Implementation
--------------------------------------------------------------------------------

CREATE FUNCTION stat(anyelement)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jsonb_stats_sfunc(jsonb, text, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_agg(text, jsonb) (
    sfunc = jsonb_stats_sfunc,
    stype = jsonb,
    initcond = '{}'
);

CREATE FUNCTION jsonb_stats_summary_accum(jsonb, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jsonb_stats_to_summary_round(jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_summary_agg(jsonb) (
    sfunc = jsonb_stats_summary_accum,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_to_summary_round
);

CREATE FUNCTION jsonb_stats_summary_merge(jsonb, jsonb)
RETURNS jsonb
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE jsonb_stats_summary_merge_agg(jsonb) (
    sfunc = jsonb_stats_summary_merge,
    stype = jsonb,
    initcond = '{}',
    finalfunc = jsonb_stats_to_summary_round
);
