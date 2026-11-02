-- jsonb_stats 3.0.0 -> 3.1.0: difference summaries (stats_delta).
-- Additive only: existing functions and stored data are unchanged.

CREATE FUNCTION "jsonb_stats_to_delta"("x" jsonb) RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_to_delta_wrapper';
CREATE FUNCTION "jsonb_stats_negate"("x" jsonb) RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_negate_wrapper';
CREATE FUNCTION "jsonb_stats_delta_merge"("a" jsonb, "b" jsonb) RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_merge_wrapper';
CREATE FUNCTION "jsonb_stats_delta_summary"("stock" jsonb, "period" jsonb DEFAULT NULL) RETURNS jsonb
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_summary_wrapper';
CREATE FUNCTION "jsonb_stats_delta_sfunc"("internal" internal, "d" jsonb) RETURNS internal
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_sfunc_wrapper';
CREATE FUNCTION "jsonb_stats_delta_minvfunc"("internal" internal, "d" jsonb) RETURNS internal
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_minvfunc_wrapper';
CREATE FUNCTION "jsonb_stats_delta_combine"("state1" internal, "state2" internal) RETURNS internal
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_combine_wrapper';
CREATE FUNCTION "jsonb_stats_delta_serial"("internal" internal) RETURNS bytea
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_serial_wrapper';
CREATE FUNCTION "jsonb_stats_delta_deserial"("bytes" bytea, "_internal" internal) RETURNS internal
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_deserial_wrapper';
CREATE FUNCTION "jsonb_stats_delta_final"("internal" internal) RETURNS jsonb
IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_delta_final_wrapper';

CREATE AGGREGATE jsonb_stats_delta_agg(jsonb) (
    sfunc = jsonb_stats_delta_sfunc,
    stype = internal,
    finalfunc = jsonb_stats_delta_final,
    combinefunc = jsonb_stats_delta_combine,
    serialfunc = jsonb_stats_delta_serial,
    deserialfunc = jsonb_stats_delta_deserial,
    msfunc = jsonb_stats_delta_sfunc,
    minvfunc = jsonb_stats_delta_minvfunc,
    mstype = internal,
    mfinalfunc = jsonb_stats_delta_final,
    parallel = safe
);
