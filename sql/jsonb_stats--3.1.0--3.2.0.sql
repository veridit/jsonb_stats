-- jsonb_stats 3.1.0 -> 3.2.0: finding what stands out.
-- Additive only: existing functions and stored data are unchanged.

CREATE FUNCTION "jsonb_stats_values"("x" jsonb)
RETURNS TABLE ("key" TEXT, "stat_type" TEXT, "value" NUMERIC, "text_value" TEXT, "count" bigint)
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_values_wrapper';
CREATE FUNCTION "jsonb_stats_compare"("a" jsonb, "b" jsonb) RETURNS jsonb
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'jsonb_stats_compare_wrapper';
