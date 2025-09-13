\echo Use "CREATE EXTENSION jsonb_stats_zig" to load this file. \quit

-- For now, we only implement the text version as a proof of concept.
CREATE FUNCTION stat("value" text)
RETURNS jsonb
AS '$libdir/jsonb_stats_zig', 'stat_text'
LANGUAGE C STRICT;

