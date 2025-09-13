CREATE EXTENSION jsonb_stats_zig;

-- Test stat(text)
SELECT stat('hello'::text);
