# `jsonb_stats` Development Tasks

## TODO

## Done

- [x] `jsonb_stats_merge_agg` and `jsonb_stats_agg` tolerate NULL input (sfuncs accept `Option<JsonB>`, skip NULLs like standard PG aggregates)

- [x] `jsonb_stats_to_agg(stats jsonb)` — scalar function: converts stats → stats_agg for merging with existing aggregates
- [x] `combinefunc` + `parallel = safe` on aggregates (enables PostgreSQL parallel aggregation)
- [x] Comparison regression test: Rust vs PL/pgSQL output side-by-side for all functions
- [x] `stat()` accepts `varchar` natively (no `::text` cast needed)
