# `jsonb_stats` Development Tasks

## Done

- [x] `jsonb_stats_to_agg(stats jsonb)` — scalar function: converts stats → stats_agg for merging with existing aggregates
- [x] `combinefunc` + `parallel = safe` on aggregates (enables PostgreSQL parallel aggregation)
- [x] Comparison regression test: Rust vs PL/pgSQL output side-by-side for all functions
- [x] `stat()` accepts `varchar` natively (no `::text` cast needed)
