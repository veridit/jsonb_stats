# `jsonb_stats` Development Tasks

## Next

- [ ] `jsonb_stats_summarize(stats jsonb)` — scalar function: accum+final for a single row (avoids awkward subquery wrapping of aggregate)
- [ ] `combinefunc` + `parallel = safe` on aggregates (enables PostgreSQL parallel aggregation)
- [ ] Comparison regression test: Rust vs PL/pgSQL output side-by-side for all functions
