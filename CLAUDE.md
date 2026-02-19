# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`jsonb_stats` is a PostgreSQL extension for statistical aggregation using JSONB. It provides hierarchical analytics on dynamically-created variables without schema modifications, using online algorithms (Welford's method) for mergeable summaries and coefficient of variation for normalized change detection.

The PL/pgSQL reference (`dev/reference_plpgsql.sql`) is the authoritative spec — every Rust function must produce identical output, then be faster.

## Build & Test Commands

The Makefile handles the pgrx environment variables (macOS SDK path workaround):

```bash
make test        # Run all tests (47 tests: correctness + error handling + benchmarks)
make run         # Launch psql with extension loaded
make install     # Install into system PostgreSQL
make package     # Build installable package
```

Benchmark results are written to `/tmp/jsonb_stats_benchmarks.txt` after each test run.

## Key Source Files

- `dev/reference_plpgsql.sql` — **The authoritative spec.** Every Rust function must match its PL/pgSQL counterpart.
- `src/lib.rs` — pg_module_magic, module declarations, extension_sql for aggregates, tests
- `src/stat.rs` — stat(), stats(), jsonb_stats_sfunc
- `src/accum.rs` — jsonb_stats_accum + jsonb_stats_accum_sfunc (Internal state)
- `src/merge.rs` — jsonb_stats_merge + jsonb_stats_merge_sfunc (Internal state)
- `src/parallel.rs` — jsonb_stats_combine, jsonb_stats_serial, jsonb_stats_deserial (parallel aggregation)
- `src/final_fn.rs` — jsonb_stats_final + jsonb_stats_final_internal
- `src/state.rs` — StatsState/AggEntry native Rust types for Internal aggregate state
- `src/helpers.rs` — get_f64, get_i64, get_str, num_value, round2

## Coding Standards

### Rust (pgrx)

- Follow standard Rust conventions (rustfmt, clippy)
- Use `pgrx::JsonB` for JSONB arguments and return values
- Use `serde_json::Value` for internal JSON manipulation
- Use `pgrx::error!()` / `pgrx::warning!()` instead of `panic!()`
- Every function must be `PARALLEL SAFE` and `IMMUTABLE`
- **Fail fast**: Unknown types, invalid values, type mismatches → `pgrx::error!()` immediately. No silent skips or default fallbacks.

### SQL

- Named dollar quotes for function bodies: `AS $function_name$`
- Dollar-quoting in `format()`: `format($$ ... $$)`
- Named arguments for 3+ parameter calls
- Explicit `AS` for table aliases

## Testing Philosophy

The PL/pgSQL reference implementation (`dev/reference_plpgsql.sql`) is dual-purpose:

1. **Correctness spec**: Every Rust function must produce **identical output** to its PL/pgSQL counterpart for all inputs.
2. **Performance baseline**: Rust must **beat** PL/pgSQL on benchmarks.

## Development Workflow

Follow a hypothesis-driven cycle:
1. State hypothesis about root cause
2. Create/identify minimal reproducing test
3. Propose change with expected outcome
4. Run tests and benchmarks
5. Analyze results — if wrong, revert and restart from step 1
6. Update `todo.md` with outcomes

When repeated iterations fail, switch to **simplify and rebuild**: strip to a non-crashing stub, verify stability, then incrementally re-introduce logic (logical binary search) to isolate the root cause.

## Git Setup

```bash
git config core.hooksPath devops/githooks   # Required one-time setup
```

The `tmp/` directory is a scratch space for experiments — the pre-commit hook prevents committing files from it.
