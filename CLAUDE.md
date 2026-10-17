# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`jsonb_stats` is a PostgreSQL extension for statistical aggregation using JSONB. It provides hierarchical analytics on dynamically-created variables without schema modifications, using exact, mergeable numeric summaries (count, sum, sum_sq, min, max as arbitrary-precision decimals) and coefficient of variation for normalized change detection. Output must be byte-identical for any row order, grouping, merge order, parallel plan or CPU.

The PL/pgSQL reference (`dev/reference_plpgsql.sql`) is the authoritative spec — every Rust function must produce identical output, then be faster.

## Build & Test Commands

The Makefile handles the pgrx environment variables (macOS SDK path workaround):

```bash
make test        # Run all tests (pg_tests: correctness + exactness + error handling + benchmarks; plus unit tests)
make test PG_VERSION="pg18 test_exact"   # Only the exactness/determinism suite (fast)
make fixture-check   # Fail if dev/fixtures/exact_fixture.sql is stale (make fixture regenerates)
make xarch           # Docker: identical output on linux amd64 and arm64
make upgrade-test    # Docker: previous release -> ALTER EXTENSION UPDATE -> verify
make run         # Launch psql with extension loaded
make install     # Install into system PostgreSQL
make package     # Build installable package
```

Benchmark results are written to `/tmp/jsonb_stats_benchmarks.txt` after each test run.

## Key Source Files

- `dev/reference_plpgsql.sql` — **The authoritative spec.** Every Rust function must match its PL/pgSQL counterpart.
- `src/lib.rs` — pg_module_magic, module declarations, extension_sql for aggregates, tests
- `src/stat.rs` — stat(), stats(), jsonb_stats_sfunc
- `src/state.rs` — StatsState/AggEntry/NumFields: all accumulate, merge, parse and summary logic (one implementation for every path)
- `src/exact.rs` — Decimal: exact arbitrary-precision decimal arithmetic, exact rounding and square roots
- `src/accum.rs` — jsonb_stats_accum + jsonb_stats_accum_sfunc (Internal state)
- `src/merge.rs` — jsonb_stats_merge + jsonb_stats_merge_sfunc (Internal state)
- `src/parallel.rs` — jsonb_stats_combine, jsonb_stats_serial, jsonb_stats_deserial (parallel aggregation)
- `src/final_fn.rs` — jsonb_stats_final + jsonb_stats_final_internal
- `src/tests_exact.rs` — exactness/determinism pg_tests (included into `mod tests`)
- `dev/exact_oracle.py` — independent Python rational-arithmetic oracle; generates `dev/fixtures/exact_fixture.sql` (`python3 dev/exact_oracle.py > dev/fixtures/exact_fixture.sql`)

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

1. **Correctness spec**: Every Rust function must produce **identical output** to its PL/pgSQL counterpart for all inputs. Compare `::text`, not jsonb equality, so number scale is part of the contract.
2. **Performance baseline**: Rust must **beat** PL/pgSQL on benchmarks.

Numeric results are additionally checked against `dev/exact_oracle.py`, which is independent of both implementations. Never use f64 for numeric aggregate state or derived values.

## Development Workflow

Follow a hypothesis-driven cycle:
1. State hypothesis about root cause
2. Create/identify minimal reproducing test
3. Propose change with expected outcome
4. Run tests and benchmarks
5. Analyze results — if wrong, revert and restart from step 1
6. Update `todo.md` with outcomes

When repeated iterations fail, switch to **simplify and rebuild**: strip to a non-crashing stub, verify stability, then incrementally re-introduce logic (logical binary search) to isolate the root cause.

## Releases

Every version bump in `Cargo.toml` needs `sql/jsonb_stats--<prev>--<new>.sql`, so `ALTER EXTENSION jsonb_stats UPDATE` works. pgrx copies these into the extension dir. Diff the generated schema between the two versions (`cargo pgrx schema`) and put any CREATE/DROP/ALTER in the script. Keep the script even when it is empty. Add a "X to Y" section to README "Upgrading", and run `make upgrade-test`.

## Git Setup

```bash
git config core.hooksPath devops/githooks   # Required one-time setup
```

The `tmp/` directory is a scratch space for experiments — the pre-commit hook prevents committing files from it.
