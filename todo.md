A living document of upcoming tasks.
Tasks are checked ✅ when done and made brief.
Keep a journal.md that tracks the state of the current ongoing task and relevant details.

# `jsonb_stats` Development Tasks

This file tracks the major development tasks for the `jsonb_stats` extension.

## Completed

-   [x] Standardize `stats_summary` structure and function names.
-   [x] Define API and create SQL stubs (`jsonb_stats--1.0.sql`).
-   [x] Port PL/pgSQL implementation into the extension.
-   [x] Create regression test for PL/pgSQL API (`sql/002_jsonb_stats_api_plpgsql.sql`).
-   [x] Create regression test for C API (`sql/003_jsonb_stats_api_c.sql`).
-   [x] Implement `stat_c(anyelement)`.
-   [x] Implement `jsonb_stats_agg_c(text, jsonb)`.
-   [x] Implement `jsonb_stats_summary_agg_c(jsonb)`.
-   [x] Implement `jsonb_stats_summary_merge_agg_c(jsonb)`.
-   [x] Define C structures for aggregate states.
-   [x] Add documentation for C implementation details.
-   [x] Add advanced regression tests for edge cases and arrays.
-   [x] Implement full support for array types in `jsonb_stats_summary_agg` (in progress, basic counting done).
-   [x] Add a reference PL/pgSQL implementation and a regression test to compare it against the C version.
-   [x] Enhance C implementation to match `statbus` reference (variance, stddev, etc.).
-   [x] Implement `coefficient_of_variation_pct` in C implementation.
-   [x] Enhance C array summary to match `statbus` reference (unique element counts).
-   [x] Refactor tests to use a `reference` view for validation, based on `statbus`.
-   [x] Create a new regression test (`003`) that implements the full example scenario from the `README.md`.
