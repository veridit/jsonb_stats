# StatBus hand-off: jsonb_stats 3.0.0

Prepared for STATBUS-512. Nothing in the StatBus repo has been changed.

## Steps

1. In `postgres/Dockerfile`, bump `ARG jsonb_stats_release=c4a2595` to the
   jsonb_stats 3.0.0 commit, after it is pushed to `veridit/jsonb_stats`.
2. There is no `ALTER EXTENSION ... UPDATE` path. Recreate the extension
   (`DROP EXTENSION jsonb_stats CASCADE; CREATE EXTENSION jsonb_stats;`)
   and the objects that depend on it, using a migration or a fresh DB build.
3. Recompute every stored `stats_summary` / `stats_agg`. 3.0 rejects stored
   2.x numeric summaries because they lack `sum_sq`:
   `jsonb_stats: int_agg for key '...' lacks 'sum_sq' (created by jsonb_stats < 3.0)`.
4. Re-record the expected test outputs (test 306 and any other test that
   prints statistics). Changes to expect:
   - every numeric `*_agg` gains `"sum_sq"`
   - `sum_sq_diff`, `variance`, `stddev` and `coefficient_of_variation_pct`
     change in their last digits, now exact
   - `float_agg` `sum` is decimal-exact
   - `dec2_agg` prints `sum`/`min`/`max` at scale 2
5. Optional: add `sum_sq: number;` to the numeric summary type in
   `app/src/app/types.d.ts`.

## Compatibility check against StatBus master (read-only survey)

| 3.0 change | StatBus usage | Impact |
|---|---|---|
| `stat()` mapping (smallint/bigint/real/numeric/arrays) | `stat(value_int)` (integer), `stat(value_float)`, `stat(value_string)`, `stat(value_bool)` | none |
| `jsonb_stats_merge` returns a finalized `stats_agg` | used inside `jsonb_stats_merge_agg(...)` | none: the merge_agg reads the exact state and ignores derived fields |
| `jsonb_stats_merge('{}','{}')` returns `{"type":"stats_agg"}` | no equality checks against `'{}'` were found | none expected |
| new validation errors (non-integral int, dec2 > 2 decimals, a key changing type) | `stat_for_unit` has a check constraint per value column | none expected, unless a stat code changes type between units |
| legacy summaries rejected | stored summaries in derived tables | recompute (step 3) |

The old PL/pgSQL `jsonb_stats_to_summary` / `jsonb_stats_summary_merge`
migrations from 2024 were replaced by
`20260219121154_integrate_jsonb_stats_extension`. They are not affected.
