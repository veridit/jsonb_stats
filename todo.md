# jsonb_stats todo

## Difference summaries (`stats_delta`), version 3.1.0

Status: implemented on master, pushed, NOT tagged. Release waits until the
STATBUS-558 prototype (hibiscus) confirms it works.

Scope: statistics only. jsonb_stats keeps per variable what was added and
removed; why units entered or left (births, deaths, moves) is counted by the
caller in its own columns, not here.

API: `jsonb_stats_to_delta` (entry), `jsonb_stats_negate` (exit, not an undo),
`jsonb_stats_delta_merge`, `jsonb_stats_delta_agg` (parallel, moving window),
`jsonb_stats_delta_to_agg(stock, period)` -> `stats_agg` with
`"min_max": false`; with a period also `in`/`out` {count, sum} and
`change_pct`, next to `coefficient_of_variation_pct`.

Verified: make test, clippy, fixture-check, xarch, upgrade-test from v3.0.0;
on statbus_local data the delta stock equals every stored
statistical_history_facet.stats_summary by value (298,264 rows; all events
there fall in one month, so mid-period cases are covered by the oracle only).

Known and accepted: float sums print without trailing zeros (equal by value,
jsonb `=` and `IS DISTINCT FROM` compare by value); delta_agg is about 1.3x
merge_agg (two flows).

Next: wait for hibiscus's prototype; fix anything it needs; then release per
the CLAUDE.md checklist (tag v3.1.0, GitHub release).

## Finding what stands out: `jsonb_stats_values` and `jsonb_stats_compare`

Status: building overnight 2026-11-03/04 (owner asleep). Push master, NO tag;
3.1 vs 3.2 decided in the morning. Tell hibiscus only after the owner says.

Purpose: help a statistician find units and aggregates that stand out
(errors and phenomena), independent of the variable's scale. Peer groups,
thresholds, medians and ranking stay in caller SQL; jsonb_stats only makes
dynamic variables easy to reach and gives exact unit-free measures.

### jsonb_stats_values(x jsonb) RETURNS TABLE(key text, stat_type text, value numeric, text_value text, count bigint)

Unpacks variables into rows so PostgreSQL's own tools (percentile_cont,
GROUP BY key, joins between periods) work on any variable.
- `stats` (one unit): defined as `jsonb_stats_values(jsonb_stats_to_agg(x))`,
  so validation and scales are exactly those of accumulation: numeric value =
  the value, count 1; categories one row each (arr: per distinct element with
  its count, so `{x,x}` gives x with count 2; empty array no rows).
- `stats_agg` (incl. `"min_max": false` from delta_to_agg): numeric keys one
  row: value = sum, count = count, stat_type = base type (int, not int_agg).
  str/bool/date/arr: one row per category: text_value = category, count =
  its count, value NULL.
- `stats_delta`: rejected (fail fast) with a hint to read it with
  jsonb_stats_delta_to_agg first; a delta is changes, not values.
- Row order: keys in jsonb order (length, then bytes), categories likewise.
- Errors as elsewhere: unknown types, malformed values, non-object input.
- NULL input: no rows (STRICT).

### jsonb_stats_compare(a jsonb, b jsonb) RETURNS jsonb

a = the reference (previous period, or the peers), b = the subject (this
period, or the cell). Both `stats_agg` (any producer, incl. min_max:false).
Output `{"type": "stats_compare", <key>: {...}}` for every numeric key present
in a or b (a missing side counts as empty: count 0). Counted keys are skipped.
Per key:
- `type`: base type (int, nat, dec2, float)
- `count_a`, `count_b`
- `change_pct` = 100 (sum_b - sum_a) / |sum_a|; NULL when sum_a = 0
- `mean_change_pct` = 100 (mean_b - mean_a) / |mean_a|; NULL when either
  count is 0 or sum_a = 0
- `z` = (mean_b - mean_a) / sqrt(var_a/n_a + var_b/n_b) (Welch); NULL when
  n_a < 2 or n_b < 2 or both variances 0. Exact: with N = n*sum_sq - sum^2,
  var = N/(n(n-1)), so
  z^2 = (S_b n_a - S_a n_b)^2 (n_a-1)(n_b-1) / (N_a n_b^2 (n_b-1) + N_b n_a^2 (n_a-1))
  and z = sign(S_b n_a - S_a n_b) * sqrt(z^2), rounded once to 2 decimals
  (same isqrt rounding as stddev). A ranking score, not a significance test:
  for the same cell in two periods the groups overlap, so |z| is overstated.
- All rounded to 2 decimals half away from zero, from exact integers.
- Errors: type mismatch for a key (int vs float, numeric vs counted); invalid
  summaries as in stats_agg parsing.
