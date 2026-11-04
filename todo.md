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
no min/max; with a period also `in`/`out` {count, sum} and
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

Status: DONE overnight 2026-11-03/04, pushed to master, NOT tagged (3.1 vs 3.2
decided in the morning). See "Morning report" below.

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
- `stats_agg` (incl. stocks from delta_to_agg, without min/max): numeric keys one
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
period, or the cell). Both `stats_agg` (any producer, incl. stocks from deltas).
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

### Morning report (2026-11-04)

Built: `jsonb_stats_values` and `jsonb_stats_compare`, as designed above.
Spec (dev/reference_plpgsql.sql), Rust (src/compare.rs), oracle
(dev/compare_oracle.py, textbook definitions: variance from the values, not
from sum_sq, so it also checks the z formula), 9 tests (src/tests_compare.rs),
xarch checks, upgrade script, README "Finding what stands out", ARCHITECTURE,
CHANGELOG (in the 3.1.0 section).

Verified: make test 118/118; clippy -D warnings; fixture-check; xarch
IDENTICAL (amd64 = arm64, incl. new checks); upgrade-test from v3.0.0; local
3.0.0 -> UPDATE catalog = fresh install (33 functions, incl. volatility,
parallel, strict). Oracle: 53/53 compare cases (13 edge + 40 random) exact.

Real data (read-only copy of statbus_local, 201,925 units; only variable
there is employees): values unpacks all units in 1.7 s; robust peer outliers
(median/IQR per unit type x 2-digit activity, peers >= 20) in 0.3 s, 10,472
flagged; compare of 42 activity cells against all units of their type in
3 ms. statbus_local has one period only, so change was checked on the Norway
history below.

Two periods, real data (read-only copy of hibiscus's 10-year Norway test db,
6,696 units, 444 with statistics; employees only; snapshots 2015-2024):
- values: 3,422 unit-years unpacked in 32 ms; unit year-over-year ratios
  against the activity's median ratio: 3,047 pairs in 1.6 s, 0 flagged at
  3x (this data is partly synthetic yearly copies, so few real changes).
- compare: 364 activity-year cells, each year against the previous, in
  22 ms. Ranked by change_pct the top is a 1 -> 3 unit cell (+1116%, z
  NULL); ranked by |z| the top is legal units in F. (11 -> 17 units, +125%,
  z 1.65). That is what z is for: a size-aware ranking.
- Cross-check: z for establishments in S. 2016 -> 2017 is 0.89 from
  jsonb_stats_compare and 0.89 from plain SQL avg/var_samp over the unit
  values.

Changed 2026-11-04 (owner, via hibiscus/STATBUS-558): no "min_max" marker.
min/max are optional in every stats_agg (both or neither); merge, merge_agg,
final, to_delta, delta_agg, values and compare accept summaries without them,
and a merge keeps them only when every input has them. Count 0 is allowed
without min/max.

Open for the owner:
1. Release as 3.1.0 (one release with deltas) or tag the deltas as 3.1.0 and
   these as 3.2.0. Master has both in 3.1.0 now.
2. hibiscus is told once now (owner's instruction), for later use, not for
   the first prototype.

### Release, once the owner chooses (nothing tagged yet)

Option A, one release 3.1.0 (master as is, HEAD):
    git tag -a v3.1.0 -m "jsonb_stats 3.1.0: stats_delta, values, compare" && git push origin v3.1.0
    awk '/^## 3.1.0/{f=1;next} /^## /{if(f)exit} f' CHANGELOG.md > /tmp/notes.md
    gh release create v3.1.0 -R veridit/jsonb_stats --title "jsonb_stats 3.1.0" --notes-file /tmp/notes.md --verify-tag

Option B, 3.1.0 = deltas (commit 17779bd, before values/compare) and 3.2.0 =
values/compare: needs a new commit on master first (version 3.2.0 in
Cargo.toml, move the two functions from sql/jsonb_stats--3.0.0--3.1.0.sql to
a new jsonb_stats--3.1.0--3.2.0.sql, split the CHANGELOG section), then
re-run make test / xarch / upgrade-test (OLD_REF=v3.1.0 and v3.0.0) before
tagging both. Note: 17779bd's 3.1.0 upgrade script would then differ from
master's, so v3.1.0 must be tagged at 17779bd, not at HEAD.
