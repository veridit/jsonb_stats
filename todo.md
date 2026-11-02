# jsonb_stats todo

## Difference summaries (`stats_delta`) for stock over time

Status: proposal, not started. Written 2026-11-02 from STATBUS-558 (statbus repo),
for an agent to pick up. Read ARCHITECTURE.md and CLAUDE.md first: the PL/pgSQL
reference (`dev/reference_plpgsql.sql`) is the authoritative spec, the Python
oracle (`dev/exact_oracle.py`) checks exactness, and output must be identical for
any row order, grouping, merge order and parallel plan.

### Why

STATBUS stores the history of its units as a dense cube: one row per month and
combination of dimensions. On 10 years of Norway data, 98.8% of month rows only
repeat the month before (482,903 month rows; 526,437 rows in all). The same
stock can be kept as a difference table: a unit adds +1 to its combination on
the day it starts existing there and -1 on the day it stops, and the stock at a
date is the running sum up to that date. Measured in STATBUS on that data, with
counts only: 6,079 rows instead of 526,437, all 137 months equal to the dense
table, and a region breakdown at a date in 1.9 ms against 3.0 ms.

Counts work today. The statistics (`stats_summary`, every statistical variable)
need jsonb_stats to support the same running sum: add a unit's summary on entry,
subtract it on exit.

### What already fits

The numeric state is `count`, `sum = Σx`, `sum_sq = Σx²` as exact decimals, and
merge only adds. These three form a group: subtraction is exact addition of the
negated state, so a running sum of +/- changes returns exactly to the stock.
The `counts` maps of `str_agg`, `bool_agg`, `arr_agg` and `date_agg` are
integers per value and subtract the same way.

### What stands in the way

1. `NumFields::from_summary` rejects `count < 1`, so a negative change cannot be
   stored or parsed.
2. It rejects `count * sum_sq < sum^2`. A single negative change, or a partial
   running sum, breaks that check by design.
3. `min`/`max` (and `date_agg` min_date/max_date) are merged by comparison and
   cannot be undone when a unit leaves.

### Proposal

A companion type; the meaning of `stats_agg` does not change.

- `"type": "stats_delta"`: the same per-variable entries, numeric entries hold
  only `count`, `sum`, `sum_sq` with any sign; counted entries hold `counts`
  with any sign. No `min`/`max`, no derived fields.
- `jsonb_stats_to_delta(stats_agg) -> stats_delta`: the entry event.
- `jsonb_stats_negate(stats_agg | stats_delta) -> stats_delta`: the exit event.
- `jsonb_stats_delta_agg(jsonb)`: adds deltas. Internal state, parallel
  combine/serial/deserial like `jsonb_stats_merge_agg`. Also a moving-aggregate
  form (`msfunc`, `minvfunc`, `mstype`) so a window frame such as
  `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW` (rolling 12 months) drops old rows
  instead of recomputing.
- Final function: checks the result is a valid stock (count >= 0,
  `count * sum_sq >= sum^2`, counts >= 0; fail fast otherwise, as everywhere in
  this codebase), drops entries whose count is 0, and emits a `stats_agg`
  without `min`/`max`, marked so the type stays self-describing (for example
  `"min_max": false`). Derived fields (mean, variance, stddev,
  `coefficient_of_variation_pct`) are computed exactly as today from
  count, sum, sum_sq.

Example use in STATBUS:

```sql
-- history_delta(<dims>, on_date, n int, stats jsonb /* stats_delta */)
SELECT month,
       sum(n)                       OVER w AS units,
       jsonb_stats_delta_agg(stats) OVER w AS stats
  FROM (SELECT date_trunc('month', on_date) AS month,
               sum(n) AS n, jsonb_stats_delta_agg(stats) AS stats
          FROM history_delta WHERE <filters> GROUP BY 1) AS p
WINDOW w AS (ORDER BY month);
```

### min and max

STATBUS reads only `sum` (and the unit count) from summaries today; no screen
uses `min`/`max`. Proposal: leave them out of `stats_delta`. Point-in-time
summaries (`stats_agg` from `jsonb_stats_agg`/`jsonb_stats_merge_agg`) keep them
as now. Exact min/max at a date, when needed, comes from the units existing at
that date. To decide with the maintainer.

### Change detection

`coefficient_of_variation_pct` stays the standardized, unit-less measure for
finding where to zoom in, computed from the running count, sum and sum_sq of
each period. Whether jsonb_stats should also compare two periods (a function
taking the previous and the current summary) is open: decide with the
maintainer which measure, building on the CV rather than introducing another.

### Steps

1. Spec in `dev/reference_plpgsql.sql`: `stats_delta`, to_delta, negate,
   delta_agg, final. Extend `dev/exact_oracle.py` to negative counts.
2. Rust in `src/state.rs` (one implementation for every path, as now).
3. Tests: entry/exit round trip returns exactly to the empty state; a running
   sum over random +/- events equals `jsonb_stats_merge_agg` of the units alive
   at each date (count, sum, sum_sq, mean, variance, stddev, CV identical);
   parallel and order independence; moving-aggregate window equals the plain
   window; invalid stock fails fast.
4. Upgrade script and release per the release checklist.

### Agreed plan (2026-11-02, with maintainer)

Scope: delta core + flow split. No histogram, no higher moments, no z-score,
no compare function. min/max dropped from deltas (`"min_max": false`).

Flows: the aggregate keeps three exact states per variable: net (stock),
`inflow` (+ events) and `outflow` (- events, stored positive). A unit changing
value inside a combination is `-old, +new`. Births/entries are +, deaths/exits -.
Invariant: `stock_prev = stock - inflow + outflow`, so one output row carries
everything needed for scale-free change detection, no previous row required:

- `change_pct = (sum - sum_prev) / sum_prev * 100` (net, like exists_change)
- `churn_pct = (inflow_sum + outflow_sum) / sum_prev * 100` (gross turnover,
  like exists_added_count + exists_removed_count)
- `coefficient_of_variation_pct` as today (spread across units at that date)
- null when sum_prev = 0 (no division blowups).

Example (Oslo, NACE C, employees, Jan -> Feb): stock 50,000. 30 births bring
600, 20 deaths take 900, 50 units change size: -2,000 old, +2,100 new.
inflow 2,700, outflow 2,900, stock 49,800. change_pct -0.4 (looks quiet),
churn_pct 11.2 (very turbulent: worth zooming in).

Whether a window's flows are "in this period" or "in the frame" follows the
frame: a plain running window accumulates flows since start, so statbus uses
the per-period delta row (GROUP BY month) for flows and the window for stock.
Final function therefore exposes both; spec defines it precisely.

Steps (commit after each, performance: same i128 fast path as merge_agg,
no extra allocation per row, bench vs jsonb_stats_merge_agg must not regress):
1. Spec in dev/reference_plpgsql.sql.
2. Oracle in dev/exact_oracle.py (negative counts, flows).
3. Rust in src/state.rs: signed state, inflow/outflow, combine/serial/deserial,
   msfunc/minvfunc, final with validation.
4. Tests (round trip, running sum == alive-units merge per date, order/parallel
   independence, moving == plain window, invalid stock fails, flow identity).
5. Bench, upgrade script, CHANGELOG, README. No tag/release before review.
