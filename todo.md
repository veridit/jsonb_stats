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

### Design decisions (made overnight, 2026-11-02, review in the morning)

- A `stats_delta` entry keeps two flows, not one signed state:
  `{"type": "int_delta", "in": {count, sum, sum_sq}, "out": {count, sum, sum_sq}}`.
  Each flow is a sum of real summaries, so it is always a valid nonnegative
  state (obstacles 1 and 2 above disappear); the stock is `in - out`, checked
  only when a stock is read. Counted kinds: `{"in": {"counts"}, "out": {"counts"}}`
  (arr also `count`). Types: int/nat/dec2/float/str/bool/arr/date `_delta`.
- Canonical form, so output is identical for any order/grouping/window path:
  int/nat sums at scale 0, dec2 at 2 (sum_sq 4), float trimmed of trailing
  zeros (trim_scale); zero counts dropped; entries with both flows empty dropped.
- Variables named `min_max` are rejected in deltas (reserved marker key).
- API:
  - `jsonb_stats_to_delta(stats_agg) -> stats_delta` (entry event)
  - `jsonb_stats_negate(stats_agg | stats_delta) -> stats_delta` (exit; swaps in/out)
  - `jsonb_stats_delta_merge(a, b) -> stats_delta` (scalar add)
  - `jsonb_stats_delta_agg(stats_delta) -> stats_delta` (Internal state, parallel,
    moving-aggregate inverse for windows)
  - `jsonb_stats_delta_summary(stock stats_delta[, period stats_delta]) -> stats_agg`
    with `"min_max": false`: per numeric key count, sum, sum_sq, mean,
    sum_sq_diff, variance, stddev, CV (as today, mean/sum_sq_diff null when
    count 0); with a period also `in`/`out` {count, sum}, `change_pct`,
    `churn_pct` (denominator |previous sum|, null when 0). Counted kinds get
    net counts (and in/out counts with a period). Fails fast when the stock or
    the previous stock (stock - period) is invalid.

### Morning report (overnight 2026-11-02/03)

Done, committed on master, NOT tagged/pushed/released (awaiting review):
- Spec (dev/reference_plpgsql.sql), Rust (src/delta.rs), oracle
  (dev/delta_oracle.py -> dev/fixtures/delta_fixture.sql), tests
  (src/tests_delta.rs, 8 tests), xarch checks, benchmark, docs
  (README "Stock over time", ARCHITECTURE, CHANGELOG 3.1.0), version 3.1.0,
  upgrade script sql/jsonb_stats--3.0.0--3.1.0.sql.
- Verified: make test 110/110; make clippy (-D warnings) clean; fixture-check;
  make xarch IDENTICAL (amd64 == arm64, incl. delta checks); make upgrade-test
  OLD_REF=v3.0.0 passes;
  local upgrade 3.0.0 -> 3.1.0 gives a catalog identical to a fresh install.
- Tests prove: running window == oracle for 24 months x (int, dec2, float,
  nat, str) incl. in/out/change_pct/churn_pct; stock == jsonb_stats_agg of
  alive units (minus min/max); entry+exit == empty; negate(negate)=to_delta;
  order/grouping/parallel independence; rolling window (minvfunc) == fresh
  sum; Rust == PL/pgSQL byte for byte; 11 fail-fast cases match reference.
- Bench (M-series, pg18): delta_agg 67 ms vs merge_agg 50 ms on the same 1K
  summaries (ratio 1.3, it carries two flows); Rust 404x PL/pgSQL.

Toolchain note: Homebrew llvm 22 broke pgrx 0.16 bindgen ("no field
rd_tableam"); Makefile now pins Apple's libclang (LIBCLANG_PATH).

Real-data check (local statbus_local, 15,611 unit versions, 311 regions,
3 unit types, copied read-only to a scratch db): stock per (unit_type,
region) at 23 month starts from 729 delta rows equals jsonb_stats_merge_agg
of the alive units in all 16,629 non-empty cells (16,767 dense rows; the
other 138 have no statistics). Values equal; float sums print without
trailing zeros in deltas (canonical form, e.g. 515001329351999.533 vs
...9.5330), so compare numerically, not as text, against stats_agg.

Open questions for you:
1. churn_pct uses |in.sum| + |out.sum|. A unit changing 10 -> 12 counts as
   out 10 + in 10+2, so value changes inflate churn (as in the example). If
   you want births/deaths-only churn, statbus can emit changes as a separate
   delta stream; or we add a "net change only" mode. Decide on real data.
2. Delta for date_agg drops min/max like numeric (counts only).
3. Float printing: delta stocks print float sum/sum_sq trimmed (see above),
   so they are not text-identical to jsonb_stats_agg. Needed for
   path-independence; tell me if statbus compares text.
4. Name check: jsonb_stats_delta_summary vs e.g. jsonb_stats_delta_final.
Then: tag v3.1.0 per release checklist, and integrate in statbus (STATBUS-558).
