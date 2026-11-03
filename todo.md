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
