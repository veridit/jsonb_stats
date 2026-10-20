#!/bin/bash
# Real upgrade test: install the previous release (git ref $OLD_REF, default
# v2.0.0), store aggregates, ALTER EXTENSION UPDATE to the working tree
# version, and check that stored summaries keep working and that a recompute
# gives the exact result.
# Runs inside the jsonb-stats-builder image (see Dockerfile.builder):
#   docker run --rm -v $PWD:/src_ro:ro --tmpfs /cache:exec,size=6g \
#     jsonb-stats-builder bash /src_ro/dev/xarch/upgrade_test.sh
set -euo pipefail
OLD_REF=${OLD_REF:-v2.0.0}
PGC=/usr/lib/postgresql/18/bin/pg_config
export CARGO_TARGET_DIR=/cache/target

git config --global --add safe.directory '*'
git clone -q /src_ro /old && git -C /old checkout -q "$OLD_REF"
mkdir -p /new && cp -r /src_ro/. /new/
rm -rf /old/.cargo /new/.cargo   # macOS-only linker flags
OLD_VER=$(sed -n 's/^version = "\(.*\)"/\1/p' /old/Cargo.toml | head -1)
NEW_VER=$(sed -n 's/^version = "\(.*\)"/\1/p' /new/Cargo.toml | head -1)
echo "== upgrade path: $OLD_VER ($OLD_REF) -> $NEW_VER"
[ -f "/new/sql/jsonb_stats--$OLD_VER--$NEW_VER.sql" ] \
  || { echo "missing sql/jsonb_stats--$OLD_VER--$NEW_VER.sql"; exit 1; }

echo "== installing old ($OLD_REF)"
(cd /old && cargo pgrx install --release --pg-config $PGC >/tmp/old.log 2>&1) || { tail -20 /tmp/old.log; exit 1; }
cp /usr/share/postgresql/18/extension/jsonb_stats--$OLD_VER.sql /tmp/
cp /usr/lib/postgresql/18/lib/jsonb_stats.so /tmp/jsonb_stats-old.so

echo "== installing new (working tree)"
(cd /new && cargo pgrx install --release --pg-config $PGC >/tmp/new.log 2>&1) || { tail -20 /tmp/new.log; exit 1; }
cp /tmp/jsonb_stats--$OLD_VER.sql /usr/share/postgresql/18/extension/
ls /usr/share/postgresql/18/extension/jsonb_stats*

# Start a cluster with the OLD library loaded first, so CREATE EXTENSION of the
# old version runs against the old .so, exactly like a deployed installation.
cp /usr/lib/postgresql/18/lib/jsonb_stats.so /tmp/jsonb_stats-new.so
cp /tmp/jsonb_stats-old.so /usr/lib/postgresql/18/lib/jsonb_stats.so
mkdir -p /pgdata && chown postgres /pgdata
su postgres -c "/usr/lib/postgresql/18/bin/initdb -D /pgdata -A trust >/dev/null"
su postgres -c "/usr/lib/postgresql/18/bin/pg_ctl -D /pgdata -l /tmp/pg.log -w start >/dev/null"
P="su postgres -c"
$P "psql -X -At -v ON_ERROR_STOP=1 -v old_ver=$OLD_VER" <<'EOF'
CREATE EXTENSION jsonb_stats VERSION :'old_ver';
CREATE TABLE src(region text, stats jsonb);
INSERT INTO src VALUES ('EU','{"n":{"type":"int","value":150}}'),
                       ('EU','{"n":{"type":"int","value":50}}'),
                       ('US','{"n":{"type":"int","value":2500}}');
CREATE MATERIALIZED VIEW facet AS SELECT region, jsonb_stats_agg(stats) AS agg FROM src GROUP BY region;
SELECT 'v2 stored has sum_sq: ' || (agg->'n' ? 'sum_sq') FROM facet WHERE region = 'EU';
-- Larger, realistic 2.x data: all numeric types, wide magnitudes, and 2.x
-- merge_agg outputs (lossy in 2.x) stored as a second level.
SELECT setseed(0.42);
CREATE TABLE big_src AS
SELECT i, (i % 40) AS grp, jsonb_build_object(
    'turnover', jsonb_build_object('type', 'int', 'value', floor(random() * 90000000)::bigint),
    'staff',    jsonb_build_object('type', 'nat', 'value', floor(random() * 5000)::int),
    'price',    jsonb_build_object('type', 'dec2', 'value', round((random() * 100000)::numeric, 2)),
    'ratio',    jsonb_build_object('type', 'float', 'value', random() * 1000)
) AS stats
FROM generate_series(1, 20000) AS i;
CREATE TABLE big_l2 AS SELECT grp, jsonb_stats_agg(stats) AS agg FROM big_src GROUP BY grp;
CREATE TABLE big_l3 AS SELECT grp % 4 AS region, jsonb_stats_merge_agg(agg) AS agg FROM big_l2 GROUP BY 1;
EOF
su postgres -c "/usr/lib/postgresql/18/bin/pg_ctl -D /pgdata -w stop >/dev/null"

echo "== swap in $NEW_VER library and ALTER EXTENSION UPDATE"
cp /tmp/jsonb_stats-new.so /usr/lib/postgresql/18/lib/jsonb_stats.so
su postgres -c "/usr/lib/postgresql/18/bin/pg_ctl -D /pgdata -l /tmp/pg.log -w start >/dev/null"
$P "psql -X -At -v ON_ERROR_STOP=1" <<'EOF'
ALTER EXTENSION jsonb_stats UPDATE;
SELECT 'version: ' || extversion FROM pg_extension WHERE extname = 'jsonb_stats';
-- Stored 2.x summaries keep working with no recompute (sum_sq rebuilt on read).
CREATE TEMP TABLE before_refresh AS SELECT jsonb_stats_merge_agg(agg)->'n' AS r FROM facet;
SELECT 'stored 2.x, merged:  ' || r::text FROM before_refresh;
-- Incremental use on old data: merge in one new row without recomputing.
SELECT 'stored 2.x + new row: ' || (jsonb_stats_merge(f.agg, jsonb_stats_to_agg('{"n":{"type":"int","value":10}}'))->'n')::text
FROM facet f WHERE region = 'US';
REFRESH MATERIALIZED VIEW facet;
SELECT 'after REFRESH:       ' || (jsonb_stats_merge_agg(agg)->'n')::text FROM facet;
SELECT 'direct:              ' || (jsonb_stats_agg(stats)->'n')::text FROM src;
SELECT 'CHECK legacy == exact here: ' ||
       ((SELECT r FROM before_refresh) = (SELECT jsonb_stats_agg(stats)->'n' FROM src))::text;
SELECT 'CHECK refresh == direct:    ' ||
       ((SELECT jsonb_stats_merge_agg(agg)->'n' FROM facet) = (SELECT jsonb_stats_agg(stats)->'n' FROM src))::text;
-- Large legacy data: every stored 2.x level must be accepted; count/sum/min/max
-- exact; derived fields within a tight relative tolerance of the exact result.
CREATE TEMP TABLE exact AS SELECT jsonb_stats_agg(stats) AS a FROM big_src;
CREATE TEMP TABLE from_l2 AS SELECT jsonb_stats_merge_agg(agg) AS a FROM big_l2;
CREATE TEMP TABLE from_l3 AS SELECT jsonb_stats_merge_agg(agg) AS a FROM big_l3;
SELECT 'LEGACY ' || lvl || ' ' || k || ': '
    || 'count/min/max ' || CASE WHEN (l.a->k->'count', l.a->k->'min', l.a->k->'max') = (e.a->k->'count', e.a->k->'min', e.a->k->'max') THEN 'ok' ELSE 'DIFF' END
    || ', sum ' || CASE WHEN abs((l.a->k->>'sum')::numeric - (e.a->k->>'sum')::numeric) <= 1e-6 * greatest(1, abs((e.a->k->>'sum')::numeric)) THEN 'ok' ELSE 'DIFF' END
    || ', stddev rel err ' || to_char(abs((l.a->k->>'stddev')::numeric / (e.a->k->>'stddev')::numeric - 1), 'FM0.0000000000')
    || ', mean ' || CASE WHEN l.a->k->'mean' = e.a->k->'mean' THEN 'equal' ELSE 'differs' END
FROM (SELECT 'L2' AS lvl, a FROM from_l2 UNION ALL SELECT 'L3', a FROM from_l3) l, exact e,
     unnest(ARRAY['turnover', 'staff', 'price', 'ratio']) AS k
ORDER BY lvl, k;
SELECT 'CHECK legacy stddev within 1e-6: ' || bool_and(abs((l.a->k->>'stddev')::numeric / (e.a->k->>'stddev')::numeric - 1) < 1e-6)::text
FROM (SELECT a FROM from_l2 UNION ALL SELECT a FROM from_l3) l, exact e, unnest(ARRAY['turnover', 'staff', 'price', 'ratio']) AS k;
-- After recompute from source, everything is exact again.
CREATE TEMP TABLE re_l2 AS SELECT grp, jsonb_stats_agg(stats) AS agg FROM big_src GROUP BY grp;
SELECT 'CHECK recomputed == exact: ' || ((SELECT jsonb_stats_merge_agg(agg) FROM re_l2) = (SELECT a FROM exact))::text;
EOF
