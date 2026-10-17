#!/bin/bash
# Real upgrade test: install jsonb_stats 2.0.0 (from git ref $OLD_REF), store
# aggregates, ALTER EXTENSION UPDATE to the working tree version, and check
# that stored 2.x summaries keep working (sum_sq rebuilt on read) and that a
# recompute gives the exact result.
# Runs inside the jsonb-stats-builder image (see Dockerfile.builder):
#   docker run --rm -v $PWD:/src_ro:ro --tmpfs /cache:exec,size=6g \
#     jsonb-stats-builder bash /src_ro/dev/xarch/upgrade_test.sh
set -euo pipefail
OLD_REF=${OLD_REF:-c4a2595}
PGC=/usr/lib/postgresql/18/bin/pg_config
export CARGO_TARGET_DIR=/cache/target

git config --global --add safe.directory '*'
git clone -q /src_ro /old && git -C /old checkout -q "$OLD_REF"
mkdir -p /new && cp -r /src_ro/. /new/
rm -rf /old/.cargo /new/.cargo   # macOS-only linker flags

echo "== installing old ($OLD_REF)"
(cd /old && cargo pgrx install --release --pg-config $PGC >/tmp/old.log 2>&1) || { tail -20 /tmp/old.log; exit 1; }
cp /usr/share/postgresql/18/extension/jsonb_stats--2.0.0.sql /tmp/
cp /usr/lib/postgresql/18/lib/jsonb_stats.so /tmp/jsonb_stats-2.so

echo "== installing new (working tree)"
(cd /new && cargo pgrx install --release --pg-config $PGC >/tmp/new.log 2>&1) || { tail -20 /tmp/new.log; exit 1; }
cp /tmp/jsonb_stats--2.0.0.sql /usr/share/postgresql/18/extension/
ls /usr/share/postgresql/18/extension/jsonb_stats*

# Start a cluster with the OLD library loaded first, so CREATE EXTENSION 2.0.0
# runs against the 2.x .so, exactly like a deployed 2.x installation.
cp /usr/lib/postgresql/18/lib/jsonb_stats.so /tmp/jsonb_stats-3.so
cp /tmp/jsonb_stats-2.so /usr/lib/postgresql/18/lib/jsonb_stats.so
mkdir -p /pgdata && chown postgres /pgdata
su postgres -c "/usr/lib/postgresql/18/bin/initdb -D /pgdata -A trust >/dev/null"
su postgres -c "/usr/lib/postgresql/18/bin/pg_ctl -D /pgdata -l /tmp/pg.log -w start >/dev/null"
P="su postgres -c"
$P "psql -X -At -v ON_ERROR_STOP=1" <<'EOF'
CREATE EXTENSION jsonb_stats VERSION '2.0.0';
CREATE TABLE src(region text, stats jsonb);
INSERT INTO src VALUES ('EU','{"n":{"type":"int","value":150}}'),
                       ('EU','{"n":{"type":"int","value":50}}'),
                       ('US','{"n":{"type":"int","value":2500}}');
CREATE MATERIALIZED VIEW facet AS SELECT region, jsonb_stats_agg(stats) AS agg FROM src GROUP BY region;
SELECT 'v2 stored has sum_sq: ' || (agg->'n' ? 'sum_sq') FROM facet WHERE region = 'EU';
EOF
su postgres -c "/usr/lib/postgresql/18/bin/pg_ctl -D /pgdata -w stop >/dev/null"

echo "== swap in 3.0.0 library and ALTER EXTENSION UPDATE"
cp /tmp/jsonb_stats-3.so /usr/lib/postgresql/18/lib/jsonb_stats.so
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
EOF
