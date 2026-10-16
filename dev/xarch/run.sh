#!/bin/bash
# Runs inside postgres:18 of either arch with /out mounted.
set -e
arch=$(uname -m)
cp /out/aarch64/usr/share/postgresql/18/extension/jsonb_stats* /usr/share/postgresql/18/extension/
if [ "$arch" = x86_64 ]; then
  cp /out/x86_64/jsonb_stats.so /usr/lib/postgresql/18/lib/jsonb_stats.so
else
  cp /out/aarch64/usr/lib/postgresql/18/lib/jsonb_stats.so /usr/lib/postgresql/18/lib/jsonb_stats.so
fi
docker-entrypoint.sh postgres >/tmp/pg.log 2>&1 &
until [ "$(grep -c 'ready to accept' /tmp/pg.log 2>/dev/null)" -ge 2 ]; do sleep 1; done
echo "arch=$arch"
psql -X -q -U postgres -v ON_ERROR_STOP=1 -At \
  -f /dev_src/reference_plpgsql.sql -f /dev_src/fixtures/exact_fixture.sql \
  -f /dev_src/xarch_checks.sql -f /dev_src/xarch_checks_rust.sql
