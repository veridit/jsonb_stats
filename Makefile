PGRX_PG_CONFIG_PATH ?= /Applications/Postgres.app/Contents/Versions/18/bin/pg_config
BINDGEN_EXTRA_CLANG_ARGS ?= -isysroot $(shell xcrun --show-sdk-path)

export PGRX_PG_CONFIG_PATH
export BINDGEN_EXTRA_CLANG_ARGS

PG_VERSION ?= pg18
SHELL := /bin/bash

.PHONY: test test-crash run install package fixture fixture-check xarch upgrade-test

# Real upgrade test (needs Docker): install OLD_REF (previous release tag),
# store aggregates, ALTER EXTENSION UPDATE to this tree, verify results.
OLD_REF ?= v2.0.0
upgrade-test:
	docker build --platform linux/arm64 -t jsonb-stats-builder -f dev/xarch/Dockerfile.builder dev/xarch
	docker run --rm --platform linux/arm64 -e OLD_REF=$(OLD_REF) -v $(CURDIR):/src_ro:ro --tmpfs /cache:exec,size=8g \
		jsonb-stats-builder bash /src_ro/dev/xarch/upgrade_test.sh

test:
	cargo pgrx test $(PG_VERSION)

# Regenerate the exact-oracle fixture used by src/tests_exact.rs
fixture:
	python3 dev/exact_oracle.py > dev/fixtures/exact_fixture.sql

# Fail if the committed fixture is stale relative to dev/exact_oracle.py
fixture-check:
	python3 dev/exact_oracle.py | diff -q - dev/fixtures/exact_fixture.sql

# Cross-architecture determinism check (needs Docker). Builds the extension for
# linux arm64 (native) and x86_64 (cross), runs dev/xarch_checks*.sql on both
# postgres:18 platforms, and fails unless the outputs are identical.
XARCH_OUT ?= target/xarch
xarch:
	mkdir -p $(XARCH_OUT)/out
	docker build --platform linux/arm64 -t jsonb-stats-builder -f dev/xarch/Dockerfile.builder dev/xarch
	docker run --rm --platform linux/arm64 -v $(CURDIR):/src_ro:ro -v $(CURDIR)/$(XARCH_OUT)/out:/out \
		--tmpfs /cache:exec,size=6g jsonb-stats-builder bash /src_ro/dev/xarch/build.sh
	for p in amd64 arm64; do \
		docker run --rm --platform linux/$$p -e POSTGRES_HOST_AUTH_METHOD=trust \
			-v $(CURDIR)/dev:/dev_src:ro -v $(CURDIR)/$(XARCH_OUT)/out:/out:ro \
			--entrypoint bash postgres:18-bookworm /dev_src/xarch/run.sh > $(XARCH_OUT)/result-$$p.txt || exit 1; \
	done
	cat $(XARCH_OUT)/result-amd64.txt
	test $$(wc -l < $(XARCH_OUT)/result-amd64.txt) -gt 5
	diff <(tail -n +2 $(XARCH_OUT)/result-amd64.txt) <(tail -n +2 $(XARCH_OUT)/result-arm64.txt) && echo "xarch: IDENTICAL"

test-crash: install
	psql -d postgres -f dev/test-crash.sql

run:
	cargo pgrx run $(PG_VERSION)

install:
	cargo pgrx install

package:
	cargo pgrx package
