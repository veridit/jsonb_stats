PGRX_PG_CONFIG_PATH ?= /Applications/Postgres.app/Contents/Versions/18/bin/pg_config
BINDGEN_EXTRA_CLANG_ARGS ?= -isysroot $(shell xcrun --show-sdk-path)

export PGRX_PG_CONFIG_PATH
export BINDGEN_EXTRA_CLANG_ARGS

PG_VERSION ?= pg18

.PHONY: test test-crash run install package

test:
	cargo pgrx test $(PG_VERSION)

test-crash: install
	psql -d postgres -f dev/test-crash.sql

run:
	cargo pgrx run $(PG_VERSION)

install:
	cargo pgrx install

package:
	cargo pgrx package
