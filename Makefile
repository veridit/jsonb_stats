MODULE_big = jsonb_stats
EXTENSION = jsonb_stats
EXTENSION_VERSION = 1.0
DATA = $(EXTENSION)--$(EXTENSION_VERSION).sql

DOCS = README.md
#README.html: README.md
#	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > README.html

SQL_FILES = $(wildcard sql/[0-9]*_*.sql)

REGRESS = $(if $(TESTS),$(TESTS),$(patsubst sql/%.sql,%,$(sort $(SQL_FILES))))

OBJS = jsonb_stats.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# test is a convenient alias for installcheck.
# To run all tests: `make test`
# To run a single test: `make test TESTS=001_some_test`
# To run a subset of tests: `make test TESTS="001_some_test 002_another_test"`
.PHONY: test setup_test_expected_files
test: setup_test_expected_files installcheck

# Create empty expected files for new tests if they don't exist.
setup_test_expected_files:
	mkdir -p expected
	@for test in $(REGRESS); do \
		if [ ! -f expected/$$test.out ]; then \
			touch expected/$$test.out; \
		fi; \
	done

# New target to run vimdiff for the first failing test
vimdiff-fail-first:
	@first_fail=$$(grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | head -n 1); \
	if [ -n "$$first_fail" ]; then \
		echo "Running vimdiff for test: $$first_fail"; \
		vim -d expected/$$first_fail.out results/$$first_fail.out < /dev/tty; \
	else \
		echo "No failing tests found."; \
	fi

# New target to run vimdiff for all failing tests
vimdiff-fail-all:
	@grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
		echo "Next test: $$test"; \
		echo "Press C to continue, s to skip, or b to break (default: C)"; \
		read -n 1 -s input < /dev/tty; \
		if [ "$$input" = "b" ]; then \
			break; \
		elif [ "$$input" = "s" ]; then \
			continue; \
		fi; \
		echo "Running vimdiff for test: $$test"; \
		vim -d expected/$$test.out results/$$test.out < /dev/tty; \
	done

# New target to show diff for all failing tests
diff-fail-all:
	@grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
		echo "Showing diff for test: $$test"; \
		diff -u "expected/$$test.out" "results/$$test.out" || true; \
	done
	@if grep -q 'not ok' regression.out; then exit 1; fi

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
