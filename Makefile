MODULE_big = jsonb_stats
EXTENSION = jsonb_stats
DATA = jsonb_stats--1.0.sql
DOCS = README.md
OBJS = jsonb_stats.o

SQL_FILES = $(wildcard sql/[0-9]*_*.sql)

REGRESS_ALL = $(patsubst sql/%.sql,%,$(SQL_FILES))
REGRESS_FAST_LIST = $(filter-out 004_benchmark,$(REGRESS_ALL))

# By default, run all tests. If 'fast' is a command line goal, run the fast subset.
REGRESS_TO_RUN = $(REGRESS_ALL)
ifeq (fast,$(filter fast,$(MAKECMDGOALS)))
	REGRESS_TO_RUN = $(REGRESS_FAST_LIST)
endif

REGRESS = $(if $(TESTS),$(TESTS),$(REGRESS_TO_RUN))
REGRESS_OPTS = --dbname=jsonb_stats_regression

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

# Dynamic analysis with AddressSanitizer (ASan)
# To use, run: make test-asan
# This builds the extension with memory error detectors and runs the tests.
ifeq ($(SANITIZE),true)
	CFLAGS += -fsanitize=address -fno-omit-frame-pointer
	SHLIB_LINK += -fsanitize=address
endif

include $(PGXS)

# test is a convenient alias for installcheck.
# To run all tests: `make test`
# To run fast tests (excluding benchmarks): `make test fast`
# To run a single test: `make test TESTS=001_jsonb_stats_api`
# To run a subset of tests: `make test TESTS="001_jsonb_stats_api 003_readme_scenario"`
.PHONY: test setup_test_files
test: setup_test_files installcheck

# Create empty expected files for new tests if they don't exist.
setup_test_files:
	@mkdir -p expected
	@for test in $(REGRESS); do \
		if [ ! -f expected/$$test.out ]; then \
			touch expected/$$test.out; \
		fi; \
	done

# The 'fast' target is a dummy. Its presence in `make test fast` is used to
# trigger the conditional logic that selects the fast test suite.
.PHONY: fast
fast:
	@:

# Target to show diff for all failing tests. Use `make diff-fail-all vim` for vimdiff.
.PHONY: diff-fail-all vim tidy test-asan
diff-fail-all:
ifeq (vim,$(filter vim,$(MAKECMDGOALS)))
	@grep 'not ok' regression.out 2>/dev/null | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
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
else
	@grep 'not ok' regression.out 2>/dev/null | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
		echo "Showing diff for test: $$test"; \
		diff -u "expected/$$test.out" "results/$$test.out" || true; \
	done
	@if grep -q 'not ok' regression.out 2>/dev/null; then exit 1; fi
endif

vim:
	@:

# Target for static analysis using clang-tidy
tidy:
	@echo "Running clang-tidy..."
	@clang-tidy $(firstword $(OBJS:.o=.c)) -- $(CFLAGS)

# Target for dynamic analysis using AddressSanitizer (ASan)
test-asan:
	@echo "Running tests with AddressSanitizer..."
	@$(MAKE) clean > /dev/null
	@$(MAKE) SANITIZE=true test || true
	@echo
	@echo "======================================================================="
	@echo "--- ASan logs (if any) from regression.log ---"
	@echo "======================================================================="
	@cat regression.log || true
	@echo "======================================================================="
