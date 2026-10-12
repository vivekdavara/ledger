# Local defaults. CI sets LEDGER_TEST_DATABASE_URL itself.
LEDGER_TEST_DATABASE_URL ?= postgres://localhost:5432/ledger_test?sslmode=disable
export LEDGER_TEST_DATABASE_URL

.PHONY: bench check lint test

# What CI runs.
check: lint test

lint:
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then echo "not gofmt-formatted:"; echo "$$unformatted"; exit 1; fi
	go vet ./...

test:
	go test -race -count=1 ./...

# The cost of the commit-time check (README, "Results"). Needs a quiet machine.
bench:
	go test -run '^$$' -bench PostEntry -benchtime 2s -count 3 ./internal/store
