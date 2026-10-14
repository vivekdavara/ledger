# Local defaults. CI sets LEDGER_TEST_DATABASE_URL itself.
LEDGER_TEST_DATABASE_URL ?= postgres://localhost:5432/ledger_test?sslmode=disable
export LEDGER_TEST_DATABASE_URL

.PHONY: bench bench-contention bench-outbox check lint proto test

# What CI runs.
check: lint test

lint:
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then echo "not gofmt-formatted:"; echo "$$unformatted"; exit 1; fi
	go vet ./...
	go vet -tags faults ./...

# The faults build (DESIGN.md, "Load and failure injection (day 4)") is tested on its own: its points crash the
# process, so only internal/faults's own tests, which watch a child process crash, run with the tag.
test:
	go test -race -count=1 -timeout 4m ./...
	go test -race -count=1 -tags faults ./internal/faults

# README, "Results": the commit-time check's cost (BenchmarkPostEntry) and the service's
# (BenchmarkServicePostEntry). Needs a quiet machine.
bench:
	go test -run '^$$' -bench PostEntry -benchtime 2s -count 3 ./internal/store

# README, "Results" (day 3): the isolation levels under contention, on small tables and on a padded ledger.
bench-contention:
	go test -run '^$$' -bench Contention -benchtime 3000x -count 3 ./internal/store
	go test -run '^$$' -bench Contention -benchtime 3000x -count 3 ./internal/store -args -padding 20000

# README, "Results" (day 3): one publisher draining 10,000 events, and one reconciliation of 200,000 postings.
bench-outbox:
	go test -run '^$$' -bench Publish -benchtime 3x -count 3 ./internal/outbox
	go test -run '^$$' -bench Reconcile -benchtime 3x -count 3 -timeout 20m ./internal/store

# Lint proto/ and regenerate internal/gen from it. The generators are pinned in tools/go.mod, so nothing needs
# installing; the generated code is committed, so building never needs them.
proto:
	go tool -modfile=tools/go.mod buf lint
	go tool -modfile=tools/go.mod buf generate
