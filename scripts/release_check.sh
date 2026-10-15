#!/usr/bin/env bash
# The release check (README, "Release check"): what CI runs, the same again on a clean export of the commit (so it
# needs nothing that isn't committed), then every measurement in README "Results" again at this commit, and the crash
# tests with their logs. Each step's output goes to bin/release/<time>/<step>.txt; the check stops at the first step
# that fails.
#
#   scripts/release_check.sh          all of it
#   scripts/release_check.sh quick    the two checks only
#
# The measurements need a quiet machine (nothing else using the CPU or Postgres), PostgreSQL 14 on :5432 with the
# databases scripts/dev-db.sh creates, ports 8103 and 9103 free, and jq.
set -euo pipefail

cd "$(dirname "$0")/.."
export LEDGER_TEST_DATABASE_URL="${LEDGER_TEST_DATABASE_URL:-postgres://localhost:5432/ledger_test?sslmode=disable}"
dir="bin/release/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$dir"

step() {
  local name="$1"
  shift
  local start=$SECONDS
  echo "== $name: $*"
  if ! "$@" >"$dir/$name.txt" 2>&1; then
    echo "   FAILED after $((SECONDS - start)) s; see $dir/$name.txt"
    exit 1
  fi
  echo "   ok in $((SECONDS - start)) s"
}

echo "release check of $(git rev-parse --short=12 HEAD) ($(git describe --tags --always --dirty)), $(date)"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "warning: tracked files have changes: the clean export checks the commit, every other step the working tree"
fi

export_dir="$(mktemp -d)"
trap 'rm -rf "$export_dir"' EXIT
git archive HEAD | tar -x -C "$export_dir"

step check make check
step clean-export make -C "$export_dir" check
[ "${1:-}" = quick ] && exit 0

step bench make bench
step bench-contention make bench-contention
step bench-outbox make bench-outbox
step load make load
step crash-tests go test -race -count=1 -v -run 'TestKillsDuringLoad|TestAKilledPublisher|TestACrash' ./cmd/ledgerd
echo "all steps passed; the outputs are in $dir"
