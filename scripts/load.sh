#!/usr/bin/env bash
# README "Results" (day 4): the load generator against a real ledgerd, in BenchmarkContention's layouts, so the API's
# cost shows against the in-process numbers. For each isolation level, API and layout, each run gets:
#
#   1. a fresh schema in the test database, padded as `make bench-contention` pads it (20,000 accounts and 10,000
#      entries, then ANALYZE), so Postgres plans its reads as for tables of that size;
#   2. `ledgerd serve` on it (127.0.0.1:8103 and :9103), with a pool of 16 connections, one per client, as the
#      benchmark has;
#   3. `ledgerload` with 16 workers, as one JSON line appended to the results file (and, last, one worker alone,
#      for what the API adds to a single posting);
#   4. `ledgerd reconcile`, which must exit 0; then the server is stopped and the schema dropped.
#
#   scripts/load.sh [runs] [transfers]        default: 3 runs of 3000 transfers each
#
# Results go to bin/load/<time>.jsonl, and a table of medians is printed at the end. Needs jq.
set -euo pipefail

runs="${1:-3}"
transfers="${2:-3000}"
base="${LEDGER_TEST_DATABASE_URL:-postgres://localhost:5432/ledger_test?sslmode=disable}"
cd "$(dirname "$0")/.."
mkdir -p bin/load
out="bin/load/$(date +%Y%m%d-%H%M%S).jsonl"
log="${out%.jsonl}.log"

go build -o bin/ledgerd ./cmd/ledgerd
go build -o bin/ledgerload ./cmd/ledgerload

server=""
schema=""
cleanup() {
  if [ -n "$server" ]; then kill -TERM "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true; fi
  if [ -n "$schema" ]; then psql -q "$base" -c "SET client_min_messages = warning; DROP SCHEMA IF EXISTS $schema CASCADE" || true; fi
}
trap cleanup EXIT

# one_run LEVEL API LAYOUT WORKERS TRANSFERS RUN: one run on a fresh padded schema, appended to $out.
one_run() {
  local level="$1" api="$2" layout="$3" workers="$4" count="$5" run="$6"
  schema="load_$(date +%s)_${RANDOM}"
  local url="${base}&search_path=${schema}&pool_max_conns=16"
  psql -q -v ON_ERROR_STOP=1 "$base" -c "CREATE SCHEMA $schema"
  LEDGER_DATABASE_URL="$url" bin/ledgerd migrate 2>>"$log"
  # The padding, exactly as internal/store's pad helper writes it: entry k joins padding accounts 2k-1 and 2k.
  psql -q -1 -v ON_ERROR_STOP=1 "$base" <<SQL
SET search_path = $schema;
INSERT INTO accounts (name, type, currency, allow_negative)
  SELECT 'padding:' || n, 'liability', 'USD', true FROM generate_series(1, 20000) n;
INSERT INTO journal_entries (description, posting_count) SELECT 'padding', 2 FROM generate_series(1, 10000);
WITH pads AS (SELECT id, row_number() OVER (ORDER BY id) AS n FROM accounts WHERE name LIKE 'padding:%'),
     padded AS (SELECT id, row_number() OVER (ORDER BY id) AS n FROM journal_entries
                 WHERE description = 'padding' LIMIT 10000)
  INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
  SELECT e.id, p.id, 'USD', CASE WHEN p.n % 2 = 0 THEN 1 ELSE -1 END, 0, 1
    FROM pads p JOIN padded e ON e.n = (p.n + 1) / 2;
ANALYZE accounts, journal_entries, postings, idempotency_keys;
SQL
  local serve_log="bin/load/serve-$$.log"
  LEDGER_DATABASE_URL="$url" LEDGER_HTTP_ADDR=127.0.0.1:8103 LEDGER_GRPC_ADDR=127.0.0.1:9103 \
    LEDGER_ISOLATION="$level" bin/ledgerd serve 2>"$serve_log" &
  server=$!
  for _ in $(seq 100); do
    curl -sf http://127.0.0.1:8103/readyz >/dev/null && break
    sleep 0.1
  done
  local addr=127.0.0.1:8103
  [ "$api" = grpc ] && addr=127.0.0.1:9103
  local result
  result="$(bin/ledgerload -api "$api" -addr "$addr" -layout "$layout" -workers "$workers" -transfers "$count" \
    -seed "$run" -json)"
  LEDGER_DATABASE_URL="$url" bin/ledgerd reconcile >/dev/null 2>>"$log" ||
    { echo "ledgerd reconcile found differences after $level $api $layout run $run" >&2; exit 1; }
  kill -TERM "$server"
  wait "$server"
  server=""
  # The server's own retries, from its last log line (ledgerd logs them when it stops).
  local retries
  retries="$(grep '"msg":"stopped"' "$serve_log" | tail -1 | jq '.posting_retries')"
  cat "$serve_log" >>"$log"
  rm -f "$serve_log"
  jq -c --arg level "$level" --argjson run "$run" --argjson retries "$retries" \
    '. + {level: $level, run: $run, server_retries: $retries}' <<<"$result" >>"$out"
  psql -q "$base" -c "SET client_min_messages = warning; DROP SCHEMA $schema CASCADE"
  schema=""
  tail -1 "$out" | jq -r '"\(.level) \(.api) \(.layout) \(.workers) workers, run \(.run): \(.transfers_per_second | floor)/s, p50 \(.p50_us) µs, p99 \(.p99_us) µs, server retries \(.server_retries), gave up \(.gave_up), mismatches \(.mismatches | length)"'
}

for level in read-committed serializable; do
  for api in rest grpc; do
    for layout in disjoint random hot; do
      for run in $(seq "$runs"); do
        one_run "$level" "$api" "$layout" 16 "$transfers" "$run"
      done
    done
  done
done
# One client, no contention: what the API adds to a single posting (against make bench's in-process numbers).
for api in rest grpc; do
  for run in $(seq "$runs"); do
    one_run read-committed "$api" disjoint 1 2000 "$run"
  done
done

echo
echo "medians of $runs runs ($out):"
jq -rs '
  def median: sort | if length % 2 == 1 then .[length / 2 | floor] else (.[length / 2 - 1] + .[length / 2]) / 2 end;
  group_by([.workers, .level, .api, .layout]) | .[] |
  "\(.[0].level) \(.[0].api) \(.[0].layout), \(.[0].workers) workers: \(map(.transfers_per_second) | median | floor)/s, p50 \(map(.p50_us) | median | floor) µs, p99 \(map(.p99_us) | median | floor) µs, retries/transfer \(map(.server_retries / .transfers) | median * 100 | round / 100), gave up \(map(.gave_up) | min)-\(map(.gave_up) | max), mismatches \(map(.mismatches | length) | add)"
' "$out"
