#!/usr/bin/env bash
# Creates the two local databases on the machine's Postgres 14: `ledger` for running ledgerd, and `ledger_test`
# for the tests (each test makes its own schema inside it and drops it afterwards). Safe to run again.
set -euo pipefail

port="${PGPORT:-5432}"
for db in ledger ledger_test; do
  if [ "$(psql -p "$port" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = '$db'")" = "1" ]; then
    echo "$db: exists"
  else
    createdb -p "$port" "$db"
    echo "$db: created"
  fi
done
