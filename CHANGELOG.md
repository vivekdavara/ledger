# Changelog

## v1.0.0 (2026-10-15)

The first release: a double-entry payments ledger with REST and gRPC APIs over PostgreSQL 14. The design is in
[DESIGN.md](DESIGN.md), the measured results in the [README](README.md#results).

### The ledger's rules (day 1)

- Money is `int64` minor units in 8 currencies (exponents 0, 2 and 3), parsed and formatted exactly: `"12.345"` in
  USD is an error, never a rounding.
- An entry has 2 to 64 postings that sum to zero in each currency. Go validates it for a clear error, and the
  database checks it again at commit (deferred constraint triggers), whoever writes it.
- The database keeps every balance: posting triggers lock the account, write the new balance and number the
  account's postings in a gapless chain. History is append-only, a balance changes only through postings, and a
  protected account can't go below zero.
- Migrations 0001 to 0009 are built into the binary and applied in one transaction under an advisory lock, each
  with a checksum.

### The API (day 2)

- REST on :8103 and gRPC on :9103, both thin layers over one service: create an account (idempotent by name), read
  it, page its history by version, post an entry.
- Posting requires an idempotency key. The key, a hash of the request and the entry's id are stored in the entry's
  own transaction: a retry gets the original back byte for byte, and the key with another entry is refused.
- One error table for both APIs: stable reasons, HTTP statuses, gRPC codes with `google.rpc.ErrorInfo`.
- Every request is bounded to 10 s; on SIGTERM in-flight work gets a grace period, then is cut off.

### Concurrency, the outbox and reconciliation (day 3)

- Postings run at `READ COMMITTED` by default (`SERIALIZABLE` with `LEDGER_ISOLATION`), deciding only on rows they
  have locked, with accounts locked in id order and bounded retries.
- A transactional outbox: a trigger writes one event per entry in the entry's transaction; `ledgerd publish`
  delivers them as JSON lines, at least once, each account's in order.
- `ledgerd reconcile` recomputes the books in one snapshot with ten checks, and exits 0, 3 (differences) or 1.

### Load and crashes (day 4)

- `ledgerload` (and `make load`) drives either API with chosen contention, then checks every balance and version
  against the answers it got.
- A test-only build (`-tags faults`) has crash points around the commit and the outbox's mark. Crash tests kill a
  real `ledgerd` with `SIGKILL` and show every transfer posted exactly once and every event delivered.

### The release (day 5)

- `ledgerd version`: the version the go command stamped from git, the commit, the schema version and the Go release.
- `GET /v1/entries/{id}` and `GetEntry` read a posted entry back as posting it answered.
- The gRPC port answers the standard health check (`grpc.health.v1.Health/Check`) as `/readyz` does.
- `ledgerload` asks the ledger whether it is ready before sending anything.
- Fixed, from a review of the whole repo: a stop with a zero `LEDGER_SHUTDOWN_GRACE` always exited 1, though nothing
  was cut off; REST refused `after_version` past 2^31, its own paging cursor for a busy enough account; a missing
  idempotency key was `malformed_request` over REST but `invalid_argument` over gRPC (now `invalid_argument` over
  both); a typo in `serve`'s settings migrated the database before failing; six places where the docs no longer
  matched the code.
- `scripts/release_check.sh`: `make check`, the same on a clean export of the commit, then every measurement in the
  README again. Tests run two packages at a time, under Postgres's 100 connections.
