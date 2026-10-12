# Ledger: design

A double-entry payments ledger: the system of record that says how much money every account holds and why. Every
movement of money is a **journal entry** made of **postings**, and the postings of an entry always sum to zero in
each currency, so money is moved, never created or destroyed. Go 1.27, PostgreSQL 14 (pgx), gRPC + REST.

This file is the plan for all five days. Day 1 builds the parts everything else stands on: the money type, the
entry validator, the schema with its database-enforced invariants, the migration runner, a health endpoint and CI.

## Vocabulary

- **Account**: a named bucket of money in one currency, such as `wallet:alice` or `revenue:fees`. It has a
  **type**: asset, liability, equity, revenue or expense.
- **Journal entry**: one business event ("Alice paid Bob $20 with a $0.60 fee"). It is atomic: all of its
  postings are recorded, or none are.
- **Posting**: one line of an entry: an account, a currency and a signed amount.

### Sign convention

Amounts are signed integers in the currency's minor unit (cents for USD, yen for JPY, fils for KWD).
**Positive is a debit, negative is a credit.** An account's stored `balance` is the plain sum of its postings.

What a person calls the balance depends on the account's **normal side**:

| Type | Normal side | Balance as shown | Example |
|---|---|---|---|
| asset | debit | `balance` | `cash:bank` (money the platform holds) |
| expense | debit | `balance` | `expense:card-fees` |
| liability | credit | `-balance` | `wallet:alice` (money the platform owes Alice) |
| equity | credit | `-balance` | `equity:owner` |
| revenue | credit | `-balance` | `revenue:fees` |

So a healthy customer wallet has a negative stored balance (it is a credit balance), and shows as positive. One
signed column keeps the zero-sum rule a plain `SUM(amount) = 0`. The alternative, separate debit and credit
columns as TigerBeetle stores them, makes every sum two sums.

### Examples

Alice tops up $50 by card. The platform's bank balance goes up and so does what it owes Alice:

| Account | Type | Amount (cents) |
|---|---|---|
| `cash:bank` | asset | +5000 |
| `wallet:alice` | liability | −5000 |

Alice pays Bob $20, and the platform keeps a 3% fee:

| Account | Type | Amount (cents) |
|---|---|---|
| `wallet:alice` | liability | +2000 |
| `wallet:bob` | liability | −1940 |
| `revenue:fees` | revenue | −60 |

A currency conversion is one entry with two balanced currencies. Each account has one currency, so the
conversion goes through an FX account per currency:

| Account | Currency | Amount |
|---|---|---|
| `wallet:alice:usd` | USD | +10000 |
| `fx:usd` | USD | −10000 |
| `fx:eur` | EUR | +9200 |
| `wallet:alice:eur` | EUR | −9200 |

USD sums to zero and EUR sums to zero. The rate is the ratio of the two legs; the FX accounts' balances are the
platform's currency position.

A mistake is never edited away. It is corrected by a **reversing entry** with every amount negated, so the history
shows both the mistake and its correction.

## Invariants and where each is enforced

The application checks every rule first, to give a clear error. The database checks the rules again, so a bug in
the application, a second writer, or a hand-typed `psql` session can't break them. Every database check fails
closed: a statement that gets around the expected order of operations is rejected, never accepted.

| # | Invariant | Application (Go) | Database |
|---|---|---|---|
| I1 | The postings of an entry sum to zero in each currency | `ledger.Entry.Validate` | deferred constraint trigger, checked at commit |
| I2 | An entry has between 2 and 64 postings, exactly as many as it declares | `Validate` | the same trigger: `posting_count` must match |
| I3 | A posting's currency is its account's currency | checked against the account | composite foreign key `(account_id, currency)` |
| I4 | An account's `balance` is the sum of its postings; each posting records the balance after it and the account's version, with no gaps | the database does it | `BEFORE INSERT` trigger on postings; `UNIQUE (account_id, account_version)` |
| I5 | A protected account never goes below zero on its normal side | checked before posting | `CHECK` on accounts, evaluated on every balance change |
| I6 | Entries and postings are never updated or deleted | no code path does it | triggers reject `UPDATE`, `DELETE` and `TRUNCATE` |
| I7 | A balance changes only through a posting | no code path does it | trigger on accounts rejects direct changes |

I1 and I4 together give the property the property tests check from day 3: **in every currency, the balances of all
accounts sum to zero**, whatever transfers ran and in whatever order.

### Why a deferred constraint trigger (I1, I2)

A `CHECK` constraint sees one row, but I1 is about a group of rows. The rule can only be checked once all of an
entry's postings exist, so it runs at commit: `CREATE CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED`.

The check runs from two places:

- After each posting insert, so postings can't be appended to an entry that was committed earlier. Adding a
  balanced pair to an old entry would keep I1, but I2 catches it: the entry declared its `posting_count` when it
  was created, entries are immutable, and the count would no longer match.
- After each entry insert, so an entry with no postings at all can't commit.

**An optimisation that was wrong (migrations 0004 and 0005).** For an entry of N postings the check runs N + 1
times at commit, each run reading all N postings: quadratic in the entry's size. Migration 0004 tried to run it
once per entry: each entry recorded the transaction that created it, and a posting's check was skipped when its
entry was the current transaction's own, on the grounds that the entry's own check would run at commit anyway.
That reasoning was wrong. `SET CONSTRAINTS ALL IMMEDIATE` runs the entry's check early, and once run it is used
up, so a posting appended afterwards was never checked: an entry committed with three postings against two
declared, out of balance (found by a review agent; `TestAnEarlyCheckDoesNotExcuseLaterPostings`). Migration 0005
undid it. Every posting's check is the full check again. The last posting's check runs after the last posting
exists, whenever the client makes the checks run, so the final entry is always seen. The measured cost is under
"Results" in the README.

### Why the database keeps the balance (I4, I5)

Each account row holds a cached `balance` and a `version`, kept by two triggers on postings (migration 0006):

```sql
-- BEFORE INSERT (ledger_prepare_posting): lock the account and work out the posting's balance and version.
SELECT balance, version INTO current_balance, current_version
  FROM accounts WHERE id = NEW.account_id AND currency = NEW.currency FOR UPDATE;
NEW.balance_after := current_balance + NEW.amount;
NEW.account_version := current_version + 1;

-- AFTER INSERT (ledger_apply_posting): write them to the account, only for a row that was really inserted.
UPDATE accounts SET balance = NEW.balance_after, version = NEW.account_version
 WHERE id = NEW.account_id AND version = NEW.account_version - 1;
```

Why two triggers and not one: Postgres runs `BEFORE` triggers first and only then decides whether to insert the
row. As first written (0002), the `BEFORE` trigger moved the balance itself. `INSERT ... ON CONFLICT DO NOTHING`
on a conflicting posting then skipped the row and kept the balance change: a balance moved with no posting. An
`AFTER` row trigger runs only for rows that were inserted. Both review agents found this on day 1.

- **The cache can't drift.** No code path can insert a posting without moving the balance, so reading a balance
  is one row, not a sum over the account's history.
- **Overdraft protection is atomic.** The `CHECK` on accounts runs on the `AFTER` trigger's `UPDATE`, under the
  row lock the `BEFORE` trigger took, so two concurrent payments can't both spend the same $10.
- **The history is a chain.** `account_version` numbers an account's postings 1, 2, 3, … with no gaps, and each
  posting's `balance_after` is the previous one plus its amount. The reconciliation job (day 3) checks the chain,
  and statements paginate by version.

The cost is a hot row: every entry that touches `revenue:fees` updates the same row, and those updates serialise.
That is a real limit for high-volume ledgers. Day 4 measures it under contention, and the known fixes are listed
under "Limitations".

An account appears at most once per entry (`UNIQUE (entry_id, account_id)`). The overdraft check runs per posting,
so two postings to one account (+100 then −50) could fail on an intermediate state that the entry as a whole never
reaches. Netting them into one posting is always possible.

## Money

- **Integers, never floats.** Amounts are `int64` in Go and `BIGINT` in Postgres, in minor units.
- **Currencies** live in a `currencies` table (code, exponent), seeded with a few ISO 4217 codes: USD, EUR, GBP,
  CAD, CHF and INR (exponent 2), JPY (0) and KWD (3). The Go registry in `internal/money` has the same list, and a
  test checks the two agree.
- **Exact parsing.** `"12.34"` in USD is 1234. `"12.345"` in USD is an error, not a rounding.
- **Bounds.** A posting is at most 10^15 minor units either way (ten trillion dollars), checked in Go and by a
  `CHECK`. With at most 64 postings, no sum in an entry can overflow `int64`: 64 × 10^15 is below 9.2 × 10^18.
  A constant in `internal/ledger` stops compiling if either bound changes enough to break that, so the entry
  validator can add with plain `+`. Sums that have no such bound (balances) use `money.Add`, which reports
  overflow, and Postgres sums `BIGINT` as `NUMERIC`. The bound also keeps every amount exact in a JSON number read
  by JavaScript (2^53 ≈ 9.007 × 10^15).
- A balance that would leave `BIGINT` makes the posting fail with `bigint out of range`; it never wraps.

## Schema

Three migrations, one per kind of rule:

- `0001_schema.sql`: the tables, and every rule one row can check by itself (types, bounds, foreign keys,
  uniqueness, the overdraft `CHECK`).
- `0002_balances.sql`: the database keeps the balances (I4, I5, I7): the posting trigger and the account guard.
- `0003_entries.sql`: the rules about a whole entry (I1, I2) checked at commit, and append-only history (I6).
- `0004_check_entries_once.sql` and `0005_undo_check_skip.sql`: an optimisation of the commit-time check, and its
  reversal once a review showed it unsound (see "An optimisation that was wrong" above). Applied migrations are
  never edited: their checksums are recorded wherever they ran, so a fix is a new migration.
- `0006_apply_after_insert.sql`: the balance is written by an `AFTER INSERT` trigger, so a row Postgres skips
  moves nothing.
- `0007_pin_search_path.sql`: every ledger function resolves names in its own schema (see "Limitations").

```
currencies       code PK, exponent
accounts         id PK, name UNIQUE, type, currency → currencies, allow_negative,
                 balance, version, created_at, UNIQUE (id, currency)
journal_entries  id PK, description, posting_count, created_at
postings         id PK, entry_id → journal_entries, (account_id, currency) → accounts (id, currency),
                 amount, balance_after, account_version,
                 UNIQUE (entry_id, account_id), UNIQUE (account_id, account_version)
```

Ids are `BIGINT GENERATED ALWAYS AS IDENTITY`: small, ordered, and easy to lock in a fixed order. This is an
internal service, so guessable ids are acceptable; a public API would put opaque ids in front.

Errors from the database carry a constraint name, so the Go code can map a rejection to an API error without
parsing messages. The triggers set one too (`RAISE ... USING CONSTRAINT = 'entry_balanced'`):

| Constraint | Raised when |
|---|---|
| `entry_balanced` | at commit, an entry's postings don't sum to zero in some currency |
| `entry_posting_count` | at commit, an entry doesn't have exactly the postings it declared |
| `accounts_no_overdraft` | a posting would take a protected account below zero on its normal side |
| `postings_account_currency_fkey` | the account doesn't exist, or isn't in the posting's currency |
| `postings_one_per_account` | an entry has two postings to one account |
| `postings_amount_range` | an amount is zero or beyond 10^15 |
| `append_only` | an `UPDATE`, `DELETE` or `TRUNCATE` on entries or postings |
| `balance_via_postings_only` | a balance or version is set directly, or an account is created with one |
| `account_identity_immutable` | an account's id, type, currency or creation time is changed |

**The guard's trigger-depth test.** The posting trigger updates the account row, which fires the account guard
again. The guard tells the two apart with `pg_trigger_depth()`: depth 2 means the update came from the posting
trigger, depth 1 means a client sent it. A role allowed to create triggers could get to depth 2 another way, but
such a role could also just disable the guard; the defence against that is the role setup under "Limitations".

## Migrations

Plain SQL files embedded in the binary (`internal/store/migrations/NNNN_name.sql`) and applied by a small runner:

- Everything runs in one transaction, under a transaction-level advisory lock keyed by the schema. Two instances
  starting at once apply the migrations once; the second waits, then finds nothing to do. Without the lock this
  fails in practice: `CREATE TABLE IF NOT EXISTS` isn't safe under concurrency, and in the runner's test 7 of 8
  concurrent runners failed on `pg_type_typname_nsp_index` when the lock was removed.
- The transaction runs at `READ COMMITTED` whatever the session's default. Under `REPEATABLE READ` or
  `SERIALIZABLE` its snapshot would be fixed by the lock statement, before the wait, and a runner that waited
  couldn't see what the first one applied (7 of 8 runners failed that way in the review's test).
- `schema_migrations` records each version with a SHA-256 of its file. If an applied migration's file changes
  later, startup fails instead of running a schema that doesn't match the code.
- Postgres DDL is transactional, so a failing migration leaves nothing behind.

Why not goose or golang-migrate: the runner is about a hundred lines, its behaviour is fully tested, and it has no
dependency. Its limit: a statement that can't run in a transaction (`CREATE INDEX CONCURRENTLY`) needs a different
path, which nothing here needs yet.

## Concurrency (days 2 and 3)

- **Day 2: serializable transactions with retries.** A transfer runs at `SERIALIZABLE`. On a serialization failure
  (`40001`) or a deadlock (`40P01`) the whole transaction is retried with backoff and jitter, a bounded number of
  times. Postings are inserted in account-id order, so the row locks are always taken in one order.
- **Day 3: row locking compared with optimistic concurrency.** The same transfer under `READ COMMITTED`, where
  concurrent updates of an account row wait for each other, against `SERIALIZABLE`, where one of them fails and
  retries. The comparison measures throughput and retries on a hot account and on spread-out accounts. A property
  test runs random concurrent transfers and checks that every currency still sums to zero and every account's
  chain is intact.

## API (day 2)

gRPC on port 9103 and REST on 8103, both thin layers over one service:

- `CreateAccount(name, type, currency, allow_negative)`
- `PostEntry(idempotency_key, description, postings[])`: atomic and serializable. Errors are distinct: unbalanced,
  unknown account, currency mismatch, insufficient funds, idempotency-key reuse with a different request.
- `GetAccount(id)`: the balance, both stored and as shown.
- `ListPostings(account_id, after_version, limit)`: the history, paginated by version.

**Idempotency keys.** The client sends a key with each `PostEntry`. The key, a hash of the canonical request and
the resulting entry id are stored in the same transaction as the entry. A retry with the same key and request
returns the original entry; the same key with a different request is rejected. Exactly-once effects under retries
and crashes is what day 4 tests by failure injection.

## Outbox and reconciliation (day 3)

- **Transactional outbox.** Each posted entry writes an `entry_posted` event row in the same transaction. A
  publisher reads unpublished rows in order (`FOR UPDATE SKIP LOCKED`), delivers them, and marks them sent. Events
  are delivered at least once, so consumers dedupe by event id.
- **Reconciliation job.** It recomputes everything from postings and compares it with the stored state: each
  balance against the sum of its postings and against the last `balance_after`, version gaps, every entry's sums,
  and each currency's total. It reports any difference; it never fixes data silently.

## Load and failure injection (day 4)

A Go load generator posts transfers over the API with configurable contention (how many accounts, how skewed the
choice is), and reports throughput and latency percentiles. Failure injection kills a request between commit and
response, then retries it with the same idempotency key, and checks that the effect happened exactly once.

## Testing

- **Unit and property tests** (`testing` and [rapid](https://pkg.go.dev/pgregory.net/rapid)) for money and the
  entry validator: random balanced entries validate, changing any amount breaks the balance, and whether an entry
  is valid never depends on the order of its postings, even with several defects. (Which rule is reported can:
  `Validate` names the first one it meets.)
- **Database tests** run against a real Postgres 14, each in a fresh schema of its own, so tests can run in
  parallel and never see each other's rows. They skip when `LEDGER_TEST_DATABASE_URL` is unset, and fail instead
  when `CI` is set, so CI can't pass by skipping.
- **A differential test** checks that the Go validator and the database, two implementations of the same rules,
  agree. It draws balanced entries over real accounts, then applies none or exactly one of nine defects. Both
  must give the same verdict for the same reason. Afterwards exactly the valid entries are committed, and
  `checkBooks` recomputes the ledger from its postings: every currency sums to zero, every balance and version
  matches its postings, and every account's history is an unbroken chain.
- **Tests that are checked against a broken build.** The concurrency and commit-time tests were each run once
  with the mechanism they test removed (the advisory lock, either deferred trigger), to see them fail.

## Limitations

- **Hot accounts.** Every posting to an account updates its row, so a busy account serialises its writers. The
  known fixes, not built here: split the account into N sub-accounts and sum them on read, or post to a hot
  account in batches.
- **Single database.** No sharding. Entries that span shards would need a distributed transaction.
- **The database owner can switch the triggers off.** The application should connect as a role that owns
  nothing. Because the trigger functions run with the caller's rights, that role currently needs `UPDATE` on
  accounts as well as `INSERT` and `SELECT` on entries and postings (the guard still stops it setting a balance
  directly). Making the trigger functions `SECURITY DEFINER` would remove the `UPDATE`; their pinned
  `search_path` (0007) is what makes that safe. Day 1 doesn't set up roles.
- **Pinned search paths.** Each ledger function resolves table names in the schema it was created in, with
  `pg_temp` last, so a session's temporary tables can't stand in for the ledger's (found in review on day 1). A
  new function must be pinned too; `TestEveryFunctionPinsItsSearchPath` fails until it is.
- **One entry per statement.** Two postings to the same account in one `INSERT` (from different entries) read
  the same version in their `BEFORE` triggers, and the second fails on `postings_account_version_unique`. It fails
  closed, and the service posts one entry at a time anyway.
- **No business time yet.** `created_at` is when the entry was recorded. A separate effective date (for
  backdated corrections) is a later addition.
- **Single-currency accounts.** A multi-currency customer has one account per currency.
