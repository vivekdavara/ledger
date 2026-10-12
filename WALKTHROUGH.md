# Walkthrough

The questions an interviewer is likely to ask about Ledger, with short answers that point at the code. This file
grows each day; this version covers day 1 (the money type, the validator, the schema and its invariants, the
migration runner, the binary). Read DESIGN.md first for the full picture.

**Reading order:** `internal/money/money.go` → `internal/ledger/entry.go` → the three migrations in
`internal/store/migrations/` → `internal/store/migrate.go` → `internal/store/differential_test.go`.

## The model

**What is double-entry, and why would a payments company use it?**
Every movement of money is a journal entry whose postings sum to zero: money leaves one account and arrives in
another, so it is never created or destroyed by a bug. If every entry balances, all the balances in a currency
sum to zero, which turns "did we lose money?" into a query (`checkBooks` in `internal/store/differential_test.go`
runs it). It also keeps the history: a balance is explained by its postings.

**Why one signed amount column instead of debit and credit columns?**
With debits positive and credits negative, the rule is a plain `SUM(amount) = 0` per currency, and a balance is a
plain sum. The price is that a credit-normal account, such as a customer wallet (a liability: money the platform
owes), has a negative stored balance. The shown balance flips the sign for liability, equity and revenue
accounts (DESIGN.md, "Sign convention").

**How does a currency conversion balance if USD and EUR can't be added?**
The rule is per currency. A conversion is one entry with a USD pair and an EUR pair, each summing to zero,
through an FX account per currency (DESIGN.md, "Examples"). `TestValidateRejects` has the case that sums to zero
in total but not per currency: rejected.

**How do you fix a mistake?**
Post a reversing entry: every amount negated (`Entry.Reversed`). History is never edited, so the books show the
mistake and its correction. `TestAReversalRestoresEveryBalance` checks that a reversal restores every balance while
all the postings stay.

## Money

**Why integers and not floats or decimals?**
Floats can't represent 0.10 exactly. Amounts are `int64` counts of the currency's minor unit (cents, yen, fils),
and each currency has an exponent (USD 2, JPY 0, KWD 3). `Currency.Parse` converts `"12.34"` to 1234 exactly and
rejects `"12.345"` in USD rather than round it. Postgres `NUMERIC` would also be exact, but an integer is smaller,
faster, and what payment APIs such as Stripe's use.

**Can a sum overflow?**
Not within an entry: a posting is at most 10^15 minor units and an entry has at most 64 postings, so every partial
sum is under 6.4 × 10^16, far below `int64`'s 9.2 × 10^18. The constant `_ = uint64(math.MaxInt64 -
MaxPostings*money.MaxAmount)` in `entry.go` stops compiling if either bound grows too far, so the validator can use
plain `+`. Where there is no bound (balances), `money.Add` reports overflow; Go's `+` wraps silently, and four
postings of 2^62 would "sum" to zero (`TestAddCatchesTheWrapToZero`). In the database a balance past `bigint` is an
error, never a wrap (`TestABalanceThatWouldOverflowFailsInsteadOfWrapping`).

**Why 10^15 and not the full int64 range?**
It makes the overflow argument a one-liner, it is ten trillion dollars, and it keeps every amount exact in a JSON
number read by JavaScript, whose doubles are exact only up to 2^53 ≈ 9.007 × 10^15.

## The invariants

**Where is "postings sum to zero" enforced?**
Twice. `ledger.Entry.Validate` checks it first and says which rule broke and which posting. The database checks
it again at commit (`0003_entries.sql`, `0004_check_entries_once.sql`), so a bug in the service, a second writer,
or someone in `psql` can't commit an unbalanced entry. The differential test checks that the two implementations
agree.

**Why a deferred constraint trigger and not a CHECK?**
A `CHECK` sees one row; this rule is about a group of rows, and it only holds once all of an entry's postings
exist. `CREATE CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED` runs it at commit.

**Why two triggers, and what is `posting_count` for?**
The entry trigger catches an entry with no postings at all (no posting trigger would fire). The posting trigger
catches postings added later to an entry committed earlier. Adding a balanced pair keeps the sum at zero, so the
sum alone can't catch that. The entry declares its `posting_count` when it is created, entries are immutable, and
the count no longer matches. With either trigger removed, a test fails: `TestPostingCountMustMatch` (and, for the
entry trigger, `TestChecksThatRunEarlyFailClosed`).

**Doesn't checking after every posting make big entries slow?**
Yes, quadratically: an entry of N postings is checked N + 1 times at commit, each time reading all N. Migration
0004 tried to check once per entry. It stamped each entry with its transaction and skipped a posting's check when
the entry was the current transaction's own, since the entry's own check would run at commit. A review agent
showed the reasoning wrong: `SET CONSTRAINTS ALL IMMEDIATE` runs the entry's check early, and once run it is used
up, so a posting appended afterwards was never checked, and an unbalanced entry committed. Migration 0005 undid
it (`TestAnEarlyCheckDoesNotExcuseLaterPostings`). The lesson: an optimisation that skips a safety check needs a
proof that holds whatever order the client makes things happen in. The measured cost is in the README's
"Results".

**What if a client runs `SET CONSTRAINTS ALL IMMEDIATE`?**
The check then runs right after the entry insert, before the postings exist, and the insert fails
(`TestChecksThatRunEarlyFailClosed`). Every check fails closed: getting around the expected order rejects the
entry, it never lets one through.

**Why does the database keep the balance instead of the service?**
A `BEFORE INSERT` trigger on postings locks the account row and works out the new balance and version; an `AFTER
INSERT` trigger writes them to the account (`0006_apply_after_insert.sql`). No code path can insert a posting without moving the balance, so the cache
can't drift; each account's postings form a gapless chain (version 1, 2, 3 …, each `balance_after` the previous
plus the amount). The client can't choose those values either
(`TestTheClientCannotChooseBalanceAfterOrVersion`).

**How is overdraft protection safe under concurrency?**
The overdraft rule is a `CHECK` on the account row, and Postgres evaluates it inside the trigger's `UPDATE`, which
holds the row lock. Two payments against the same $10 serialise on that lock, and the second sees the first's
result. `TestConcurrentSpendsCannotOverdraw` races ten $5 payments against $30: always six succeed and four are
refused, checked 20 runs in a row.

**What's the cost of keeping balances that way?**
A hot row. Every entry that touches `revenue:fees` updates the same row, so those transactions take turns. Day 4
measures it under contention; the known fixes (sub-accounts summed on read, batching) are in DESIGN.md,
"Limitations".

**Why can an account appear only once per entry?**
The overdraft check runs per posting. Two postings to one account (+100, then −50) could fail on an intermediate
balance that the entry as a whole never reaches. Netting them into one posting is always possible, so the schema
forbids it (`postings_one_per_account`).

**How do you stop anyone from changing history or setting a balance by hand?**
Triggers reject `UPDATE`, `DELETE` and `TRUNCATE` on entries and postings (`append_only`). An account guard rejects
a new account with a balance and any direct change to a balance or version (`balance_via_postings_only`), plus
changes to id, type, currency or creation time (`account_identity_immutable`). The guard tells the posting
trigger's update from a client's by `pg_trigger_depth()`: 2 when it comes from the posting trigger, 1 when a
client sends it.

**What can't the database stop?**
The table owner can disable triggers. In production the service should connect as a role that owns nothing and
has only `INSERT` and `SELECT` on these tables; day 1 doesn't set that up (DESIGN.md, "Limitations").

**Tell me about a bug you found in your own design.**
On day 1 the balance moved in the `BEFORE INSERT` trigger. Postgres runs `BEFORE` triggers first and only then
decides whether to insert the row, so `INSERT ... ON CONFLICT DO NOTHING` on a posting that conflicted returned
`INSERT 0 0`, no error, and left the account's balance moved with no posting behind it: money from nothing, and no
commit-time check either, since no row existed to trigger one. Two independent review agents found it. The fix
was to split the work (0006): the `BEFORE` trigger only locks and computes, and the `AFTER` trigger, which runs
only for inserted rows, writes the balance. `TestASkippedPostingMovesNoBalance` tries three `ON CONFLICT` forms and
a forced primary-key conflict.

**Why does every function pin its `search_path`?**
The trigger functions name tables without a schema, so they used to resolve them through the caller's
`search_path`, and `pg_temp` comes first there. Any role may create a temporary table by default, and one named
`accounts` took the triggers' reads and writes: postings committed while the real balance stayed at zero
(`TestATempTableCannotStandInForTheLedgersTables`). Migration 0007 pins each function to its own schema with
`pg_temp` last, and `TestEveryFunctionPinsItsSearchPath` fails for any new function that isn't pinned.

## Migrations and tests

**How do migrations work, and why the advisory lock?**
Plain SQL files embedded in the binary, applied in one transaction under a transaction-level advisory lock keyed by
the schema, each recorded with a SHA-256 of its file (`migrate.go`). The lock matters because `CREATE TABLE IF NOT
EXISTS` is not safe when two processes run it at once: with the lock removed, 7 of 8 concurrent runners in
`TestMigrateConcurrentRunnersApplyOnce` failed on `pg_type_typname_nsp_index`. The checksum stops a binary from
running against a schema built from a different version of a file. The transaction is always `READ COMMITTED`:
under a `SERIALIZABLE` default, the lock statement would fix the snapshot before the wait, and a runner that waited
couldn't see the first runner's work (a review found this; the race test now runs at all three levels).

**How are the database tests isolated?**
Each test gets a fresh schema (`storetest.EmptySchema`) and a pool whose `search_path` is only that schema, so
tests run in parallel and never see each other's rows; the schema is dropped afterwards. Without
`LEDGER_TEST_DATABASE_URL` they skip, but in CI (where `CI` is set) they fail, so CI can't pass by skipping.

**What is the differential test?**
`TestPropertyDatabaseAgreesWithTheValidator` draws balanced entries over real accounts, applies none or exactly one
of nine defects (unbalanced, zero or out-of-range amount, unknown currency, duplicate account, one posting, long
description, NUL, invalid UTF-8), and requires both checkers to give the same verdict for the same reason. Then
exactly the valid entries must be in the database, and the books must still balance.

**Why are `/livez` and `/readyz` separate?**
Liveness answers "should this process be restarted?" and checks nothing, so a database outage doesn't make an
orchestrator restart every instance. Readiness answers "should this instance get traffic?": the database must
answer within 2 s and the schema must be at the binary's newest migration (`store.Ready`).
