# Walkthrough

The questions an interviewer is likely to ask about Ledger, with short answers that point at the code. This file
grows each day; this version covers day 1 (the money type, the validator, the schema and its invariants, the
migration runner, the binary), day 2 (the API: REST and gRPC over one service, posting with retries, idempotency
keys) and day 3 (the isolation levels compared, the outbox, reconciliation). Read DESIGN.md first for the full
picture.

**Reading order:** `internal/money/money.go` → `internal/ledger/entry.go` → the migrations in
`internal/store/migrations/` (0001–0003 are the design, 0005–0007 the fixes from the review) →
`internal/store/migrate.go` → `internal/store/differential_test.go`. For day 2: `internal/store/ledger.go`
(`PostEntry` and `postOnce`) → `internal/ledger/posted.go` (`Entry.Hash`) → `internal/apierr/apierr.go` →
`internal/server/api.go` → `internal/grpcapi/grpcapi.go` → `internal/store/service_test.go` and
`internal/store/concurrency_test.go` → `cmd/ledgerd/main.go` (`serve`). For day 3: DESIGN.md "Concurrency" →
`internal/store/isolation_test.go` → `internal/store/predicate_locks_test.go` →
`internal/store/transfers_property_test.go` → `internal/store/migrations/0009_outbox.sql` →
`internal/outbox/outbox.go` (`PublishOnce`, `Run`) and its tests → `internal/store/reconcile.go` →
`internal/store/reconcile_test.go`.

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

## The API (day 2)

**Walk me through posting a payment.**
`Store.PostEntry` (`internal/store/ledger.go`) validates the key and the entry, hashes the entry, and runs an
attempt: look the key up (outside any transaction), then one `SERIALIZABLE` transaction that locks the entry's
accounts in id order with `SELECT ... ORDER BY id FOR UPDATE`, checks the rules that need the accounts (exists,
currency, overdraft, balance range), inserts the entry, its postings in one statement and the key, and commits; the
deferred checks run at the commit. An attempt aborted with `40001`, `40P01`, or `23505` on the key is retried with
backoff. DESIGN.md has it step by step.

**What does the idempotency key protect against, exactly?**
A client that doesn't know whether its request worked: a timeout, a dropped connection, a crash before it read the
answer. It retries with the same key. The key and the entry are written in one transaction, so the key exists if
and only if the entry does; the retry either finds the key and gets the original entry back, or finds nothing and
posts. Either way the money moves once. `TestARetriedRequestPostsOnce` covers the sequential case and
`TestConcurrentRetriesOfOneRequestPostOnce` the concurrent one.

**What if two retries with the same key arrive at the same moment?**
Both find the key unused. Both try to lock the same accounts; one gets the locks, posts and commits. The other was
waiting on a row the winner changed, and under `SERIALIZABLE` that is a serialization failure (`40001`), not a
wait-and-continue. It retries, now finds the key, and replays. Two different entries under one key, with no account
in common, collide on the key's primary key instead (`23505`); that is retried the same way and refused as a
different entry (`TestTwoEntriesRacingForOneKey`). The concurrent test holds twelve requests at a barrier (a test
hook after the lookup) until all have found the key unused, so the race really happens: one posts, eleven retry and
replay, and the test asserts at least eleven retries.

**Why hash the request at all? Isn't the key enough?**
The key says "this is the same operation"; the hash checks that it really is. A client bug that reuses a key for
a different payment would otherwise get the old payment's answer and believe the new one went through. With the
hash it gets `idempotency_key_reused`. The hash covers the description and the postings as a set, sorted by
account id (`Entry.Hash`): a retry that lists the postings in another order is the same entry, so it replays.

**Why doesn't a rejected request store its key?**
The rejection's transaction rolls back, and storing the failure would need a second transaction. A retry after
"insufficient funds" sees the accounts as they are now: if money arrived in between, it posts. The effect still
happens at most once per key. Stripe saves the first answer, failures included; that's a defensible alternative,
named in DESIGN.md.

**Why `SERIALIZABLE` if you lock the rows anyway?**
The locks give the order; `SERIALIZABLE` makes the transaction's reads safe. The key lookup and the account checks
are reads, and the decision to post depends on them. Under `READ COMMITTED` the locked rows would be re-read after
the wait, which works for the balances, but every read the code adds later would need the same care. Here,
anything Postgres can't serialize aborts with `40001` and is retried. The price is retries on a hot account
instead of waits: in one run, the eight-worker test logged 83 retries for 80 transfers among four wallets. Day 3
measures the two strategies against each other.

**How do you avoid deadlocks?**
Every attempt locks its accounts in ascending id order before touching them, in one statement, and `EXPLAIN`
shows the plan is `LockRows` over a `Sort` on id, so rows are locked as they come out sorted. Two entries that
share accounts wait in the same order and can't form a cycle. `TestAccountsAreLockedInIDOrder` checks it with
`NOWAIT` probes while an attempt waits: waiting for the lower account it holds nothing, waiting for the higher it
holds the lower. Another client can still deadlock with the service; `40P01` is retried
(`TestADeadlockIsRetried`, where the test's session sets a long `deadlock_timeout` so the attempt is the one
Postgres aborts).

**What does the service check, and what does the database check?**
Both check everything. The service checks first so the error can name the rule and the posting ("posting 0:
insufficient funds: account 2 (wallet:alice) has 30.00 USD, the posting takes 30.01"). Its checks run on the rows it
has locked, so they can't go stale before the insert. The database checks again with its constraints and triggers,
and if it ever rejects something the service let through, `translate` maps the constraint name to the same error.

**How do REST and gRPC stay consistent?**
Both take the `ledger.Service` interface and only translate. Errors go through one table, `internal/apierr`: a
stable reason, an HTTP status and a gRPC code per error. REST puts the reason in its JSON, gRPC in a
`google.rpc.ErrorInfo` (`INSUFFICIENT_FUNDS`), with the posting's index in both. In the hand check, a key used over
REST replayed over gRPC.

**Why are REST amounts strings?**
A JSON number is a double in JavaScript, exact only up to 2^53. A posting is capped at 10^15 minor units, but a
balance isn't. Strings in the major unit (`"19.40"`) are exact everywhere and readable, and `money.Parse` refuses
`"19.405"` in USD instead of rounding it. gRPC uses `int64` minor units; its JSON form (grpcurl) prints them as
strings for the same reason.

**Why is an unknown account a 422 and not a 404?**
404 is for the resource in the URL. `POST /v1/entries` naming a missing account would read as "no such endpoint".
`GET /v1/accounts/999` is a 404. In gRPC both are `NOT_FOUND`, with different reasons (`UNKNOWN_ACCOUNT`,
`NOT_FOUND`).

**What happens when the retries run out?**
`ErrConflict`: HTTP 409, gRPC `ABORTED`, reason `conflict`. The key wasn't stored, so the client can retry with the
same key safely (`TestRetriesRunOut`). The policy is 10 attempts, waits uniform in [0, min(100 ms, 1 ms ×
2^(n−1))]: "full jitter", so colliding transactions spread out instead of colliding again in step.

**How do you test a serialization failure deterministically?**
A test hook (`export_test.go`) runs code at named points of an attempt. At "begin", `TestASerializationFailureIsRetried`
has another transaction post from the same wallet and hold its row, and commit as soon as the attempt waits for
that row (it watches `pg_blocking_pids`). The attempt's snapshot predates the commit, so the row it gets is one
changed since: `40001`. The second attempt posts against the new balance: exactly one retry, and Alice's posting
lands at version 3 with the right balance.

**Why is account creation idempotent by name, and not by key?**
Names are already unique, so the name is a natural key: a retry finds its account. `INSERT ... ON CONFLICT DO
NOTHING RETURNING` then a `SELECT` by name. The same attributes give the existing account (HTTP 200 instead of
201); different ones are `account_name_taken`.

**What is migration 0008's CHECK on balances for?**
The API shows a credit-normal account's balance negated. Every `bigint` can be negated except -2^63, so
`accounts_balance_negatable` keeps balances above it and the shown balance always fits in an `int64`. Reaching it
takes over 9,000 maximum postings, so `TestABalanceStopsShortOfTheSmallestBigint` sets the balance with the
triggers off (`session_replication_role = replica`) and shows the CHECK still refuses it: constraints aren't
triggers.

**How is the gRPC code generated without protoc?**
The generators are Go programs: buf, protoc-gen-go and protoc-gen-go-grpc are pinned in a separate module,
`tools/go.mod`, and run with `go tool -modfile=tools/go.mod`. A separate module keeps buf's many dependencies out
of the service's `go.mod`. `make proto` lints and regenerates; the output is committed.

**How does ledgerd shut down?**
Both listeners open before either serves, so a taken port fails startup instead of leaving half a service
(`TestServeFailsIfAPortIsTaken`). On SIGTERM, `http.Server.Shutdown` and gRPC's `GracefulStop` run together with a
10 s grace; then `Stop` cuts off whatever is still running.

**Tell me about a bug in your design that a review found (day 2).**
The idempotency-key lookup was the first statement of the `SERIALIZABLE` transaction. A read that finds nothing
takes a predicate lock on where the key would be (the index page, or the whole table while it is small), so every
concurrent request that wrote a key there looked like a conflict: two transfers with nothing in common made each other retry, and under load some
ran out of attempts. With 16 workers each on its own two wallets, three runs gave 215–324 retries and 1–2 requests
gave up. A replay needs no transaction (a key commits with its entry, and history is append-only), so the lookup
moved before the transaction: 68–142 retries, none gave up. `TestUnrelatedEntriesDontMakeEachOtherRetry` holds one
entry at its locks while an unrelated one posts, and fails with the lookup back inside. The lesson: under
`SERIALIZABLE`, every read is a promise Postgres has to keep, so read only what the decision needs.

**What happens to a request still running when the server shuts down?**
It gets the grace period (`LEDGER_SHUTDOWN_GRACE`, 10 s). After that, every REST request's context, which derives
from one base context, is cancelled and the connections closed, and gRPC's `Stop` cancels the calls; cancelled
database work rolls back, so nothing half-posts, and the client retries with its key. Before the review, REST
requests ran on and the pool's `Close` waited for them. `TestShutdownCutsOffWorkStillRunningAfterTheGrace` holds the
accounts' rows: run returns within 5 s of a 500 ms grace, and nothing posts when the lock goes.

**What does the REST layer refuse that `encoding/json` would accept?**
Invalid UTF-8 (the package turns each bad byte into U+FFFD, so two different names became one account), a key given
twice (the last wins), and field names in another case (it matches case-insensitively, even with
`DisallowUnknownFields`). A token pre-pass (`strictKeys`) refuses the last two; `utf8.Valid` the first. A body must
also be `application/json`, which a cross-site form can't send, and at most 64 KB (413 above).

**How does paging behave while new postings arrive?**
History pages by account version, not by offset: a page is "the next N postings after version v", and versions are
dense and never change. A posting that arrives between two pages gets a higher version, so the client sees it on a
later page, once; nothing shifts the way it would under `OFFSET`. `TestPagingIsStableWhilePostingsArrive` posts
between pages and sees versions 1 to 7 once each, in order. The query is one statement (a `LEFT JOIN LATERAL` from
the account), so "no such account" and "no postings yet" come from the same snapshot.

## Concurrency, the outbox and reconciliation (day 3)

**The service posts at `READ COMMITTED`. Isn't that unsafe for a ledger?**
Not for this code path, because every decision it makes reads either a row it has locked or a row it wrote. The
accounts are locked `FOR UPDATE` in id order before any check; at `READ COMMITTED` a `FOR UPDATE` that waited for
another transaction hands back the row as that transaction committed it, so the overdraft check sees the real
balance. The posting trigger computes `balance_after` from the row it locked. The commit-time check reads only the
entry's own postings. The idempotency key is backed by its primary key. `TestAWaitingAttemptDecidesOnTheCommittedBalance`
changes Alice's balance while an attempt waits for her row and gets the right answer at both levels; the
concurrency tests and both property tests run at both levels. What `READ COMMITTED` gives up is protection for a
future check that reads rows without locking them: that check would have to lock them, or be a constraint.

**So why did you start with `SERIALIZABLE`, and why change?**
It is the safe default when you haven't proved anything about the code. Day 3 measured it: on a ledger of 20,000
accounts, `READ COMMITTED` posted 1.2 to 1.7 times as many transfers a second with no retries, while `SERIALIZABLE`
retried 0.55 to 0.80 times a transfer and gave up on 2 to 3% of the transfers to one hot account. A `conflict`
is a payment a customer sees fail. With the safety argument above and the tests at both levels, the measurement
decided it (`ec83ff1`). The level stays a setting (`LEDGER_ISOLATION`).

**Both modes take `FOR UPDATE`. Is this really row locking against optimistic concurrency?**
Partly, and it's worth saying so. Postgres's `SERIALIZABLE` is optimistic (serializable snapshot isolation: run
freely, abort one transaction when a dangerous pattern of reads and writes appears), but the ledger keeps its row
locks in both modes, and couldn't drop them: the posting trigger locks the account to compute `balance_after`
whatever the level. So at `SERIALIZABLE` two transfers on one account still queue on the row, and the waiter then
fails with `40001` instead of carrying on: it pays for both the wait and the retry. What the comparison measures is
whether serializable's extra checking is worth its aborts, on a design whose rules are already enforced by locks
and constraints. A truly lock-free scheme (read a version, write only if it is unchanged, retry otherwise) would
need the balance written by the application rather than a trigger, which day 1 rejected because the database
would no longer guarantee the balance.

**Why would `SERIALIZABLE` abort two transfers that share no account?**
Because of what they read. Postgres's serializable mode records every read (SIRead locks), and of two concurrent
transactions that each wrote something the other had read, it aborts one. `TestWhatASerializablePostingHasRead`
lists them: on small tables a posting has read the whole of `journal_entries` and `postings` (sequential scans in
the commit-time check), and every other posting inserts into those tables, so any two concurrent postings form a
cycle. On a padded ledger the reads are index pages, but every new entry lands on the same last leaf page of
`journal_entries_pkey` and `postings_one_per_account`, so they still collide. The reads are of the entry's own
rows, which nobody else can change, but Postgres can't know that. Measured: 3.3 to 3.8% of transfers that shared
nothing gave up on small tables.

**How do you test concurrency without flaky races?**
Three ways. Deterministic schedules: a test transaction holds a row and commits only once the attempt is seen
waiting for it (`pg_blocking_pids`), which gives a real `40001`, or a real wait-then-proceed at `READ COMMITTED`.
Property tests that don't depend on the schedule: `TestPropertyConcurrentTransfersMatchAModel` sends random batches
at once and checks the result against a model in which only the set of accepted entries matters (addition
commutes), plus the version chain each account's returned postings must form. And the reconciliation run at the
end of the concurrency tests (`checkBooks`).

**Explain the outbox. Why a trigger and not the service?**
Every journal entry needs an event for downstream systems. Publishing to a broker after the commit loses events if
the process dies in between; before the commit announces entries that roll back. So the event is a row written in
the entry's own transaction, by an `AFTER INSERT` trigger on `journal_entries` (migration 0009), and a publisher
delivers it later. A trigger, so an entry written any way (a `psql` fix, a batch job) gets its event too, which
fits the rest of the design: the database enforces what must always be true. The row holds only the entry id;
entries and postings are append-only, so the publisher builds the event at delivery and it is always the same.

**How does the publisher avoid losing or duplicating events?**
It can't avoid duplicates, so it doesn't lose anything: claim a batch (`FOR UPDATE SKIP LOCKED`), deliver it, mark
it published, commit, all in one transaction. A delivery failure leaves the events unpublished (with `attempts`
and `last_error`). If the delivery succeeded but the mark didn't commit, the events go out again: at least once.
Consumers dedupe by event id. `TestEventsWhoseMarkIsLostAreDeliveredAgain` cancels the context the moment the sink
returns, as a crash would, and sees each event twice. Exactly-once would need the sink in the same transaction.

**Are events in order?**
Per account, with one publisher, for entries the service posted. The service locks an entry's accounts before it
inserts the entry, so the event of an account's next posting is created only after the previous posting's
transaction committed; a publisher that can see the later event can see the earlier one. Across accounts there
is no order (ids are assigned at insert, not commit), and two publishers can each hold a batch and deliver an
account's events out of order. Consumers that care use `account_version`. `TestOnePublisherKeepsEachAccountsOrder`
runs six concurrent posters at both levels and checks every account's versions arrive 1, 2, 3, ... with none
missing.

**Why `SKIP LOCKED`, and why poll instead of `LISTEN/NOTIFY`?**
`SKIP LOCKED` lets several publishers run without waiting for each other's batches;
`TestTwoPublishersShareTheWork` fails within 5 s without it, because the second publisher would block. Polling:
`NOTIFY` in every posting's transaction would wake the publisher instantly, but Postgres serialises the commits of
notifying transactions on a single lock, which would put every posting in one queue. A 200 ms poll on a partial
index of the unpublished rows costs almost nothing.

**What does the reconciliation check, and why one snapshot?**
Eight things, recomputed from the postings: each currency sums to zero; each balance and version match the sum and
count of the account's postings; each balance matches the last `balance_after`; each history is a chain; each
entry balances per currency and has the postings it declares; each entry has its event; no protected account is
overdrawn. One `REPEATABLE READ` read-only transaction, so an entry posted mid-run is in all of the report or none.
`TestReconcilingWhilePostingSeesOneMoment` takes reports while four workers post; at `READ COMMITTED` one report
said 13 entries, 28 postings and 15 events, which is no state the ledger was ever in.

**How do you test a check for corruption the database won't let you create?**
As the tables' owner, inside the test's own schema: switch a guard trigger off, or drop a constraint, make the
damage, and switch it back. `TestEachKindOfDamageIsReported` does six kinds (a balance moved by hand, a posting
deleted, an amount changed, a `balance_after` rewritten, an event deleted, an overdraft let through) and requires
exactly the checks that compare what was broken to report it, and no others. That also shows why the application
should connect as a role that owns nothing (DESIGN.md, "Limitations").

**Why doesn't the reconciliation fix what it finds?**
Because a difference is evidence. A balance that doesn't match its postings could be a lost posting or a hand-made
balance change, and the fix is different for each. `ledgerd reconcile` exits 3 so monitoring can page someone, and
the report says what and where.

**Tell me about something that went wrong on day 3.**
Two things. Running every concurrency test at both levels doubled the parallel tests, and eight tests with 16
connections each went past Postgres's 100-connection limit: "too many clients already". CI's Postgres has the same
limit, so test pools now open 8 connections, and only the two measurements that need one per worker ask for 16.
And one commit went in with a failing test, because my command ran the commit after `make check` regardless of its
result: the outbox-order test used the default retry budget at `SERIALIZABLE`, and under the full suite's load some
transfers gave up. It wasn't pushed; I fixed the test (it tests order, so it gets a generous budget, and runs at both
levels) and amended the commit, and every commit after that ran only if `make check` passed.
