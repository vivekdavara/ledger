package store_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// The accounts of DESIGN.md's examples.
type book struct {
	cash, alice, bob, fees int64
}

func newBook(t *testing.T, db *pgxpool.Pool) book {
	t.Helper()
	return book{
		cash:  createAccount(t, db, account{"cash:bank", "asset", "USD", false}),
		alice: createAccount(t, db, account{"wallet:alice", "liability", "USD", false}),
		bob:   createAccount(t, db, account{"wallet:bob", "liability", "USD", false}),
		fees:  createAccount(t, db, account{"revenue:fees", "revenue", "USD", false}),
	}
}

func TestPostingsMoveBalancesAndChainVersions(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "Alice tops up $50", leg{b.cash, "USD", 5000}, leg{b.alice, "USD", -5000})
	mustPost(t, db, "Alice pays Bob $20, 3% fee",
		leg{b.alice, "USD", 2000}, leg{b.bob, "USD", -1940}, leg{b.fees, "USD", -60})

	want := map[int64][2]int64{b.cash: {5000, 1}, b.alice: {-3000, 2}, b.bob: {-1940, 1}, b.fees: {-60, 1}}
	for id, w := range want {
		if bal, ver := balanceOf(t, db, id); bal != w[0] || ver != w[1] {
			t.Errorf("account %d: balance %d version %d, want %d and %d", id, bal, ver, w[0], w[1])
		}
	}
	// Alice's history is a chain: each posting's balance_after is the one before plus its amount.
	rows, err := db.Query(context.Background(),
		`SELECT amount, balance_after, account_version FROM postings WHERE account_id = $1 ORDER BY account_version`,
		b.alice)
	if err != nil {
		t.Fatal(err)
	}
	var history [][3]int64
	var amount, after, version int64
	if _, err := pgx.ForEachRow(rows, []any{&amount, &after, &version}, func() error {
		history = append(history, [3]int64{amount, after, version})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0] != [3]int64{-5000, -5000, 1} || history[1] != [3]int64{2000, -3000, 2} {
		t.Fatalf("alice's history is %v", history)
	}
	var total int64
	if err := db.QueryRow(context.Background(), `SELECT sum(balance) FROM accounts`).Scan(&total); err != nil || total != 0 {
		t.Fatalf("balances sum to %d (%v), want 0", total, err)
	}
}

func TestTheClientCannotChooseBalanceAfterOrVersion(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	entry, err := insertEntry(tx, "forged", 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []leg{{b.cash, "USD", 700}, {b.alice, "USD", -700}} {
		if _, err := tx.Exec(ctx, `INSERT INTO postings (entry_id, account_id, currency, amount, balance_after,
			account_version) VALUES ($1, $2, $3, $4, 123456789, 42)`, entry, l.account, l.currency, l.amount); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var after, version int64
	if err := db.QueryRow(ctx, `SELECT balance_after, account_version FROM postings WHERE account_id = $1`, b.cash).
		Scan(&after, &version); err != nil {
		t.Fatal(err)
	}
	if after != 700 || version != 1 {
		t.Fatalf("stored balance_after %d, version %d; want the real 700 and 1", after, version)
	}
}

func TestOverdraftIsRejected(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "top-up", leg{b.cash, "USD", 3000}, leg{b.alice, "USD", -3000})

	// Alice has $30.00. $30.01 is one cent too many.
	_, err := post(db, "too much", leg{b.alice, "USD", 3001}, leg{b.bob, "USD", -3001})
	if got := constraint(err); got != "accounts_no_overdraft" {
		t.Fatalf("got %v (constraint %q), want accounts_no_overdraft", err, got)
	}
	if bal, ver := balanceOf(t, db, b.alice); bal != -3000 || ver != 1 {
		t.Fatalf("the rejected entry changed alice: balance %d, version %d", bal, ver)
	}
	// An asset is protected the same way on its own (debit) side: the bank can't pay out money it doesn't hold.
	_, err = post(db, "payout", leg{b.bob, "USD", 1}, leg{b.cash, "USD", -3001})
	if got := constraint(err); got != "accounts_no_overdraft" {
		t.Fatalf("asset overdraft: got %v (constraint %q)", err, got)
	}
	// Exactly everything she has is fine, and leaves her at zero.
	mustPost(t, db, "all of it", leg{b.alice, "USD", 3000}, leg{b.bob, "USD", -3000})
	if bal, _ := balanceOf(t, db, b.alice); bal != 0 {
		t.Fatalf("alice has %d, want 0", bal)
	}
	// An account that allows a negative balance may go below zero.
	overdraft := createAccount(t, db, account{"wallet:carol", "liability", "USD", true})
	mustPost(t, db, "carol's overdraft", leg{overdraft, "USD", 500}, leg{b.bob, "USD", -500})
}

func TestConcurrentSpendsCannotOverdraw(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "top-up", leg{b.cash, "USD", 3000}, leg{b.alice, "USD", -3000})

	// Ten $5 payments race for Alice's $30: exactly six can succeed, whatever the interleaving.
	const payments = 10
	errs := make([]error, payments)
	var wg sync.WaitGroup
	for i := range payments {
		wg.Go(func() { _, errs[i] = post(db, "coffee", leg{b.alice, "USD", 500}, leg{b.bob, "USD", -500}) })
	}
	wg.Wait()
	ok, refused := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case constraint(err) == "accounts_no_overdraft":
			refused++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}
	if ok != 6 || refused != 4 {
		t.Fatalf("%d payments succeeded and %d were refused, want 6 and 4", ok, refused)
	}
	if bal, ver := balanceOf(t, db, b.alice); bal != 0 || ver != 7 {
		t.Fatalf("alice ends at balance %d, version %d; want 0 and 7", bal, ver)
	}
}

func TestBalanceChangesOnlyThroughPostings(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "top-up", leg{b.cash, "USD", 3000}, leg{b.alice, "USD", -3000})
	cases := []struct {
		name, sql  string
		constraint string
	}{
		{"set a balance", `UPDATE accounts SET balance = -1000000 WHERE id = $1`, "balance_via_postings_only"},
		{"set a version", `UPDATE accounts SET version = 9 WHERE id = $1`, "balance_via_postings_only"},
		{"change the type", `UPDATE accounts SET type = 'asset' WHERE id = $1`, "account_identity_immutable"},
		{"change the currency", `UPDATE accounts SET currency = 'EUR' WHERE id = $1`, "account_identity_immutable"},
		{"change the creation time", `UPDATE accounts SET created_at = now() - interval '1 day' WHERE id = $1`,
			"account_identity_immutable"},
	}
	for _, tc := range cases {
		err := rolledBack(t, db, func(tx pgx.Tx) error {
			_, err := tx.Exec(context.Background(), tc.sql, b.alice)
			return err
		})
		if got := constraint(err); got != tc.constraint {
			t.Errorf("%s: got %v (constraint %q), want %q", tc.name, err, got, tc.constraint)
		}
	}
	// A new account can't start with money.
	err := rolledBack(t, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(context.Background(),
			`INSERT INTO accounts (name, type, currency, balance) VALUES ('mint', 'asset', 'USD', 1000000)`)
		return err
	})
	if got := constraint(err); got != "balance_via_postings_only" {
		t.Errorf("an account created with money: got %v (constraint %q)", err, got)
	}
	// Renaming, and switching overdraft protection, are ordinary updates.
	if _, err := db.Exec(context.Background(), `UPDATE accounts SET name = 'wallet:alice:main', allow_negative = true
		WHERE id = $1`, b.alice); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if bal, ver := balanceOf(t, db, b.alice); bal != -3000 || ver != 1 {
		t.Fatalf("a rename changed the balance: %d, version %d", bal, ver)
	}
}

func TestProtectingAnOverdrawnAccountIsRejected(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	carol := createAccount(t, db, account{"wallet:carol", "liability", "USD", true})
	mustPost(t, db, "carol's overdraft", leg{carol, "USD", 500}, leg{b.bob, "USD", -500})
	_, err := db.Exec(context.Background(), `UPDATE accounts SET allow_negative = false WHERE id = $1`, carol)
	if got := constraint(err); got != "accounts_no_overdraft" {
		t.Fatalf("got %v (constraint %q), want accounts_no_overdraft", err, got)
	}
}

func TestABalanceThatWouldOverflowFailsInsteadOfWrapping(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	ctx := context.Background()
	big := createAccount(t, db, account{"cash:huge", "asset", "USD", true})
	other := createAccount(t, db, account{"cash:other", "asset", "USD", true})
	// The guard rightly forbids setting a balance, so switch it off for this one setup statement. The test schema
	// belongs to the test's role, which may do that; an application role shouldn't (DESIGN.md, "Limitations").
	for _, sql := range []string{
		`ALTER TABLE accounts DISABLE TRIGGER accounts_guard`,
		`UPDATE accounts SET balance = 9223372036854775800 WHERE name = 'cash:huge'`,
		`ALTER TABLE accounts ENABLE TRIGGER accounts_guard`,
	} {
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	_, err := post(db, "past the top", leg{big, "USD", 10}, leg{other, "USD", -10})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "22003" {
		t.Fatalf("got %v, want SQLSTATE 22003 (numeric value out of range)", err)
	}
	if bal, _ := balanceOf(t, db, big); bal != 9223372036854775800 {
		t.Fatalf("balance is %d after the failed posting; it should be unchanged", bal)
	}
}

// Found in review: when the balance moved in a BEFORE trigger, Postgres could still skip the row afterwards
// (ON CONFLICT DO NOTHING, or DO UPDATE whose WHERE is false), and the balance stayed moved with no posting.
func TestASkippedPostingMovesNoBalance(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	entry := mustPost(t, db, "top-up", leg{b.cash, "USD", 5000}, leg{b.alice, "USD", -5000})
	var postingID int64
	if err := db.QueryRow(context.Background(), `SELECT id FROM postings WHERE account_id = $1`, b.cash).
		Scan(&postingID); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		 VALUES ($1, $2, 'USD', 700, 0, 1) ON CONFLICT DO NOTHING`,
		`INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		 VALUES ($1, $2, 'USD', 700, 0, 1) ON CONFLICT (entry_id, account_id) DO NOTHING`,
		`INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		 VALUES ($1, $2, 'USD', 700, 0, 1)
		 ON CONFLICT (entry_id, account_id) DO UPDATE SET amount = EXCLUDED.amount WHERE false`,
	} {
		tag, err := db.Exec(context.Background(), sql, entry, b.cash)
		if err != nil || tag.RowsAffected() != 0 {
			t.Fatalf("%s: %v, %d rows; want a silently skipped row", sql, err, tag.RowsAffected())
		}
	}
	// A conflict on the primary key (the id forced), on an account the entry doesn't use.
	spare := createAccount(t, db, account{"cash:spare", "asset", "USD", true})
	tag, err := db.Exec(context.Background(), `INSERT INTO postings
		(id, entry_id, account_id, currency, amount, balance_after, account_version) OVERRIDING SYSTEM VALUE
		VALUES ($1, $2, $3, 'USD', 700, 0, 1) ON CONFLICT DO NOTHING`, postingID, entry, spare)
	if err != nil || tag.RowsAffected() != 0 {
		t.Fatalf("primary-key conflict: %v, %d rows", err, tag.RowsAffected())
	}
	for id, want := range map[int64][2]int64{b.cash: {5000, 1}, spare: {0, 0}} {
		if bal, ver := balanceOf(t, db, id); bal != want[0] || ver != want[1] {
			t.Errorf("account %d: balance %d version %d after skipped inserts, want %d and %d", id, bal, ver,
				want[0], want[1])
		}
	}
	checkBooks(t, db)
}

// Found in review: the trigger functions resolved their unqualified table names through the caller's search_path,
// where a temporary table (pg_temp is searched first) could stand in for accounts.
func TestATempTableCannotStandInForTheLedgersTables(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	ctx := context.Background()
	conn, err := db.Acquire(ctx) // a temporary table belongs to one session, so use one connection throughout
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	schema := schemaOf(t, db)
	for _, sql := range []string{
		`CREATE TEMP TABLE accounts (id bigint, currency text, balance bigint, version bigint)`,
		`INSERT INTO pg_temp.accounts SELECT id, currency, 0, 0 FROM ` + schema + `.accounts`,
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	defer func() { _, _ = conn.Exec(ctx, `DROP TABLE pg_temp.accounts`) }()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := insertEntry(tx, "top-up", 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []leg{{b.cash, "USD", 900}, {b.alice, "USD", -900}} {
		if _, err := tx.Exec(ctx, `INSERT INTO `+schema+`.postings
			(entry_id, account_id, currency, amount, balance_after, account_version) VALUES ($1, $2, $3, $4, 0, 1)`,
			entry, l.account, l.currency, l.amount); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if bal, ver := balanceOf(t, db, b.cash); bal != 900 || ver != 1 {
		t.Fatalf("the real cash account has balance %d, version %d; want 900 and 1", bal, ver)
	}
	checkBooks(t, db)
}

func TestEveryFunctionPinsItsSearchPath(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	want := "search_path=" + schemaOf(t, db) + ", pg_temp"
	rows, err := db.Query(context.Background(), `SELECT p.oid::regprocedure::text, coalesce(p.proconfig, '{}')
		FROM pg_proc p WHERE p.pronamespace = current_schema()::regnamespace`)
	if err != nil {
		t.Fatal(err)
	}
	var name string
	var config []string
	n := 0
	if _, err := pgx.ForEachRow(rows, []any{&name, &config}, func() error {
		n++
		if !slices.Contains(config, want) {
			t.Errorf("%s has settings %v; want %q", name, config, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n < 6 {
		t.Fatalf("found %d functions, expected at least 6", n)
	}
}

func schemaOf(t *testing.T, db *pgxpool.Pool) string {
	t.Helper()
	var schema string
	if err := db.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestABalanceStopsShortOfTheSmallestBigint(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	wallet := createAccount(t, db, account{"wallet:big", "liability", "USD", true})
	// Reaching -2^63 through postings takes over 9,000 of the largest ones, so the test sets the balance with the
	// triggers off (session_replication_role needs a superuser, which the test databases have). The CHECK still
	// runs: constraints aren't triggers.
	set := func(balance int64) error {
		return rolledBack(t, db, func(tx pgx.Tx) error {
			if _, err := tx.Exec(context.Background(), `SET LOCAL session_replication_role = replica`); err != nil {
				return err
			}
			_, err := tx.Exec(context.Background(), `UPDATE accounts SET balance = $1 WHERE id = $2`, balance, wallet)
			return err
		})
	}
	if err := set(math.MinInt64); constraint(err) != "accounts_balance_negatable" {
		t.Fatalf("balance -2^63: got %v, want accounts_balance_negatable", err)
	}
	if err := set(math.MinInt64 + 1); err != nil {
		t.Fatalf("balance -2^63 + 1: %v", err)
	}
}
