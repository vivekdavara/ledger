package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

func TestUnbalancedEntriesAreRejectedAtCommit(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	eur1 := createAccount(t, db, account{"fx:eur", "asset", "EUR", true})
	eur2 := createAccount(t, db, account{"wallet:alice:eur", "liability", "EUR", true})
	usd := createAccount(t, db, account{"fx:usd", "asset", "USD", true})
	cases := []struct {
		name    string
		legs    []leg
		message string
	}{
		{"a cent short", []leg{{usd, "USD", 5000}, {b.alice, "USD", -4999}}, "off in USD by 1"},
		{"zero in total but not per currency", []leg{{usd, "USD", 5}, {eur1, "EUR", -5}}, "off in EUR by -5, USD by 5"},
		{"two currencies off", []leg{{usd, "USD", 5}, {b.alice, "USD", -4}, {eur1, "EUR", 7}, {eur2, "EUR", -9}},
			"off in EUR by -2, USD by 1"},
	}
	for _, tc := range cases {
		_, err := post(db, tc.name, tc.legs...)
		if got := constraint(err); got != "entry_balanced" || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("%s: got %v (constraint %q), want entry_balanced %q", tc.name, err, got, tc.message)
		}
	}
	// Nothing of the rejected entries remains: no entries, no postings, every balance and version untouched.
	if n := count(t, db, `SELECT count(*) FROM journal_entries`); n != 0 {
		t.Fatalf("%d rejected entries were kept", n)
	}
	if n := count(t, db, `SELECT count(*) FROM accounts WHERE balance <> 0 OR version <> 0`); n != 0 {
		t.Fatalf("%d accounts moved", n)
	}
}

func TestPostingCountMustMatch(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	commit := func(fn func(tx pgx.Tx) error) error {
		ctx := context.Background()
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	cases := map[string]func(tx pgx.Tx) error{
		"no postings": func(tx pgx.Tx) error {
			_, err := insertEntry(tx, "empty", 2)
			return err
		},
		"fewer than declared": func(tx pgx.Tx) error {
			e, err := insertEntry(tx, "declares three", 3)
			if err != nil {
				return err
			}
			if err := insertPosting(tx, e, b.cash, "USD", 100); err != nil {
				return err
			}
			return insertPosting(tx, e, b.alice, "USD", -100)
		},
	}
	for name, fn := range cases {
		if err := commit(fn); constraint(err) != "entry_posting_count" {
			t.Errorf("%s: got %v, want entry_posting_count", name, err)
		}
	}

	// A balanced pair can't be slipped into an entry committed earlier: the zero sum survives, the count doesn't.
	entry := mustPost(t, db, "top-up", leg{b.cash, "USD", 5000}, leg{b.alice, "USD", -5000})
	vault := createAccount(t, db, account{"cash:vault", "asset", "USD", false})
	err := commit(func(tx pgx.Tx) error {
		if err := insertPosting(tx, entry, vault, "USD", 100); err != nil {
			return err
		}
		return insertPosting(tx, entry, b.bob, "USD", -100)
	})
	if got := constraint(err); got != "entry_posting_count" || !strings.Contains(err.Error(), "declares 2 postings but has 4") {
		t.Fatalf("appending to a committed entry: got %v (constraint %q)", err, got)
	}
	if bal, ver := balanceOf(t, db, b.bob); bal != 0 || ver != 0 {
		t.Fatalf("the rejected append moved bob: balance %d, version %d", bal, ver)
	}
}

func TestChecksThatRunEarlyFailClosed(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	// A client that makes the deferred checks immediate gets them run after the entry insert, before its
	// postings exist. The entry is rejected: running the check early never lets a bad entry through.
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	_, err = insertEntry(tx, "early", 2)
	if got := constraint(err); got != "entry_posting_count" {
		t.Fatalf("got %v (constraint %q), want entry_posting_count", err, got)
	}
}

func TestOneStatementCanInsertAllPostings(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "top-up", leg{b.cash, "USD", 5000}, leg{b.alice, "USD", -5000})
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	entry, err := insertEntry(tx, "payment", 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		VALUES ($1, $2, 'USD', 2000, 0, 1), ($1, $3, 'USD', -1940, 0, 1), ($1, $4, 'USD', -60, 0, 1)`,
		entry, b.alice, b.bob, b.fees); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("a balanced multi-row insert was rejected: %v", err)
	}
	if bal, ver := balanceOf(t, db, b.alice); bal != -3000 || ver != 2 {
		t.Fatalf("alice: balance %d, version %d", bal, ver)
	}
}

func TestHistoryIsAppendOnly(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "top-up", leg{b.cash, "USD", 5000}, leg{b.alice, "USD", -5000})
	for _, sql := range []string{
		`UPDATE postings SET amount = amount * 2`,
		`UPDATE postings SET balance_after = 0`,
		`DELETE FROM postings`,
		`UPDATE journal_entries SET description = 'edited'`,
		`DELETE FROM journal_entries`,
		`TRUNCATE postings`,
		`TRUNCATE journal_entries CASCADE`,
	} {
		err := rolledBack(t, db, func(tx pgx.Tx) error {
			_, err := tx.Exec(context.Background(), sql)
			return err
		})
		if got := constraint(err); got != "append_only" {
			t.Errorf("%s: got %v (constraint %q), want append_only", sql, err, got)
		}
	}
	if n := count(t, db, `SELECT count(*) FROM postings`); n != 2 {
		t.Fatalf("%d postings left, want 2", n)
	}
}

func TestAReversalRestoresEveryBalance(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	b := newBook(t, db)
	mustPost(t, db, "top-up", leg{b.cash, "USD", 5000}, leg{b.alice, "USD", -5000})
	mustPost(t, db, "payment, posted by mistake", leg{b.alice, "USD", 2000}, leg{b.bob, "USD", -1940},
		leg{b.fees, "USD", -60})
	mustPost(t, db, "reversal of the mistake", leg{b.alice, "USD", -2000}, leg{b.bob, "USD", 1940},
		leg{b.fees, "USD", 60})
	for id, want := range map[int64][2]int64{b.alice: {-5000, 3}, b.bob: {0, 2}, b.fees: {0, 2}} {
		if bal, ver := balanceOf(t, db, id); bal != want[0] || ver != want[1] {
			t.Errorf("account %d: balance %d version %d, want %d and %d", id, bal, ver, want[0], want[1])
		}
	}
	if n := count(t, db, `SELECT count(*) FROM postings`); n != 8 {
		t.Fatalf("%d postings, want all 8: the mistake stays in the history", n)
	}
}
