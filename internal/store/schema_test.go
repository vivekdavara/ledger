package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/money"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// constraint returns the name of the constraint a Postgres error reports, or "" for any other error.
func constraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

type account struct {
	name, typ, currency string
	allowNegative       bool
}

func createAccount(t *testing.T, db *pgxpool.Pool, a account) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(context.Background(),
		`INSERT INTO accounts (name, type, currency, allow_negative) VALUES ($1, $2, $3, $4) RETURNING id`,
		a.name, a.typ, a.currency, a.allowNegative).Scan(&id)
	if err != nil {
		t.Fatalf("create account %q: %v", a.name, err)
	}
	return id
}

// rolledBack runs fn in a transaction that is always rolled back, and returns fn's error. Each scenario checks
// one statement and leaves nothing behind.
func rolledBack(t *testing.T, db *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return fn(tx)
}

func insertEntry(tx pgx.Tx, description string, postingCount int) (int64, error) {
	var id int64
	err := tx.QueryRow(context.Background(),
		`INSERT INTO journal_entries (description, posting_count) VALUES ($1, $2) RETURNING id`,
		description, postingCount).Scan(&id)
	return id, err
}

func insertPosting(tx pgx.Tx, entryID, accountID int64, currency string, amount int64) error {
	// balance_after and account_version are placeholders: from migration 0002 a trigger sets them.
	_, err := tx.Exec(context.Background(),
		`INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		 VALUES ($1, $2, $3, $4, 0, 1)`, entryID, accountID, currency, amount)
	return err
}

func TestOpenErrorsHideThePassword(t *testing.T) {
	for _, url := range []string{"postgres://ledger:s3cret-pw@%zz/ledger", "postgres://ledger:s3cret-pw@127.0.0.1:1/x"} {
		_, err := store.Open(context.Background(), url)
		if err == nil || strings.Contains(err.Error(), "s3cret-pw") {
			t.Errorf("Open(%q) error = %v; want an error without the password", url, err)
		}
	}
}

func TestCurrenciesMatchTheGoRegistry(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	rows, err := db.Query(context.Background(), `SELECT code, exponent FROM currencies ORDER BY code`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (money.Currency, error) {
		var c money.Currency
		err := r.Scan(&c.Code, &c.Exponent)
		return c, err
	})
	if err != nil {
		t.Fatal(err)
	}
	want := money.Currencies()
	if len(got) != len(want) {
		t.Fatalf("database has %v, Go has %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("database has %v, Go has %v", got, want)
		}
	}
}

func TestAccountRules(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	createAccount(t, db, account{"wallet:alice", "liability", "USD", false})
	cases := []struct {
		name       string
		a          account
		constraint string
	}{
		{"empty name", account{"", "asset", "USD", false}, "accounts_name_length"},
		{"long name", account{strings.Repeat("a", 201), "asset", "USD", false}, "accounts_name_length"},
		{"duplicate name", account{"wallet:alice", "liability", "USD", false}, "accounts_name_unique"},
		{"unknown type", account{"cash", "cash", "USD", false}, "accounts_type_known"},
		{"unknown currency", account{"x", "asset", "XXX", false}, "accounts_currency_fkey"},
		{"lowercase currency", account{"y", "asset", "usd", false}, "accounts_currency_fkey"},
	}
	for _, tc := range cases {
		err := rolledBack(t, db, func(tx pgx.Tx) error {
			_, err := tx.Exec(context.Background(),
				`INSERT INTO accounts (name, type, currency, allow_negative) VALUES ($1, $2, $3, $4)`,
				tc.a.name, tc.a.typ, tc.a.currency, tc.a.allowNegative)
			return err
		})
		if got := constraint(err); got != tc.constraint {
			t.Errorf("%s: got %v (constraint %q), want constraint %q", tc.name, err, got, tc.constraint)
		}
	}
}

func TestEntryRules(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	cases := []struct {
		name, description string
		postingCount      int
		constraint        string
	}{
		{"one posting", "", 1, "entries_posting_count_range"},
		{"65 postings", "", 65, "entries_posting_count_range"},
		{"501 characters", strings.Repeat("é", 501), 2, "entries_description_length"},
	}
	for _, tc := range cases {
		err := rolledBack(t, db, func(tx pgx.Tx) error {
			_, err := insertEntry(tx, tc.description, tc.postingCount)
			return err
		})
		if got := constraint(err); got != tc.constraint {
			t.Errorf("%s: got %v (constraint %q), want %q", tc.name, err, got, tc.constraint)
		}
	}
}

func TestPostingRules(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	// allow_negative, so the overdraft check never fires first and each case hits the rule it is about.
	usd := createAccount(t, db, account{"cash:usd", "asset", "USD", true})
	other := createAccount(t, db, account{"cash:usd:2", "asset", "USD", true})
	cases := []struct {
		name       string
		insert     func(tx pgx.Tx, entry int64) error
		constraint string
	}{
		{"zero amount", func(tx pgx.Tx, e int64) error { return insertPosting(tx, e, usd, "USD", 0) },
			"postings_amount_range"},
		{"above the maximum", func(tx pgx.Tx, e int64) error {
			return insertPosting(tx, e, usd, "USD", money.MaxAmount+1)
		}, "postings_amount_range"},
		{"below the minimum", func(tx pgx.Tx, e int64) error {
			return insertPosting(tx, e, usd, "USD", -money.MaxAmount-1)
		}, "postings_amount_range"},
		{"currency is not the account's", func(tx pgx.Tx, e int64) error { return insertPosting(tx, e, usd, "EUR", 5) },
			"postings_account_currency_fkey"},
		{"no such account", func(tx pgx.Tx, e int64) error { return insertPosting(tx, e, 999999, "USD", 5) },
			"postings_account_currency_fkey"},
		{"no such entry", func(tx pgx.Tx, e int64) error { return insertPosting(tx, e+1000, usd, "USD", 5) },
			"postings_entry_fkey"},
		{"same account twice", func(tx pgx.Tx, e int64) error {
			if err := insertPosting(tx, e, usd, "USD", 5); err != nil {
				return err
			}
			return insertPosting(tx, e, usd, "USD", -5)
		}, "postings_one_per_account"},
	}
	for _, tc := range cases {
		err := rolledBack(t, db, func(tx pgx.Tx) error {
			entry, err := insertEntry(tx, tc.name, 2)
			if err != nil {
				return err
			}
			return tc.insert(tx, entry)
		})
		if got := constraint(err); got != tc.constraint {
			t.Errorf("%s: got %v (constraint %q), want %q", tc.name, err, got, tc.constraint)
		}
	}
	// The boundary amounts themselves are allowed.
	err := rolledBack(t, db, func(tx pgx.Tx) error {
		entry, err := insertEntry(tx, "boundaries", 2)
		if err != nil {
			return err
		}
		if err := insertPosting(tx, entry, usd, "USD", money.MaxAmount); err != nil {
			return err
		}
		return insertPosting(tx, entry, other, "USD", -money.MaxAmount)
	})
	if err != nil {
		t.Fatalf("boundary amounts rejected: %v", err)
	}
}
