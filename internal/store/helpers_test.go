package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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

// leg is one posting of an entry built in a test.
type leg struct {
	account  int64
	currency string
	amount   int64
}

// post records an entry and its postings in one transaction and commits it. It returns the entry's id, or the
// error from whichever statement failed, the commit included.
func post(db *pgxpool.Pool, description string, legs ...leg) (int64, error) {
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id, err := insertEntry(tx, description, len(legs))
	if err != nil {
		return 0, err
	}
	for _, l := range legs {
		if err := insertPosting(tx, id, l.account, l.currency, l.amount); err != nil {
			return 0, err
		}
	}
	return id, tx.Commit(ctx)
}

func mustPost(t *testing.T, db *pgxpool.Pool, description string, legs ...leg) int64 {
	t.Helper()
	id, err := post(db, description, legs...)
	if err != nil {
		t.Fatalf("post %q: %v", description, err)
	}
	return id
}

// balanceOf returns an account's stored balance and version.
func balanceOf(t *testing.T, db *pgxpool.Pool, account int64) (balance, version int64) {
	t.Helper()
	err := db.QueryRow(context.Background(), `SELECT balance, version FROM accounts WHERE id = $1`, account).
		Scan(&balance, &version)
	if err != nil {
		t.Fatalf("balance of %d: %v", account, err)
	}
	return balance, version
}
