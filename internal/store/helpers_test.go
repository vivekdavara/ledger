package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/store"
)

// levels are the isolation levels a Store can post at. The tests of what PostEntry guarantees under concurrency run
// at each of them (atEachLevel).
var levels = []store.Isolation{store.ReadCommitted, store.Serializable}

// atEachLevel runs test once per isolation level, as parallel subtests named after the level.
func atEachLevel(t *testing.T, test func(t *testing.T, level store.Isolation)) {
	t.Helper()
	for _, level := range levels {
		t.Run(string(level), func(t *testing.T) {
			t.Parallel()
			test(t, level)
		})
	}
}

// at returns a store posting at the given level, with the default retry policy unless one is given.
func at(db *pgxpool.Pool, level store.Isolation, retry ...store.RetryPolicy) *store.Store {
	opts := store.Options{Isolation: level}
	if len(retry) > 0 {
		opts.Retry = &retry[0]
	}
	return store.NewWithOptions(db, opts)
}

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

// pad adds accounts unused accounts and entries two-posting entries between them (each padding account in at most
// one), then runs ANALYZE, so that Postgres plans the ledger's queries as it would for tables of that size: through
// their indexes rather than by scanning a page or two.
func pad(tb testing.TB, db *pgxpool.Pool, accounts, entries int) {
	tb.Helper()
	if 2*entries > accounts {
		tb.Fatalf("%d entries need %d padding accounts, not %d", entries, 2*entries, accounts)
	}
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, step := range []struct {
		sql string
		n   int
	}{
		{`INSERT INTO accounts (name, type, currency, allow_negative)
		  SELECT 'padding:' || n, 'liability', 'USD', true FROM generate_series(1, $1::int) n`, accounts},
		{`INSERT INTO journal_entries (description, posting_count) SELECT 'padding', 2 FROM generate_series(1, $1::int)`,
			entries},
		// Entry k gets padding accounts 2k-1 and 2k, so no account is in two postings of this one statement.
		{`WITH pads AS (SELECT id, row_number() OVER (ORDER BY id) AS n FROM accounts WHERE name LIKE 'padding:%'),
		       padded AS (SELECT id, row_number() OVER (ORDER BY id) AS n FROM journal_entries
		                   WHERE description = 'padding' LIMIT $1::int)
		  INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		  SELECT e.id, p.id, 'USD', CASE WHEN p.n % 2 = 0 THEN 1 ELSE -1 END, 0, 1
		    FROM pads p JOIN padded e ON e.n = (p.n + 1) / 2`, entries},
	} {
		if _, err := tx.Exec(ctx, step.sql, step.n); err != nil {
			tb.Fatalf("pad: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatalf("pad: %v", err)
	}
	analyze(tb, db)
}

// analyze refreshes the planner's statistics for the ledger's tables in db's schema only: a bare ANALYZE would do
// every schema in the test database, other tests' included.
func analyze(tb testing.TB, db *pgxpool.Pool) {
	tb.Helper()
	if _, err := db.Exec(context.Background(), `ANALYZE accounts, journal_entries, postings, idempotency_keys`); err != nil {
		tb.Fatal(err)
	}
}
