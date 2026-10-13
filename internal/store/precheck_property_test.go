package store_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"pgregory.net/rapid"
)

// TestPropertyTheServicesChecksAgreeWithTheDatabase: checkAgainstAccounts, which PostEntry runs under the row locks,
// and the database's constraints must give the same verdict on a posting, for the same reason, for every account
// type, overdraft setting and balance, out to the edges of bigint. Each case sets one account's balance with the
// triggers off (session_replication_role, which the test databases' superuser may set), then posts against it with
// the checks the database runs, in a transaction that is rolled back.
func TestPropertyTheServicesChecksAgreeWithTheDatabase(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	ctx := context.Background()
	var other int64 // the balancing leg: an asset that may go negative
	if err := db.QueryRow(ctx, `INSERT INTO accounts (name, type, currency, allow_negative)
		VALUES ('other', 'asset', 'USD', true) RETURNING id`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	edges := []int64{math.MinInt64 + 1, math.MinInt64 + money.MaxAmount, -money.MaxAmount, -1, 0, 1, money.MaxAmount,
		math.MaxInt64 - money.MaxAmount, math.MaxInt64}
	verdicts := map[string]int{}
	rapid.Check(t, func(rt *rapid.T) {
		typ := rapid.SampledFrom(ledger.AccountTypes).Draw(rt, "type")
		allowNegative := rapid.Bool().Draw(rt, "allow negative")
		balance := rapid.OneOf(rapid.SampledFrom(edges), rapid.Int64Range(-3*money.MaxAmount, 3*money.MaxAmount)).
			Draw(rt, "balance")
		amount := rapid.OneOf(rapid.SampledFrom([]int64{-money.MaxAmount, -1, 1, money.MaxAmount}),
			rapid.Int64Range(-money.MaxAmount, money.MaxAmount)).Filter(func(v int64) bool { return v != 0 }).
			Draw(rt, "amount")
		// A protected account can't hold a balance on the wrong side to begin with; the CHECK refuses to store it.
		acct := ledger.Account{Type: typ, AllowNegative: allowNegative, Balance: balance, Currency: "USD"}
		if !allowNegative && acct.Overdraws(0) {
			rt.Skip("a protected account can't start overdrawn")
		}
		e := ledger.Entry{Postings: []ledger.Posting{{AccountID: 0, Currency: "USD", Amount: amount},
			{AccountID: other, Currency: "USD", Amount: -amount}}}

		tx, err := db.Begin(ctx)
		if err != nil {
			rt.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := tx.QueryRow(ctx, `INSERT INTO accounts (name, type, currency, allow_negative)
			VALUES ('subject', $1, 'USD', $2) RETURNING id, name, created_at`, typ, allowNegative).
			Scan(&acct.ID, &acct.Name, &acct.CreatedAt); err != nil {
			rt.Fatal(err)
		}
		e.Postings[0].AccountID = acct.ID
		for _, sql := range []string{`SET LOCAL session_replication_role = replica`,
			`UPDATE accounts SET balance = $1 WHERE id = $2`, `SET LOCAL session_replication_role = origin`} {
			args := []any{}
			if sql[0] == 'U' {
				args = []any{balance, acct.ID}
			}
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				rt.Fatal(err)
			}
		}
		var otherAcct ledger.Account
		if err := tx.QueryRow(ctx, `SELECT id, name, type, currency, allow_negative, balance, version, created_at
			FROM accounts WHERE id = $1`, other).Scan(
			&otherAcct.ID, &otherAcct.Name, &otherAcct.Type, &otherAcct.Currency, &otherAcct.AllowNegative,
			&otherAcct.Balance, &otherAcct.Version, &otherAcct.CreatedAt); err != nil {
			rt.Fatal(err)
		}
		goErr := store.CheckAgainstAccounts(e, []ledger.Account{acct, otherAcct})
		dbErr := postRaw(ctx, tx, e)

		goRule, dbRule := ruleOf(goErr), dbRuleOf(dbErr)
		if goRule != dbRule {
			rt.Fatalf("%s (allow_negative %v) at balance %d, posting %d: the service says %v, the database %v",
				typ, allowNegative, balance, amount, goErr, dbErr)
		}
		verdicts[goRule]++
	})
	t.Logf("verdicts: %v", verdicts)
}

// postRaw posts e inside tx under a savepoint, as one entry, and runs the commit-time checks at once.
func postRaw(ctx context.Context, tx pgx.Tx, e ledger.Entry) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = sp.Rollback(ctx) }()
	var id int64
	if err := sp.QueryRow(ctx, `INSERT INTO journal_entries (posting_count) VALUES ($1) RETURNING id`,
		len(e.Postings)).Scan(&id); err != nil {
		return err
	}
	for _, p := range e.Postings {
		if _, err := sp.Exec(ctx, `INSERT INTO postings (entry_id, account_id, currency, amount) VALUES ($1, $2, $3, $4)`,
			id, p.AccountID, p.Currency, p.Amount); err != nil {
			return err
		}
	}
	_, err = sp.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`)
	return err
}

func ruleOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return "insufficient funds"
	case errors.Is(err, ledger.ErrBalanceRange):
		return "balance out of range"
	}
	return "other: " + err.Error()
}

func dbRuleOf(err error) string {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	switch {
	case err == nil:
		return "ok"
	case ok && pgErr.ConstraintName == "accounts_no_overdraft":
		return "insufficient funds"
	case ok && (pgErr.Code == "22003" || pgErr.ConstraintName == "accounts_balance_negatable"):
		return "balance out of range"
	}
	return "other: " + err.Error()
}
