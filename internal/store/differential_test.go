package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"pgregory.net/rapid"
)

const accountsPerCurrency = 8

// accountsByCurrency creates accountsPerCurrency accounts in every currency. They allow negative balances, so
// the overdraft rule (which only the database can check) never decides a case here.
func accountsByCurrency(t *testing.T, db *pgxpool.Pool) map[string][]int64 {
	t.Helper()
	types := []string{"asset", "liability", "equity", "revenue", "expense"}
	out := make(map[string][]int64)
	for _, c := range money.Currencies() {
		for i := range accountsPerCurrency {
			name := fmt.Sprintf("%s:%d", strings.ToLower(c.Code), i)
			out[c.Code] = append(out[c.Code], createAccount(t, db, account{name, types[i%len(types)], c.Code, true}))
		}
	}
	return out
}

// entryOver draws a balanced entry whose postings use existing accounts in the right currencies: one to three
// currencies, two to eight postings in each.
func entryOver(accounts map[string][]int64) *rapid.Generator[ledger.Entry] {
	return rapid.Custom(func(t *rapid.T) ledger.Entry {
		currencies := rapid.SliceOfNDistinct(rapid.SampledFrom(money.Currencies()), 1, 3,
			func(c money.Currency) string { return c.Code }).Draw(t, "currencies")
		var postings []ledger.Posting
		for _, cur := range currencies {
			ids := rapid.SliceOfNDistinct(rapid.SampledFrom(accounts[cur.Code]), 2, accountsPerCurrency,
				rapid.ID[int64]).Draw(t, "accounts")
			var sum int64
			for i, id := range ids {
				a := -sum
				if i < len(ids)-1 {
					a = rapid.Int64Range(-money.MaxAmount/ledger.MaxPostings, money.MaxAmount/ledger.MaxPostings).
						Filter(func(v int64) bool { return v != 0 && sum+v != 0 }).Draw(t, "amount")
				}
				sum += a
				postings = append(postings, ledger.Posting{AccountID: id, Currency: cur.Code, Amount: a})
			}
		}
		return ledger.Entry{
			Description: rapid.StringMatching(`[\p{L}\p{N} ]{0,20}`).Draw(t, "description"),
			Postings:    rapid.Permutation(postings).Draw(t, "order"),
		}
	})
}

// A defect breaks exactly one rule, so the two checkers must agree on which rule, not just on rejecting.
type defect struct {
	name   string
	apply  func(t *rapid.T, e *ledger.Entry) bool // false when the entry can't take this defect
	rule   error                                  // what the Go validator must say
	wantDB func(err error) bool                   // what the database must say
}

func byConstraint(name string) func(error) bool {
	return func(err error) bool { return constraint(err) == name }
}

func bySQLState(code string) func(error) bool {
	return func(err error) bool {
		var pgErr *pgconn.PgError
		return errors.As(err, &pgErr) && pgErr.Code == code
	}
}

func anyPosting(t *rapid.T, e *ledger.Entry) *ledger.Posting {
	return &e.Postings[rapid.IntRange(0, len(e.Postings)-1).Draw(t, "posting")]
}

var defects = []defect{
	{"none", func(*rapid.T, *ledger.Entry) bool { return true }, nil, func(err error) bool { return err == nil }},
	{"unbalanced", func(t *rapid.T, e *ledger.Entry) bool {
		p := anyPosting(t, e)
		d := rapid.Int64Range(-1000, 1000).Filter(func(d int64) bool { return d != 0 && p.Amount+d != 0 }).Draw(t, "d")
		p.Amount += d
		return true
	}, ledger.ErrUnbalanced, byConstraint("entry_balanced")},
	{"zero amount", func(t *rapid.T, e *ledger.Entry) bool {
		anyPosting(t, e).Amount = 0
		return true
	}, ledger.ErrZeroAmount, byConstraint("postings_amount_range")},
	{"amount out of range", func(t *rapid.T, e *ledger.Entry) bool {
		p := anyPosting(t, e)
		p.Amount = rapid.SampledFrom([]int64{money.MaxAmount + 1, -money.MaxAmount - 1, money.MaxAmount * 9}).Draw(t, "a")
		return true
	}, ledger.ErrAmountRange, byConstraint("postings_amount_range")},
	{"unknown currency", func(t *rapid.T, e *ledger.Entry) bool {
		anyPosting(t, e).Currency = rapid.SampledFrom([]string{"XXX", "usd", "", "EURO"}).Draw(t, "currency")
		return true
	}, ledger.ErrCurrency, byConstraint("postings_account_currency_fkey")},
	{"same account twice", func(t *rapid.T, e *ledger.Entry) bool {
		// Point a later posting at an earlier posting's account in the same currency, so only this rule breaks.
		for i := range e.Postings {
			for j := i + 1; j < len(e.Postings); j++ {
				if e.Postings[i].Currency == e.Postings[j].Currency {
					e.Postings[j].AccountID = e.Postings[i].AccountID
					return true
				}
			}
		}
		return false
	}, ledger.ErrDuplicateAccount, byConstraint("postings_one_per_account")},
	{"one posting", func(t *rapid.T, e *ledger.Entry) bool {
		e.Postings = e.Postings[:1]
		return true
	}, ledger.ErrPostingCount, byConstraint("entries_posting_count_range")},
	{"description too long", func(t *rapid.T, e *ledger.Entry) bool {
		e.Description = strings.Repeat("ü", ledger.MaxDescription+1)
		return true
	}, ledger.ErrDescription, byConstraint("entries_description_length")},
	{"NUL in the description", func(t *rapid.T, e *ledger.Entry) bool {
		e.Description += "\x00"
		return true
	}, ledger.ErrDescription, bySQLState("22021")},
	{"invalid UTF-8 in the description", func(t *rapid.T, e *ledger.Entry) bool {
		e.Description += "\xff"
		return true
	}, ledger.ErrDescription, bySQLState("22021")},
}

// postEntry writes e the way a client would: the entry, then its postings in order, then commit.
func postEntry(db *pgxpool.Pool, e ledger.Entry) error {
	legs := make([]leg, len(e.Postings))
	for i, p := range e.Postings {
		legs[i] = leg{p.AccountID, p.Currency, p.Amount}
	}
	_, err := post(db, e.Description, legs...)
	return err
}

// The Go validator and the database are two implementations of the same rules. Random entries, valid or with one
// defect, must get the same verdict from both, for the same reason.
func TestPropertyDatabaseAgreesWithTheValidator(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	accounts := accountsByCurrency(t, db)
	seen := make(map[string]int)
	rapid.Check(t, func(t *rapid.T) {
		e := entryOver(accounts).Draw(t, "entry")
		d := rapid.SampledFrom(defects).Draw(t, "defect")
		if !d.apply(t, &e) {
			t.Skip("this entry can't take the defect")
		}
		seen[d.name]++
		goErr := e.Validate()
		dbErr := postEntry(db, e)
		if !errors.Is(goErr, d.rule) || (goErr == nil) != (d.rule == nil) {
			t.Fatalf("defect %q: the validator said %v, want %v", d.name, goErr, d.rule)
		}
		if !d.wantDB(dbErr) {
			t.Fatalf("defect %q: the validator said %v, but the database said %v", d.name, goErr, dbErr)
		}
	})
	// Exactly the valid entries were committed, and the books still balance.
	if n := count(t, db, `SELECT count(*) FROM journal_entries`); n != seen["none"] {
		t.Errorf("%d entries were committed, but %d were valid", n, seen["none"])
	}
	checkBooks(t, db)
	t.Logf("cases per defect: %v", seen)
}

// checkBooks recomputes the ledger from its postings and fails on any difference: every currency's balances sum to
// zero, each balance is the sum of its account's postings, and each account's history is a gapless chain.
func checkBooks(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	checks := map[string]string{
		"currencies whose balances don't sum to zero": `
			SELECT count(*) FROM (SELECT currency FROM accounts GROUP BY currency HAVING sum(balance) <> 0) c`,
		"accounts whose balance or version isn't their postings'": `
			SELECT count(*) FROM accounts a
			  LEFT JOIN (SELECT account_id, sum(amount) AS total, count(*) AS n FROM postings GROUP BY account_id) p
			         ON p.account_id = a.id
			 WHERE a.balance <> coalesce(p.total, 0) OR a.version <> coalesce(p.n, 0)`,
		"postings that break their account's chain": `
			SELECT count(*) FROM (
			    SELECT balance_after, account_version,
			           coalesce(lag(balance_after) OVER w, 0)::numeric + amount AS expected, row_number() OVER w AS n
			      FROM postings WINDOW w AS (PARTITION BY account_id ORDER BY account_version)) c
			 WHERE balance_after <> expected OR account_version <> n`,
		"entries that don't balance": `
			SELECT count(*) FROM (SELECT 1 FROM postings GROUP BY entry_id, currency HAVING sum(amount) <> 0) e`,
	}
	for what, sql := range checks {
		var n int64
		if err := db.QueryRow(context.Background(), sql).Scan(&n); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if n != 0 {
			t.Errorf("%s: %d", what, n)
		}
	}
}
