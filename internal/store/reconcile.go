package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Check is one reconciliation check and what it found.
type Check struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Differences int64    `json:"differences"`
	Examples    []string `json:"examples,omitempty"` // up to MaxExamples of them, in order of the ids they name
}

// MaxExamples is how many differences a check describes; it counts them all.
const MaxExamples = 10

// Report is what Reconcile found: the size of the ledger it checked and each check's result, all as of one moment.
type Report struct {
	AsOf     time.Time `json:"as_of"`
	Accounts int64     `json:"accounts"`
	Entries  int64     `json:"entries"`
	Postings int64     `json:"postings"`
	Events   int64     `json:"events"`
	Checks   []Check   `json:"checks"`
}

// Clean reports whether no check found a difference.
func (r Report) Clean() bool {
	for _, c := range r.Checks {
		if c.Differences > 0 {
			return false
		}
	}
	return true
}

// checks are Reconcile's checks. Each query returns one row per difference: a key to order it by (the ids it names,
// zero-padded so they sort as numbers, or the currency) and a line naming the account, entry or currency.
// The database's own rules (DESIGN.md, "Invariants and where each is enforced") should make every one of them find
// nothing; Reconcile is how anyone finds out if something got past them: a trigger switched off, a hand-made fix, a
// bug in a migration.
var checks = []struct{ name, description, sql string }{
	{"currency_totals", "every currency's balances sum to zero", `
		SELECT currency, format('%s balances sum to %s', currency, sum(balance))
		  FROM accounts GROUP BY currency HAVING sum(balance) <> 0`},
	{"account_balances", "every account's balance and version are the sum and count of its postings", `
		SELECT lpad(a.id::text, 20, '0'),
		       format('account %s: balance %s at version %s, its postings sum to %s over %s', a.id, a.balance,
		              a.version, coalesce(p.total, 0), coalesce(p.n, 0))
		  FROM accounts a
		  LEFT JOIN (SELECT account_id, sum(amount) AS total, count(*) AS n FROM postings GROUP BY account_id) p
		         ON p.account_id = a.id
		 WHERE a.balance <> coalesce(p.total, 0) OR a.version <> coalesce(p.n, 0)`},
	{"last_balance_after", "every account's balance is its last posting's balance_after", `
		SELECT lpad(a.id::text, 20, '0'),
		       format('account %s: balance %s, its last posting (version %s) leaves %s', a.id, a.balance,
		              last.account_version, last.balance_after)
		  FROM accounts a
		  JOIN LATERAL (SELECT account_version, balance_after FROM postings WHERE account_id = a.id
		                 ORDER BY account_version DESC LIMIT 1) last ON true
		 WHERE a.balance <> last.balance_after`},
	{"account_chains",
		"every account's postings are versions 1, 2, 3, ... each leaving the balance before plus its amount", `
		SELECT lpad(account_id::text, 20, '0') || lpad(account_version::text, 20, '0'),
		       format('account %s: posting %s is version %s leaving %s, expected version %s leaving %s',
		              account_id, id, account_version, balance_after, n, expected)
		  FROM (SELECT id, account_id, account_version, balance_after, row_number() OVER w AS n,
		               coalesce(lag(balance_after) OVER w, 0)::numeric + amount AS expected
		          FROM postings WINDOW w AS (PARTITION BY account_id ORDER BY account_version)) c
		 WHERE account_version <> n OR balance_after <> expected`},
	{"posting_currencies", "every posting is in its account's currency", `
		SELECT lpad(p.id::text, 20, '0'),
		       format('posting %s is in %s, its account %s in %s', p.id, p.currency, a.id, a.currency)
		  FROM postings p JOIN accounts a ON a.id = p.account_id WHERE p.currency <> a.currency`},
	{"entry_balances", "every entry's postings sum to zero in each currency", `
		SELECT lpad(entry_id::text, 20, '0') || currency,
		       format('entry %s is off by %s in %s', entry_id, sum(amount), currency)
		  FROM postings GROUP BY entry_id, currency HAVING sum(amount) <> 0`},
	{"entry_posting_counts", "every entry has as many postings as it declares", `
		SELECT lpad(e.id::text, 20, '0'),
		       format('entry %s declares %s postings and has %s', e.id, e.posting_count, count(p.id))
		  FROM journal_entries e LEFT JOIN postings p ON p.entry_id = e.id
		 GROUP BY e.id HAVING count(p.id) <> e.posting_count`},
	{"entry_events", "every entry has its outbox event", `
		SELECT lpad(e.id::text, 20, '0'), format('entry %s has no event', e.id)
		  FROM journal_entries e WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.entry_id = e.id)`},
	{"orphan_rows", "every posting, event and key names an entry that exists, and every posting an account", `
		SELECT lpad(p.entry_id::text, 20, '0'), format('posting %s names entry %s, which does not exist', p.id, p.entry_id)
		  FROM postings p WHERE NOT EXISTS (SELECT 1 FROM journal_entries e WHERE e.id = p.entry_id)
		UNION ALL
		SELECT lpad(o.entry_id::text, 20, '0'), format('event %s names entry %s, which does not exist', o.id, o.entry_id)
		  FROM outbox o WHERE NOT EXISTS (SELECT 1 FROM journal_entries e WHERE e.id = o.entry_id)
		UNION ALL
		SELECT lpad(k.entry_id::text, 20, '0'), format('key %s names entry %s, which does not exist', k.key, k.entry_id)
		  FROM idempotency_keys k WHERE NOT EXISTS (SELECT 1 FROM journal_entries e WHERE e.id = k.entry_id)
		UNION ALL
		SELECT lpad(p.entry_id::text, 20, '0'),
		       format('posting %s names account %s, which does not exist', p.id, p.account_id)
		  FROM postings p WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id = p.account_id)`},
	{"protected_accounts", "no protected account is below zero on its normal side", `
		SELECT lpad(id::text, 20, '0'),
		       format('account %s (%s, a protected %s account) has balance %s', id, name, type, balance)
		  FROM accounts
		 WHERE NOT allow_negative
		   AND CASE WHEN type IN ('asset', 'expense') THEN balance < 0 ELSE balance > 0 END`},
}

// Reconcile recomputes the ledger from its postings and compares the result with what is stored: each balance
// against its postings, each account's history chain, each entry's sums and count, each currency's total, and each
// entry's event (DESIGN.md, "Reconciliation (day 3)"). It reports every difference and changes nothing.
//
// Every check reads one snapshot, from one read-only REPEATABLE READ transaction, so entries posted while it runs
// are in all of the report or in none of it.
func Reconcile(ctx context.Context, db Beginner) (Report, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // read only: there is nothing to commit
	var r Report
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&r.AsOf); err != nil {
		return Report{}, err
	}
	for _, c := range []struct {
		table string
		n     *int64
	}{{"accounts", &r.Accounts}, {"journal_entries", &r.Entries}, {"postings", &r.Postings}, {"outbox", &r.Events}} {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+c.table).Scan(c.n); err != nil {
			return Report{}, fmt.Errorf("count %s: %w", c.table, err)
		}
	}
	for _, c := range checks {
		check := Check{Name: c.name, Description: c.description}
		// The key is compared byte by byte (COLLATE "C"): a collation's own order differs between platforms (glibc's
		// en_US put "entry 10" before "entry 1" in CI, macOS the other way round).
		rows, err := tx.Query(ctx, `SELECT count(*) OVER (), d FROM (`+c.sql+`) found (k, d)
			ORDER BY k COLLATE "C", d COLLATE "C" LIMIT $1`, MaxExamples)
		if err != nil {
			return Report{}, fmt.Errorf("check %s: %w", c.name, err)
		}
		var example string
		if _, err := pgx.ForEachRow(rows, []any{&check.Differences, &example}, func() error {
			check.Examples = append(check.Examples, example)
			return nil
		}); err != nil {
			return Report{}, fmt.Errorf("check %s: %w", c.name, err)
		}
		r.Checks = append(r.Checks, check)
	}
	return r, nil
}
