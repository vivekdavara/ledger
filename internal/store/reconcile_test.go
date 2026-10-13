package store_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// found lists the checks that found differences, in the report's order.
func found(r store.Report) []string {
	var names []string
	for _, c := range r.Checks {
		if c.Differences > 0 {
			names = append(names, c.Name)
		}
	}
	return names
}

func reconcile(t *testing.T, db *pgxpool.Pool) store.Report {
	t.Helper()
	r, err := store.Reconcile(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAHealthyLedgerReconciles(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	for i := range 5 {
		if _, _, err := s.PostEntry(context.Background(), fmt.Sprintf("pay-%d", i), payment(b, 500)); err != nil {
			t.Fatal(err)
		}
	}
	r := reconcile(t, db)
	if !r.Clean() || len(r.Checks) != 10 {
		t.Fatalf("found %v in %d checks: %+v", found(r), len(r.Checks), r.Checks)
	}
	if r.Accounts != 4 || r.Entries != 6 || r.Postings != 2+5*3 || r.Events != 6 || r.AsOf.IsZero() {
		t.Fatalf("report %+v", r)
	}
}

// damage changes the ledger in ways its rules forbid, as the owner of the test's tables can: by switching a guard
// trigger off, or dropping a constraint, inside one transaction.
func damage(t *testing.T, db *pgxpool.Pool, statements ...string) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, sql := range statements {
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// Each kind of damage is reported by exactly the checks that compare what it broke, and by no other.
func TestEachKindOfDamageIsReported(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		damage func(b book, payment int64) []string
		want   []string
		// One of the differences, as reported.
		example string
	}{
		{"a balance moved by hand", func(b book, _ int64) []string {
			return []string{`ALTER TABLE accounts DISABLE TRIGGER accounts_guard`,
				fmt.Sprintf(`UPDATE accounts SET balance = balance - 100 WHERE id = %d`, b.alice),
				`ALTER TABLE accounts ENABLE TRIGGER accounts_guard`}
		}, []string{"currency_totals", "account_balances", "last_balance_after"}, "USD balances sum to -100"},
		{"a posting deleted from a history", func(b book, _ int64) []string {
			return []string{`ALTER TABLE postings DISABLE TRIGGER postings_append_only`,
				fmt.Sprintf(`DELETE FROM postings WHERE account_id = %d AND account_version = 1`, b.alice),
				`ALTER TABLE postings ENABLE TRIGGER postings_append_only`}
		}, []string{"account_balances", "account_chains", "entry_balances", "entry_posting_counts"},
			"entry 1 declares 2 postings and has 1"},
		{"a posting's amount changed", func(b book, _ int64) []string {
			return []string{`ALTER TABLE postings DISABLE TRIGGER postings_append_only`,
				fmt.Sprintf(`UPDATE postings SET amount = amount + 1 WHERE account_id = %d`, b.bob),
				`ALTER TABLE postings ENABLE TRIGGER postings_append_only`}
		}, []string{"account_balances", "account_chains", "entry_balances"}, "entry 2 is off by 1 in USD"},
		{"a balance_after rewritten", func(b book, _ int64) []string {
			return []string{`ALTER TABLE postings DISABLE TRIGGER postings_append_only`,
				fmt.Sprintf(`UPDATE postings SET balance_after = balance_after + 1 WHERE account_id = %d`, b.bob),
				`ALTER TABLE postings ENABLE TRIGGER postings_append_only`}
		}, []string{"last_balance_after", "account_chains"}, ""},
		{"an event deleted", func(_ book, payment int64) []string {
			return []string{fmt.Sprintf(`DELETE FROM outbox WHERE entry_id = %d`, payment)}
		}, []string{"entry_events"}, "entry 2 has no event"},
		{"an overdraft let through", func(b book, _ int64) []string {
			return []string{`ALTER TABLE accounts DROP CONSTRAINT accounts_no_overdraft`,
				`INSERT INTO journal_entries (description, posting_count) VALUES ('overdraft', 2)`,
				fmt.Sprintf(`INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
					VALUES (currval(pg_get_serial_sequence('journal_entries', 'id')), %d, 'USD', 4000, 0, 1),
					       (currval(pg_get_serial_sequence('journal_entries', 'id')), %d, 'USD', -4000, 0, 1)`,
					b.alice, b.bob)}
		}, []string{"protected_accounts"}, ""},
		// The kinds the first version of the checks missed (found in review on day 3).
		{"a posting's version moved", func(b book, _ int64) []string {
			return []string{`ALTER TABLE postings DISABLE TRIGGER postings_append_only`,
				fmt.Sprintf(`UPDATE postings SET account_version = account_version + 10 WHERE account_id = %d`, b.bob),
				`ALTER TABLE postings ENABLE TRIGGER postings_append_only`}
		}, []string{"account_chains"}, ""},
		{"an account's version moved", func(b book, _ int64) []string {
			return []string{`ALTER TABLE accounts DISABLE TRIGGER accounts_guard`,
				fmt.Sprintf(`UPDATE accounts SET version = version + 1 WHERE id = %d`, b.bob),
				`ALTER TABLE accounts ENABLE TRIGGER accounts_guard`}
		}, []string{"account_balances"}, ""},
		{"a balance_after at the end of bigint", func(b book, _ int64) []string {
			// The chain's sum went past bigint, and Reconcile failed instead of reporting it.
			return []string{`ALTER TABLE postings DISABLE TRIGGER postings_append_only`,
				fmt.Sprintf(`UPDATE postings SET balance_after = 9223372036854775807
					WHERE account_id = %d AND account_version = 1`, b.alice),
				`ALTER TABLE postings ENABLE TRIGGER postings_append_only`}
		}, []string{"account_chains"}, ""},
		{"postings relabelled to another currency", func(_ book, _ int64) []string {
			return []string{`ALTER TABLE postings DROP CONSTRAINT postings_account_currency_fkey`,
				`ALTER TABLE postings DISABLE TRIGGER postings_append_only`,
				`UPDATE postings SET currency = 'EUR' WHERE entry_id = 1`,
				`ALTER TABLE postings ENABLE TRIGGER postings_append_only`}
		}, []string{"posting_currencies"}, ""},
		{"an entry deleted, its postings, event and key left behind", func(_ book, payment int64) []string {
			return []string{`ALTER TABLE postings DROP CONSTRAINT postings_entry_fkey`,
				`ALTER TABLE outbox DROP CONSTRAINT outbox_entry_fkey`,
				`ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_entry_fkey`,
				`ALTER TABLE journal_entries DISABLE TRIGGER journal_entries_append_only`,
				fmt.Sprintf(`DELETE FROM journal_entries WHERE id = %d`, payment),
				`ALTER TABLE journal_entries ENABLE TRIGGER journal_entries_append_only`}
		}, []string{"orphan_rows"}, "key pay-1 names entry 2, which does not exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := storetest.Migrated(t)
			s := store.New(db)
			b := wallets(t, s)                                                             // entry 1: Alice tops up $50
			posted, _, err := s.PostEntry(context.Background(), "pay-1", payment(b, 2000)) // entry 2
			if err != nil {
				t.Fatal(err)
			}
			if r := reconcile(t, db); !r.Clean() {
				t.Fatalf("before the damage: %v", found(r))
			}
			damage(t, db, tc.damage(b, posted.ID)...)
			r := reconcile(t, db)
			if got := found(r); !slices.Equal(got, tc.want) {
				t.Fatalf("found %v, want %v: %+v", got, tc.want, r.Checks)
			}
			if tc.example != "" && !slices.ContainsFunc(r.Checks, func(c store.Check) bool {
				return slices.Contains(c.Examples, tc.example)
			}) {
				t.Fatalf("no check says %q: %+v", tc.example, r.Checks)
			}
		})
	}
}

// A check counts every difference and describes at most MaxExamples of them.
func TestAReportCountsEveryDifference(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	fundedWallets(t, s, 12, 1000) // 12 entries
	damage(t, db, `DELETE FROM outbox`)
	r := reconcile(t, db)
	i := slices.IndexFunc(r.Checks, func(c store.Check) bool { return c.Name == "entry_events" })
	if i < 0 {
		t.Fatalf("no entry_events check: %+v", r.Checks)
	}
	// In order of the entries' ids, the same on every platform: ordered as text, Linux's collation put "entry 10"
	// first in CI, and macOS put it after "entry 1".
	var want []string
	for id := 1; id <= store.MaxExamples; id++ {
		want = append(want, fmt.Sprintf("entry %d has no event", id))
	}
	if c := r.Checks[i]; c.Differences != 12 || !slices.Equal(c.Examples, want) {
		t.Fatalf("%d differences with examples %q; want 12, with examples %q", c.Differences, c.Examples, want)
	}
}

// Reconciling while entries are posted describes one moment: each report's counts agree with each other (every entry
// here has two postings and one event), and no check finds a difference.
func TestReconcilingWhilePostingSeesOneMoment(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := at(db, store.ReadCommitted)
	ids := fundedWallets(t, s, 4, 1_000_000)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var posted atomic.Int64
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				if _, _, err := s.PostEntry(ctx, fmt.Sprintf("w%d-%d", w, i), ledger.Entry{Postings: []ledger.Posting{
					pst(ids[w], "USD", 1), pst(ids[(w+1)%4], "USD", -1)}}); err != nil && ctx.Err() == nil {
					t.Errorf("transfer: %v", err)
					return
				}
				posted.Add(1)
			}
		})
	}
	reports := 0
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) || reports < 10; reports++ {
		r := reconcile(t, db)
		if !r.Clean() || r.Postings != 2*r.Entries || r.Events != r.Entries {
			stop()
			wg.Wait()
			t.Fatalf("report %d: found %v; %d entries, %d postings, %d events", reports, found(r), r.Entries,
				r.Postings, r.Events)
		}
	}
	stop()
	wg.Wait()
	if posted.Load() < 10 {
		t.Fatalf("only %d entries were posted while reconciling", posted.Load())
	}
	t.Logf("%d reports while %d entries were posted", reports, posted.Load())
}

// A healthy ledger at the edges reconciles clean (from the day-3 review): an account with no postings, an entry in
// two currencies, an account whose allow_negative was switched both ways, and balances at the ends of the range,
// +(2^63 - 1) and -(2^63 - 1), built from 9,224 entries.
func TestAHealthyLedgerAtTheEdgesReconciles(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	ctx := context.Background()
	open := func(name string, typ ledger.AccountType, cur string, neg bool) int64 {
		a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: cur, AllowNegative: neg})
		if err != nil {
			t.Fatal(err)
		}
		return a.ID
	}
	postE := func(key string, ps ...ledger.Posting) {
		if _, _, err := s.PostEntry(ctx, key, ledger.Entry{Postings: ps}); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
	}
	open("empty", ledger.Asset, "USD", false)
	usdCash, usdW := open("cash:usd", ledger.Asset, "USD", false), open("w:usd", ledger.Liability, "USD", false)
	eurW := open("w:eur", ledger.Liability, "EUR", false)
	fxU, fxE := open("fx:usd", ledger.Liability, "USD", true), open("fx:eur", ledger.Liability, "EUR", true)
	postE("fund", pst(usdCash, "USD", 1000), pst(usdW, "USD", -1000))
	postE("convert", pst(usdW, "USD", 500), pst(fxU, "USD", -500), pst(fxE, "EUR", 450), pst(eurW, "EUR", -450))
	tog := open("tog", ledger.Liability, "USD", true)
	postE("overdraw", pst(tog, "USD", 300), pst(fxU, "USD", -300))
	postE("back", pst(tog, "USD", -300), pst(fxU, "USD", 300))
	if _, err := db.Exec(ctx, `UPDATE accounts SET allow_negative = false WHERE id = $1`, tog); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE accounts SET allow_negative = true WHERE id = $1`, usdW); err != nil {
		t.Fatal(err)
	}
	postE("overdraw-2", pst(usdW, "USD", 900), pst(fxU, "USD", -900))
	open("big:asset", ledger.Asset, "JPY", false)
	open("big:liab", ledger.Liability, "JPY", false)
	if _, err := db.Exec(ctx, `
		DO $$
		DECLARE e bigint; a bigint; l bigint; amt bigint;
		BEGIN
		  SELECT id INTO a FROM accounts WHERE name = 'big:asset';
		  SELECT id INTO l FROM accounts WHERE name = 'big:liab';
		  FOR i IN 1..9224 LOOP
		    amt := CASE WHEN i <= 9223 THEN 1000000000000000 ELSE 372036854775807 END;
		    INSERT INTO journal_entries (description, posting_count) VALUES ('big', 2) RETURNING id INTO e;
		    INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
		    VALUES (e, a, 'JPY', amt, 0, 1), (e, l, 'JPY', -amt, 0, 1);
		  END LOOP;
		END $$`); err != nil {
		t.Fatal(err)
	}
	var big int64
	if err := db.QueryRow(ctx, `SELECT balance FROM accounts WHERE name = 'big:asset'`).Scan(&big); err != nil || big != 9223372036854775807 {
		t.Fatalf("big:asset %d, %v", big, err)
	}
	r := reconcile(t, db)
	if !r.Clean() {
		t.Fatalf("a healthy ledger: found %v: %+v", found(r), r.Checks)
	}
}
