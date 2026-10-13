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
	if !r.Clean() || len(r.Checks) != 8 {
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
	if c := r.Checks[i]; c.Differences != 12 || len(c.Examples) != store.MaxExamples || c.Examples[0] != "entry 1 has no event" {
		t.Fatalf("%d differences with examples %q; want 12, the first %d of them", c.Differences, c.Examples,
			store.MaxExamples)
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
