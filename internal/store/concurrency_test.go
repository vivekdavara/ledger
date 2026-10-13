package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// Two entries with no account and no key in common must not make each other retry. One posts in full while the
// other holds its locks. When the key lookup ran inside the SERIALIZABLE transaction, both read the key index's
// one page and both wrote to it, Postgres saw a cycle, and the held entry had to retry (found in review).
func TestUnrelatedEntriesDontMakeEachOtherRetry(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		ids := fundedWallets(t, store.New(db), 4, 10000)
		s1, s2 := at(db, level), at(db, level)
		store.SetHook(s1, func(point string, attempt int) {
			if point == "locked" && attempt == 1 {
				if _, _, err := s2.PostEntry(context.Background(), "other", ledger.Entry{Postings: []ledger.Posting{
					pst(ids[2], "USD", 100), pst(ids[3], "USD", -100)}}); err != nil {
					t.Errorf("the unrelated entry: %v", err)
				}
			}
		})
		if _, _, err := s1.PostEntry(context.Background(), "held", ledger.Entry{Postings: []ledger.Posting{
			pst(ids[0], "USD", 100), pst(ids[1], "USD", -100)}}); err != nil {
			t.Fatal(err)
		}
		if s1.Retries() != 0 || s2.Retries() != 0 {
			t.Fatalf("retries %d and %d, want none", s1.Retries(), s2.Retries())
		}
		checkBooks(t, db)
	})
}

// Two different entries under one key, over different accounts, so no row lock orders them: both find the key
// unused, and the second to insert it hits its primary key (23505). That attempt is retried, finds the key, and is
// refused as a different entry.
func TestTwoEntriesRacingForOneKey(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		ids := fundedWallets(t, store.New(db), 4, 10000)
		s := at(db, level)
		var arrived sync.WaitGroup
		arrived.Add(2)
		store.SetHook(s, func(point string, attempt int) {
			if point == "looked up" && attempt == 1 {
				arrived.Done()
				arrived.Wait()
			}
		})
		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i := range 2 {
			wg.Go(func() {
				_, _, errs[i] = s.PostEntry(context.Background(), "shared", ledger.Entry{Postings: []ledger.Posting{
					pst(ids[2*i], "USD", 100), pst(ids[2*i+1], "USD", -100)}})
			})
		}
		wg.Wait()
		posted, refused := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				posted++
			case errors.Is(err, ledger.ErrKeyReused):
				refused++
			default:
				t.Fatalf("got %v", err)
			}
		}
		if posted != 1 || refused != 1 || s.Retries() != 1 {
			t.Fatalf("%d posted, %d refused, %d retries; want 1, 1, 1", posted, refused, s.Retries())
		}
		checkBooks(t, db)
	})
}

// A deadlock between an attempt and another session (40P01) is retried. The attempt holds the lower account and
// waits for the higher one, which a test transaction holds; the test transaction then asks for the lower one.
// The session whose deadlock check finds the cycle is the one aborted, so the test's session checks only after 30 s
// (deadlock_timeout, which the test databases' superuser may set) and the attempt, after the default 1 s, finds it.
func TestADeadlockIsRetried(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		ids := fundedWallets(t, store.New(db), 2, 10000)
		low, high := ids[0], ids[1]
		ctx := context.Background()
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SET LOCAL deadlock_timeout = '30s'`); err != nil {
			t.Fatal(err)
		}
		var holder int
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid() FROM accounts WHERE id = $1 FOR UPDATE`, high).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		s := at(db, level)
		done := make(chan error, 1)
		go func() {
			_, _, err := s.PostEntry(ctx, "deadlock", ledger.Entry{Postings: []ledger.Posting{
				pst(high, "USD", 100), pst(low, "USD", -100)}})
			done <- err
		}()
		if !waitUntilBlocked(db, holder, 1) {
			t.Fatal("the attempt never waited for the higher account")
		}
		// The cycle: the test waits for the attempt's lower account, and gets it once the attempt is aborted.
		if _, err := tx.Exec(ctx, `SELECT 1 FROM accounts WHERE id = $1 FOR UPDATE`, low); err != nil {
			t.Fatalf("the test's side: %v", err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("the entry: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("the entry never finished")
		}
		if s.Retries() != 1 {
			t.Fatalf("%d retries, want 1 (the deadlock)", s.Retries())
		}
		checkBooks(t, db)
	})
}

// The accounts are locked in id order whatever order the request lists them in. While an attempt waits for the
// lower account, the higher one is still free; while it waits for the higher one, it holds the lower.
func TestAccountsAreLockedInIDOrder(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		ids := fundedWallets(t, store.New(db), 2, 10000)
		low, high := ids[0], ids[1]
		ctx := context.Background()
		free := func(id int64) bool {
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			_, err = tx.Exec(ctx, `SELECT 1 FROM accounts WHERE id = $1 FOR UPDATE NOWAIT`, id)
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "55P03" {
				return false
			} else if err != nil {
				t.Fatal(err)
			}
			return true
		}
		for i, held := range []int64{low, high} {
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var holder int
			if err := tx.QueryRow(ctx, `SELECT pg_backend_pid() FROM accounts WHERE id = $1 FOR UPDATE`, held).Scan(&holder); err != nil {
				t.Fatal(err)
			}
			s := at(db, level)
			done := make(chan error, 1)
			go func() {
				// The higher account first in the request.
				_, _, err := s.PostEntry(ctx, fmt.Sprintf("order-%d", i), ledger.Entry{Postings: []ledger.Posting{
					pst(high, "USD", 100), pst(low, "USD", -100)}})
				done <- err
			}()
			if !waitUntilBlocked(db, holder, 1) {
				t.Fatal("the attempt never waited")
			}
			other := high
			if held == high {
				other = low
			}
			if wantFree := held == low; free(other) != wantFree {
				t.Errorf("waiting for account %d, account %d free: %v, want %v", held, other, !wantFree, wantFree)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		checkBooks(t, db)
	})
}

func TestTheKeyIsCheckedBeforeTheEntry(t *testing.T) {
	t.Parallel()
	s := store.New(storetest.Migrated(t))
	unbalanced := ledger.Entry{Postings: []ledger.Posting{pst(1, "USD", 100), pst(2, "USD", -99)}}
	if _, _, err := s.PostEntry(context.Background(), "has space", unbalanced); !errors.Is(err, ledger.ErrIdempotencyKey) {
		t.Fatalf("a bad key and a bad entry: %v, want the key's error first", err)
	}
}

func TestHistoryDefaultAndLargestPages(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	ids := fundedWallets(t, s, 2, 1000000)
	ctx := context.Background()
	for i := range store.DefaultPageSize {
		if _, _, err := s.PostEntry(ctx, fmt.Sprintf("p%d", i), ledger.Entry{Postings: []ledger.Posting{
			pst(ids[0], "USD", 1), pst(ids[1], "USD", -1)}}); err != nil {
			t.Fatal(err)
		}
	}
	// The wallet has its funding plus DefaultPageSize payments: one more than a default page.
	if page, err := s.History(ctx, ids[0], 0, 0); err != nil || len(page) != store.DefaultPageSize {
		t.Fatalf("default page: %d lines, %v; want %d", len(page), err, store.DefaultPageSize)
	}
	if page, err := s.History(ctx, ids[0], 0, store.MaxPageSize); err != nil || len(page) != store.DefaultPageSize+1 {
		t.Fatalf("the largest page: %d lines, %v; want all %d", len(page), err, store.DefaultPageSize+1)
	}
}

// TestDisjointTransfersRetries measures how often transfers that share nothing make each other retry, with the
// default policy: 16 workers, each moving money back and forth between its own two wallets, 25 times. It logs the
// retries and the requests that ran out of attempts, and fails only on a wrong result (README, "Results").
func TestDisjointTransfersRetries(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		const workers, each = 16, 25
		ids := fundedWallets(t, store.New(db), 2*workers, 100000)
		s := at(db, level)
		var wg sync.WaitGroup
		var mu sync.Mutex
		gaveUp := 0
		for w := range workers {
			wg.Go(func() {
				for i := range each {
					from, to := ids[2*w], ids[2*w+1]
					if i%2 == 1 {
						from, to = to, from
					}
					_, _, err := s.PostEntry(context.Background(), fmt.Sprintf("w%d-%d", w, i), ledger.Entry{
						Postings: []ledger.Posting{pst(from, "USD", 10), pst(to, "USD", -10)}})
					switch {
					case errors.Is(err, ledger.ErrConflict):
						mu.Lock()
						gaveUp++
						mu.Unlock()
					case err != nil:
						t.Errorf("worker %d transfer %d: %v", w, i, err)
					}
				}
			})
		}
		wg.Wait()
		checkBooks(t, db)
		t.Logf("%s: %d workers x %d transfers on their own accounts: %d retries, %d gave up", level, workers, each,
			s.Retries(), gaveUp)
	})
}

// History says a page is stable however many postings arrive while a client pages: the next page starts after the
// last version seen, so postings that arrive in between are neither skipped nor repeated.
func TestPagingIsStableWhilePostingsArrive(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	ids := fundedWallets(t, s, 2, 1000000)
	ctx := context.Background()
	pay := func(key string) {
		if _, _, err := s.PostEntry(ctx, key, ledger.Entry{Postings: []ledger.Posting{
			pst(ids[0], "USD", 1), pst(ids[1], "USD", -1)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		pay(fmt.Sprintf("before-%d", i))
	}
	var seen []int64
	after := int64(0)
	for page := 0; ; page++ {
		if page == 10 { // seven postings in pages of two: a page that repeats would otherwise loop forever
			t.Fatalf("still paging after 10 pages: %v", seen)
		}
		lines, err := s.History(ctx, ids[0], after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) == 0 {
			break
		}
		for _, l := range lines {
			seen = append(seen, l.Posting.AccountVersion)
		}
		after = lines[len(lines)-1].Posting.AccountVersion
		if page < 3 {
			pay(fmt.Sprintf("during-%d", page)) // arrives between pages
		}
	}
	// The funding, three payments before, three during: versions 1 to 7, each once, in order.
	want := []int64{1, 2, 3, 4, 5, 6, 7}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("versions seen %v, want %v", seen, want)
	}
}

// Ten creates of one name at once, with two types between them: one creates it, the callers asking for the same
// account get it back, and the others are told the name is taken.
func TestConcurrentCreatesWithClashingTypes(t *testing.T) {
	t.Parallel()
	s := store.New(storetest.Migrated(t))
	results := make([]error, 10)
	created := make([]bool, 10)
	var wg sync.WaitGroup
	for i := range 10 {
		typ := ledger.Asset
		if i%2 == 1 {
			typ = ledger.Expense
		}
		wg.Go(func() {
			_, created[i], results[i] = s.CreateAccount(context.Background(),
				ledger.NewAccount{Name: "contested", Type: typ, Currency: "USD"})
		})
	}
	wg.Wait()
	fresh, sameType, taken := 0, 0, 0
	var winner ledger.AccountType
	for i, err := range results {
		typ := ledger.Asset
		if i%2 == 1 {
			typ = ledger.Expense
		}
		switch {
		case err == nil && created[i]:
			fresh++
			winner = typ
		case err == nil:
			sameType++
		case errors.Is(err, ledger.ErrNameTaken):
			taken++
		default:
			t.Fatalf("create %d: %v", i, err)
		}
	}
	if fresh != 1 || sameType != 4 || taken != 5 {
		t.Fatalf("%d created, %d got the existing %s account, %d refused; want 1, 4, 5", fresh, sameType, winner, taken)
	}
}
