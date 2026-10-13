package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// retryWhileTheOriginalCommits posts original with key pay-1 and, from its commit hook, sends retry with the same key
// through a second store, then commits once the retry waits for the accounts' rows. It returns both results.
func retryWhileTheOriginalCommits(t *testing.T, level store.Isolation, original func(book) ledger.Entry,
	retry func(book) ledger.Entry) (first, second ledger.PostedEntry, replayed bool, err error) {
	t.Helper()
	db := storetest.Migrated(t)
	ctx := context.Background()
	s1, s2 := at(db, level), at(db, level)
	b := wallets(t, s1) // Alice has $50
	done := make(chan struct{})
	store.SetCommitHook(s1, func(tx pgx.Tx) error {
		var pid int
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		go func() {
			defer close(done)
			second, replayed, err = s2.PostEntry(ctx, "pay-1", retry(b))
		}()
		if !waitUntilBlocked(db, pid, 1) {
			t.Errorf("the retry never waited for the original's rows")
		}
		return nil
	})
	first, _, firstErr := s1.PostEntry(ctx, "pay-1", original(b))
	<-done
	if firstErr != nil {
		t.Fatalf("the original: %v", firstErr)
	}
	checkBooks(t, db)
	return first, second, replayed, err
}

// The retry of a $40 payment from Alice's $50, sent while the original holds her row. Once the original commits she
// has $10, so the retry's own check refuses it; but its key has posted, and it must replay. At read committed this
// is the ordinary contended case (found in review on day 3: the client was told the payment failed); at
// serializable the wait ends in 40001 and the retry's lookup finds the key.
func TestARetryThatWaitedForTheOriginalReplaysIt(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		pay := func(b book) ledger.Entry { return payment(b, 4000) }
		first, second, replayed, err := retryWhileTheOriginalCommits(t, level, pay, pay)
		if err != nil || !replayed || second.ID != first.ID {
			t.Fatalf("the retry got entry %d, replayed %v, %v; want a replay of entry %d", second.ID, replayed, err,
				first.ID)
		}
	})
}

// A different entry under a key that has posted is ErrKeyReused, not whatever the accounts would say of it.
func TestADifferentEntryWaitingOnAPostedKeyIsRefusedAsReused(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		_, _, _, err := retryWhileTheOriginalCommits(t, level,
			func(b book) ledger.Entry { return payment(b, 4000) },
			func(b book) ledger.Entry { return payment(b, 4100) })
		if !errors.Is(err, ledger.ErrKeyReused) {
			t.Fatalf("got %v, want ErrKeyReused", err)
		}
	})
}

// The narrow window, at both levels: the retry finds the key unused, and the original commits before the retry's
// transaction begins, so the retry sees Alice's $10 without waiting for anything.
func TestARetryThatLookedUpJustBeforeTheOriginalCommittedReplaysIt(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		ctx := context.Background()
		s1, s2 := at(db, level), at(db, level)
		b := wallets(t, s1)
		var first ledger.PostedEntry
		var firstErr error
		store.SetHook(s2, func(point string, attempt int) {
			if point == "looked up" && attempt == 1 {
				first, _, firstErr = s1.PostEntry(ctx, "pay-1", payment(b, 4000))
			}
		})
		second, replayed, err := s2.PostEntry(ctx, "pay-1", payment(b, 4000))
		if firstErr != nil {
			t.Fatalf("the original: %v", firstErr)
		}
		if err != nil || !replayed || second.ID != first.ID {
			t.Fatalf("the retry got entry %d, replayed %v, %v; want a replay of entry %d", second.ID, replayed, err,
				first.ID)
		}
		checkBooks(t, db)
	})
}

// A rejection that has nothing to do with a key is still reported as itself, and leaves no trace.
func TestARejectionWithAnUnusedKeyIsReported(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	_, _, err := s.PostEntry(context.Background(), "too-much", payment(b, 6000)) // Alice has $50
	if verr, ok := errors.AsType[*ledger.ValidationError](err); !ok || !errors.Is(err, ledger.ErrInsufficientFunds) ||
		verr.Posting != 0 {
		t.Fatalf("got %v, want insufficient funds naming posting 0", err)
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE key = 'too-much'`); n != 0 {
		t.Fatalf("the key was stored")
	}
}

// Twelve copies of one request at once, for $40 of Alice's $50, so only the first can afford it and every other
// copy's own check refuses it once the first has posted. Each must still get the posted entry back.
// TestConcurrentRetriesOfOneRequestPostOnce pays $20, which every copy's check allows, so it never tested this.
func TestConcurrentCopiesOfAPaymentOnlyOneCanAffordAllGetIt(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		s := at(db, level, store.RetryPolicy{Attempts: 50, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
		b := wallets(t, s)
		const n = 12
		var arrived sync.WaitGroup
		arrived.Add(n)
		store.SetHook(s, func(point string, attempt int) {
			if point == "looked up" && attempt == 1 {
				arrived.Done()
				arrived.Wait()
			}
		})
		results := make([]struct {
			posted   ledger.PostedEntry
			replayed bool
			err      error
		}, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				r := &results[i]
				r.posted, r.replayed, r.err = s.PostEntry(context.Background(), "pay-1", payment(b, 4000))
			})
		}
		wg.Wait()
		fresh := 0
		for i, r := range results {
			if r.err != nil || r.posted.ID != results[0].posted.ID {
				t.Fatalf("copy %d got entry %d, %v; want entry %d", i, r.posted.ID, r.err, results[0].posted.ID)
			}
			if !r.replayed {
				fresh++
			}
		}
		if fresh != 1 {
			t.Fatalf("%d copies posted, want 1", fresh)
		}
		if bal, ver := balanceOf(t, db, b.alice); bal != -1000 || ver != 2 {
			t.Fatalf("alice: balance %d version %d, want one $40 payment", bal, ver)
		}
		checkBooks(t, db)
	})
}
