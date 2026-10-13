package store_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"pgregory.net/rapid"
)

// TestPropertyEachKeyPostsOnce sends random requests concurrently, each several times, some with a different entry
// under the same key. Whatever the interleaving: each key posts exactly one entry, every send of that entry gets it
// back (one of them fresh, the rest replays), every send of another entry under the key is refused, and the books
// balance.
func TestPropertyEachKeyPostsOnce(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.NewWithRetry(db, store.RetryPolicy{Attempts: 50, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
	ctx := context.Background()
	// Accounts that may go negative, so every valid entry posts whatever order the sends land in.
	var accounts []int64
	for i := range 6 {
		a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("wallet:%d", i), Type: ledger.Liability,
			Currency: "USD", AllowNegative: true})
		if err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, a.ID)
	}
	entry := rapid.Custom(func(t *rapid.T) ledger.Entry {
		ids := rapid.SliceOfNDistinct(rapid.SampledFrom(accounts), 2, 4, rapid.ID[int64]).Draw(t, "accounts")
		e := ledger.Entry{Description: rapid.StringMatching(`[a-z]{0,8}`).Draw(t, "description")}
		var sum int64
		for i, id := range ids {
			amount := -sum
			if i < len(ids)-1 {
				amount = rapid.Int64Range(-5000, 5000).Filter(func(v int64) bool { return v != 0 && sum+v != 0 }).
					Draw(t, "amount")
			}
			sum += amount
			e.Postings = append(e.Postings, ledger.Posting{AccountID: id, Currency: "USD", Amount: amount})
		}
		return e
	})
	type send struct {
		key   string
		entry ledger.Entry
	}
	type result struct {
		send
		posted   ledger.PostedEntry
		replayed bool
		err      error
	}
	round := 0
	rapid.Check(t, func(t *rapid.T) {
		round++
		var sends []send
		keys := rapid.IntRange(1, 4).Draw(t, "keys")
		for k := range keys {
			key := fmt.Sprintf("r%d-k%d", round, k)
			e := entry.Draw(t, "entry")
			for range rapid.IntRange(1, 4).Draw(t, "retries") {
				sends = append(sends, send{key, e})
			}
			if rapid.Bool().Draw(t, "conflicting") {
				other := entry.Draw(t, "other")
				if other.Hash() != e.Hash() {
					sends = append(sends, send{key, other})
				}
			}
		}
		sends = rapid.Permutation(sends).Draw(t, "order")
		results := make([]result, len(sends))
		var wg sync.WaitGroup
		for i, sd := range sends {
			wg.Go(func() {
				posted, replayed, err := s.PostEntry(ctx, sd.key, sd.entry)
				results[i] = result{sd, posted, replayed, err}
			})
		}
		wg.Wait()

		byKey := make(map[string][]result)
		for _, r := range results {
			byKey[r.key] = append(byKey[r.key], r)
		}
		for key, rs := range byKey {
			var winner *result
			for i, r := range rs {
				if r.err == nil && !r.replayed {
					if winner != nil {
						t.Fatalf("key %s posted twice: entries %d and %d", key, winner.posted.ID, r.posted.ID)
					}
					winner = &rs[i]
				}
			}
			if winner == nil {
				t.Fatalf("key %s posted nothing: %+v", key, rs)
			}
			for _, r := range rs {
				switch same := r.entry.Hash() == winner.entry.Hash(); {
				case same && (r.err != nil || !reflect.DeepEqual(r.posted, winner.posted)):
					t.Fatalf("key %s: a send of the posted entry got %+v, %v; want entry %d", key, r.posted, r.err,
						winner.posted.ID)
				case !same && !errors.Is(r.err, ledger.ErrKeyReused):
					t.Fatalf("key %s: a send of a different entry got %v, want ErrKeyReused", key, r.err)
				}
			}
			var stored int64
			if err := db.QueryRow(ctx, `SELECT entry_id FROM idempotency_keys WHERE key = $1`, key).Scan(&stored); err != nil ||
				stored != winner.posted.ID {
				t.Fatalf("key %s: stored entry %d (%v), want %d", key, stored, err, winner.posted.ID)
			}
		}
	})
	keys := count(t, db, `SELECT count(*) FROM idempotency_keys`)
	if entries := count(t, db, `SELECT count(*) FROM journal_entries`); entries != keys {
		t.Errorf("%d entries for %d keys", entries, keys)
	}
	checkBooks(t, db)
	t.Logf("%d keys over %d rounds, %d retries", keys, round, s.Retries())
}
