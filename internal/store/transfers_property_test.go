package store_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"pgregory.net/rapid"
)

// TestPropertyConcurrentTransfersMatchAModel sends batches of random entries at once, in two currencies, over
// protected and unprotected accounts, so they collide on rows and some overdraw. After each batch the ledger must be
// what the accepted entries alone imply, whatever the interleaving:
//
//   - an entry is accepted, or refused for insufficient funds and leaves no trace; nothing else happens to it;
//   - each account's balance is its balance before the batch plus the accepted postings to it;
//   - the postings PostEntry returned for an account number its versions on from the batch's start with no gap or
//     repeat, and each one's balance_after is the one before plus its amount: one serial order explains them all;
//   - no protected account is overdrawn at any version, and every currency still sums to zero (checkBooks).
func TestPropertyConcurrentTransfersMatchAModel(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		db := storetest.Migrated(t)
		// Generous attempts: this tests the books, not the retry budget.
		s := at(db, level, store.RetryPolicy{Attempts: 1000, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
		ctx := context.Background()
		type acct struct {
			id       int64
			currency string
		}
		var accounts []acct
		open := func(name string, typ ledger.AccountType, currency string, allowNegative bool) int64 {
			a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: currency,
				AllowNegative: allowNegative})
			if err != nil {
				t.Fatal(err)
			}
			accounts = append(accounts, acct{a.ID, currency})
			return a.ID
		}
		for _, cur := range []string{"USD", "EUR"} {
			cash := open("cash:"+cur, ledger.Asset, cur, false)
			open("revenue:fees:"+cur, ledger.Revenue, cur, false)
			open("suspense:"+cur, ledger.Liability, cur, true)
			for i := range 3 {
				w := open(fmt.Sprintf("wallet:%d:%s", i, cur), ledger.Liability, cur, false)
				if _, _, err := s.PostEntry(ctx, fmt.Sprintf("fund-%d-%s", i, cur), ledger.Entry{Postings: []ledger.Posting{
					pst(cash, cur, 5000), pst(w, cur, -5000)}}); err != nil {
					t.Fatal(err)
				}
			}
		}
		// legs draws 2 or 3 balanced postings in one currency over distinct accounts.
		legs := func(t *rapid.T, cur string) []ledger.Posting {
			var pool []int64
			for _, a := range accounts {
				if a.currency == cur {
					pool = append(pool, a.id)
				}
			}
			ids := rapid.SliceOfNDistinct(rapid.SampledFrom(pool), 2, 3, rapid.ID[int64]).Draw(t, "accounts")
			var postings []ledger.Posting
			var sum int64
			for i, id := range ids {
				amount := -sum
				if i < len(ids)-1 {
					amount = rapid.Int64Range(-3000, 3000).Filter(func(v int64) bool { return v != 0 && sum+v != 0 }).
						Draw(t, "amount")
				}
				sum += amount
				postings = append(postings, ledger.Posting{AccountID: id, Currency: cur, Amount: amount})
			}
			return postings
		}
		entry := rapid.Custom(func(t *rapid.T) ledger.Entry {
			first := rapid.SampledFrom([]string{"USD", "EUR"}).Draw(t, "currency")
			e := ledger.Entry{Postings: legs(t, first)}
			if rapid.Bool().Draw(t, "two currencies") { // a conversion: balanced in each currency
				other := map[string]string{"USD": "EUR", "EUR": "USD"}[first]
				e.Postings = append(e.Postings, legs(t, other)...)
			}
			return e
		})

		round, accepted, refused := 0, 0, 0
		rapid.Check(t, func(rt *rapid.T) {
			round++
			before := map[int64]ledger.Account{}
			for _, a := range accounts {
				got, err := s.Account(ctx, a.id)
				if err != nil {
					rt.Fatal(err)
				}
				before[a.id] = got
			}
			batch := rapid.SliceOfN(entry, 1, 24).Draw(rt, "batch")
			type result struct {
				posted ledger.PostedEntry
				err    error
			}
			results := make([]result, len(batch))
			var wg sync.WaitGroup
			for i, e := range batch {
				wg.Go(func() {
					posted, _, err := s.PostEntry(ctx, fmt.Sprintf("r%d-%d", round, i), e)
					results[i] = result{posted, err}
				})
			}
			wg.Wait()

			want := map[int64]int64{}
			for id, a := range before {
				want[id] = a.Balance
			}
			byAccount := map[int64][]ledger.PostedPosting{}
			for i, r := range results {
				switch {
				case errors.Is(r.err, ledger.ErrInsufficientFunds):
					refused++
					continue
				case r.err != nil:
					rt.Fatalf("entry %d: %v", i, r.err)
				}
				accepted++
				for _, p := range r.posted.Postings {
					want[p.AccountID] += p.Amount
					byAccount[p.AccountID] = append(byAccount[p.AccountID], p)
				}
			}
			for id, b := range before {
				got, err := s.Account(ctx, id)
				if err != nil {
					rt.Fatal(err)
				}
				if got.Balance != want[id] {
					rt.Fatalf("account %d (%s): balance %d, but the accepted entries make it %d", id, got.Name,
						got.Balance, want[id])
				}
				if got.Overdraws(0) {
					rt.Fatalf("account %d (%s) is overdrawn: %d", id, got.Name, got.Balance)
				}
				postings := byAccount[id]
				slices.SortFunc(postings, func(x, y ledger.PostedPosting) int {
					return cmp.Compare(x.AccountVersion, y.AccountVersion)
				})
				balance := b.Balance
				for i, p := range postings {
					balance += p.Amount
					if p.AccountVersion != b.Version+int64(i)+1 || p.BalanceAfter != balance {
						rt.Fatalf("account %d: posting %d of the batch is version %d with balance %d after; want "+
							"version %d with %d", id, i, p.AccountVersion, p.BalanceAfter, b.Version+int64(i)+1, balance)
					}
				}
				if got.Version != b.Version+int64(len(postings)) {
					rt.Fatalf("account %d: version %d, want %d", id, got.Version, b.Version+int64(len(postings)))
				}
			}
		})
		if n := count(t, db, `SELECT count(*) FROM journal_entries WHERE id NOT IN (
			SELECT entry_id FROM idempotency_keys WHERE key LIKE 'fund-%')`); n != accepted {
			t.Errorf("%d entries in the ledger, %d accepted", n, accepted)
		}
		// No protected account was ever overdrawn, at any version.
		if n := count(t, db, `SELECT count(*) FROM postings p JOIN accounts a ON a.id = p.account_id
			WHERE NOT a.allow_negative AND CASE WHEN a.type IN ('asset', 'expense') THEN p.balance_after < 0
			                                    ELSE p.balance_after > 0 END`); n != 0 {
			t.Errorf("%d postings left a protected account overdrawn", n)
		}
		checkBooks(t, db)
		t.Logf("%s: %d rounds, %d entries accepted, %d refused for funds, %d retries", level, round, accepted,
			refused, s.Retries())
	})
}
