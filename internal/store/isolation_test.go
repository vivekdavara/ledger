package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

func TestParseIsolation(t *testing.T) {
	t.Parallel()
	for _, level := range levels {
		if got, err := store.ParseIsolation(string(level)); err != nil || got != level {
			t.Errorf("ParseIsolation(%q) = %q, %v", level, got, err)
		}
	}
	for _, bad := range []string{"", "SERIALIZABLE", "read committed", "repeatable-read", "read-uncommitted"} {
		if got, err := store.ParseIsolation(bad); err == nil {
			t.Errorf("ParseIsolation(%q) = %q, want an error", bad, got)
		}
	}
}

func TestAStoreRefusesAnUnknownLevel(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewWithOptions accepted an unknown level")
		}
	}()
	store.NewWithOptions(nil, store.Options{Isolation: "repeatable-read"})
}

func TestTheDefaultLevel(t *testing.T) {
	t.Parallel()
	if got := store.New(nil).Isolation(); got != store.DefaultIsolation {
		t.Fatalf("New posts at %q, want %q", got, store.DefaultIsolation)
	}
	if got := at(nil, store.ReadCommitted).Isolation(); got != store.ReadCommitted {
		t.Fatalf("got %q", got)
	}
}

// An attempt that waits for an account's row decides on the row as the other transaction committed it, not as it was
// when the attempt began. At read committed the row lock hands over the newest row and the attempt goes on, with no
// retry; at serializable the attempt fails with 40001 and its retry reads the new row. A check made on the older
// balance would pass the overdraft below, and the database's CHECK would then refuse it without naming the posting.
func TestAWaitingAttemptDecidesOnTheCommittedBalance(t *testing.T) {
	t.Parallel()
	atEachLevel(t, func(t *testing.T, level store.Isolation) {
		for _, tc := range []struct {
			name       string
			concurrent func(b book) (from, to, cents int64)
			pay        int64
			refused    bool
			balance    int64 // Alice's stored balance afterwards
		}{
			// Alice has $50 and spends $40 while the attempt waits: a $20 payment is now an overdraft.
			{"a spend makes it an overdraft", func(b book) (int64, int64, int64) { return b.alice, b.bob, 4000 },
				2000, true, -1000},
			// Alice has $50 and tops up $50 while the attempt waits: an $80 payment is now covered.
			{"a top-up covers it", func(b book) (int64, int64, int64) { return b.cash, b.alice, 5000 },
				8000, false, -2000},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				db := storetest.Migrated(t)
				s := at(db, level)
				b := wallets(t, s)
				store.SetHook(s, func(point string, attempt int) {
					if point == "begin" && attempt == 1 {
						from, to, cents := tc.concurrent(b)
						conflict(t, db, from, to, cents)
					}
				})
				_, _, err := s.PostEntry(context.Background(), "pay-1", payment(b, tc.pay))
				verr, isValidation := errors.AsType[*ledger.ValidationError](err)
				switch {
				case tc.refused && !(isValidation && errors.Is(err, ledger.ErrInsufficientFunds) && verr.Posting == 0):
					t.Fatalf("got %v; want insufficient funds naming posting 0, from the service's own check", err)
				case !tc.refused && err != nil:
					t.Fatalf("got %v, want the payment posted", err)
				}
				wantRetries := map[store.Isolation]int64{store.ReadCommitted: 0, store.Serializable: 1}[level]
				if s.Retries() != wantRetries {
					t.Fatalf("%d retries at %s, want %d", s.Retries(), level, wantRetries)
				}
				if bal, _ := balanceOf(t, db, b.alice); bal != tc.balance {
					t.Fatalf("alice's balance %d, want %d", bal, tc.balance)
				}
				checkBooks(t, db)
			})
		}
	})
}
