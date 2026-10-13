package store_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// wallets creates the accounts of DESIGN.md's examples through the service, and funds Alice with $50.
func wallets(t *testing.T, s *store.Store) book {
	t.Helper()
	ctx := context.Background()
	open := func(name string, typ ledger.AccountType) int64 {
		a, created, err := s.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: "USD"})
		if err != nil || !created {
			t.Fatalf("create %s: created %v, %v", name, created, err)
		}
		return a.ID
	}
	b := book{cash: open("cash:bank", ledger.Asset), alice: open("wallet:alice", ledger.Liability),
		bob: open("wallet:bob", ledger.Liability), fees: open("revenue:fees", ledger.Revenue)}
	if _, _, err := s.PostEntry(ctx, "top-up-1", ledger.Entry{Description: "Alice tops up $50",
		Postings: []ledger.Posting{pst(b.cash, "USD", 5000), pst(b.alice, "USD", -5000)}}); err != nil {
		t.Fatalf("top up: %v", err)
	}
	return b
}

func pst(account int64, currency string, amount int64) ledger.Posting {
	return ledger.Posting{AccountID: account, Currency: currency, Amount: amount}
}

func payment(b book, cents int64) ledger.Entry {
	fee := cents * 3 / 100
	return ledger.Entry{Description: fmt.Sprintf("Alice pays Bob %d cents", cents), Postings: []ledger.Posting{
		pst(b.alice, "USD", cents), pst(b.bob, "USD", -(cents - fee)), pst(b.fees, "USD", -fee)}}
}

func TestCreateAccountIsIdempotentByName(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	ctx := context.Background()
	want := ledger.NewAccount{Name: "wallet:alice", Type: ledger.Liability, Currency: "USD"}
	first, created, err := s.CreateAccount(ctx, want)
	if err != nil || !created {
		t.Fatalf("first: created %v, %v", created, err)
	}
	if first.ID <= 0 || first.Name != want.Name || first.Type != want.Type || first.Currency != "USD" ||
		first.Balance != 0 || first.Version != 0 || first.CreatedAt.IsZero() {
		t.Fatalf("first: %+v", first)
	}
	again, created, err := s.CreateAccount(ctx, want)
	if err != nil || created || again != first {
		t.Fatalf("again: %+v, created %v, %v; want the first account back", again, created, err)
	}
	for _, clash := range []ledger.NewAccount{
		{Name: want.Name, Type: ledger.Asset, Currency: "USD"},
		{Name: want.Name, Type: ledger.Liability, Currency: "EUR"},
		{Name: want.Name, Type: ledger.Liability, Currency: "USD", AllowNegative: true},
	} {
		if _, _, err := s.CreateAccount(ctx, clash); !errors.Is(err, ledger.ErrNameTaken) {
			t.Errorf("%+v: got %v, want ErrNameTaken", clash, err)
		}
	}
	if _, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: "x", Type: "cash", Currency: "USD"}); !errors.Is(err,
		ledger.ErrAccountType) {
		t.Errorf("unknown type: %v", err)
	}
	if n := count(t, db, `SELECT count(*) FROM accounts`); n != 1 {
		t.Fatalf("%d accounts, want 1", n)
	}
}

func TestConcurrentCreatesOfOneAccountMakeOne(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	ids := make([]int64, 8)
	createdCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			a, created, err := s.CreateAccount(context.Background(),
				ledger.NewAccount{Name: "revenue:fees", Type: ledger.Revenue, Currency: "USD"})
			if err != nil {
				t.Errorf("create %d: %v", i, err)
			}
			mu.Lock()
			defer mu.Unlock()
			ids[i] = a.ID
			if created {
				createdCount++
			}
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("ids %v, want one account", ids)
		}
	}
	if createdCount != 1 {
		t.Fatalf("%d calls created it, want 1", createdCount)
	}
}

func TestAccountShowsTheBalance(t *testing.T) {
	t.Parallel()
	s := store.New(storetest.Migrated(t))
	b := wallets(t, s)
	a, err := s.Account(context.Background(), b.alice)
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance != -5000 || a.Shown() != 5000 || a.Version != 1 {
		t.Fatalf("alice: balance %d, shown %d, version %d; want -5000, 5000, 1", a.Balance, a.Shown(), a.Version)
	}
	if _, err := s.Account(context.Background(), 999999); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("missing account: %v", err)
	}
}

func TestHistoryPagesByVersion(t *testing.T) {
	t.Parallel()
	s := store.New(storetest.Migrated(t))
	b := wallets(t, s)
	ctx := context.Background()
	for i := range 4 {
		if _, _, err := s.PostEntry(ctx, fmt.Sprintf("pay-%d", i), payment(b, int64(100*(i+1)))); err != nil {
			t.Fatal(err)
		}
	}
	// Alice: the top-up, then four payments. Pages of two: versions 1-2, 3-4, 5, then nothing.
	var versions [][]int64
	var balances []int64
	after := int64(0)
	for {
		page, err := s.History(ctx, b.alice, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		var v []int64
		for _, line := range page {
			v = append(v, line.Posting.AccountVersion)
			balances = append(balances, line.Posting.BalanceAfter)
			if line.Description == "" || line.PostedAt.IsZero() || line.Posting.AccountID != b.alice {
				t.Errorf("line %+v", line)
			}
		}
		versions = append(versions, v)
		after = page[len(page)-1].Posting.AccountVersion
	}
	if !reflect.DeepEqual(versions, [][]int64{{1, 2}, {3, 4}, {5}}) {
		t.Fatalf("pages %v", versions)
	}
	if !reflect.DeepEqual(balances, []int64{-5000, -4900, -4700, -4400, -4000}) {
		t.Fatalf("balances %v", balances)
	}
	if page, err := s.History(ctx, b.alice, 0, 0); err != nil || len(page) != 5 {
		t.Fatalf("default limit: %d lines, %v", len(page), err)
	}
	empty, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: "unused", Type: ledger.Asset, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if page, err := s.History(ctx, empty.ID, 0, 10); err != nil || len(page) != 0 {
		t.Fatalf("an account with no postings: %v, %v", page, err)
	}
	if _, err := s.History(ctx, 999999, 0, 10); !errors.Is(err, ledger.ErrAccountNotFound) {
		t.Fatalf("missing account: %v", err)
	}
	for _, bad := range []struct {
		after int64
		limit int
	}{{0, -1}, {0, store.MaxPageSize + 1}, {-1, 10}} {
		if _, err := s.History(ctx, b.alice, bad.after, bad.limit); !errors.Is(err, store.ErrPage) {
			t.Errorf("after %d limit %d: %v", bad.after, bad.limit, err)
		}
	}
}

func TestPostEntryReturnsWhatItPosted(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	// Postings in the request's order are fees, Bob, Alice; the result is in account-id order.
	e := payment(b, 2000)
	e.Postings[0], e.Postings[2] = e.Postings[2], e.Postings[0]
	posted, replayed, err := s.PostEntry(context.Background(), "pay-1", e)
	if err != nil || replayed {
		t.Fatalf("replayed %v, %v", replayed, err)
	}
	want := []ledger.PostedPosting{
		{AccountID: b.alice, Currency: "USD", Amount: 2000, BalanceAfter: -3000, AccountVersion: 2},
		{AccountID: b.bob, Currency: "USD", Amount: -1940, BalanceAfter: -1940, AccountVersion: 1},
		{AccountID: b.fees, Currency: "USD", Amount: -60, BalanceAfter: -60, AccountVersion: 1},
	}
	if posted.ID <= 0 || posted.Description != e.Description || posted.CreatedAt.IsZero() || len(posted.Postings) != 3 {
		t.Fatalf("posted %+v", posted)
	}
	for i, p := range posted.Postings {
		if p.ID <= 0 || p.EntryID != posted.ID {
			t.Errorf("posting %d: %+v", i, p)
		}
		p.ID, p.EntryID = 0, 0
		if p != want[i] {
			t.Errorf("posting %d: %+v, want %+v", i, p, want[i])
		}
	}
	checkBooks(t, db)
}

func TestARetriedRequestPostsOnce(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	ctx := context.Background()
	e := payment(b, 2000)
	first, _, err := s.PostEntry(ctx, "pay-1", e)
	if err != nil {
		t.Fatal(err)
	}
	// The same request again, and the same postings in another order: the original entry, posted once.
	reordered := e
	reordered.Postings = []ledger.Posting{e.Postings[2], e.Postings[0], e.Postings[1]}
	for _, retry := range []ledger.Entry{e, reordered} {
		again, replayed, err := s.PostEntry(ctx, "pay-1", retry)
		if err != nil || !replayed || !reflect.DeepEqual(again, first) {
			t.Fatalf("retry: replayed %v, %v\n got %+v\nwant %+v", replayed, err, again, first)
		}
	}
	if bal, ver := balanceOf(t, db, b.alice); bal != -3000 || ver != 2 {
		t.Fatalf("alice: balance %d version %d, want -3000 and 2 (one payment)", bal, ver)
	}
	// The same key for anything else is refused.
	other := payment(b, 2001)
	described := e
	described.Description = "something else"
	for name, different := range map[string]ledger.Entry{"another amount": other, "another description": described} {
		if _, _, err := s.PostEntry(ctx, "pay-1", different); !errors.Is(err, ledger.ErrKeyReused) {
			t.Errorf("%s: %v, want ErrKeyReused", name, err)
		}
	}
	if n := count(t, db, `SELECT count(*) FROM journal_entries`); n != 2 {
		t.Fatalf("%d entries, want the top-up and one payment", n)
	}
}

func TestRejectionsNameThePostingAndLeaveNoTrace(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	eur, _, err := s.CreateAccount(context.Background(),
		ledger.NewAccount{Name: "wallet:alice:eur", Type: ledger.Liability, Currency: "EUR"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		entry   ledger.Entry
		rule    error
		posting int
		detail  string
	}{
		{"unknown account", ledger.Entry{Postings: []ledger.Posting{pst(b.cash, "USD", 100), pst(999999, "USD", -100)}},
			ledger.ErrAccountNotFound, 1, "there is no account 999999"},
		{"wrong currency", ledger.Entry{Postings: []ledger.Posting{pst(eur.ID, "USD", 100), pst(b.bob, "USD", -100)}},
			ledger.ErrCurrencyMismatch, 0, "is in EUR, the posting is in USD"},
		{"overdraft", ledger.Entry{Postings: []ledger.Posting{pst(b.bob, "USD", -5001), pst(b.alice, "USD", 5001)}},
			ledger.ErrInsufficientFunds, 1, "has 50.00 USD, the posting takes 50.01"},
		{"unbalanced", ledger.Entry{Postings: []ledger.Posting{pst(b.alice, "USD", 100), pst(b.bob, "USD", -99)}},
			ledger.ErrUnbalanced, -1, "off in USD by 1"},
	}
	for _, tc := range cases {
		_, _, err := s.PostEntry(context.Background(), "key-"+strings.ReplaceAll(tc.name, " ", "-"), tc.entry)
		var verr *ledger.ValidationError
		if !errors.As(err, &verr) || !errors.Is(err, tc.rule) || verr.Posting != tc.posting ||
			!strings.Contains(err.Error(), tc.detail) {
			t.Errorf("%s: got %v, want %v at posting %d mentioning %q", tc.name, err, tc.rule, tc.posting, tc.detail)
		}
	}
	if n := count(t, db, `SELECT count(*) FROM journal_entries`); n != 1 {
		t.Fatalf("%d entries, want only the top-up", n)
	}
	// A rejected request's key isn't used up: it can post a valid entry.
	if _, replayed, err := s.PostEntry(context.Background(), "key-overdraft", payment(b, 100)); err != nil || replayed {
		t.Fatalf("reuse a rejected key: replayed %v, %v", replayed, err)
	}
	checkBooks(t, db)
}

func TestKeysAreCheckedBeforeAnythingElse(t *testing.T) {
	t.Parallel()
	s := store.New(storetest.Migrated(t))
	b := wallets(t, s)
	for _, key := range []string{"", "has space", "tab\t", "é", strings.Repeat("k", ledger.MaxKey+1)} {
		if _, _, err := s.PostEntry(context.Background(), key, payment(b, 100)); !errors.Is(err, ledger.ErrIdempotencyKey) {
			t.Errorf("key %q: %v", key, err)
		}
	}
	if _, _, err := s.PostEntry(context.Background(), strings.Repeat("k", ledger.MaxKey), payment(b, 100)); err != nil {
		t.Errorf("a %d-byte key: %v", ledger.MaxKey, err)
	}
}

func TestTheDatabaseChecksKeysToo(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	posted, _, err := s.PostEntry(context.Background(), "ok", payment(b, 100))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "has space", "é", strings.Repeat("k", ledger.MaxKey+1)} {
		_, err := db.Exec(context.Background(),
			`INSERT INTO idempotency_keys (key, request_hash, entry_id) VALUES ($1, $2, $3)`,
			key, make([]byte, 32), posted.ID)
		if constraint(err) != "idempotency_keys_key_format" {
			t.Errorf("key %q: %v", key, err)
		}
	}
	_, err = db.Exec(context.Background(), `INSERT INTO idempotency_keys (key, request_hash, entry_id) VALUES ('other', $1, $2)`,
		make([]byte, 32), posted.ID)
	if constraint(err) != "idempotency_keys_one_per_entry" {
		t.Errorf("a second key for one entry: %v", err)
	}
}

func TestASerializationFailureIsRetried(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s)
	// On the first attempt, after its snapshot, another request pays from Alice's wallet and commits. The first
	// attempt's lock on that row then fails with 40001, and the second attempt posts against the new balance.
	other := store.New(db)
	store.SetHook(s, func(point string, attempt int) {
		if point == "snapshot" && attempt == 1 {
			if _, _, err := other.PostEntry(context.Background(), "concurrent", payment(b, 1000)); err != nil {
				t.Errorf("concurrent payment: %v", err)
			}
		}
	})
	posted, replayed, err := s.PostEntry(context.Background(), "pay-1", payment(b, 2000))
	if err != nil || replayed {
		t.Fatalf("replayed %v, %v", replayed, err)
	}
	if s.Retries() != 1 {
		t.Fatalf("%d retries, want 1", s.Retries())
	}
	if got := posted.Postings[0]; got.AccountID != b.alice || got.BalanceAfter != -2000 || got.AccountVersion != 3 {
		t.Fatalf("alice's posting %+v, want balance -2000 at version 3", got)
	}
	checkBooks(t, db)
}

func TestRetriesRunOut(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.NewWithRetry(db, store.RetryPolicy{Attempts: 3})
	b := wallets(t, s)
	other := store.New(db)
	store.SetHook(s, func(point string, attempt int) {
		if point == "snapshot" {
			key := fmt.Sprintf("concurrent-%d", attempt)
			if _, _, err := other.PostEntry(context.Background(), key, payment(b, 100)); err != nil {
				t.Errorf("concurrent payment: %v", err)
			}
		}
	})
	_, _, err := s.PostEntry(context.Background(), "pay-1", payment(b, 2000))
	if !errors.Is(err, ledger.ErrConflict) || err.Error() != ledger.ErrConflict.Error()+
		": gave up after 3 attempts, the last ended by a serialization failure" {
		t.Fatalf("got %q, want ErrConflict after 3 attempts and no Postgres text", err)
	}
	// The cause is still there for code that looks for it.
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "40001" {
		t.Fatalf("the last attempt's 40001 isn't reachable: %v", err)
	}
	if s.Retries() != 2 {
		t.Fatalf("%d retries, want 2", s.Retries())
	}
	if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE key = 'pay-1'`); n != 0 {
		t.Fatalf("the key was stored for a request that failed")
	}
	checkBooks(t, db)
}

func TestARetryWaitsForTheContext(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.NewWithRetry(db, store.RetryPolicy{Attempts: 5, BaseDelay: time.Hour, MaxDelay: time.Hour})
	b := wallets(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	store.SetHook(s, func(point string, attempt int) {
		if point == "snapshot" && attempt == 1 {
			if _, _, err := store.New(db).PostEntry(context.Background(), "concurrent", payment(b, 100)); err != nil {
				t.Errorf("concurrent payment: %v", err)
			}
			time.AfterFunc(50*time.Millisecond, cancel)
		}
	})
	start := time.Now()
	if _, _, err := s.PostEntry(ctx, "pay-1", payment(b, 2000)); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if waited := time.Since(start); waited > 10*time.Second {
		t.Fatalf("waited %v after the cancel", waited)
	}
}

func TestConcurrentRetriesOfOneRequestPostOnce(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.NewWithRetry(db, store.RetryPolicy{Attempts: 50, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
	b := wallets(t, s)
	const n = 12
	// Every request reads the key as unused before any of them goes on, so they all race to post.
	var arrived sync.WaitGroup
	arrived.Add(n)
	store.SetHook(s, func(point string, attempt int) {
		if point == "snapshot" && attempt == 1 {
			arrived.Done()
			arrived.Wait()
		}
	})
	ids := make([]int64, n)
	fresh := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			posted, replayed, err := s.PostEntry(context.Background(), "pay-1", payment(b, 2000))
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[i] = posted.ID
			if !replayed {
				fresh++
			}
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("entry ids %v, want one entry", ids)
		}
	}
	if fresh != 1 {
		t.Fatalf("%d requests posted, want 1 (the rest replayed)", fresh)
	}
	if bal, ver := balanceOf(t, db, b.alice); bal != -3000 || ver != 2 {
		t.Fatalf("alice: balance %d version %d, want one payment", bal, ver)
	}
	if s.Retries() < n-1 {
		t.Fatalf("%d retries; each of the %d requests that lost the race should have retried", s.Retries(), n-1)
	}
}

// TestConcurrentTransfersKeepTheBooks runs random payments between a few accounts from many goroutines at once, so
// they collide on the same rows, and then recomputes the ledger.
func TestConcurrentTransfersKeepTheBooks(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.NewWithRetry(db, store.RetryPolicy{Attempts: 100, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
	ids := fundedWallets(t, s, 4, 10000)
	const workers, each = 8, 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	rejected := 0
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				from, to := ids[(w+i)%len(ids)], ids[(w+2*i+1)%len(ids)]
				if from == to {
					to = ids[(w+2*i+2)%len(ids)]
				}
				e := ledger.Entry{Description: "transfer", Postings: []ledger.Posting{
					pst(from, "USD", int64(100+37*i)), pst(to, "USD", -int64(100+37*i))}}
				_, _, err := s.PostEntry(context.Background(), fmt.Sprintf("w%d-%d", w, i), e)
				switch {
				case errors.Is(err, ledger.ErrInsufficientFunds):
					mu.Lock()
					rejected++
					mu.Unlock()
				case err != nil:
					t.Errorf("worker %d transfer %d: %v", w, i, err)
				}
			}
		})
	}
	wg.Wait()
	checkBooks(t, db)
	posted := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE key LIKE 'w%'`)
	if posted+rejected != workers*each {
		t.Fatalf("%d posted and %d rejected of %d", posted, rejected, workers*each)
	}
	t.Logf("%d posted, %d rejected for funds, %d retries", posted, rejected, s.Retries())
}

// fundedWallets creates n customer wallets and gives each one the same amount from a cash account.
func fundedWallets(t *testing.T, s *store.Store, n int, cents int64) []int64 {
	t.Helper()
	ctx := context.Background()
	cash, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: "cash:bank", Type: ledger.Asset, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, n)
	for i := range ids {
		a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("wallet:%d", i), Type: ledger.Liability,
			Currency: "USD"})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = a.ID
		if _, _, err := s.PostEntry(ctx, fmt.Sprintf("fund-%d", i), ledger.Entry{Postings: []ledger.Posting{
			pst(cash.ID, "USD", cents), pst(a.ID, "USD", -cents)}}); err != nil {
			t.Fatal(err)
		}
	}
	return ids
}
