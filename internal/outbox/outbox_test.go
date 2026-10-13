package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/outbox"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// sink records what it is given. deliver, when set, decides each delivery's outcome.
type sink struct {
	mu        sync.Mutex
	delivered []outbox.Event
	calls     int
	deliver   func(ctx context.Context, events []outbox.Event) error
}

func (s *sink) Deliver(ctx context.Context, events []outbox.Event) error {
	s.mu.Lock()
	s.calls++
	deliver := s.deliver
	s.mu.Unlock()
	if deliver != nil {
		if err := deliver(ctx, events); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered = append(s.delivered, events...)
	return nil
}

func (s *sink) events() []outbox.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]outbox.Event(nil), s.delivered...)
}

func ids(events []outbox.Event) []int64 {
	out := make([]int64, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}

func pst(account int64, cents int64) ledger.Posting {
	return ledger.Posting{AccountID: account, Currency: "USD", Amount: cents}
}

// ledgerWith returns a migrated ledger with n funded wallets and the store that funded them.
func ledgerWith(t *testing.T, n int) (*pgxpool.Pool, *store.Store, []int64) {
	t.Helper()
	db := storetest.Migrated(t)
	s := store.New(db)
	ctx := context.Background()
	cash, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: "cash", Type: ledger.Asset, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	wallets := make([]int64, n)
	for i := range wallets {
		a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("wallet:%d", i), Type: ledger.Liability,
			Currency: "USD"})
		if err != nil {
			t.Fatal(err)
		}
		wallets[i] = a.ID
		if _, _, err := s.PostEntry(ctx, fmt.Sprintf("fund-%d", i), ledger.Entry{Postings: []ledger.Posting{
			pst(cash.ID, 1_000_000), pst(a.ID, -1_000_000)}}); err != nil {
			t.Fatal(err)
		}
	}
	return db, s, wallets
}

func unpublished(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEachEventIsPublishedOnceInOrder(t *testing.T) {
	t.Parallel()
	db, s, wallets := ledgerWith(t, 2) // two funding entries
	ctx := context.Background()
	var posted []ledger.PostedEntry
	for i := range 3 {
		e, _, err := s.PostEntry(ctx, fmt.Sprintf("pay-%d", i), ledger.Entry{Description: fmt.Sprintf("payment %d", i),
			Postings: []ledger.Posting{pst(wallets[0], 100), pst(wallets[1], -100)}})
		if err != nil {
			t.Fatal(err)
		}
		posted = append(posted, e)
	}
	sk := &sink{}
	p := outbox.New(db, sk, outbox.Options{BatchSize: 2})
	for i, want := range []int{2, 2, 1, 0} {
		if n, err := p.PublishOnce(ctx); err != nil || n != want {
			t.Fatalf("batch %d: published %d, %v; want %d", i, n, err, want)
		}
	}
	got := sk.events()
	if len(got) != 5 || sk.calls != 3 {
		t.Fatalf("%d events in %d deliveries, want 5 in 3", len(got), sk.calls)
	}
	for i := 1; i < len(got); i++ {
		if got[i].ID <= got[i-1].ID {
			t.Fatalf("event ids %v are not in order", ids(got))
		}
	}
	// The last three events are the payments, as PostEntry returned them.
	for i, e := range got[2:] {
		want := posted[i]
		if e.Type != outbox.EntryPosted || e.EntryID != want.ID || e.Description != want.Description ||
			!e.PostedAt.Equal(want.CreatedAt) || len(e.Postings) != len(want.Postings) {
			t.Fatalf("event %+v for entry %+v", e, want)
		}
		for j, p := range e.Postings {
			w := want.Postings[j]
			if p != (outbox.Posting{ID: w.ID, AccountID: w.AccountID, Currency: w.Currency, Amount: w.Amount,
				BalanceAfter: w.BalanceAfter, AccountVersion: w.AccountVersion}) {
				t.Fatalf("event %d posting %d: %+v, want %+v", e.ID, j, p, w)
			}
		}
	}
	if n := unpublished(t, db); n != 0 {
		t.Fatalf("%d events still unpublished", n)
	}
	var attempts int
	if err := db.QueryRow(ctx, `SELECT max(attempts) FROM outbox`).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempts %d, %v; want 1 each", attempts, err)
	}
}

func TestAFailedDeliveryIsRecordedAndRetried(t *testing.T) {
	t.Parallel()
	db, _, _ := ledgerWith(t, 3)
	ctx := context.Background()
	sk := &sink{deliver: func(context.Context, []outbox.Event) error { return errors.New("the broker is down") }}
	p := outbox.New(db, sk, outbox.Options{})
	if n, err := p.PublishOnce(ctx); n != 0 || err == nil || !strings.Contains(err.Error(), "the broker is down") {
		t.Fatalf("published %d, %v; want the sink's error", n, err)
	}
	var attempts int
	var lastError string
	if err := db.QueryRow(ctx, `SELECT min(attempts), min(last_error) FROM outbox WHERE published_at IS NULL`).
		Scan(&attempts, &lastError); err != nil || attempts != 1 || lastError != "the broker is down" {
		t.Fatalf("attempts %d, last error %q, %v", attempts, lastError, err)
	}
	if n := unpublished(t, db); n != 3 {
		t.Fatalf("%d unpublished, want all 3", n)
	}
	sk.deliver = nil // the broker is back
	if n, err := p.PublishOnce(ctx); n != 3 || err != nil {
		t.Fatalf("published %d, %v; want 3", n, err)
	}
	var cleared int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE attempts = 2 AND last_error IS NULL`).Scan(&cleared); err != nil ||
		cleared != 3 {
		t.Fatalf("%d events with 2 attempts and the error cleared, %v; want 3", cleared, err)
	}
}

// Delivery is at least once: when the sink took the events but the publisher's mark never committed (here its
// context is cancelled the moment the sink returns, as in a crash), the events are delivered again, and a consumer
// that dedupes by id sees each one once.
func TestEventsWhoseMarkIsLostAreDeliveredAgain(t *testing.T) {
	t.Parallel()
	db, _, _ := ledgerWith(t, 3)
	ctx, crash := context.WithCancel(context.Background())
	sk := &sink{}
	sk.deliver = func(context.Context, []outbox.Event) error {
		sk.deliver = nil // see below: only the first delivery crashes
		defer crash()
		return nil
	}
	p := outbox.New(db, sk, outbox.Options{})
	if n, err := p.PublishOnce(ctx); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("published %d, %v; want the mark to fail", n, err)
	}
	if n := unpublished(t, db); n != 3 {
		t.Fatalf("%d unpublished after the crash, want 3", n)
	}
	if n, err := p.PublishOnce(context.Background()); n != 3 || err != nil {
		t.Fatalf("after the restart: published %d, %v; want 3", n, err)
	}
	got := sk.events()
	if len(got) != 6 {
		t.Fatalf("%d deliveries of events, want each of the 3 twice", len(got))
	}
	if once := outbox.Dedupe(map[int64]bool{}, got); len(once) != 3 || fmt.Sprint(ids(once)) != fmt.Sprint(ids(got[:3])) {
		t.Fatalf("deduped %v from %v", ids(once), ids(got))
	}
}

// Two publishers never deliver the same event while the first holds it: the second skips the claimed rows and takes
// the next ones, without waiting.
func TestTwoPublishersShareTheWork(t *testing.T) {
	t.Parallel()
	db, _, _ := ledgerWith(t, 4)
	ctx := context.Background()
	holding, release := make(chan struct{}), make(chan struct{})
	slow := &sink{deliver: func(context.Context, []outbox.Event) error {
		close(holding)
		<-release
		return nil
	}}
	fast := &sink{}
	first := outbox.New(db, slow, outbox.Options{BatchSize: 2})
	second := outbox.New(db, fast, outbox.Options{BatchSize: 10})
	done := make(chan error, 1)
	go func() {
		_, err := first.PublishOnce(ctx)
		done <- err
	}()
	<-holding
	letGo := sync.OnceFunc(func() { close(release) })
	defer letGo()
	// Without SKIP LOCKED the second publisher would wait for the first's rows, and the first waits for the test.
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := second.PublishOnce(bounded)
	if err != nil || n != 2 {
		t.Fatalf("the second publisher published %d, %v; want the 2 the first doesn't hold, at once", n, err)
	}
	letGo()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	a, b := ids(slow.events()), ids(fast.events())
	if len(a) != 2 || len(b) != 2 || a[1] >= b[0] {
		t.Fatalf("first delivered %v, second %v; want the oldest two and the next two", a, b)
	}
	if n := unpublished(t, db); n != 0 {
		t.Fatalf("%d unpublished", n)
	}
}

// One publisher delivers each account's postings in version order while entries are posted concurrently: the service
// locks an entry's accounts before it inserts the entry, so a later version's event is written after the earlier
// version's entry committed, and a publisher that can see the later event can see the earlier one.
func TestOnePublisherKeepsEachAccountsOrder(t *testing.T) {
	t.Parallel()
	for _, level := range []store.Isolation{store.ReadCommitted, store.Serializable} {
		t.Run(string(level), func(t *testing.T) {
			t.Parallel()
			db, _, wallets := ledgerWith(t, 4)
			// Generous attempts: this tests the order of the events, not the retry budget.
			s := store.NewWithOptions(db, store.Options{Isolation: level,
				Retry: &store.RetryPolicy{Attempts: 1000, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}})
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			sk := &sink{}
			p := outbox.New(db, sk, outbox.Options{BatchSize: 3, PollInterval: time.Millisecond})
			running := make(chan error, 1)
			go func() { running <- p.Run(ctx, func(err error) { t.Errorf("publish: %v", err) }) }()

			var wg sync.WaitGroup
			for w := range 6 {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(w), 2))
					for i := range 30 {
						x := rng.IntN(len(wallets))
						y := (x + 1 + rng.IntN(len(wallets)-1)) % len(wallets)
						if _, _, err := s.PostEntry(context.Background(), fmt.Sprintf("w%d-%d", w, i), ledger.Entry{
							Postings: []ledger.Posting{pst(wallets[x], 1), pst(wallets[y], -1)}}); err != nil {
							t.Errorf("transfer: %v", err)
							return
						}
					}
				})
			}
			wg.Wait()
			for deadline := time.Now().Add(10 * time.Second); unpublished(t, db) > 0; time.Sleep(5 * time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatalf("%d events still unpublished after 10 s", unpublished(t, db))
				}
			}
			stop()
			if err := <-running; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run returned %v, want context.Canceled", err)
			}
			got := sk.events()
			if len(got) != 4+6*30 {
				t.Fatalf("%d events delivered, want %d", len(got), 4+6*30)
			}
			if err := outbox.CheckOrder(got); err != nil {
				t.Fatal(err)
			}
			// The stream is a complete record: a consumer that dedupes it and adds up each account's postings, in
			// the order they arrive, ends with every account's balance, and each posting's balance_after is the
			// running total so far.
			balances := map[int64]int64{}
			for _, e := range outbox.Dedupe(map[int64]bool{}, got) {
				for _, p := range e.Postings {
					balances[p.AccountID] += p.Amount
					if p.BalanceAfter != balances[p.AccountID] {
						t.Fatalf("event %d: account %d's balance_after %d, the stream so far gives %d", e.ID,
							p.AccountID, p.BalanceAfter, balances[p.AccountID])
					}
				}
			}
			for id, rebuilt := range balances {
				a, err := s.Account(context.Background(), id)
				if err != nil || a.Balance != rebuilt {
					t.Fatalf("account %d: balance %d (%v), the stream gives %d", id, a.Balance, err, rebuilt)
				}
			}
			if len(balances) != 5 { // cash and the four wallets
				t.Fatalf("the stream names %d accounts, want 5", len(balances))
			}
		})
	}
}

func TestRunBacksOffAfterFailuresAndCarriesOn(t *testing.T) {
	t.Parallel()
	db, _, _ := ledgerWith(t, 2)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	failures := 0
	sk := &sink{}
	sk.deliver = func(context.Context, []outbox.Event) error {
		if failures < 3 {
			failures++
			return errors.New("down")
		}
		return nil
	}
	p := outbox.New(db, sk, outbox.Options{PollInterval: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond})
	var logged []error
	var mu sync.Mutex
	running := make(chan error, 1)
	start := time.Now()
	go func() {
		running <- p.Run(ctx, func(err error) { mu.Lock(); logged = append(logged, err); mu.Unlock() })
	}()
	for deadline := time.Now().Add(10 * time.Second); unpublished(t, db) > 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("never published")
		}
	}
	// Three failures waited 10, 20 and 40 ms before the delivery that worked.
	if took := time.Since(start); took < 70*time.Millisecond {
		t.Fatalf("published after %v; the backoff after three failures is at least 70 ms", took)
	}
	stop()
	if err := <-running; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logged) != 3 {
		t.Fatalf("%d failures logged, want 3", len(logged))
	}
}

func TestJSONLinesWritesOneEventALine(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	events := []outbox.Event{
		{ID: 1, Type: outbox.EntryPosted, EntryID: 7, PostedAt: time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC),
			Postings: []outbox.Posting{{ID: 1, AccountID: 2, Currency: "USD", Amount: 100, BalanceAfter: 100,
				AccountVersion: 1}}},
		{ID: 2, Type: outbox.EntryPosted, EntryID: 8},
	}
	if err := (&outbox.JSONLines{W: &buf}).Deliver(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], `{"id":1,"type":"entry_posted","entry_id":7,`) ||
		!strings.Contains(lines[0], `"postings":[{"posting_id":1,"account_id":2,"currency":"USD","amount":100,`+
			`"balance_after":100,"account_version":1}]`) {
		t.Fatalf("got %q", buf.String())
	}
	var back outbox.Event
	if err := json.Unmarshal([]byte(lines[0]), &back); err != nil || back.ID != 1 || len(back.Postings) != 1 {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
}

func TestCheckOrder(t *testing.T) {
	t.Parallel()
	event := func(id int64, versions ...[2]int64) outbox.Event {
		e := outbox.Event{ID: id}
		for _, v := range versions {
			e.Postings = append(e.Postings, outbox.Posting{AccountID: v[0], AccountVersion: v[1]})
		}
		return e
	}
	for _, tc := range []struct {
		name   string
		events []outbox.Event
		ok     bool
	}{
		{"in order", []outbox.Event{event(1, [2]int64{1, 1}, [2]int64{2, 1}), event(2, [2]int64{1, 2})}, true},
		{"a consumer joining mid-stream", []outbox.Event{event(9, [2]int64{1, 5}), event(10, [2]int64{1, 6})}, true},
		{"a gap", []outbox.Event{event(1, [2]int64{1, 1}), event(3, [2]int64{1, 3})}, false},
		{"swapped", []outbox.Event{event(2, [2]int64{1, 2}), event(1, [2]int64{1, 1})}, false},
		{"a repeat", []outbox.Event{event(1, [2]int64{1, 1}), event(1, [2]int64{1, 1})}, false},
	} {
		if err := outbox.CheckOrder(tc.events); (err == nil) != tc.ok || (err != nil && !errors.Is(err, outbox.ErrOutOfOrder)) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}

func TestADeliveryErrorIsMadeStorable(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 499) + "é and more" // the é is bytes 499 and 500
	for _, tc := range []struct{ in, want string }{
		{"the broker is down", "the broker is down"},
		{long, strings.Repeat("x", 499)},
		{"bad frame \x00\x01", "bad frame \x01"},
		{"not UTF-8: \xff", "not UTF-8: �"},
	} {
		got := outbox.Recordable(tc.in)
		if got != tc.want || !utf8.ValidString(got) || len(got) > 500 {
			t.Errorf("Recordable(%.40q) = %.40q, want %.40q", tc.in, got, tc.want)
		}
	}
}

// A failed delivery is recorded whatever its error says: cut at byte 500 alone, a character across the cut or a NUL
// anywhere made Postgres refuse the record (22021), and the attempt went unrecorded without a word.
func TestAnyDeliveryErrorIsRecorded(t *testing.T) {
	t.Parallel()
	for _, message := range []string{strings.Repeat("x", 499) + "é and more", "bad frame \x00\x01"} {
		db, _, _ := ledgerWith(t, 2)
		ctx := context.Background()
		failure := errors.New(message)
		sk := &sink{deliver: func(context.Context, []outbox.Event) error { return failure }}
		if _, err := outbox.New(db, sk, outbox.Options{}).PublishOnce(ctx); !errors.Is(err, failure) ||
			strings.Contains(err.Error(), "recording the attempt failed") {
			t.Fatalf("%.30q: got %v, want the delivery's error alone", message, err)
		}
		var attempts int
		var lastError string
		if err := db.QueryRow(ctx, `SELECT min(attempts), min(last_error) FROM outbox`).Scan(&attempts, &lastError); err != nil ||
			attempts != 1 || lastError != outbox.Recordable(message) {
			t.Fatalf("%.30q: attempts %d, last_error %.30q, %v", message, attempts, lastError, err)
		}
	}
}

// shortWriter takes only the first budget bytes of the write that would pass them, fails it, and takes everything
// after that (a full disk that was then cleared).
type shortWriter struct {
	buf    bytes.Buffer
	budget int
	failed bool
}

func (w *shortWriter) Write(p []byte) (int, error) {
	if !w.failed && len(p) > w.budget {
		w.failed = true
		w.buf.Write(p[:w.budget])
		return w.budget, errors.New("no space left on device")
	}
	if !w.failed {
		w.budget -= len(p)
	}
	return w.buf.Write(p)
}

// A write cut off partway through a line: the events are delivered again, and a consumer that reads lines and skips
// those that don't parse gets every one of them. Before the fix, the retry's first line was glued onto the fragment,
// and that event never reached the consumer, though it was marked published.
func TestALineCutOffByAFailedWriteLosesNoEvent(t *testing.T) {
	t.Parallel()
	for _, budget := range []int{20, 300} { // inside the first event's line, and inside the second's
		db, _, _ := ledgerWith(t, 3)
		ctx := context.Background()
		w := &shortWriter{budget: budget}
		p := outbox.New(db, &outbox.JSONLines{W: w}, outbox.Options{})
		if _, err := p.PublishOnce(ctx); err == nil {
			t.Fatal("the short write didn't fail the delivery")
		}
		if n, err := p.PublishOnce(ctx); n != 3 || err != nil {
			t.Fatalf("budget %d: published %d, %v; want 3", budget, n, err)
		}
		seen, unparsed := map[int64]bool{}, 0
		for _, line := range strings.Split(strings.TrimSuffix(w.buf.String(), "\n"), "\n") {
			var e outbox.Event
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				unparsed++
				continue
			}
			seen[e.ID] = true
		}
		if len(seen) != 3 || unparsed != 1 {
			t.Fatalf("budget %d: a line consumer read %d of 3 events and skipped %d lines; output %q", budget,
				len(seen), unparsed, w.buf.String())
		}
	}
}

// A stop lets the batch in flight finish, delivered and marked: here the stop comes while the sink holds the batch.
func TestAStopLetsTheBatchInFlightFinish(t *testing.T) {
	t.Parallel()
	db, _, _ := ledgerWith(t, 3)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	inDelivery := make(chan struct{})
	sk := &sink{deliver: func(context.Context, []outbox.Event) error {
		close(inDelivery) // the 3 events are one batch, so this runs once
		<-ctx.Done()      // the stop arrives during the delivery
		return nil
	}}
	running := make(chan error, 1)
	go func() { running <- outbox.New(db, sk, outbox.Options{}).Run(ctx, nil) }()
	<-inDelivery
	stop()
	if err := <-running; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if n := unpublished(t, db); n != 0 || len(sk.events()) != 3 {
		t.Fatalf("%d of %d delivered events left unpublished by the stop", n, len(sk.events()))
	}
}

func TestTheBackoffDoublesUpToItsCap(t *testing.T) {
	t.Parallel()
	o := outbox.Options{PollInterval: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	var waits []time.Duration
	for wait, i := time.Duration(0), 0; i < 5; i++ {
		wait = outbox.NextBackoff(o, wait)
		waits = append(waits, wait)
	}
	want := []time.Duration{10, 20, 40, 50, 50}
	for i := range want {
		want[i] *= time.Millisecond
	}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Fatalf("waits %v, want %v", waits, want)
	}
}

// A connection lost during a delivery loses nothing: here the publisher's backend is terminated while the sink holds
// the batch, the mark can't commit, and the next batch, on a new connection, delivers the events again.
func TestALostConnectionDuringDeliveryLosesNoEvent(t *testing.T) {
	t.Parallel()
	db, _, _ := ledgerWith(t, 3)
	ctx := context.Background()
	// A pool of its own, with a name, so the test ends exactly the publisher's backend and no other test's.
	name := fmt.Sprintf("outbox-lost-connection-%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(storetest.SchemaURL(t, db))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = name
	publisherDB, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer publisherDB.Close()
	sk := &sink{}
	lost := false
	sk.deliver = func(context.Context, []outbox.Event) error {
		if !lost {
			lost = true
			// The pid first, then that one backend: Postgres doesn't promise to filter on the name before it calls
			// pg_terminate_backend in the same WHERE clause, and a first version of this test ended its own session.
			var pid int
			ended := false
			err := db.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE application_name = $1`, name).Scan(&pid)
			if err == nil {
				err = db.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&ended)
			}
			if err != nil || !ended {
				t.Errorf("ending the publisher's backend: %v, %v", ended, err)
			}
		}
		return nil
	}
	p := outbox.New(publisherDB, sk, outbox.Options{})
	if n, err := p.PublishOnce(ctx); n != 0 || err == nil {
		t.Fatalf("published %d, %v; want the mark to fail with the connection", n, err)
	}
	if n := unpublished(t, db); n != 3 {
		t.Fatalf("%d unpublished after the lost connection, want 3", n)
	}
	if n, err := p.PublishOnce(ctx); n != 3 || err != nil {
		t.Fatalf("on a new connection: published %d, %v; want 3", n, err)
	}
	if got := sk.events(); len(got) != 6 || len(outbox.Dedupe(map[int64]bool{}, got)) != 3 {
		t.Fatalf("%d deliveries, want each of the 3 events twice", len(got))
	}
}
