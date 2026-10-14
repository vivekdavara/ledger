package load_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/load"
	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// api is one of the ledger's two APIs, served in this process over a schema of the test's own.
type api struct {
	name   string
	client load.Client
}

// apis serves both APIs over one store on a fresh schema and returns a client of each, and the schema's pool.
func apis(t *testing.T) (*pgxpool.Pool, []api) {
	t.Helper()
	return apisAt(t, store.DefaultIsolation)
}

// apisAt is apis with the store posting at the given level.
func apisAt(t *testing.T, level store.Isolation) (*pgxpool.Pool, []api) {
	t.Helper()
	db := storetest.MigratedWithConns(t, 8)
	svc := store.NewWithOptions(db, store.Options{Isolation: level})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	httpSrv := httptest.NewServer(server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, svc, log))
	t.Cleanup(httpSrv.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpcapi.New(svc, log)
	go func() { _ = grpcSrv.Serve(ln) }()
	t.Cleanup(grpcSrv.Stop)
	rest := load.NewREST(httpSrv.URL, 8)
	grpcClient, err := load.NewGRPC(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rest.Close(); grpcClient.Close() })
	return db, []api{{"rest", rest}, {"grpc", grpcClient}}
}

// reconciled fails the test unless the schema's books reconcile.
func reconciled(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	report, err := store.Reconcile(context.Background(), db)
	if err != nil || !report.Clean() {
		t.Fatalf("reconcile: %v, %+v", err, report)
	}
}

func count(t *testing.T, db *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEveryLayoutPostsEachTransferOnceAndTheBooksAgree(t *testing.T) {
	t.Parallel()
	db, apis := apis(t)
	for _, a := range apis {
		for _, layout := range load.Layouts {
			cfg := load.Config{Layout: layout, Wallets: 10, Workers: 4, Transfers: 120, Pad: 20,
				Prefix: a.name + "-" + string(layout) + ":"}
			res, err := load.Run(context.Background(), a.client, cfg)
			if err != nil {
				t.Fatalf("%s %s: %v", a.name, layout, err)
			}
			if res.Posted != 120 || res.Replayed+res.GaveUp+res.Unknown+len(res.Refused) != 0 || len(res.Mismatches) > 0 {
				t.Fatalf("%s %s: %+v", a.name, layout, res)
			}
			if res.P50 <= 0 || res.P50 > res.P99 || res.P99 > res.Max {
				t.Fatalf("%s %s: percentiles %v %v %v", a.name, layout, res.P50, res.P99, res.Max)
			}
			if n := count(t, db, `SELECT count(*) FROM idempotency_keys WHERE key LIKE $1`, cfg.Prefix+"t%"); n != 120 {
				t.Fatalf("%s %s: %d transfer keys, want 120", a.name, layout, n)
			}
		}
	}
	// Each run: 2 + wallets accounts and 20 padding accounts; wallets funding entries, 10 padding entries and 120
	// transfers. Disjoint has 2 wallets per worker.
	wantEntries := 0
	for _, wallets := range []int{8, 10, 10} {
		wantEntries += wallets + 10 + 120
	}
	if n := count(t, db, `SELECT count(*) FROM journal_entries`); n != 2*wantEntries {
		t.Fatalf("%d entries, want %d", n, 2*wantEntries)
	}
	reconciled(t, db)
}

// faulty wraps a client and changes what the first request of every few transfers does, to show that Run's check
// and settling work. Requests sent again pass through unchanged.
type faulty struct {
	load.Client
	seen  sync.Map
	n     atomic.Int64
	every int64
	fault func(ctx context.Context, c load.Client, key string, e ledger.Entry) (load.Posted, error)
}

func (f *faulty) PostEntry(ctx context.Context, key string, e ledger.Entry) (load.Posted, error) {
	if _, again := f.seen.LoadOrStore(key, true); !again && strings.Contains(key, ":t") && f.n.Add(1)%f.every == 0 {
		return f.fault(ctx, f.Client, key, e)
	}
	return f.Client.PostEntry(ctx, key, e)
}

// errLost stands for an answer that never arrived: not an answer from the ledger, so its outcome is unknown.
var errLost = errors.New("connection reset by peer")

func TestTheCheckCatchesATransferPostedTwiceOrLost(t *testing.T) {
	t.Parallel()
	db, apis := apis(t)
	for name, fault := range map[string]func(context.Context, load.Client, string, ledger.Entry) (load.Posted, error){
		// The ledger posts it twice: what a broken idempotency key would do.
		"twice": func(ctx context.Context, c load.Client, key string, e ledger.Entry) (load.Posted, error) {
			if _, err := c.PostEntry(ctx, key+"-again", e); err != nil {
				return load.Posted{}, err
			}
			return c.PostEntry(ctx, key, e)
		},
		// The answer says posted, and nothing was.
		"lost": func(ctx context.Context, c load.Client, key string, e ledger.Entry) (load.Posted, error) {
			return load.Posted{EntryID: 1}, nil
		},
	} {
		c := &faulty{Client: apis[0].client, every: 7, fault: fault}
		res, err := load.Run(context.Background(), c, load.Config{Wallets: 10, Workers: 4, Transfers: 70,
			Prefix: name + ":"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Mismatches) == 0 {
			t.Fatalf("%s: the check found nothing: %+v", name, res)
		}
		t.Logf("%s: %d mismatches, the first: %s", name, len(res.Mismatches), res.Mismatches[0])
	}
	reconciled(t, db) // the ledger itself is consistent: it did what it was asked
}

func TestUnansweredTransfersAreSettledOnce(t *testing.T) {
	t.Parallel()
	_, apis := apis(t)
	for _, a := range apis {
		for _, retry := range []bool{false, true} {
			var lostAfter, lostBefore atomic.Int64
			c := &faulty{Client: a.client, every: 5, fault: func(ctx context.Context, c load.Client, key string,
				e ledger.Entry) (load.Posted, error) {
				// Alternately, the request is lost on its way (nothing posted) or its answer is (it posted).
				if lostAfter.Load() == lostBefore.Load() {
					lostAfter.Add(1)
					if _, err := c.PostEntry(ctx, key, e); err != nil {
						return load.Posted{}, err
					}
					return load.Posted{}, errLost
				}
				lostBefore.Add(1)
				return load.Posted{}, errLost
			}}
			res, err := load.Run(context.Background(), c, load.Config{Wallets: 10, Workers: 4, Transfers: 100,
				Retry: retry, Prefix: fmt.Sprintf("%s-%v:", a.name, retry)})
			if err != nil {
				t.Fatal(err)
			}
			lost := int(lostAfter.Load() + lostBefore.Load())
			// A transfer whose answer was lost had posted, so sending it again replays it. One lost on its way
			// hadn't, so sending it again posts it. Both end posted once, and the books agree.
			if lost != 20 || len(res.Mismatches) > 0 || res.Posted+res.Replayed != 100 ||
				res.Replayed != int(lostAfter.Load()) || res.Retries != lost {
				t.Fatalf("%s retry=%v: lost %d (%d after posting): %+v", a.name, retry, lost, lostAfter.Load(), res)
			}
			if retry && res.Unknown != 0 || !retry && res.Unknown != lost {
				t.Fatalf("%s retry=%v: %d unknown, %d lost", a.name, retry, res.Unknown, lost)
			}
		}
	}
}

func TestOutcomes(t *testing.T) {
	t.Parallel()
	_, apis := apis(t)
	ctx := context.Background()
	for _, a := range apis {
		alice, err := a.client.CreateAccount(ctx, ledger.NewAccount{Name: "alice", Type: ledger.Liability, Currency: "USD"})
		if err != nil {
			t.Fatal(err)
		}
		bob, err := a.client.CreateAccount(ctx, ledger.NewAccount{Name: "bob", Type: ledger.Liability, Currency: "USD"})
		if err != nil {
			t.Fatal(err)
		}
		if again, err := a.client.CreateAccount(ctx, ledger.NewAccount{Name: "alice", Type: ledger.Liability,
			Currency: "USD"}); err != nil || again != alice {
			t.Fatalf("%s: creating alice again: %d, %v", a.name, again, err)
		}
		pay := ledger.Entry{Postings: []ledger.Posting{{AccountID: alice, Currency: "USD", Amount: 100},
			{AccountID: bob, Currency: "USD", Amount: -100}}}
		_, err = a.client.PostEntry(ctx, a.name+"-overdraft", pay)
		if load.Classify(err) != load.Refused || load.Reason(err) != "insufficient_funds" {
			t.Fatalf("%s: overdraft: %v (%v)", a.name, err, load.Classify(err))
		}
		if b, err := a.client.Balance(ctx, alice); err != nil || b != 0 {
			t.Fatalf("%s: balance %d, %v", a.name, b, err)
		}
		if _, err := a.client.Balance(ctx, 1<<40); load.Classify(err) != load.Refused || load.Reason(err) != "not_found" {
			t.Fatalf("%s: a missing account: %v", a.name, err)
		}
	}

	// No answer at all: nothing listens there any more.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	rest := load.NewREST("http://"+addr, 1)
	grpcClient, err := load.NewGRPC(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer grpcClient.Close()
	for name, c := range map[string]load.Client{"rest": rest, "grpc": grpcClient} {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := c.Balance(ctx, 1)
		cancel()
		if load.Classify(err) != load.Unknown {
			t.Fatalf("%s: nothing listening: %v (%v)", name, err, load.Classify(err))
		}
	}

	// A conflict, and a 5xx that isn't the ledger's own answer.
	conflict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/entries" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"error": {"reason": "conflict", "message": "gave up"}}`)
			return
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}))
	defer conflict.Close()
	c := load.NewREST(conflict.URL, 1)
	if _, err := c.PostEntry(ctx, "k", ledger.Entry{}); load.Classify(err) != load.Conflict {
		t.Fatalf("conflict: %v", err)
	}
	if _, err := c.Balance(ctx, 1); load.Classify(err) != load.Unknown || load.Reason(err) != "unavailable" {
		t.Fatalf("502: %v (%v)", err, load.Classify(err))
	}
}

func TestConfigsThatCantRunAreRefused(t *testing.T) {
	t.Parallel()
	for _, cfg := range []load.Config{{Layout: "skewed"}, {Wallets: 1}, {Skew: 0.5}, {Skew: 1}, {Pad: -1}} {
		if _, err := load.Run(context.Background(), nil, cfg); err == nil {
			t.Errorf("%+v: no error", cfg)
		}
	}
}

func TestASkewConcentratesTheTransfers(t *testing.T) {
	t.Parallel()
	db, apis := apis(t)
	for _, skew := range []float64{0, 2} {
		prefix := fmt.Sprintf("skew-%v:", skew)
		res, err := load.Run(context.Background(), apis[1].client, load.Config{Layout: load.Hot, Wallets: 20,
			Workers: 4, Transfers: 200, Skew: skew, Prefix: prefix})
		if err != nil || len(res.Mismatches) > 0 {
			t.Fatalf("%v, %+v", err, res)
		}
		// The share of the transfers paid by the busiest wallet: about 1/20 uniformly, most of them at skew 2.
		busiest := count(t, db, `SELECT max(n) FROM (SELECT count(*) AS n FROM postings p JOIN accounts a
			ON a.id = p.account_id WHERE a.name LIKE $1 AND p.amount = 10 GROUP BY a.id) s`, prefix+"wallet:%")
		t.Logf("skew %v: the busiest wallet paid %d of 200", skew, busiest)
		if skew == 0 && busiest > 40 || skew == 2 && busiest < 100 {
			t.Fatalf("skew %v: the busiest wallet paid %d of 200", skew, busiest)
		}
	}
}

// Twelve copies of one payment at once, six over each API, for $40 of Alice's $50: only one can post, and every
// copy that looked the key up before it committed then finds $10 left. Each copy must still get the posted entry
// back, not "insufficient funds": the day-3 review's bug, end to end. Ten rounds at each level, each on a fresh
// Alice.
func TestConcurrentCopiesOverBothAPIsAllGetTheEntry(t *testing.T) {
	t.Parallel()
	for _, level := range []store.Isolation{store.ReadCommitted, store.Serializable} {
		t.Run(string(level), func(t *testing.T) {
			t.Parallel()
			_, apis := apisAt(t, level)
			ctx := context.Background()
			cash, err := apis[0].client.CreateAccount(ctx, ledger.NewAccount{Name: "cash", Type: ledger.Asset, Currency: "USD"})
			if err != nil {
				t.Fatal(err)
			}
			for round := range 10 {
				alice, err1 := apis[0].client.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("alice-%d", round),
					Type: ledger.Liability, Currency: "USD"})
				bob, err2 := apis[0].client.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("bob-%d", round),
					Type: ledger.Liability, Currency: "USD"})
				_, err3 := apis[0].client.PostEntry(ctx, fmt.Sprintf("fund-%d", round), ledger.Entry{
					Postings: []ledger.Posting{{AccountID: cash, Currency: "USD", Amount: 5000},
						{AccountID: alice, Currency: "USD", Amount: -5000}}})
				if err := errors.Join(err1, err2, err3); err != nil {
					t.Fatal(err)
				}
				pay := ledger.Entry{Postings: []ledger.Posting{{AccountID: alice, Currency: "USD", Amount: 4000},
					{AccountID: bob, Currency: "USD", Amount: -4000}}}
				const copies = 12
				results := make([]struct {
					posted load.Posted
					err    error
				}, copies)
				start := make(chan struct{})
				var wg sync.WaitGroup
				for i := range copies {
					wg.Go(func() {
						<-start
						results[i].posted, results[i].err = apis[i%2].client.PostEntry(ctx, fmt.Sprintf("pay-%d", round), pay)
					})
				}
				close(start)
				wg.Wait()
				fresh := 0
				for i, r := range results {
					if r.err != nil || r.posted.EntryID != results[0].posted.EntryID {
						t.Fatalf("round %d, copy %d over %s: entry %d, %v; want entry %d", round, i, apis[i%2].name,
							r.posted.EntryID, r.err, results[0].posted.EntryID)
					}
					if !r.posted.Replayed {
						fresh++
					}
				}
				if b, err := apis[1].client.Balance(ctx, alice); fresh != 1 || err != nil || b != -1000 {
					t.Fatalf("round %d: %d copies posted, alice %d, %v", round, fresh, b, err)
				}
			}
		})
	}
}
