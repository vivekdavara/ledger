package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/faults"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/load"
	"github.com/vivekdavara/ledger/internal/outbox"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// The tests in this file crash ledgerd for real: they run it as a child process, built with -tags faults, and let
// its crash points (internal/faults) or the test itself end it with SIGKILL (DESIGN.md, "Load and failure injection
// (day 4)").

// faultsBuild is ledgerd built with -tags faults, once per test run, in a directory TestMain removes.
var faultsBuild struct {
	once      sync.Once
	dir, path string
	err       error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if faultsBuild.dir != "" {
		os.RemoveAll(faultsBuild.dir)
	}
	os.Exit(code)
}

func faultsBinary(t *testing.T) string {
	t.Helper()
	faultsBuild.once.Do(func() {
		goBin, err := exec.LookPath("go")
		if err != nil {
			faultsBuild.err = err
			return
		}
		if faultsBuild.dir, err = os.MkdirTemp("", "ledgerd-faults-"); err != nil {
			faultsBuild.err = err
			return
		}
		faultsBuild.path = filepath.Join(faultsBuild.dir, "ledgerd")
		if out, err := exec.Command(goBin, "build", "-tags", "faults", "-o", faultsBuild.path, ".").CombinedOutput(); err != nil {
			faultsBuild.err = fmt.Errorf("%v: %s", err, out)
		}
	})
	if faultsBuild.err != nil {
		t.Fatalf("build ledgerd with -tags faults: %v", faultsBuild.err)
	}
	return faultsBuild.path
}

// child is a ledgerd process.
type child struct {
	cmd            *exec.Cmd
	stdout, stderr syncBuffer
	exited         chan struct{}
}

// startChild runs bin with the given subcommand and environment (on top of the test's own). The child is killed at
// the end of the test if it is still running.
func startChild(t *testing.T, bin, subcommand string, vars map[string]string) *child {
	t.Helper()
	c := &child{cmd: exec.Command(bin, subcommand), exited: make(chan struct{})}
	c.cmd.Env = childEnv(vars)
	c.cmd.Stdout, c.cmd.Stderr = &c.stdout, &c.stderr
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.cmd.Wait(); close(c.exited) }()
	t.Cleanup(func() { c.kill() })
	return c
}

// childEnv is the test's environment without any LEDGER_ setting, plus vars: a LEDGER_FAULTS or LEDGER_ISOLATION
// in the shell that runs the tests must not arm or change the children (found in review).
func childEnv(vars map[string]string) []string {
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "LEDGER_") })
	for k, v := range vars {
		env = append(env, k+"="+v)
	}
	return env
}

// kill ends the child with SIGKILL, unless it has ended already, and waits for it.
func (c *child) kill() {
	select {
	case <-c.exited:
	default:
		_ = c.cmd.Process.Kill()
		<-c.exited
	}
}

// wait waits for the child to end by itself.
func (c *child) wait(t *testing.T, within time.Duration) {
	t.Helper()
	select {
	case <-c.exited:
	case <-time.After(within):
		t.Fatalf("ledgerd still running after %v: %s", within, c.stderr.String())
	}
}

// killedBySIGKILL reports whether the child, which has ended, was ended by SIGKILL.
func (c *child) killedBySIGKILL() bool {
	status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// stoppedCleanly reports whether the child, which has ended, ended as a stop signal should end it: exit 0, or
// SIGTERM's default action, which is what a SIGTERM that arrives before Go's runtime has handed signals to main
// does (exit 143 in a shell). Nothing can catch that one.
func (c *child) stoppedCleanly() bool {
	status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	return c.cmd.ProcessState.ExitCode() == 0 || ok && status.Signaled() && status.Signal() == syscall.SIGTERM
}

// stop sends SIGTERM and waits for a clean exit.
func (c *child) stop(t *testing.T) {
	t.Helper()
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	c.wait(t, 30*time.Second)
	if !c.stoppedCleanly() {
		t.Fatalf("ledgerd ended with %v after SIGTERM: %s", c.cmd.ProcessState, c.stderr.String())
	}
}

// freeAddr returns a localhost address with a port that was free a moment ago. A restarted ledgerd listens on the
// same addresses, so that its clients can reconnect.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// serveChild starts `ledgerd serve` and returns once it answers /readyz.
func serveChild(t *testing.T, bin string, vars map[string]string) *child {
	t.Helper()
	c := startChild(t, bin, "serve", vars)
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get("http://" + vars["LEDGER_HTTP_ADDR"] + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return c
			}
		}
		select {
		case <-c.exited:
			t.Fatalf("ledgerd exited before it was ready: %s", c.stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("ledgerd not ready after 30 s: %s", c.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// clientFor returns a load client of the named API at the addresses vars gives ledgerd.
func clientFor(t *testing.T, api string, vars map[string]string) load.Client {
	t.Helper()
	var c load.Client = load.NewREST("http://"+vars["LEDGER_HTTP_ADDR"], 16)
	if api == "grpc" {
		g, err := load.NewGRPC(vars["LEDGER_GRPC_ADDR"])
		if err != nil {
			t.Fatal(err)
		}
		c = g
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// reconcileClean runs `ledgerd reconcile` (in this process: it arms no crash point) and fails the test unless it
// exits 0.
func reconcileClean(t *testing.T, url string) {
	t.Helper()
	var out, logs syncBuffer
	if code := run(context.Background(), []string{"reconcile"}, env(map[string]string{"LEDGER_DATABASE_URL": url}),
		&out, &logs, nil); code != 0 {
		t.Fatalf("ledgerd reconcile exited %d: %s%s", code, out.String(), logs.String())
	}
}

func queryInt(t *testing.T, db *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// postUntilAnswered sends the entry with its key until the ledger answers (a restarted ledgerd may take a moment to
// be reachable again).
func postUntilAnswered(t *testing.T, c load.Client, key string, e ledger.Entry) (load.Posted, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		posted, err := c.PostEntry(ctx, key, e)
		cancel()
		if load.Classify(err) != load.Unknown || time.Now().After(deadline) {
			return posted, err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A crash around the commit leaves the client without an answer, so it sends the request again with its key, to a
// restarted ledgerd. Before the commit nothing had posted, and the retry posts it; after the commit it had, and the
// retry gets that entry back. Alice pays $40 of her $50, so a second posting of the payment would be refused for
// funds: the retry after the commit must be a replay, not "insufficient funds".
func TestACrashAroundTheCommitIsSettledByTheRetry(t *testing.T) {
	bin := faultsBinary(t)
	for _, api := range []string{"rest", "grpc"} {
		for _, point := range []string{faults.BeforeCommit, faults.AfterCommit} {
			t.Run(api+"/"+point, func(t *testing.T) {
				t.Parallel()
				db := storetest.Migrated(t)
				url := storetest.SchemaURL(t, db)
				ctx := context.Background()
				svc := store.New(db)
				open := func(name string, typ ledger.AccountType) int64 {
					a, _, err := svc.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: "USD"})
					if err != nil {
						t.Fatal(err)
					}
					return a.ID
				}
				cash, alice, bob := open("cash", ledger.Asset), open("alice", ledger.Liability), open("bob", ledger.Liability)
				if _, _, err := svc.PostEntry(ctx, "fund", ledger.Entry{Postings: []ledger.Posting{
					{AccountID: cash, Currency: "USD", Amount: 5000},
					{AccountID: alice, Currency: "USD", Amount: -5000}}}); err != nil {
					t.Fatal(err)
				}

				vars := map[string]string{"LEDGER_DATABASE_URL": url, "LEDGER_HTTP_ADDR": freeAddr(t),
					"LEDGER_GRPC_ADDR": freeAddr(t), "LEDGER_FAULTS": point + ":1"}
				crashing := serveChild(t, bin, vars)
				c := clientFor(t, api, vars)
				pay := ledger.Entry{Description: "alice pays bob", Postings: []ledger.Posting{
					{AccountID: alice, Currency: "USD", Amount: 4000}, {AccountID: bob, Currency: "USD", Amount: -4000}}}
				if _, err := c.PostEntry(ctx, "pay", pay); load.Classify(err) != load.Unknown {
					t.Fatalf("the first request: %v (%v); want no answer", err, load.Classify(err))
				}
				crashing.wait(t, 10*time.Second)
				if !crashing.killedBySIGKILL() {
					t.Fatalf("ledgerd ended with %v, not SIGKILL: %s", crashing.cmd.ProcessState, crashing.stderr.String())
				}
				postedBefore := queryInt(t, db, `SELECT count(*) FROM idempotency_keys WHERE key = 'pay'`)
				if want := map[string]int64{faults.BeforeCommit: 0, faults.AfterCommit: 1}[point]; postedBefore != want {
					t.Fatalf("after the crash the payment is posted %d times, want %d", postedBefore, want)
				}

				delete(vars, "LEDGER_FAULTS")
				restarted := serveChild(t, bin, vars)
				posted, err := postUntilAnswered(t, c, "pay", pay)
				if err != nil {
					t.Fatalf("the retry: %v", err)
				}
				if posted.Replayed != (point == faults.AfterCommit) {
					t.Fatalf("the retry: replayed %v after a crash at %s", posted.Replayed, point)
				}
				if again, err := c.PostEntry(ctx, "pay", pay); err != nil || !again.Replayed || again.EntryID != posted.EntryID {
					t.Fatalf("a third send: %+v, %v; want a replay of entry %d", again, err, posted.EntryID)
				}
				if b, err := c.Account(ctx, alice); err != nil || b != (load.Account{Balance: -1000, Version: 2}) {
					t.Fatalf("alice: %d, %v; want -1000 ($10 left)", b, err)
				}
				if n := queryInt(t, db, `SELECT count(*) FROM journal_entries WHERE description = 'alice pays bob'`); n != 1 {
					t.Fatalf("the payment posted %d times", n)
				}
				restarted.stop(t)
				reconcileClean(t, url)
			})
		}
	}
}

// killFromOutside starts a supervisor's spec that has the supervisor kill the child itself, with SIGKILL, at a random
// moment of its life, whatever it is doing (mid-request, mid-transaction, between batches). "kill" alone arms no
// crash point; "kill:" followed by a LEDGER_FAULTS spec arms one as a backstop, for a child that would otherwise
// finish all its work before the moment comes.
const killFromOutside = "kill"

// supervisor keeps one ledgerd child running: whenever it dies it starts another with the next spec from specs
// (cycling), as a process manager would. A spec is LEDGER_FAULTS for that child, or killFromOutside. A child that ends
// by SIGKILL after writing a crash point's line was crashed by the point; one that ends by SIGKILL without it was
// killed from outside. (At first a killer goroutine sent SIGKILL at random intervals, and a run could end with no
// kill at all and still pass: the review removed the killer and every test stayed green.)
type supervisor struct {
	bin, sub string
	vars     map[string]string
	specs    []string         // for each start in turn
	onExit   func(c *child)   // sees each child that ended, before the next starts
	killAt   [2]time.Duration // when a killFromOutside child is killed: uniform in [min, max) after it starts
	// killFromOutput counts killAt from the child's first output on stdout instead, for a child that does its work
	// in bursts there (ledgerd publish).
	killFromOutput bool

	mu                          sync.Mutex
	current                     *child
	starts, pointCrashes, kills int
	stopping                    bool
	done                        chan struct{}
	failed                      chan struct{} // closed when the supervisor fails, so the test can stop its load
	failure                     error
}

func (s *supervisor) run() {
	defer close(s.done)
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))
	for i := 0; ; i++ {
		spec := s.specs[i%len(s.specs)]
		vars := maps.Clone(s.vars)
		points, killed := spec, false
		if rest, ok := strings.CutPrefix(spec, killFromOutside); ok {
			points, killed = strings.TrimPrefix(rest, ":"), true
		}
		if points != "" {
			vars["LEDGER_FAULTS"] = points
		}
		c := &child{cmd: exec.Command(s.bin, s.sub), exited: make(chan struct{})}
		c.cmd.Env = childEnv(vars)
		c.cmd.Stdout, c.cmd.Stderr = &c.stdout, &c.stderr
		s.mu.Lock()
		if s.stopping { // checked under the lock that stop takes, so a stop never misses a child just started
			s.mu.Unlock()
			return
		}
		if err := c.cmd.Start(); err != nil {
			s.fail(err)
			s.mu.Unlock()
			return
		}
		s.current, s.starts = c, s.starts+1
		s.mu.Unlock()
		go func() { _ = c.cmd.Wait(); close(c.exited) }()
		if killed {
			after := s.killAt[0] + time.Duration(rng.Int64N(int64(s.killAt[1]-s.killAt[0])))
			go func() {
				for s.killFromOutput && c.stdout.String() == "" {
					select {
					case <-c.exited:
						return
					case <-time.After(time.Millisecond):
					}
				}
				select {
				case <-c.exited:
				case <-time.After(after):
					s.mu.Lock()
					if !s.stopping {
						_ = c.cmd.Process.Kill()
					}
					s.mu.Unlock()
				}
			}()
		}
		<-c.exited
		if s.onExit != nil {
			s.onExit(c)
		}
		s.mu.Lock()
		stopping := s.stopping
		sigkill := c.killedBySIGKILL()
		switch {
		case sigkill && strings.Contains(c.stderr.String(), faults.FiredPrefix+" "):
			s.pointCrashes++
		case sigkill && killed:
			s.kills++
		case !stopping || !c.stoppedCleanly():
			// Only SIGKILL ends a child before the stop, and only a clean stop after it. A child just started when the
			// stop comes may get it while it starts up, which is clean too (TestAStopDuringStartupIsClean).
			s.fail(fmt.Errorf("ledgerd (spec %q) ended with %v: %s", spec, c.cmd.ProcessState, c.stderr.String()))
		}
		failed := s.failure != nil
		s.mu.Unlock()
		if stopping || failed {
			return
		}
	}
}

// fail records the supervisor's failure and tells the test; s.mu is held.
func (s *supervisor) fail(err error) {
	if s.failure == nil {
		s.failure = err
		close(s.failed)
	}
}

func startSupervisor(t *testing.T, s *supervisor) *supervisor {
	t.Helper()
	s.done, s.failed = make(chan struct{}), make(chan struct{})
	go s.run()
	t.Cleanup(func() {
		s.mu.Lock()
		s.stopping = true
		if s.current != nil {
			_ = s.current.cmd.Process.Kill()
		}
		s.mu.Unlock()
		<-s.done
	})
	return s
}

// stop ends the running child with SIGTERM, expecting a clean exit, and waits for the supervisor.
func (s *supervisor) stop(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	s.stopping = true
	if s.current != nil {
		_ = s.current.cmd.Process.Signal(syscall.SIGTERM)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the supervisor didn't stop")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		t.Fatal(s.failure)
	}
}

// stopOnFailure returns ctx, cancelled as soon as the supervisor fails: the load stops at once, and the test
// reports the failure, instead of retrying against a ledgerd that will never come back until the test times out
// (found in review).
func (s *supervisor) stopOnFailure(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.failed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// counts returns how many children were started, crashed by a crash point, and killed from outside.
func (s *supervisor) counts() (starts, pointCrashes, kills int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts, s.pointCrashes, s.kills
}

// Load from 8 clients while ledgerd keeps dying: at the 40th commit of a process, just before it or just after it,
// and every third process killed from outside at a random moment of its life. The clients send each transfer again with its key until it is
// answered. At the end each transfer has posted exactly once: as many transfers keys and entries as transfers, every
// account holding what the answers imply, and ledgerd reconcile exits 0. Over each API, at each isolation level.
func TestKillsDuringLoadPostEachTransferOnce(t *testing.T) {
	bin := faultsBinary(t)
	for _, run := range []struct{ api, level string }{{"rest", "read-committed"}, {"grpc", "read-committed"},
		{"rest", "serializable"}, {"grpc", "serializable"}} {
		api := run.api
		t.Run(api+"/"+run.level, func(t *testing.T) {
			t.Parallel()
			db := storetest.Migrated(t)
			url := storetest.SchemaURL(t, db)
			vars := map[string]string{"LEDGER_DATABASE_URL": url, "LEDGER_HTTP_ADDR": freeAddr(t),
				"LEDGER_GRPC_ADDR": freeAddr(t), "LEDGER_ISOLATION": run.level}
			sup := startSupervisor(t, &supervisor{bin: bin, sub: "serve", vars: vars,
				specs:  []string{faults.AfterCommit + ":40", faults.BeforeCommit + ":40", killFromOutside},
				killAt: [2]time.Duration{50 * time.Millisecond, 300 * time.Millisecond}})

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			ctx, stopLoad := sup.stopOnFailure(ctx)
			defer stopLoad()
			const transfers = 600
			res, err := load.Run(ctx, clientFor(t, api, vars), load.Config{Layout: load.Random, Wallets: 20,
				Workers: 8, Transfers: transfers, Retry: true, Prefix: "chaos:"})
			starts, pointCrashes, kills := sup.counts()
			sup.stop(t) // reports the supervisor's failure, if the load stopped for one
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s at %s: %d starts, %d crashes at a point, %d kills; posted %d, replayed %d, sent again %d; "+
				"%.0f transfers a second, p50 %v, p99 %v, max %v", api, run.level, starts, pointCrashes, kills, res.Posted,
				res.Replayed, res.Retries, res.PerSecond(), res.P50, res.P99, res.Max)

			if len(res.Mismatches) > 0 {
				t.Fatalf("the books don't agree with the answers: %q", res.Mismatches)
			}
			if res.Posted+res.Replayed != transfers || len(res.Refused) > 0 || res.GaveUp+res.Unknown > 0 {
				t.Fatalf("%+v", res)
			}
			// Crashes after a commit leave transfers posted and unanswered: their retries replay. 620 commits allow
			// at most 15 point crashes, and every third process is killed from outside instead.
			if pointCrashes < 5 || kills < 1 || res.Replayed == 0 {
				t.Fatalf("%d crashes at a point, %d kills and %d replays: the test didn't crash ledgerd as it should",
					pointCrashes, kills, res.Replayed)
			}
			if n := queryInt(t, db, `SELECT count(*) FROM idempotency_keys WHERE key LIKE $1`, res.Names+"t%"); n != transfers {
				t.Fatalf("%d transfer keys, want %d", n, transfers)
			}
			if n := queryInt(t, db, `SELECT count(*) FROM journal_entries`); n != 20+transfers {
				t.Fatalf("%d entries, want %d (20 fundings and the transfers)", n, 20+transfers)
			}
			reconcileClean(t, url)
		})
	}
}

// ledgerd publish is killed again and again while it drains a backlog of 2,000 events: just after its sink took a
// batch and before the batch is marked (its 1st, 2nd or 3rd batch, in turn), and every fourth process from outside,
// within 10 ms of its first event (a crash point at its 10th batch stops it draining everything first).
// Each kill's batch is delivered again by the next publisher, so events arrive at least once; a consumer that
// dedupes by event id sees every event once, each account's in version order; ledgerd reconcile exits 0.
func TestAKilledPublisherDeliversEveryEventAtLeastOnce(t *testing.T) {
	t.Parallel()
	bin := faultsBinary(t)
	db := storetest.Migrated(t)
	url := storetest.SchemaURL(t, db)
	ctx := context.Background()
	svc := store.New(db)
	var accounts [4]int64
	for i := range accounts {
		a, _, err := svc.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("wallet:%d", i), Type: ledger.Liability,
			Currency: "USD", AllowNegative: true})
		if err != nil {
			t.Fatal(err)
		}
		accounts[i] = a.ID
	}
	const events = 2000
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := w; i < events; i += 4 {
				from, to := accounts[i%4], accounts[(i+1+i/4%3)%4]
				if _, _, err := svc.PostEntry(ctx, fmt.Sprintf("e%d", i), ledger.Entry{Postings: []ledger.Posting{
					{AccountID: from, Currency: "USD", Amount: 1}, {AccountID: to, Currency: "USD", Amount: -1}}}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	var mu sync.Mutex
	var stream []outbox.Event
	torn := 0
	sup := startSupervisor(t, &supervisor{bin: bin, sub: "publish", vars: map[string]string{"LEDGER_DATABASE_URL": url},
		specs: []string{faults.AfterDeliver + ":1", faults.AfterDeliver + ":2", faults.AfterDeliver + ":3",
			killFromOutside + ":" + faults.AfterDeliver + ":10"},
		killAt: [2]time.Duration{0, 10 * time.Millisecond}, killFromOutput: true,
		onExit: func(c *child) {
			mu.Lock()
			defer mu.Unlock()
			for line := range strings.Lines(c.stdout.String()) {
				var e outbox.Event
				if err := json.Unmarshal([]byte(line), &e); err != nil {
					torn++ // a line cut off by a kill: consumers skip what doesn't parse
					continue
				}
				stream = append(stream, e)
			}
		}})
	deadline := time.Now().Add(2 * time.Minute)
	for queryInt(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NULL`) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("events still unpublished after 2 minutes")
		}
		time.Sleep(20 * time.Millisecond)
	}
	starts, pointCrashes, kills := sup.counts()
	sup.stop(t)

	mu.Lock()
	defer mu.Unlock()
	deduped := outbox.Dedupe(map[int64]bool{}, stream)
	t.Logf("%d publishers, %d killed after delivering, %d killed from outside; %d lines, %d events after dedupe, %d torn",
		starts, pointCrashes, kills, len(stream), len(deduped), torn)
	if len(deduped) != events {
		t.Fatalf("%d distinct events delivered, want %d", len(deduped), events)
	}
	if err := outbox.CheckOrder(deduped); err != nil {
		t.Fatal(err)
	}
	if pointCrashes < 5 || kills < 1 || len(stream)-len(deduped) < 100*pointCrashes/2 {
		t.Fatalf("%d crashes after a delivery, %d kills and %d events delivered twice: the crashes didn't happen as "+
			"they should", pointCrashes, kills, len(stream)-len(deduped))
	}
	reconcileClean(t, url)
}

// A child gets none of the test's own LEDGER_ settings: a LEDGER_FAULTS in the shell would arm the restart that must
// run unarmed, and a LEDGER_ISOLATION would change the level under test (found in review).
func TestChildrenDontInheritLedgerSettings(t *testing.T) {
	t.Setenv("LEDGER_FAULTS", faults.AfterCommit+":1")
	t.Setenv("LEDGER_ISOLATION", "serializable")
	env := childEnv(map[string]string{"LEDGER_DATABASE_URL": "postgres://somewhere"})
	var ledgers []string
	for _, kv := range env {
		if strings.HasPrefix(kv, "LEDGER_") {
			ledgers = append(ledgers, kv)
		}
	}
	if len(ledgers) != 1 || ledgers[0] != "LEDGER_DATABASE_URL=postgres://somewhere" {
		t.Fatalf("the child's LEDGER_ settings: %q", ledgers)
	}
}
