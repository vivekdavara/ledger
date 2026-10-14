package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/faults"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/load"
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
	c.cmd.Env = os.Environ()
	for k, v := range vars {
		c.cmd.Env = append(c.cmd.Env, k+"="+v) // a later entry wins
	}
	c.cmd.Stdout, c.cmd.Stderr = &c.stdout, &c.stderr
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.cmd.Wait(); close(c.exited) }()
	t.Cleanup(func() { c.kill() })
	return c
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

// stop sends SIGTERM and waits for a clean exit.
func (c *child) stop(t *testing.T) {
	t.Helper()
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	c.wait(t, 30*time.Second)
	if code := c.cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("ledgerd exited %d after SIGTERM: %s", code, c.stderr.String())
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
				if b, err := c.Balance(ctx, alice); err != nil || b != -1000 {
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
