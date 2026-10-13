package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ledgerv1 "github.com/vivekdavara/ledger/internal/gen/ledger/v1"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// syncBuffer is a bytes.Buffer that the server goroutine and the test can share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"serve", "extra"}} {
		var out bytes.Buffer
		if code := run(context.Background(), args, env(nil), &out, nil); code != 2 || !strings.Contains(out.String(), "usage") {
			t.Errorf("run(%q) = %d, %q; want 2 and the usage", args, code, out.String())
		}
	}
}

func TestADatabaseURLIsRequired(t *testing.T) {
	var out bytes.Buffer
	if code := run(context.Background(), []string{"serve"}, env(nil), &out, nil); code != 1 ||
		!strings.Contains(out.String(), "LEDGER_DATABASE_URL is not set") {
		t.Fatalf("got %d, %q", code, out.String())
	}
}

func TestAnUnreachableDatabaseFailsWithoutShowingThePassword(t *testing.T) {
	var out bytes.Buffer
	vars := map[string]string{"LEDGER_DATABASE_URL": "postgres://ledger:s3cret-pw@127.0.0.1:1/ledger?connect_timeout=2"}
	if code := run(context.Background(), []string{"migrate"}, env(vars), &out, nil); code != 1 {
		t.Fatalf("got exit %d, want 1", code)
	}
	if strings.Contains(out.String(), "s3cret-pw") {
		t.Fatalf("the log shows the password: %s", out.String())
	}
}

func TestMigrateAppliesOnceThenNothing(t *testing.T) {
	vars := map[string]string{"LEDGER_DATABASE_URL": storetest.SchemaURL(t, storetest.EmptySchema(t))}
	migrations, err := store.LoadMigrations(store.Migrations())
	if err != nil {
		t.Fatal(err)
	}
	all := make([]string, len(migrations))
	for i, m := range migrations {
		all[i] = strconv.Itoa(m.Version)
	}
	for i, want := range []string{`"applied":[` + strings.Join(all, ",") + `]`, `"applied":[]`} {
		var out bytes.Buffer
		if code := run(context.Background(), []string{"migrate"}, env(vars), &out, nil); code != 0 ||
			!strings.Contains(out.String(), want) {
			t.Fatalf("run %d: exit %d, log %s; want %s", i+1, code, out.String(), want)
		}
	}
}

func TestServeAnswersBothAPIsThenShutsDownCleanly(t *testing.T) {
	vars := map[string]string{
		"LEDGER_DATABASE_URL": storetest.SchemaURL(t, storetest.EmptySchema(t)),
		"LEDGER_HTTP_ADDR":    "127.0.0.1:0", // any free port
		"LEDGER_GRPC_ADDR":    "127.0.0.1:0",
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var logs syncBuffer
	listening := make(chan addrs, 1)
	exit := make(chan int, 1)
	go func() { exit <- run(ctx, []string{"serve"}, env(vars), &logs, listening) }()

	var at addrs
	select {
	case at = <-listening:
	case code := <-exit:
		t.Fatalf("exited with %d before listening: %s", code, logs.String())
	case <-time.After(30 * time.Second):
		t.Fatalf("not listening after 30 s: %s", logs.String())
	}
	for path, want := range map[string]string{"/livez": `"alive"`, "/readyz": `"ready"`} {
		resp, err := http.Get("http://" + at.HTTP + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || !strings.Contains(string(body), want) {
			t.Fatalf("GET %s: %d %s", path, resp.StatusCode, body)
		}
	}
	// An account created over REST is the one gRPC reads: one service behind both.
	resp, err := http.Post("http://"+at.HTTP+"/v1/accounts", "application/json",
		strings.NewReader(`{"name":"wallet:alice","type":"liability","currency":"USD"}`))
	if err != nil {
		t.Fatal(err)
	}
	var created struct{ ID int64 }
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("create over REST: %d %v", resp.StatusCode, err)
	}
	resp.Body.Close()
	conn, err := grpc.NewClient(at.GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	got, err := ledgerv1.NewLedgerServiceClient(conn).GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: created.ID})
	if err != nil || got.GetAccount().GetName() != "wallet:alice" {
		t.Fatalf("read over gRPC: %v, %v", got, err)
	}

	stop() // what SIGTERM does in main
	select {
	case code := <-exit:
		if code != 0 || !strings.Contains(logs.String(), `"msg":"stopped"`) {
			t.Fatalf("exit %d, log %s", code, logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("still running 15 s after the stop signal")
	}
	for _, addr := range []string{at.HTTP, at.GRPC} {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			c.Close()
			t.Errorf("%s still accepts connections after shutdown", addr)
		}
	}
}

func TestServeFailsIfAPortIsTaken(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	vars := map[string]string{
		"LEDGER_DATABASE_URL": storetest.SchemaURL(t, storetest.EmptySchema(t)),
		"LEDGER_HTTP_ADDR":    "127.0.0.1:0",
		"LEDGER_GRPC_ADDR":    taken.Addr().String(),
	}
	var logs syncBuffer
	if code := run(context.Background(), []string{"serve"}, env(vars), &logs, nil); code != 1 ||
		!strings.Contains(logs.String(), "address already in use") {
		t.Fatalf("exit %d, log %s", code, logs.String())
	}
}

// inFlight starts serve with the given shutdown grace, opens two accounts, and holds both rows in a transaction of
// its own while one REST request and one gRPC call try to post against them. It returns once both are waiting.
type inFlight struct {
	stop       context.CancelFunc
	exit       chan int
	logs       *syncBuffer
	rest, grpc chan error
	release    func()
	db         *pgxpool.Pool
}

func startInFlight(t *testing.T, grace string) inFlight {
	t.Helper()
	db := storetest.EmptySchema(t)
	vars := map[string]string{"LEDGER_DATABASE_URL": storetest.SchemaURL(t, db), "LEDGER_HTTP_ADDR": "127.0.0.1:0",
		"LEDGER_GRPC_ADDR": "127.0.0.1:0", "LEDGER_SHUTDOWN_GRACE": grace}
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	f := inFlight{stop: stop, exit: make(chan int, 1), logs: &syncBuffer{}, rest: make(chan error, 1),
		grpc: make(chan error, 1), db: db}
	listening := make(chan addrs, 1)
	go func() { f.exit <- run(ctx, []string{"serve"}, env(vars), f.logs, listening) }()
	var at addrs
	select {
	case at = <-listening:
	case <-time.After(30 * time.Second):
		t.Fatalf("not listening: %s", f.logs.String())
	}
	for _, body := range []string{`{"name":"cash:bank","type":"asset","currency":"USD"}`,
		`{"name":"wallet:alice","type":"liability","currency":"USD"}`} {
		resp, err := http.Post("http://"+at.HTTP+"/v1/accounts", "application/json", strings.NewReader(body))
		if err != nil || resp.StatusCode != http.StatusCreated {
			t.Fatalf("create: %v %v", resp, err)
		}
		resp.Body.Close()
	}
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(context.Background(), `SELECT 1 FROM accounts FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	var holder int
	if err := tx.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	f.release = func() { once.Do(func() { _ = tx.Rollback(context.Background()) }) }
	t.Cleanup(f.release)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+at.HTTP+"/v1/entries", strings.NewReader(
			`{"postings":[{"account_id":1,"currency":"USD","amount":"1.00"},{"account_id":2,"currency":"USD","amount":"-1.00"}]}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "in-flight-rest")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			if resp.StatusCode != http.StatusCreated {
				err = fmt.Errorf("status %d", resp.StatusCode)
			}
			resp.Body.Close()
		}
		f.rest <- err
	}()
	conn, err := grpc.NewClient(at.GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		_, err := ledgerv1.NewLedgerServiceClient(conn).PostEntry(context.Background(), &ledgerv1.PostEntryRequest{
			IdempotencyKey: "in-flight-grpc", Postings: []*ledgerv1.Posting{
				{AccountId: 1, Currency: "USD", Amount: 100}, {AccountId: 2, Currency: "USD", Amount: -100}}})
		f.grpc <- err
	}()
	// Both are waiting once two sessions are blocked by the test's transaction. The second waits in the row's lock
	// queue behind the first, so it is blocked by the first, not by the test: count the whole chain.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := db.QueryRow(context.Background(), `
			WITH RECURSIVE blocked (pid) AS (
			    SELECT pid FROM pg_stat_activity WHERE $1::integer = ANY (pg_blocking_pids(pid))
			    UNION
			    SELECT a.pid FROM pg_stat_activity a JOIN blocked b ON b.pid = ANY (pg_blocking_pids(a.pid)))
			SELECT count(*) FROM blocked`, holder).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= 2 {
			return f
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions waiting on the locks after 10 s", waiting)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestShutdownLetsInFlightWorkFinishWithinTheGrace(t *testing.T) {
	f := startInFlight(t, "10s")
	f.stop()
	time.Sleep(300 * time.Millisecond) // shutting down, with both still waiting
	f.release()
	for name, ch := range map[string]chan error{"REST": f.rest, "gRPC": f.grpc} {
		select {
		case err := <-ch:
			if err != nil {
				t.Errorf("%s: %v, want the entry posted", name, err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("%s: no answer", name)
		}
	}
	select {
	case code := <-f.exit:
		if code != 0 {
			t.Fatalf("exit %d after a clean shutdown: %s", code, f.logs.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("still running")
	}
}

func TestShutdownCutsOffWorkStillRunningAfterTheGrace(t *testing.T) {
	f := startInFlight(t, "500ms")
	start := time.Now()
	f.stop()
	select {
	case code := <-f.exit:
		if took := time.Since(start); code != 1 || took > 5*time.Second {
			t.Fatalf("exit %d after %v; want 1 soon after the 500 ms grace", code, took)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("still running 20 s after the stop, with a 500 ms grace: %s", f.logs.String())
	}
	for name, ch := range map[string]chan error{"REST": f.rest, "gRPC": f.grpc} {
		select {
		case err := <-ch:
			if err == nil {
				t.Errorf("%s: posted after being cut off", name)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: the client got no answer", name)
		}
	}
	// The work that was cut off is gone: releasing the lock posts nothing.
	f.release()
	var entries int
	if err := f.db.QueryRow(context.Background(), `SELECT count(*) FROM journal_entries`).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if entries != 0 {
		t.Fatalf("%d entries posted by requests that were cut off", entries)
	}
}

func TestAStopAsServeStartsIsClean(t *testing.T) {
	db := storetest.Migrated(t)
	for i := range 50 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var logs syncBuffer
		log := slog.New(slog.NewJSONHandler(&logs, nil))
		if code := serve(ctx, db, "127.0.0.1:0", "127.0.0.1:0", 10*time.Second, log, nil); code != 0 {
			t.Fatalf("run %d: exit %d: %s", i, code, logs.String())
		}
	}
}
