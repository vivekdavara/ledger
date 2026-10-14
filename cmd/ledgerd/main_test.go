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
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/outbox"
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
		if code := run(context.Background(), args, env(nil), io.Discard, &out, nil); code != 2 || !strings.Contains(out.String(), "usage") {
			t.Errorf("run(%q) = %d, %q; want 2 and the usage", args, code, out.String())
		}
	}
}

func TestADatabaseURLIsRequired(t *testing.T) {
	var out bytes.Buffer
	if code := run(context.Background(), []string{"serve"}, env(nil), io.Discard, &out, nil); code != 1 ||
		!strings.Contains(out.String(), "LEDGER_DATABASE_URL is not set") {
		t.Fatalf("got %d, %q", code, out.String())
	}
}

func TestAnUnknownIsolationLevelIsRefused(t *testing.T) {
	vars := map[string]string{"LEDGER_DATABASE_URL": storetest.SchemaURL(t, storetest.EmptySchema(t)),
		"LEDGER_ISOLATION": "repeatable-read"}
	var out bytes.Buffer
	if code := run(context.Background(), []string{"serve"}, env(vars), io.Discard, &out, nil); code != 1 ||
		!strings.Contains(out.String(), `unknown isolation level \"repeatable-read\"`) {
		t.Fatalf("got %d, %q", code, out.String())
	}
}

func TestAnUnreachableDatabaseFailsWithoutShowingThePassword(t *testing.T) {
	var out bytes.Buffer
	vars := map[string]string{"LEDGER_DATABASE_URL": "postgres://ledger:s3cret-pw@127.0.0.1:1/ledger?connect_timeout=2"}
	if code := run(context.Background(), []string{"migrate"}, env(vars), io.Discard, &out, nil); code != 1 {
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
		if code := run(context.Background(), []string{"migrate"}, env(vars), io.Discard, &out, nil); code != 0 ||
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
		"LEDGER_ISOLATION":    "serializable", // not the default, so the log shows the setting was used
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var logs syncBuffer
	listening := make(chan addrs, 1)
	exit := make(chan int, 1)
	go func() { exit <- run(ctx, []string{"serve"}, env(vars), io.Discard, &logs, listening) }()

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

	if !strings.Contains(logs.String(), `"isolation":"serializable"}`) {
		t.Errorf("the log doesn't show LEDGER_ISOLATION's level: %s", logs.String())
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
	// A port that is free now, for HTTP: after the failed start it must be free again.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpAddr := probe.Addr().String()
	probe.Close()
	vars := map[string]string{
		"LEDGER_DATABASE_URL": storetest.SchemaURL(t, storetest.EmptySchema(t)),
		"LEDGER_HTTP_ADDR":    httpAddr,
		"LEDGER_GRPC_ADDR":    taken.Addr().String(),
	}
	var logs syncBuffer
	if code := run(context.Background(), []string{"serve"}, env(vars), io.Discard, &logs, nil); code != 1 ||
		!strings.Contains(logs.String(), "address already in use") {
		t.Fatalf("exit %d, log %s", code, logs.String())
	}
	again, err := net.Listen("tcp", httpAddr)
	if err != nil {
		t.Fatalf("the HTTP port is still held after the failed start: %v", err)
	}
	again.Close()
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
	go func() { f.exit <- run(ctx, []string{"serve"}, env(vars), io.Discard, f.logs, listening) }()
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
		if code := serve(ctx, db, store.New(db), "127.0.0.1:0", "127.0.0.1:0", 10*time.Second, log, nil); code != 0 {
			t.Fatalf("run %d: exit %d: %s", i, code, logs.String())
		}
	}
}

// A stop signal that arrives while serve or publish connects or migrates is a clean stop: exit 0, as a stop at
// any later moment is. migrate and reconcile, stopped before their one job is done, exit 1.
func TestAStopDuringStartupIsClean(t *testing.T) {
	vars := map[string]string{"LEDGER_DATABASE_URL": storetest.SchemaURL(t, storetest.EmptySchema(t)),
		"LEDGER_HTTP_ADDR": "127.0.0.1:0", "LEDGER_GRPC_ADDR": "127.0.0.1:0"}
	stopped, cancel := context.WithCancel(context.Background())
	cancel() // the signal came before the database answered
	for sub, want := range map[string]int{"serve": 0, "publish": 0, "migrate": 1, "reconcile": 1} {
		var logs syncBuffer
		if code := run(stopped, []string{sub}, env(vars), io.Discard, &logs, nil); code != want {
			t.Errorf("%s stopped during startup: exit %d, want %d: %s", sub, code, want, logs.String())
		}
	}
}

// ledgerd publish writes the outbox's events to stdout as JSON lines: the entries posted before it started, then
// one posted while it runs. A stop signal ends it with exit 0, and every event it wrote is marked published.
func TestPublishWritesEventsAsJSONLines(t *testing.T) {
	db := storetest.Migrated(t)
	svc := store.New(db)
	ctx := context.Background()
	var accounts [2]int64
	for i, name := range []string{"cash:bank", "wallet:alice"} {
		typ := map[int]ledger.AccountType{0: ledger.Asset, 1: ledger.Liability}[i]
		a, _, err := svc.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: "USD"})
		if err != nil {
			t.Fatal(err)
		}
		accounts[i] = a.ID
	}
	topUp := func(key string) ledger.PostedEntry {
		e, _, err := svc.PostEntry(ctx, key, ledger.Entry{Description: key, Postings: []ledger.Posting{
			{AccountID: accounts[0], Currency: "USD", Amount: 500}, {AccountID: accounts[1], Currency: "USD", Amount: -500}}})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	posted := []ledger.PostedEntry{topUp("top-up-1"), topUp("top-up-2")}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var stdout, logs syncBuffer
	exit := make(chan int, 1)
	go func() {
		exit <- run(runCtx, []string{"publish"}, env(map[string]string{"LEDGER_DATABASE_URL": storetest.SchemaURL(t, db)}),
			&stdout, &logs, nil)
	}()
	lines := func(n int) []string {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if got := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n"); len(got) >= n && got[0] != "" {
				return got
			}
		}
		t.Fatalf("fewer than %d lines after 10 s: %q; log %s", n, stdout.String(), logs.String())
		return nil
	}
	lines(2)
	posted = append(posted, topUp("top-up-3"))
	got := lines(3)
	stop()
	if code := <-exit; code != 0 || !strings.Contains(logs.String(), `"msg":"stopped"`) {
		t.Fatalf("exit %d, log %s", code, logs.String())
	}
	if len(got) != 3 {
		t.Fatalf("%d lines, want 3: %q", len(got), got)
	}
	for i, line := range got {
		var e outbox.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if e.Type != outbox.EntryPosted || e.EntryID != posted[i].ID || e.Description != posted[i].Description ||
			len(e.Postings) != 2 || e.Postings[1].BalanceAfter != posted[i].Postings[1].BalanceAfter {
			t.Fatalf("line %d: %+v, want entry %+v", i, e, posted[i])
		}
	}
	var unpublished int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&unpublished); err != nil ||
		unpublished != 0 {
		t.Fatalf("%d unpublished, %v", unpublished, err)
	}
}

// ledgerd reconcile prints the report and exits 0 on a clean ledger, 3 once the books are damaged, and 1, changing
// nothing, on a schema that isn't current.
func TestReconcileExitsByWhatItFound(t *testing.T) {
	db := storetest.Migrated(t)
	ctx := context.Background()
	svc := store.New(db)
	var ids []int64
	for _, a := range []ledger.NewAccount{{Name: "cash:bank", Type: ledger.Asset, Currency: "USD"},
		{Name: "wallet:alice", Type: ledger.Liability, Currency: "USD"}} {
		acct, _, err := svc.CreateAccount(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, acct.ID)
	}
	if _, _, err := svc.PostEntry(ctx, "top-up", ledger.Entry{Postings: []ledger.Posting{
		{AccountID: ids[0], Currency: "USD", Amount: 500}, {AccountID: ids[1], Currency: "USD", Amount: -500}}}); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{"LEDGER_DATABASE_URL": storetest.SchemaURL(t, db)}
	reconcile := func() (int, store.Report, string) {
		t.Helper()
		var out, logs bytes.Buffer
		code := run(ctx, []string{"reconcile"}, env(vars), &out, &logs, nil)
		var r store.Report
		if code != 1 {
			if err := json.Unmarshal(out.Bytes(), &r); err != nil {
				t.Fatalf("the report isn't JSON: %v: %s", err, out.String())
			}
		}
		return code, r, logs.String()
	}
	if code, r, logs := reconcile(); code != 0 || !r.Clean() || r.Entries != 1 || r.Postings != 2 {
		t.Fatalf("clean ledger: exit %d, report %+v, log %s", code, r, logs)
	}
	if _, err := db.Exec(ctx, `DELETE FROM outbox`); err != nil {
		t.Fatal(err)
	}
	if code, r, logs := reconcile(); code != 3 || r.Clean() || !strings.Contains(logs, `"checks":["entry_events"]`) {
		t.Fatalf("damaged ledger: exit %d, report %+v, log %s", code, r, logs)
	}
	// A schema one migration behind: reconcile refuses, and doesn't migrate it.
	if _, err := db.Exec(ctx, `DELETE FROM schema_migrations WHERE version = (SELECT max(version) FROM schema_migrations)`); err != nil {
		t.Fatal(err)
	}
	if code, _, logs := reconcile(); code != 1 || !strings.Contains(logs, "run ledgerd migrate") {
		t.Fatalf("old schema: exit %d, log %s", code, logs)
	}
	var version int
	if err := db.QueryRow(ctx, `SELECT max(version) FROM schema_migrations`).Scan(&version); err != nil || version != 8 {
		t.Fatalf("schema at version %d, %v: reconcile changed it", version, err)
	}
}
