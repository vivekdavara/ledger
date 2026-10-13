package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
