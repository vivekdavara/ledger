package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// serve runs both APIs in this process over a schema of the test's own, and returns their addresses.
func serve(t *testing.T) (restAddr, grpcAddr string) {
	t.Helper()
	db := storetest.Migrated(t)
	svc := store.New(db)
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
	return strings.TrimPrefix(httpSrv.URL, "http://"), ln.Addr().String()
}

func TestARunOverEachAPIAgreesWithTheBooks(t *testing.T) {
	restAddr, grpcAddr := serve(t)
	for api, addr := range map[string]string{"rest": restAddr, "grpc": grpcAddr} {
		var out, errs bytes.Buffer
		code := run(context.Background(), []string{"-api", api, "-addr", addr, "-layout", "hot", "-workers", "4",
			"-transfers", "60", "-wallets", "10", "-prefix", api + ":"}, &out, &errs)
		if code != 0 || !strings.Contains(out.String(), "60 transfers") ||
			!strings.Contains(out.String(), "posted 60, replayed 0, gave up 0, refused 0, unknown 0, sent again 0") ||
			!strings.Contains(out.String(), "books: every account agrees with the answers") {
			t.Fatalf("%s: exit %d\n%s%s", api, code, out.String(), errs.String())
		}
	}
	// -json: one object a script can read. The same prefix again: the accounts are found, not created again, and
	// every transfer's key was used by the first run for the same transfer, so each one is a replay.
	var out, errs bytes.Buffer
	if code := run(context.Background(), []string{"-addr", restAddr, "-json", "-layout", "hot", "-workers", "4",
		"-transfers", "60", "-wallets", "10", "-prefix", "rest:"}, &out, &errs); code != 0 {
		t.Fatalf("exit %d: %s%s", code, out.String(), errs.String())
	}
	var res result
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if res.API != "rest" || res.Transfers != 60 || res.Replayed != 60 || res.Posted != 0 || len(res.Mismatches) != 0 ||
		res.PerSecond <= 0 || res.P50 <= 0 {
		t.Fatalf("%+v", res)
	}
}

func TestUsageAndUnreachableServers(t *testing.T) {
	for _, args := range [][]string{{"-api", "soap"}, {"extra"}, {"-workers", "x"}, {"-no-such-flag"}} {
		if code := run(context.Background(), args, io.Discard, io.Discard); code != 2 {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	for _, api := range []string{"rest", "grpc"} {
		var errs bytes.Buffer
		if code := run(context.Background(), []string{"-api", api, "-addr", addr}, io.Discard, &errs); code != 1 ||
			!strings.Contains(errs.String(), "nothing answers at "+addr) {
			t.Errorf("%s: exit %d, %s", api, code, errs.String())
		}
	}
	// A config that can't run is a usage error, refused before anything is sent: addr is the port nothing listens on,
	// so a request would have been "nothing answers" and exit 1 (found in review).
	for _, args := range [][]string{{"-layout", "skewed"}, {"-skew", "0.5"}, {"-pad", "-1"}, {"-prefix", "a b"},
		{"-api", "grpc", "-wallets", "1", "-layout", "hot"}} {
		var errs bytes.Buffer
		if code := run(context.Background(), append(args, "-addr", addr), io.Discard, &errs); code != 2 ||
			strings.Contains(errs.String(), "nothing answers") {
			t.Errorf("%q: exit %d, %s", args, code, errs.String())
		}
	}
}

// A ledger that posts every tenth transfer twice: ledgerload says which accounts don't agree, and exits 3. No test
// had seen that exit code (found in review).
func TestBooksThatDontAgreeExit3(t *testing.T) {
	db := storetest.Migrated(t)
	svc := store.New(db)
	api := server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, svc,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if key := r.Header.Get("Idempotency-Key"); strings.Contains(key, ":t") && n.Add(1)%10 == 0 {
			body, _ := io.ReadAll(r.Body)
			twice := r.Clone(r.Context())
			twice.Header.Set("Idempotency-Key", key+"-twice")
			twice.Body = io.NopCloser(bytes.NewReader(body))
			api.ServeHTTP(httptest.NewRecorder(), twice)
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		api.ServeHTTP(w, r)
	}))
	defer srv.Close()
	var out, errs bytes.Buffer
	code := run(context.Background(), []string{"-addr", strings.TrimPrefix(srv.URL, "http://"), "-workers", "4",
		"-transfers", "100", "-wallets", "10"}, &out, &errs)
	if code != 3 || !strings.Contains(out.String(), "accounts don't agree with the answers") { // 3, as documented
		t.Fatalf("exit %d\n%s%s", code, out.String(), errs.String())
	}
}
