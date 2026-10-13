package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func get(t *testing.T, h http.Handler, method, path string) (int, map[string]string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	var body map[string]string
	if rec.Code != http.StatusMethodNotAllowed && rec.Code != http.StatusNotFound {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s %s: body %q is not JSON: %v", method, path, rec.Body.String(), err)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s: Content-Type %q", method, path, ct)
		}
	}
	return rec.Code, body
}

func TestLivezNeedsNothing(t *testing.T) {
	h := server.New(func(context.Context) error { return errors.New("database is down") }, quiet)
	if code, body := get(t, h, "GET", "/livez"); code != 200 || body["status"] != "alive" {
		t.Fatalf("got %d %v; want 200 alive even with the database down", code, body)
	}
}

func TestReadyzReportsTheReason(t *testing.T) {
	ready := server.New(func(context.Context) error { return nil }, quiet)
	if code, body := get(t, ready, "GET", "/readyz"); code != 200 || body["status"] != "ready" {
		t.Fatalf("got %d %v", code, body)
	}
	down := server.New(func(context.Context) error { return errors.New("database unavailable: refused") }, quiet)
	code, body := get(t, down, "GET", "/readyz")
	if code != 503 || body["status"] != "not ready" || !strings.Contains(body["reason"], "refused") {
		t.Fatalf("got %d %v; want 503 with the reason", code, body)
	}
}

func TestReadyzDoesNotHangWithTheDatabase(t *testing.T) {
	hung := server.New(func(ctx context.Context) error {
		<-ctx.Done() // a database that never answers
		return ctx.Err()
	}, quiet)
	// The request's own 10 s deadline is a backstop: without the handler's timeout this test fails, not hangs.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	rec := httptest.NewRecorder()
	hung.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil).WithContext(ctx))
	if took := time.Since(start); rec.Code != 503 || took > 5*time.Second {
		t.Fatalf("got %d after %v; want 503 within the 2 s timeout", rec.Code, took)
	}
}

func TestRoutes(t *testing.T) {
	h := server.New(func(context.Context) error { return nil }, quiet)
	cases := []struct {
		method, path string
		want         int
	}{
		{"POST", "/livez", http.StatusMethodNotAllowed},
		{"DELETE", "/readyz", http.StatusMethodNotAllowed},
		{"HEAD", "/livez", http.StatusOK}, // GET routes answer HEAD too
		{"GET", "/", http.StatusNotFound},
		{"GET", "/livez/extra", http.StatusNotFound},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("%s %s: %d, want %d", tc.method, tc.path, rec.Code, tc.want)
		}
	}
}

func TestReadyzAgainstARealDatabase(t *testing.T) {
	migrated := storetest.Migrated(t)
	h := server.New(func(ctx context.Context) error { return store.Ready(ctx, migrated) }, quiet)
	if code, body := get(t, h, "GET", "/readyz"); code != 200 {
		t.Fatalf("migrated schema: got %d %v", code, body)
	}
	empty := storetest.EmptySchema(t)
	h = server.New(func(ctx context.Context) error { return store.Ready(ctx, empty) }, quiet)
	code, body := get(t, h, "GET", "/readyz")
	migrations, err := store.LoadMigrations(store.Migrations())
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("schema is at version 0, this binary needs %d", len(migrations))
	if code != 503 || !strings.Contains(body["reason"], want) {
		t.Fatalf("unmigrated schema: got %d %v", code, body)
	}
}
