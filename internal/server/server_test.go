package server_test

import (
	"context"
	"encoding/json"
	"errors"
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
	start := time.Now()
	code, _ := get(t, hung, "GET", "/readyz")
	if took := time.Since(start); code != 503 || took > 5*time.Second {
		t.Fatalf("got %d after %v; want 503 within the 2 s timeout", code, took)
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
	if code != 503 || !strings.Contains(body["reason"], "schema is at version 0, this binary needs 3") {
		t.Fatalf("unmigrated schema: got %d %v", code, body)
	}
}
