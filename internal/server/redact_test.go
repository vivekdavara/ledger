package server_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/server"
)

// syncBuffer is a log destination the handlers and the test can share.
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

// failing is a ledger.Service whose every call fails with err.
type failing struct{ err error }

func (f failing) CreateAccount(context.Context, ledger.NewAccount) (ledger.Account, bool, error) {
	return ledger.Account{}, false, f.err
}
func (f failing) Account(context.Context, int64) (ledger.Account, error) {
	return ledger.Account{}, f.err
}
func (f failing) History(context.Context, int64, int64, int) ([]ledger.HistoryLine, error) {
	return nil, f.err
}
func (f failing) PostEntry(context.Context, string, ledger.Entry) (ledger.PostedEntry, bool, error) {
	return ledger.PostedEntry{}, false, f.err
}
func (f failing) Entry(context.Context, int64) (ledger.PostedEntry, error) {
	return ledger.PostedEntry{}, f.err
}

// An error nobody planned for, or the database being down, can carry hosts, users and queries. The client sees
// only the status text; the log gets the whole error.
func TestUnplannedAndUnavailableErrorsAreRedacted(t *testing.T) {
	_, down := pgconn.Connect(context.Background(), "postgres://ledger_admin@127.0.0.1:1/ledger?connect_timeout=2")
	cases := []struct {
		err    error
		code   int
		secret string
	}{
		{errors.New("dial tcp 10.1.2.3:5432: query SELECT secret FROM vault"), http.StatusInternalServerError, "10.1.2.3"},
		{down, http.StatusServiceUnavailable, "ledger_admin"},
	}
	for _, tc := range cases {
		var logs syncBuffer
		h := server.New(func(context.Context) error { return nil }, failing{tc.err},
			slog.New(slog.NewJSONHandler(&logs, nil)))
		for _, path := range []string{"/v1/accounts/1", "/v1/entries/1"} {
			r := call(t, h, "GET", path, "", "")
			if r.code != tc.code || r.message() != http.StatusText(tc.code) {
				t.Errorf("%s, %v: %d %q, want %d %q", path, tc.err, r.code, r.message(), tc.code, http.StatusText(tc.code))
			}
			if body := strings.Join([]string{r.message(), r.header.Get("Content-Type")}, " "); strings.Contains(body, tc.secret) {
				t.Errorf("%s, %v: the client sees %q", path, tc.err, tc.secret)
			}
		}
		if !strings.Contains(logs.String(), tc.secret) {
			t.Errorf("%v: the log doesn't have the whole error: %s", tc.err, logs.String())
		}
	}
}
