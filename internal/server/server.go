// Package server is the ledger's HTTP layer: the health endpoints and the REST API (api.go).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
)

// ReadyFunc reports whether the service can do its job right now: nil if it can, an error saying why not if not.
type ReadyFunc func(ctx context.Context) error

// readyTimeout bounds a readiness check, so a hung database makes /readyz fail fast instead of hanging with it.
const readyTimeout = 2 * time.Second

// New returns the handler for every route: the REST API over svc (api.go; left out when svc is nil), and
//
//   - GET /livez answers 200 while the process can serve HTTP at all. It checks no dependency, so an orchestrator
//     that restarts on failed liveness doesn't restart every instance when the database is down.
//   - GET /readyz answers 200 when ready reports nil, and 503 with the reason otherwise, so a load balancer stops
//     sending requests that would fail.
func New(ready ReadyFunc, svc ledger.Service, log *slog.Logger) http.Handler {
	return newHandler(ready, svc, log, RequestTimeout)
}

func newHandler(ready ReadyFunc, svc ledger.Service, log *slog.Logger, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	if svc != nil {
		api{svc, log, timeout}.routes(mux)
	}
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			log.Warn("not ready", "err", err)
			// /readyz shares the API's port: a database that doesn't answer is reported without its connection
			// details (user, database, host), which go to the log (found in review).
			reason := err.Error()
			if errors.Is(err, store.ErrUnavailable) {
				reason = store.ErrUnavailable.Error()
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "reason": reason})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return jsonErrors(mux)
}

// jsonErrors answers the router's own 404 and 405 with the API's error JSON instead of its text/plain (found in
// review). A request no route takes goes to the mux into a recorder, which keeps its status and Allow header.
func jsonErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		rec := &recorder{header: http.Header{}, status: http.StatusOK}
		mux.ServeHTTP(rec, r)
		body := errorBody{Reason: "not_found", Message: "no such route: " + r.URL.Path}
		if rec.status == http.StatusMethodNotAllowed {
			w.Header().Set("Allow", rec.header.Get("Allow"))
			body = errorBody{Reason: "method_not_allowed",
				Message: fmt.Sprintf("%s is not allowed here; allowed: %s", r.Method, rec.header.Get("Allow"))}
		}
		writeJSON(w, rec.status, errorJSON{body})
	})
}

// recorder keeps what the mux's built-in handlers write, so jsonErrors can answer in their place.
type recorder struct {
	header http.Header
	status int
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(p []byte) (int, error) { return len(p), nil }
func (r *recorder) WriteHeader(status int)      { r.status = status }

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) // the status is already sent; a failed write means the client left
}
