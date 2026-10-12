// Package server is the ledger's HTTP layer. Day 1 has the health endpoints; day 2 adds the REST API next to them.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// ReadyFunc reports whether the service can do its job right now: nil if it can, an error saying why not if not.
type ReadyFunc func(ctx context.Context) error

// readyTimeout bounds a readiness check, so a hung database makes /readyz fail fast instead of hanging with it.
const readyTimeout = 2 * time.Second

// New returns the handler for every route:
//
//   - GET /livez answers 200 while the process can serve HTTP at all. It checks no dependency, so an orchestrator
//     that restarts on failed liveness doesn't restart every instance when the database is down.
//   - GET /readyz answers 200 when ready reports nil, and 503 with the reason otherwise, so a load balancer stops
//     sending requests that would fail.
func New(ready ReadyFunc, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			log.Warn("not ready", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "reason": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) // the status is already sent; a failed write means the client left
}
