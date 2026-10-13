package server

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
)

// NewWithTimeout is New with a shorter request timeout, so a test can watch one pass.
func NewWithTimeout(ready ReadyFunc, svc ledger.Service, log *slog.Logger, timeout time.Duration) http.Handler {
	return newHandler(ready, svc, log, timeout)
}
