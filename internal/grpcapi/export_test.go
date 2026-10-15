package grpcapi

import (
	"log/slog"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
)

// NewWithMaxCallTime is New with a shorter bound on calls, so a test can watch one pass.
func NewWithMaxCallTime(svc ledger.Service, log *slog.Logger, maxCall time.Duration) *Server {
	return newServer(svc, log, maxCall)
}
