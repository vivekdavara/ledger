package grpcapi

import (
	"log/slog"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
	"google.golang.org/grpc"
)

// NewWithMaxCallTime is New with a shorter bound on calls, so a test can watch one pass.
func NewWithMaxCallTime(svc ledger.Service, log *slog.Logger, maxCall time.Duration) *grpc.Server {
	return newServer(svc, log, maxCall)
}
