package grpcapi

import (
	"context"
	"log/slog"
	"time"

	ledgerv1 "github.com/vivekdavara/ledger/internal/gen/ledger/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// readyTimeout bounds one health check, as it bounds /readyz: a hung database makes the check fail fast instead of
// hanging with it.
const readyTimeout = 2 * time.Second

// RegisterHealth adds the standard health service (grpc.health.v1.Health) to s, answering as /readyz does: Check
// runs ready, within readyTimeout, and answers SERVING when it reports nil and NOT_SERVING when it reports an
// error, for the server as a whole (service "") and for ledger.v1.LedgerService. Any other service is NOT_FOUND,
// as the health protocol says. So a gRPC client, grpcurl or a Kubernetes gRPC probe can ask what a load balancer
// asks over HTTP, on the gRPC port. Liveness stays /livez: a check that fails when the database is down would get
// every instance restarted at once.
//
// Watch and List are left unimplemented (UNIMPLEMENTED): Watch would mean polling the database for every watcher,
// and grpc-go's client-side health checking treats UNIMPLEMENTED as "no health checking", not as unhealthy.
func RegisterHealth(s *grpc.Server, ready func(context.Context) error, log *slog.Logger) {
	healthpb.RegisterHealthServer(s, &health{ready: ready, log: log})
}

type health struct {
	healthpb.UnimplementedHealthServer
	ready func(context.Context) error
	log   *slog.Logger
}

func (h *health) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	switch req.GetService() {
	case "", ledgerv1.LedgerService_ServiceDesc.ServiceName:
	default:
		return nil, status.Errorf(codes.NotFound, "unknown service %q", req.GetService())
	}
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	if err := h.ready(ctx); err != nil {
		// The reason goes to the log only: the response has no room for it, and the database's error names its
		// user, database and host.
		h.log.Warn("not ready", "err", err)
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}
