package load_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/vivekdavara/ledger/internal/load"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// This Mac's localhost is ::1 and 127.0.0.1. With grpc-go's default dns resolver, a client of localhost:<port> never
// reached a server listening on 127.0.0.1 alone: ledgerload's probe ended in DeadlineExceeded after its 5 s, day 4's
// build too, while REST (Go's own dialer) and grpcurl connected (found on day 5). Where localhost has one address, as
// on CI's Ubuntu, this passes either way.
func TestAGRPCClientOfLocalhostReachesAServerOnIPv4Alone(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer()) // SERVING for "" from the start
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	c, err := load.NewGRPC("localhost:" + port)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ready(ctx); err != nil {
		t.Fatalf("Ready over localhost:%s: %v", port, err)
	}
}
