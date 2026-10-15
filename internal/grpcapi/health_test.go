package grpcapi_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// healthClient serves LedgerService over db with the health service answering by ready, over an in-memory
// connection, and returns a health client for it.
func healthClient(t *testing.T, db *pgxpool.Pool, ready func(context.Context) error, log *slog.Logger) healthpb.HealthClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpcapi.New(store.New(db), log)
	grpcapi.RegisterHealth(srv, ready, log)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///ledger",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return healthpb.NewHealthClient(conn)
}

func check(t *testing.T, c healthpb.HealthClient, service string) (healthpb.HealthCheckResponse_ServingStatus, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	return resp.GetStatus(), err
}

func TestHealthIsServingWhenTheSchemaIsCurrent(t *testing.T) {
	db := storetest.Migrated(t)
	c := healthClient(t, db, func(ctx context.Context) error { return store.Ready(ctx, db) }, quiet)
	for _, service := range []string{"", "ledger.v1.LedgerService"} {
		if got, err := check(t, c, service); err != nil || got != healthpb.HealthCheckResponse_SERVING {
			t.Errorf("Check(%q) = %v, %v; want SERVING", service, got, err)
		}
	}
	if _, err := check(t, c, "ledger.v1.NoSuchService"); status.Code(err) != codes.NotFound {
		t.Errorf("Check of an unknown service: %v, want NotFound", err)
	}
}

func TestHealthIsNotServingBeforeMigrationAndSaysWhyInTheLog(t *testing.T) {
	db := storetest.EmptySchema(t) // never migrated: Ready reports schema version 0
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	c := healthClient(t, db, func(ctx context.Context) error { return store.Ready(ctx, db) }, log)
	if got, err := check(t, c, ""); err != nil || got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Check = %v, %v; want NOT_SERVING", got, err)
	}
	if !strings.Contains(logs.String(), "schema is at version 0") {
		t.Fatalf("the log doesn't say why: %s", logs.String())
	}
}

func TestAHungReadinessCheckFailsWithinItsTimeout(t *testing.T) {
	hung := func(ctx context.Context) error {
		<-ctx.Done()
		return errors.New("still waiting: " + ctx.Err().Error())
	}
	c := healthClient(t, storetest.Migrated(t), hung, quiet)
	start := time.Now()
	got, err := check(t, c, "")
	if err != nil || got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Check = %v, %v; want NOT_SERVING", got, err)
	}
	if took := time.Since(start); took > 4*time.Second {
		t.Fatalf("a hung check took %v, want about 2 s", took)
	}
}
