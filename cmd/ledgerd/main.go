// Command ledgerd runs the ledger service.
//
//	ledgerd migrate   apply pending migrations, then exit
//	ledgerd serve     apply pending migrations, then serve REST and gRPC until SIGINT or SIGTERM
//
// Configuration comes from the environment: LEDGER_DATABASE_URL (required), LEDGER_HTTP_ADDR (REST and the health
// endpoints, default :8103) and LEDGER_GRPC_ADDR (default :9103). Logs are JSON lines on stderr.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
)

const (
	defaultHTTPAddr = ":8103"
	defaultGRPCAddr = ":9103"
	// shutdownGrace is how long in-flight requests get to finish after a stop signal.
	shutdownGrace = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stderr, nil))
}

// addrs are the addresses ledgerd listens on, once both ports are open.
type addrs struct{ HTTP, GRPC string }

// run is main without the process, so tests can drive it: it returns the exit code, and stops serving when ctx
// is cancelled. If listening isn't nil it receives the servers' addresses once both ports are open.
func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer,
	listening chan<- addrs) int {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if len(args) != 1 || (args[0] != "migrate" && args[0] != "serve") {
		fmt.Fprintln(stderr, "usage: ledgerd migrate|serve")
		return 2
	}
	url := getenv("LEDGER_DATABASE_URL")
	if url == "" {
		log.Error("LEDGER_DATABASE_URL is not set")
		return 1
	}
	httpAddr := getenv("LEDGER_HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = defaultHTTPAddr
	}
	grpcAddr := getenv("LEDGER_GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = defaultGRPCAddr
	}

	db, err := store.Open(ctx, url)
	if err != nil {
		log.Error("open the database", "err", err)
		return 1
	}
	defer db.Close()
	applied, err := store.Migrate(ctx, db)
	if err != nil {
		log.Error("migrate", "err", err)
		return 1
	}
	log.Info("migrated", "applied", applied)
	if args[0] == "migrate" {
		return 0
	}

	return serve(ctx, db, httpAddr, grpcAddr, log, listening)
}

// serve runs the REST and gRPC servers over one store until ctx is cancelled, then stops both gracefully: no new
// connections, in-flight requests and calls get shutdownGrace to finish, and whatever is left after that is cut.
func serve(ctx context.Context, db *pgxpool.Pool, httpAddr, grpcAddr string, log *slog.Logger,
	listening chan<- addrs) int {
	svc := store.New(db)
	httpSrv := &http.Server{
		Handler:           server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, svc, log),
		ReadHeaderTimeout: 5 * time.Second, // a client that trickles headers can't hold a connection open
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	grpcSrv := grpcapi.New(svc, log)
	httpLn, err := net.Listen("tcp", httpAddr)
	if err != nil {
		log.Error("listen", "addr", httpAddr, "err", err)
		return 1
	}
	grpcLn, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		httpLn.Close()
		log.Error("listen", "addr", grpcAddr, "err", err)
		return 1
	}
	log.Info("serving", "http", httpLn.Addr().String(), "grpc", grpcLn.Addr().String())
	if listening != nil {
		listening <- addrs{HTTP: httpLn.Addr().String(), GRPC: grpcLn.Addr().String()}
	}
	httpDone, grpcDone := make(chan error, 1), make(chan error, 1)
	go func() { httpDone <- httpSrv.Serve(httpLn) }()
	go func() { grpcDone <- grpcSrv.Serve(grpcLn) }()

	code := 0
	select {
	case err := <-httpDone:
		log.Error("serve http", "err", err)
		httpDone <- err // read again below
		code = 1
	case err := <-grpcDone:
		log.Error("serve grpc", "err", err)
		grpcDone <- err
		code = 1
	case <-ctx.Done():
	}

	// Stop both at once. GracefulStop has no deadline of its own, so Stop cuts it off at the grace period.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	grpcStopped := make(chan struct{})
	go func() { grpcSrv.GracefulStop(); close(grpcStopped) }()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("shut down http", "err", err)
		code = 1
	}
	select {
	case <-grpcStopped:
	case <-shutdownCtx.Done():
		log.Error("shut down grpc", "err", "calls still running after the grace period; cutting them off")
		grpcSrv.Stop()
		<-grpcStopped
		code = 1
	}
	if err := <-httpDone; !errors.Is(err, http.ErrServerClosed) && code == 0 {
		log.Error("serve http", "err", err)
		code = 1
	}
	if err := <-grpcDone; err != nil && code == 0 {
		log.Error("serve grpc", "err", err)
		code = 1
	}
	if code == 0 {
		log.Info("stopped")
	}
	return code
}
