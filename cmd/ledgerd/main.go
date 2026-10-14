// Command ledgerd runs the ledger service.
//
//	ledgerd migrate   apply pending migrations, then exit
//	ledgerd serve     apply pending migrations, then serve REST and gRPC until SIGINT or SIGTERM
//	ledgerd publish   apply pending migrations, then deliver the outbox's events as JSON lines on stdout, at least
//	                  once each, until SIGINT or SIGTERM
//	ledgerd reconcile recompute the books from the postings and print the report as JSON on stdout; exit 0 if it
//	                  found no difference, 3 if it found some, 1 if it couldn't run. It changes nothing, migrations
//	                  included: the schema must be current.
//
// A usage error exits 2, whatever the subcommand.
//
// Configuration comes from the environment: LEDGER_DATABASE_URL (required), LEDGER_HTTP_ADDR (REST and the health
// endpoints, default :8103), LEDGER_GRPC_ADDR (default :9103), LEDGER_SHUTDOWN_GRACE (how long in-flight work
// gets after a stop signal, default 10s) and LEDGER_ISOLATION (the posting transactions' isolation level,
// read-committed or serializable; see DESIGN.md, "Concurrency (days 2 and 3)"). LEDGER_FAULTS arms crash points for
// a crash test, in a binary built with -tags faults (internal/faults); any other build refuses to start with it.
// Logs are JSON lines on stderr.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/faults"
	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/outbox"
	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
	"google.golang.org/grpc"
)

const (
	defaultHTTPAddr = ":8103"
	defaultGRPCAddr = ":9103"
	// defaultShutdownGrace is how long in-flight requests get to finish after a stop signal.
	defaultShutdownGrace = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr, nil))
}

// addrs are the addresses ledgerd listens on, once both ports are open.
type addrs struct{ HTTP, GRPC string }

// commands are ledgerd's subcommands.
var commands = []string{"migrate", "serve", "publish", "reconcile"}

// exitDifferences is reconcile's exit code when it found differences: distinct from 1, its own failure.
const exitDifferences = 3

// run is main without the process, so tests can drive it: it returns the exit code, and stops serving (or
// publishing) when ctx is cancelled. If listening isn't nil it receives the servers' addresses once both ports are
// open.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer,
	listening chan<- addrs) int {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	if len(args) != 1 || !slices.Contains(commands, args[0]) {
		fmt.Fprintln(stderr, "usage: ledgerd "+strings.Join(commands, "|"))
		return 2
	}
	if spec := getenv("LEDGER_FAULTS"); spec != "" {
		if _, err := faults.Arm(spec); err != nil {
			log.Error("LEDGER_FAULTS", "err", err)
			return 1
		}
		log.Warn("crash points armed: this process will kill itself on purpose", "faults", spec)
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

	// For serve and publish a stop signal is the normal way to end, so one that comes while they connect or migrate
	// is a clean stop too: they had done nothing yet. migrate and reconcile were asked for one job, and a stop leaves
	// it undone (found while building day 4's crash tests: serve and publish exited 1).
	stoppedEarly := func(err error) bool {
		if ctx.Err() == nil || (args[0] != "serve" && args[0] != "publish") {
			return false
		}
		log.Info("stopped before starting", "err", err)
		return true
	}
	db, err := store.Open(ctx, url)
	if err != nil {
		if stoppedEarly(err) {
			return 0
		}
		log.Error("open the database", "err", err)
		return 1
	}
	defer db.Close()
	if args[0] == "reconcile" {
		return reconcile(ctx, db, stdout, log)
	}
	applied, err := store.Migrate(ctx, db)
	if err != nil {
		if stoppedEarly(err) {
			return 0
		}
		log.Error("migrate", "err", err)
		return 1
	}
	log.Info("migrated", "applied", applied)
	switch args[0] {
	case "migrate":
		return 0
	case "publish":
		return publish(ctx, db, stdout, log)
	}

	grace := defaultShutdownGrace
	if v := getenv("LEDGER_SHUTDOWN_GRACE"); v != "" {
		if grace, err = time.ParseDuration(v); err != nil || grace < 0 {
			log.Error("LEDGER_SHUTDOWN_GRACE is not a duration such as 10s", "value", v)
			return 1
		}
	}
	isolation := store.DefaultIsolation
	if v := getenv("LEDGER_ISOLATION"); v != "" {
		if isolation, err = store.ParseIsolation(v); err != nil {
			log.Error("LEDGER_ISOLATION", "err", err)
			return 1
		}
	}
	return serve(ctx, db, store.NewWithOptions(db, store.Options{Isolation: isolation}), httpAddr, grpcAddr, grace,
		log, listening)
}

// reconcile runs the reconciliation and prints its report (DESIGN.md, "Reconciliation (day 3)").
func reconcile(ctx context.Context, db *pgxpool.Pool, out io.Writer, log *slog.Logger) int {
	if err := store.Ready(ctx, db); err != nil {
		log.Error("reconcile", "err", err)
		return 1
	}
	report, err := store.Reconcile(ctx, db)
	if err != nil {
		log.Error("reconcile", "err", err)
		return 1
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		log.Error("write the report", "err", err)
		return 1
	}
	if !report.Clean() {
		var failed []string
		for _, c := range report.Checks {
			if c.Differences > 0 {
				failed = append(failed, c.Name)
			}
		}
		log.Error("the books don't reconcile", "checks", failed)
		return exitDifferences
	}
	log.Info("reconciled", "entries", report.Entries, "postings", report.Postings)
	return 0
}

// publish delivers the outbox's events to out, one JSON line each, until ctx is cancelled (DESIGN.md, "Outbox
// (day 3)"). A failed write leaves the events unpublished, to be written again.
func publish(ctx context.Context, db *pgxpool.Pool, out io.Writer, log *slog.Logger) int {
	log.Info("publishing")
	err := outbox.New(db, &outbox.JSONLines{W: out}, outbox.Options{}).Run(ctx, func(err error) {
		log.Error("publish", "err", err)
	})
	if !errors.Is(err, context.Canceled) {
		log.Error("publish", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
}

// serve runs the REST and gRPC servers over one store until ctx is cancelled, then stops both gracefully: no new
// connections, and in-flight requests and calls get the grace period to finish. Whatever is still running after
// it is cut off: its context is cancelled, which cancels its database work, and its connection is closed.
func serve(ctx context.Context, db *pgxpool.Pool, svc *store.Store, httpAddr, grpcAddr string, grace time.Duration,
	log *slog.Logger, listening chan<- addrs) int {
	// Every REST request's context derives from base. At the end of the grace period cutOff cancels them all, which
	// stops the database work still running under them, and Close closes their connections (which by itself also
	// cancels a request's context). With neither, the handlers would run on until their own deadline and the pool's
	// Close would wait for them: TestShutdownCutsOffWorkStillRunningAfterTheGrace fails only with both removed.
	base, cutOff := context.WithCancel(context.Background())
	defer cutOff()
	httpSrv := &http.Server{
		Handler:           server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, svc, log),
		ReadHeaderTimeout: 5 * time.Second, // a client that trickles headers can't hold a connection open
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		BaseContext:       func(net.Listener) context.Context { return base },
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
	log.Info("serving", "http", httpLn.Addr().String(), "grpc", grpcLn.Addr().String(), "isolation", svc.Isolation())
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

	// Stop both at once. Shutdown and GracefulStop wait for in-flight work; at the end of the grace period it is cut
	// off: cutOff and Close for REST, Stop (which cancels the calls' contexts) for gRPC.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	grpcStopped := make(chan struct{})
	go func() { grpcSrv.GracefulStop(); close(grpcStopped) }()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("shut down http", "err", "requests still running after the grace period; cutting them off")
		cutOff()
		_ = httpSrv.Close()
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
	// ErrServerStopped means the stop came before Serve began: a stop as the server starts, which is clean.
	if err := <-grpcDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) && code == 0 {
		log.Error("serve grpc", "err", err)
		code = 1
	}
	if code == 0 {
		// The retries are the attempts aborted by Postgres (40001, 40P01) or lost to a request with the same key,
		// and tried again: what a load test can't see from outside (scripts/load.sh reads them here).
		log.Info("stopped", "posting_retries", svc.Retries())
	}
	return code
}
