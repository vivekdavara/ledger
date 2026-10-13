// Command ledgerd runs the ledger service.
//
//	ledgerd migrate   apply pending migrations, then exit
//	ledgerd serve     apply pending migrations, then serve HTTP until SIGINT or SIGTERM
//
// Configuration comes from the environment: LEDGER_DATABASE_URL (required) and LEDGER_HTTP_ADDR (default
// :8103). Logs are JSON lines on stderr.
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

	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
)

const (
	defaultHTTPAddr = ":8103"
	// shutdownGrace is how long in-flight requests get to finish after a stop signal.
	shutdownGrace = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stderr, nil))
}

// run is main without the process, so tests can drive it: it returns the exit code, and stops serving when ctx
// is cancelled. If listening isn't nil it receives the server's address once the port is open.
func run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer,
	listening chan<- string) int {
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
	addr := getenv("LEDGER_HTTP_ADDR")
	if addr == "" {
		addr = defaultHTTPAddr
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

	srv := &http.Server{
		Handler:           server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, store.New(db), log),
		ReadHeaderTimeout: 5 * time.Second, // a client that trickles headers can't hold a connection open
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("listen", "addr", addr, "err", err)
		return 1
	}
	log.Info("serving", "addr", ln.Addr().String())
	if listening != nil {
		listening <- ln.Addr().String()
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		log.Error("serve", "err", err)
		return 1
	case <-ctx.Done():
	}
	// Stop accepting connections and let in-flight requests finish, then close the pool (deferred above).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shut down", "err", err)
		return 1
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		return 1
	}
	log.Info("stopped")
	return 0
}
