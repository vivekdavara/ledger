// Package storetest gives each test a schema of its own in the test database, so tests can run in parallel and
// never see each other's rows. The database comes from LEDGER_TEST_DATABASE_URL.
package storetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	neturl "net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/store"
)

// EnvURL names the environment variable that holds the test database's URL.
const EnvURL = "LEDGER_TEST_DATABASE_URL"

// URL returns the test database's URL. When it isn't set the test is skipped, unless CI is set: there the test
// fails, so CI can't go green by skipping every database test.
func URL(t testing.TB) string {
	t.Helper()
	url := os.Getenv(EnvURL)
	if url == "" {
		if os.Getenv("CI") != "" {
			t.Fatalf("%s is not set; CI must run the database tests", EnvURL)
		}
		t.Skipf("%s is not set; see the README to run the database tests", EnvURL)
	}
	return url
}

// EmptySchema creates a new, empty schema and returns a pool whose connections use only that schema. The pool is
// closed and the schema dropped when the test ends.
func EmptySchema(t testing.TB) *pgxpool.Pool {
	t.Helper()
	url := URL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var random [8]byte
	_, _ = rand.Read(random[:]) // crypto/rand.Read never fails
	schema := "test_" + hex.EncodeToString(random[:])
	exec(t, ctx, url, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse %s: %v", EnvURL, err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 16 // enough for the concurrency tests to really run at once
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		// The test's own context is already cancelled when cleanups run, so use a fresh one.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		exec(t, ctx, url, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	return pool
}

func exec(t testing.TB, ctx context.Context, url, sql string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect to the test database: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// Migrated is EmptySchema with every built-in migration applied.
func Migrated(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := EmptySchema(t)
	if _, err := store.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// SchemaURL returns a database URL whose connections use db's schema, for code under test that opens its own pool
// from a URL. pgx passes unknown URL parameters such as search_path to the server as settings.
func SchemaURL(t testing.TB, db *pgxpool.Pool) string {
	t.Helper()
	var schema string
	if err := db.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	u, err := neturl.Parse(URL(t))
	if err != nil {
		t.Fatalf("parse %s: %v", EnvURL, err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}
