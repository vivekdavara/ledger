// Package store is the ledger's PostgreSQL layer: connecting, migrating, and (from day 2) posting entries.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open connects to Postgres and checks that the connection works. Its errors never show the password: pgx
// redacts it from the URL it quotes.
func Open(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse the database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create the connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return pool, nil
}

//go:embed migrations/*.sql
var embedded embed.FS

// Migrations returns the migrations built into the binary.
func Migrations() fs.FS {
	sub, err := fs.Sub(embedded, "migrations")
	if err != nil {
		panic(err) // "migrations" is a valid path, so Sub can't fail
	}
	return sub
}

// Migrate applies the built-in migrations that haven't been applied yet. See MigrateFS.
func Migrate(ctx context.Context, db Beginner) ([]int, error) {
	return MigrateFS(ctx, db, Migrations())
}

// Querier is what Ready needs: *pgxpool.Pool, *pgx.Conn and pgx.Tx all have it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Ready reports whether the database answers and its schema is at this binary's newest migration. A server
// that isn't ready shouldn't get traffic: it would fail every request, or run against a schema it doesn't match.
func Ready(ctx context.Context, db Querier) error {
	migrations, err := LoadMigrations(Migrations())
	if err != nil {
		return err
	}
	var version int
	err = db.QueryRow(ctx, `SELECT coalesce(max(version), 0) FROM schema_migrations`).Scan(&version)
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr) && pgErr.Code == "42P01": // undefined_table: never migrated
		version = 0
	case err != nil:
		return fmt.Errorf("database unavailable: %w", err)
	}
	if latest := len(migrations); version != latest {
		return fmt.Errorf("schema is at version %d, this binary needs %d: run ledgerd migrate", version, latest)
	}
	return nil
}
