package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// Migration is one SQL file, NNNN_name.sql, applied at most once per schema.
type Migration struct {
	Version  int
	Name     string
	SQL      string
	Checksum string // hex SHA-256 of SQL
}

var migrationName = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// ErrMigrations is wrapped by every error about the migration files themselves (bad names, gaps, an applied file
// that changed, a database newer than the files), as opposed to an error from running one.
var ErrMigrations = errors.New("migrations")

// LoadMigrations reads every file in the root of fsys. Each must be named NNNN_name.sql, and the versions must
// run 1, 2, 3, ... with no gaps or repeats, so the order is never in doubt.
func LoadMigrations(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []Migration
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if e.IsDir() || m == nil {
			return nil, fmt.Errorf("%w: %q is not named NNNN_name.sql", ErrMigrations, e.Name())
		}
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		version, _ := strconv.Atoi(m[1]) // four digits, so it always parses
		sum := sha256.Sum256(body)
		out = append(out, Migration{Version: version, Name: path.Base(e.Name()), SQL: string(body),
			Checksum: hex.EncodeToString(sum[:])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("%w: expected version %04d, found %s", ErrMigrations, i+1, m.Name)
		}
	}
	return out, nil
}

// Beginner is what Migrate needs from a connection: *pgxpool.Pool and *pgx.Conn both have it.
type Beginner interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// MigrateFS applies the pending migrations from fsys to the current schema (the first schema on the
// connection's search_path) and returns the versions it applied, in order.
//
// Everything happens in one transaction, so either every pending migration is applied or none is. The
// transaction first takes an advisory lock scoped to the schema: two processes starting at once apply each
// migration once, because the second waits for the first to commit and then finds nothing left to do.
//
// Applied migrations are checked against the files: if an applied file has changed, or the database has a
// version the files don't, MigrateFS applies nothing and returns an error wrapping ErrMigrations, rather than
// run code against a schema it doesn't match.
func MigrateFS(ctx context.Context, db Beginner, fsys fs.FS) (applied []int, err error) {
	migrations, err := LoadMigrations(fsys)
	if err != nil {
		return nil, err
	}
	// Read committed whatever the session's default. Under repeatable read or serializable the snapshot would be
	// taken by the lock statement, before the wait, so a runner that waited couldn't see what the first one
	// applied and would apply it again.
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin migration transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx) // the error being returned matters more than this one
		}
	}()

	// hashtext gives each schema its own lock key; 'ledger.migrate' keeps it apart from any other lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('ledger.migrate'), hashtext(current_schema()))`); err != nil {
		return nil, fmt.Errorf("take migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    integer PRIMARY KEY,
		name       text NOT NULL,
		checksum   text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	done, err := appliedChecksums(ctx, tx)
	if err != nil {
		return nil, err
	}
	for version, checksum := range done {
		if version < 1 || version > len(migrations) {
			return nil, fmt.Errorf("%w: the database has version %04d, which this binary doesn't have (newest is %04d)",
				ErrMigrations, version, len(migrations))
		}
		if m := migrations[version-1]; m.Checksum != checksum {
			return nil, fmt.Errorf("%w: %s was changed after it was applied; add a new migration instead",
				ErrMigrations, m.Name)
		}
	}

	applied = []int{} // not nil, so "nothing to do" logs as [] rather than null
	for _, m := range migrations {
		if _, ok := done[m.Version]; ok {
			continue
		}
		// With no arguments pgx uses the simple protocol, so a file may hold many statements.
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			return nil, fmt.Errorf("apply %s: %w", m.Name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			m.Version, m.Name, m.Checksum); err != nil {
			return nil, fmt.Errorf("record %s: %w", m.Name, err)
		}
		applied = append(applied, m.Version)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit migrations: %w", err)
	}
	return applied, nil
}

func appliedChecksums(ctx context.Context, tx pgx.Tx) (map[int]string, error) {
	rows, err := tx.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	done := make(map[int]string)
	var version int
	var checksum string
	_, err = pgx.ForEachRow(rows, []any{&version, &checksum}, func() error {
		done[version] = checksum
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	return done, nil
}
