package store_test

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// eventsInOrder checks that the outbox holds exactly one event per entry, and that event ids follow entry ids.
func eventsInOrder(t *testing.T, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (entries int) {
	t.Helper()
	var events, mismatched int
	if err := db.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM journal_entries), (SELECT count(*) FROM outbox),
		       (SELECT count(*) FROM journal_entries e FULL JOIN outbox o ON o.entry_id = e.id
		         WHERE e.id IS NULL OR o.id IS NULL)`).Scan(&entries, &events, &mismatched); err != nil {
		t.Fatal(err)
	}
	if events != entries || mismatched != 0 {
		t.Fatalf("%d entries, %d events, %d without a partner", entries, events, mismatched)
	}
	var outOfOrder int
	if err := db.QueryRow(context.Background(), `
		SELECT count(*) FROM (SELECT entry_id, lag(entry_id) OVER (ORDER BY id) AS before FROM outbox) o
		 WHERE entry_id < before`).Scan(&outOfOrder); err != nil {
		t.Fatal(err)
	}
	if outOfOrder != 0 {
		t.Fatalf("%d events are out of entry order", outOfOrder)
	}
	return entries
}

// Every entry gets one event, written in its own transaction, whichever way it was written; an entry that rolls back
// leaves no event.
func TestEveryEntryHasOneEvent(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	b := wallets(t, s) // one entry through the service
	if _, _, err := s.PostEntry(context.Background(), "pay-1", payment(b, 1000)); err != nil {
		t.Fatal(err)
	}
	mustPost(t, db, "by hand", leg{b.alice, "USD", 100}, leg{b.bob, "USD", -100})
	if _, err := post(db, "unbalanced", leg{b.alice, "USD", 100}, leg{b.bob, "USD", -99}); constraint(err) != "entry_balanced" {
		t.Fatalf("got %v", err)
	}
	if err := rolledBack(t, db, func(tx pgx.Tx) error {
		id, err := insertEntry(tx, "rolled back", 2)
		if err == nil {
			err = insertPosting(tx, id, b.alice, "USD", 1)
		}
		if err == nil {
			err = insertPosting(tx, id, b.bob, "USD", -1)
		}
		var events int
		if err == nil {
			err = tx.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE entry_id = $1`, id).Scan(&events)
		}
		if err == nil && events != 1 {
			t.Errorf("inside its transaction, the entry has %d events, want 1", events)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n := eventsInOrder(t, db); n != 3 {
		t.Fatalf("%d entries, want 3", n)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NULL AND attempts = 0`); n != 3 {
		t.Fatalf("%d new events unpublished, want 3", n)
	}
}

// migrationsUpTo is the built-in migrations up to and including version n.
func migrationsUpTo(t *testing.T, n int) fs.FS {
	t.Helper()
	all, err := store.LoadMigrations(store.Migrations())
	if err != nil {
		t.Fatal(err)
	}
	fsys := fstest.MapFS{}
	for _, m := range all[:n] {
		data, err := fs.ReadFile(store.Migrations(), m.Name)
		if err != nil {
			t.Fatal(err)
		}
		fsys[m.Name] = &fstest.MapFile{Data: data}
	}
	return fsys
}

// Entries recorded before migration 0009 get their events from it, in order.
func TestTheOutboxMigrationBackfillsEarlierEntries(t *testing.T) {
	t.Parallel()
	db := storetest.EmptySchema(t)
	ctx := context.Background()
	if _, err := store.MigrateFS(ctx, db, migrationsUpTo(t, 8)); err != nil {
		t.Fatal(err)
	}
	s := store.New(db)
	b := wallets(t, s)
	for _, key := range []string{"pay-1", "pay-2"} {
		if _, _, err := s.PostEntry(ctx, key, payment(b, 100)); err != nil {
			t.Fatal(err)
		}
	}
	if applied, err := store.Migrate(ctx, db); err != nil || len(applied) != 1 || applied[0] != 9 {
		t.Fatalf("applied %v, %v; want 9 alone", applied, err)
	}
	if n := eventsInOrder(t, db); n != 3 {
		t.Fatalf("%d entries, want 3", n)
	}
	// And from then on the trigger adds them.
	if _, _, err := s.PostEntry(ctx, "pay-3", ledger.Entry{Postings: []ledger.Posting{
		pst(b.alice, "USD", 100), pst(b.bob, "USD", -100)}}); err != nil {
		t.Fatal(err)
	}
	if n := eventsInOrder(t, db); n != 4 {
		t.Fatalf("%d entries, want 4", n)
	}
}
