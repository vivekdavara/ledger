package store_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// At serializable, Postgres records what each transaction has read (SIRead locks), and two concurrent transactions
// that each wrote something the other read form a cycle: one is aborted with 40001. This test lists what one posting
// attempt has read by the time it commits, with the commit-time checks run early (SET CONSTRAINTS ALL IMMEDIATE) so
// that their reads are in the list. Every posting writes a journal entry and its postings, so a lock on the whole of
// either table makes any two concurrent postings conflict, however unrelated (DESIGN.md, "Concurrency (day 3)").
func TestWhatASerializablePostingHasRead(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		accounts, entries int
		wholeTables       []string // tables the posting must have read in full
	}{
		// Tables of a page or two are scanned, so the reads lock whole tables: these are the false conflicts.
		{0, 0, []string{"accounts", "journal_entries", "postings"}},
		// Accounts are read through their indexes, but the commit-time check still scans the small entries and
		// postings tables.
		{10000, 0, []string{"journal_entries", "postings"}},
		// At this size every read goes through an index, and only index pages are locked.
		{20000, 10000, nil},
	} {
		t.Run(fmt.Sprintf("%d accounts and %d entries of padding", tc.accounts, tc.entries), func(t *testing.T) {
			t.Parallel()
			db := storetest.Migrated(t)
			ctx := context.Background()
			ids := fundedWallets(t, store.New(db), 2, 10000)
			pad(t, db, tc.accounts, tc.entries)
			s := at(db, store.Serializable)
			var locks []string
			store.SetCommitHook(s, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
					return err
				}
				rows, err := tx.Query(ctx, `
					SELECT locktype || ' ' || coalesce(relation::regclass::text, '') ||
					       coalesce(' page ' || page, '') || coalesce(' tuple ' || tuple, '')
					  FROM pg_locks WHERE pid = pg_backend_pid() AND mode = 'SIReadLock'
					 ORDER BY 1`)
				if err != nil {
					return err
				}
				locks, err = pgx.CollectRows(rows, pgx.RowTo[string])
				return err
			})
			if _, _, err := s.PostEntry(ctx, "pay", ledger.Entry{Postings: []ledger.Posting{
				pst(ids[0], "USD", 100), pst(ids[1], "USD", -100)}}); err != nil {
				t.Fatal(err)
			}
			t.Logf("SIRead locks:\n%s", strings.Join(locks, "\n"))
			var whole []string
			for _, l := range locks {
				if table, ok := strings.CutPrefix(l, "relation "); ok {
					whole = append(whole, table)
				}
			}
			if !slices.Equal(whole, tc.wholeTables) {
				t.Fatalf("read whole tables %v, want %v", whole, tc.wholeTables)
			}
		})
	}
}
