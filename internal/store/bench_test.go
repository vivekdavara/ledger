package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// BenchmarkPostEntry posts one balanced entry the way a client would: begin, insert the entry, insert all its
// postings in one statement, commit. It runs with the commit-time check (migration 0003's two constraint
// triggers) on and off, so the difference is what checking I1 and I2 in the database costs. The posting trigger
// (balances and versions) stays on in both.
//
//	go test -run '^$' -bench PostEntry -benchtime 2s -count 5 ./internal/store
func BenchmarkPostEntry(b *testing.B) {
	for _, n := range []int{2, 8, 64} {
		for _, check := range []bool{true, false} {
			name := fmt.Sprintf("postings=%d/check=off", n)
			if check {
				name = fmt.Sprintf("postings=%d/check=on", n)
			}
			b.Run(name, func(b *testing.B) { benchmarkPostEntry(b, n, check) })
		}
	}
}

func benchmarkPostEntry(b *testing.B, n int, check bool) {
	db := storetest.Migrated(b)
	ctx := context.Background()
	accounts := make([]int64, n)
	amounts := make([]int64, n)
	for i := range n {
		err := db.QueryRow(ctx, `INSERT INTO accounts (name, type, currency, allow_negative)
			VALUES ($1, 'asset', 'USD', true) RETURNING id`, fmt.Sprintf("bench:%d", i)).Scan(&accounts[i])
		if err != nil {
			b.Fatal(err)
		}
		amounts[i] = 1 - 2*int64(i%2) // +1, -1, +1, ...: balanced for even n
	}
	if !check {
		for _, sql := range []string{
			`ALTER TABLE journal_entries DISABLE TRIGGER journal_entries_check`,
			`ALTER TABLE postings DISABLE TRIGGER postings_check`,
		} {
			if _, err := db.Exec(ctx, sql); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		tx, err := db.Begin(ctx)
		if err != nil {
			b.Fatal(err)
		}
		var entry int64
		if err := tx.QueryRow(ctx, `INSERT INTO journal_entries (description, posting_count) VALUES ('bench', $1)
			RETURNING id`, n).Scan(&entry); err != nil {
			b.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO postings (entry_id, account_id, currency, amount, balance_after,
			account_version) SELECT $1, a, 'USD', m, 0, 1 FROM unnest($2::bigint[], $3::bigint[]) AS p(a, m)`,
			entry, accounts, amounts); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkServicePostEntry posts the same entries through store.Store.PostEntry, the API's path: validate and
// hash in Go, then one SERIALIZABLE transaction that looks up the key, locks the accounts, inserts the entry, its
// postings and the key, and commits. Against BenchmarkPostEntry's check=on rows, the difference is what the
// service adds. "replay" sends one key again and again: the cost of answering a retry.
//
//	go test -run '^$' -bench ServicePostEntry -benchtime 2s -count 5 ./internal/store
func BenchmarkServicePostEntry(b *testing.B) {
	for _, n := range []int{2, 8, 64} {
		b.Run(fmt.Sprintf("postings=%d/new", n), func(b *testing.B) { benchmarkServicePostEntry(b, n, false) })
		b.Run(fmt.Sprintf("postings=%d/replay", n), func(b *testing.B) { benchmarkServicePostEntry(b, n, true) })
	}
}

func benchmarkServicePostEntry(b *testing.B, n int, replay bool) {
	db := storetest.Migrated(b)
	s := store.New(db)
	ctx := context.Background()
	e := ledger.Entry{Description: "bench", Postings: make([]ledger.Posting, n)}
	for i := range n {
		a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("bench:%d", i), Type: ledger.Asset,
			Currency: "USD", AllowNegative: true})
		if err != nil {
			b.Fatal(err)
		}
		e.Postings[i] = ledger.Posting{AccountID: a.ID, Currency: "USD", Amount: 1 - 2*int64(i%2)}
	}
	if _, _, err := s.PostEntry(ctx, "replayed", e); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		key := "replayed"
		if !replay {
			i++
			key = fmt.Sprintf("bench-%d", i)
		}
		if _, replayed, err := s.PostEntry(ctx, key, e); err != nil || replayed != replay {
			b.Fatalf("replayed %v, %v", replayed, err)
		}
	}
	if s.Retries() != 0 {
		b.Fatalf("%d retries with one client", s.Retries())
	}
}
