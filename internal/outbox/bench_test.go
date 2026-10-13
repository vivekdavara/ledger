package outbox_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/vivekdavara/ledger/internal/outbox"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// BenchmarkPublish measures how fast one publisher drains an outbox of 10,000 two-posting entries into a sink that
// encodes each event as JSON and discards it (README, "Results", day 3). Each op publishes all 10,000; between ops
// the events are marked unpublished again, off the clock.
func BenchmarkPublish(b *testing.B) {
	const entries = 10000
	for _, batch := range []int{100, 1000} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			db := storetest.Migrated(b)
			ctx := context.Background()
			// Entry k moves 1 cent between accounts 2k-1 and 2k, so no account is in two postings of the statement.
			// One transaction: the commit-time checks need each entry's postings in it.
			tx, err := db.Begin(ctx)
			if err != nil {
				b.Fatal(err)
			}
			for _, sql := range []string{
				`INSERT INTO accounts (name, type, currency, allow_negative)
				 SELECT 'wallet:' || n, 'liability', 'USD', true FROM generate_series(1, 2 * $1::int) n`,
				`INSERT INTO journal_entries (description, posting_count) SELECT 'transfer', 2 FROM generate_series(1, $1::int)`,
				`INSERT INTO postings (entry_id, account_id, currency, amount, balance_after, account_version)
				 SELECT (a.id + 1) / 2, a.id, 'USD', CASE WHEN a.id % 2 = 0 THEN 1 ELSE -1 END, 0, 1
				   FROM accounts a WHERE a.id <= 2 * $1::int`,
			} {
				if _, err := tx.Exec(ctx, sql, entries); err != nil {
					b.Fatal(err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				b.Fatal(err)
			}
			if _, err := db.Exec(ctx, `ANALYZE accounts, journal_entries, postings, outbox`); err != nil {
				b.Fatal(err)
			}
			p := outbox.New(db, &outbox.JSONLines{W: io.Discard}, outbox.Options{BatchSize: batch})
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				if _, err := db.Exec(ctx, `UPDATE outbox SET published_at = NULL`); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				published := 0
				for {
					n, err := p.PublishOnce(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if n == 0 {
						break
					}
					published += n
				}
				if published != entries {
					b.Fatalf("published %d of %d", published, entries)
				}
			}
			b.ReportMetric(float64(entries*b.N)/b.Elapsed().Seconds(), "events/s")
		})
	}
}
