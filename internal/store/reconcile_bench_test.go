package store_test

import (
	"context"
	"testing"

	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

// BenchmarkReconcile measures one reconciliation of a ledger of 100,000 two-posting entries over 200,000 accounts
// (README, "Results", day 3).
func BenchmarkReconcile(b *testing.B) {
	db := storetest.Migrated(b)
	pad(b, db, 200000, 100000)
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		r, err := store.Reconcile(ctx, db)
		if err != nil {
			b.Fatal(err)
		}
		if !r.Clean() || r.Postings != 200000 {
			b.Fatalf("report %+v", r)
		}
	}
	b.ReportMetric(float64(200000*b.N)/b.Elapsed().Seconds(), "postings/s")
}
