package store_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

var padding = flag.Int("padding", 0, "BenchmarkContention: unused accounts to add, with half as many entries")

// contentionWorkers is how many goroutines post at once in BenchmarkContention: as many as the test pool's
// connections, so every worker has one.
const contentionWorkers = 16

// BenchmarkContention compares the two isolation levels on three layouts of accounts (README, "Results", day 3). Each
// op is one PostEntry of a two-posting transfer, and contentionWorkers goroutines share the b.N ops:
//
//   - disjoint: each worker moves money back and forth between two wallets of its own, so no two transfers share an
//     account;
//   - random: each transfer is between two of 100 wallets, chosen uniformly;
//   - hot: each transfer pays one merchant account from one of 100 wallets, so every transfer locks the merchant.
//
// With -padding N the ledger also holds N unused accounts and N/2 entries between them (pad), so that Postgres plans
// its reads as for tables of that size, through their indexes, instead of scanning a page or two.
//
// Besides ns/op it reports transfers/s, retries/op, gave-up (transfers that ran out of attempts, ErrConflict) and
// the p50 and p99 of one PostEntry's latency. Run it with a fixed count, e.g. -benchtime 2000x.
func BenchmarkContention(b *testing.B) {
	for _, layout := range []string{"disjoint", "random", "hot"} {
		for _, level := range levels {
			b.Run(layout+"/"+string(level), func(b *testing.B) { benchmarkContention(b, layout, level, *padding) })
		}
	}
}

func benchmarkContention(b *testing.B, layout string, level store.Isolation, padding int) {
	db := storetest.MigratedWithConns(b, contentionWorkers)
	ctx := context.Background()
	setup := store.New(db)
	open := func(name string, typ ledger.AccountType) int64 {
		a, _, err := setup.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: "USD"})
		if err != nil {
			b.Fatal(err)
		}
		return a.ID
	}
	cash := open("cash:bank", ledger.Asset)
	merchant := open("merchant", ledger.Liability)
	wallets := make([]int64, max(100, 2*contentionWorkers))
	for i := range wallets {
		wallets[i] = open(fmt.Sprintf("wallet:%d", i), ledger.Liability)
		if _, _, err := setup.PostEntry(ctx, fmt.Sprintf("fund-%d", i), ledger.Entry{Postings: []ledger.Posting{
			pst(cash, "USD", 1_000_000_000), pst(wallets[i], "USD", -1_000_000_000)}}); err != nil {
			b.Fatal(err)
		}
	}
	pad(b, db, padding, padding/2)
	transfer := func(worker, i int, rng *rand.Rand) ledger.Entry {
		var from, to int64
		switch layout {
		case "disjoint":
			from, to = wallets[2*worker], wallets[2*worker+1]
			if i%2 == 1 {
				from, to = to, from
			}
		case "random":
			x := rng.IntN(len(wallets))
			y := (x + 1 + rng.IntN(len(wallets)-1)) % len(wallets)
			from, to = wallets[x], wallets[y]
		case "hot":
			from, to = wallets[rng.IntN(len(wallets))], merchant
		}
		return ledger.Entry{Postings: []ledger.Posting{pst(from, "USD", 10), pst(to, "USD", -10)}}
	}

	s := at(db, level)
	var next, gaveUp atomic.Int64
	latencies := make([][]time.Duration, contentionWorkers)
	var wg sync.WaitGroup
	b.ResetTimer()
	start := time.Now()
	for w := range contentionWorkers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			for i := 0; ; i++ {
				n := next.Add(1)
				if n > int64(b.N) {
					return
				}
				began := time.Now()
				_, _, err := s.PostEntry(ctx, fmt.Sprintf("t%d", n), transfer(w, i, rng))
				latencies[w] = append(latencies[w], time.Since(began))
				switch {
				case errors.Is(err, ledger.ErrConflict):
					gaveUp.Add(1)
				case err != nil:
					b.Errorf("transfer %d: %v", n, err)
					return
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	all := slices.Concat(latencies...)
	slices.Sort(all)
	percentile := func(p float64) float64 { return float64(all[int(p*float64(len(all)-1))].Microseconds()) }
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "transfers/s")
	b.ReportMetric(float64(s.Retries())/float64(b.N), "retries/op")
	b.ReportMetric(float64(gaveUp.Load()), "gave-up")
	b.ReportMetric(percentile(0.50), "p50-µs")
	b.ReportMetric(percentile(0.99), "p99-µs")
}
