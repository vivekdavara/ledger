// Command ledgerload posts transfers to a running ledgerd through its REST or gRPC API, with a chosen contention, and
// reports throughput, latency, and whether the books agree with the answers (DESIGN.md, "Load and failure injection
// (day 4)"):
//
//	ledgerload -api grpc -layout hot -workers 16 -transfers 3000
//
// It exits 0 when every account of the run holds what the answers imply, 3 when one doesn't, 1 when the run couldn't
// finish (a setup request refused, the timeout reached) and 2 on a usage error. -json prints the result as one JSON
// object instead of text.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/vivekdavara/ledger/internal/load"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// exitMismatch is the exit code when the books don't agree with the answers: distinct from 1, a run that failed.
const exitMismatch = 3

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("ledgerload", flag.ContinueOnError)
	flags.SetOutput(stderr)
	api := flags.String("api", "rest", "the API to load: rest or grpc")
	addr := flags.String("addr", "", "the API's host:port (default localhost:8103 for rest, localhost:9103 for grpc)")
	layout := flags.String("layout", string(load.Random), "how transfers choose accounts: disjoint, random or hot")
	wallets := flags.Int("wallets", 100, "random and hot: how many wallets transfers choose from")
	skew := flags.Float64("skew", 0, "random and hot: 0 chooses wallets uniformly; above 1, by a Zipf law with this exponent")
	workers := flags.Int("workers", 16, "concurrent clients")
	transfers := flags.Int("transfers", 3000, "transfers in all")
	pad := flags.Int("pad", 0, "unused accounts to create first, with half as many entries among them")
	retry := flags.Bool("retry", false, "send a transfer again with its key after a conflict or no answer, until answered")
	seed := flags.Uint64("seed", 1, "chooses the transfers")
	prefix := flags.String("prefix", "", "starts the run's account names and keys (default random)")
	timeout := flags.Duration("timeout", 10*time.Minute, "give up on the run after this long")
	asJSON := flags.Bool("json", false, "print the result as JSON")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() > 0 || !slices.Contains([]string{"rest", "grpc"}, *api) {
		fmt.Fprintln(stderr, "usage: ledgerload [flags]; -api is rest or grpc; see -h")
		return 2
	}

	var client load.Client
	switch *api {
	case "rest":
		if *addr == "" {
			*addr = "localhost:8103"
		}
		client = load.NewREST("http://"+*addr, *workers)
	case "grpc":
		if *addr == "" {
			*addr = "localhost:9103"
		}
		c, err := load.NewGRPC(*addr)
		if err != nil {
			fmt.Fprintln(stderr, "ledgerload:", err)
			return 1
		}
		client = c
	}
	defer client.Close()

	// Run waits out a ledgerd that goes away during the run (a crash test restarts it); one that isn't there at the
	// start is a mistake to report at once. Any answer will do, "no such account" included.
	probe, cancelProbe := context.WithTimeout(ctx, 5*time.Second)
	_, err := client.Account(probe, 1)
	cancelProbe()
	if load.Classify(err) == load.Unknown {
		fmt.Fprintf(stderr, "ledgerload: nothing answers at %s: %v\n", *addr, err)
		return 1
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	res, err := load.Run(ctx, client, load.Config{Layout: load.Layout(*layout), Wallets: *wallets, Skew: *skew,
		Workers: *workers, Transfers: *transfers, Pad: *pad, Retry: *retry, Prefix: *prefix, Seed: *seed})
	if err != nil {
		fmt.Fprintln(stderr, "ledgerload:", err)
		return 1
	}
	if *asJSON {
		if err := json.NewEncoder(stdout).Encode(summary(*api, res)); err != nil {
			fmt.Fprintln(stderr, "ledgerload:", err)
			return 1
		}
	} else {
		report(stdout, *api, res)
	}
	if len(res.Mismatches) > 0 {
		return exitMismatch
	}
	return 0
}

// result is the JSON form of a run's result: times in microseconds, so a script can read them as numbers.
type result struct {
	API        string         `json:"api"`
	Layout     string         `json:"layout"`
	Wallets    int            `json:"wallets"`
	Skew       float64        `json:"skew"`
	Workers    int            `json:"workers"`
	Transfers  int            `json:"transfers"`
	Retry      bool           `json:"retry"`
	Seconds    float64        `json:"seconds"`
	PerSecond  float64        `json:"transfers_per_second"`
	Posted     int            `json:"posted"`
	Replayed   int            `json:"replayed"`
	GaveUp     int            `json:"gave_up"`
	Refused    map[string]int `json:"refused"`
	Unknown    int            `json:"unknown"`
	Retries    int            `json:"retries"`
	P50        int64          `json:"p50_us"`
	P90        int64          `json:"p90_us"`
	P99        int64          `json:"p99_us"`
	Max        int64          `json:"max_us"`
	Mismatches []string       `json:"mismatches"`
}

func summary(api string, r load.Result) result {
	return result{API: api, Layout: string(r.Config.Layout), Wallets: r.Config.Wallets, Skew: r.Config.Skew,
		Workers: r.Config.Workers, Transfers: r.Config.Transfers, Retry: r.Config.Retry, Seconds: r.Elapsed.Seconds(),
		PerSecond: r.PerSecond(), Posted: r.Posted, Replayed: r.Replayed, GaveUp: r.GaveUp, Refused: r.Refused,
		Unknown: r.Unknown, Retries: r.Retries, P50: r.P50.Microseconds(), P90: r.P90.Microseconds(),
		P99: r.P99.Microseconds(), Max: r.Max.Microseconds(), Mismatches: append([]string{}, r.Mismatches...)}
}

func report(w io.Writer, api string, r load.Result) {
	c := r.Config
	fmt.Fprintf(w, "%s %s (%d wallets, skew %v, %d workers): %d transfers in %.2f s, %.0f a second\n", api, c.Layout,
		c.Wallets, c.Skew, c.Workers, c.Transfers, r.Elapsed.Seconds(), r.PerSecond())
	var refused []string
	for _, reason := range slices.Sorted(maps.Keys(r.Refused)) {
		refused = append(refused, fmt.Sprintf("%s %d", reason, r.Refused[reason]))
	}
	if len(refused) == 0 {
		refused = []string{"0"}
	}
	fmt.Fprintf(w, "  posted %d, replayed %d, gave up %d, refused %s, unknown %d, sent again %d\n", r.Posted,
		r.Replayed, r.GaveUp, strings.Join(refused, ", "), r.Unknown, r.Retries)
	ms := func(d time.Duration) string { return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000) }
	fmt.Fprintf(w, "  latency p50 %s, p90 %s, p99 %s, max %s\n", ms(r.P50), ms(r.P90), ms(r.P99), ms(r.Max))
	if len(r.Mismatches) == 0 {
		fmt.Fprintln(w, "  books: every account agrees with the answers")
		return
	}
	fmt.Fprintf(w, "  books: %d accounts don't agree with the answers\n", len(r.Mismatches))
	for _, m := range r.Mismatches {
		fmt.Fprintln(w, "   ", m)
	}
}
