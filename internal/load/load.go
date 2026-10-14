package load

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	mathrand "math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
)

// Layout is how transfers choose their accounts: how much they contend.
type Layout string

const (
	// Disjoint: each worker moves money back and forth between two wallets of its own, so no two transfers share an
	// account.
	Disjoint Layout = "disjoint"
	// Random: each transfer is between two different wallets, chosen uniformly or, with Skew, by a Zipf law.
	Random Layout = "random"
	// Hot: each transfer pays one merchant account from a wallet, so every transfer locks the merchant.
	Hot Layout = "hot"
)

// Layouts lists the layouts, in the order the results tables give them.
var Layouts = []Layout{Disjoint, Random, Hot}

// Config describes a run. A zero field means its default.
type Config struct {
	Layout    Layout  // default Random
	Wallets   int     // Random and Hot: how many wallets transfers choose from; default 100 (Disjoint has 2 per worker)
	Skew      float64 // Random and Hot: 0 chooses wallets uniformly; above 1, by a Zipf law, wallet k weighing (1+k)^-Skew
	Workers   int     // concurrent clients; default 16
	Transfers int     // transfers in all; default 3000
	// Pad creates that many unused accounts first, with Pad/2 entries among them, so that Postgres plans its reads
	// as for tables of that size rather than a page or two (DESIGN.md, "Why serializable aborts transfers that share
	// nothing").
	Pad int
	// Retry sends a transfer again, with its key, after a conflict or an unknown outcome, until it is answered or
	// ctx ends. Without it a conflict is counted as given up, and an unknown outcome is settled after the run.
	Retry  bool
	Prefix string // starts every account name and key of the run; default random, so runs can share a ledger
	Seed   uint64 // chooses the transfers; runs with the same seed and config send the same transfers
}

func (c Config) withDefaults() Config {
	if c.Layout == "" {
		c.Layout = Random
	}
	if c.Wallets <= 0 {
		c.Wallets = 100
	}
	if c.Workers <= 0 {
		c.Workers = 16
	}
	if c.Transfers <= 0 {
		c.Transfers = 3000
	}
	if c.Prefix == "" {
		var b [4]byte
		_, _ = rand.Read(b[:]) // never fails
		c.Prefix = "load-" + hex.EncodeToString(b[:]) + ":"
	}
	if c.Layout == Disjoint {
		c.Wallets = 2 * c.Workers
	}
	return c
}

func (c Config) validate() error {
	switch {
	case !slices.Contains(Layouts, c.Layout):
		return fmt.Errorf("load: unknown layout %q (the layouts are disjoint, random and hot)", c.Layout)
	case c.Wallets < 2:
		return errors.New("load: at least 2 wallets")
	case c.Skew != 0 && !(c.Skew > 1):
		return errors.New("load: a skew is 0 (uniform) or a Zipf exponent above 1")
	case c.Pad < 0:
		return errors.New("load: padding is not negative")
	}
	return nil
}

// Result is what a run did, as its clients saw it.
type Result struct {
	Config  Config
	Elapsed time.Duration // the transfers only: setup, padding and the check aren't counted
	// Each transfer ends in exactly one of these. Posted and Replayed count the transfers whose answer said they
	// posted: Replayed when the answer was a replay, because an earlier attempt of the same transfer had posted it.
	Posted, Replayed int
	GaveUp           int            // conflicts, without Retry
	Refused          map[string]int // other refusals, by reason
	// Unknown counts the transfers that got no answer, without Retry; each was then sent again with its key until
	// answered ("settled"), and is also counted where that answer puts it.
	Unknown int
	Retries int // requests sent again, during the run and when settling
	// The latency of a transfer runs from its first request to its answer, retries included; given-up and refused
	// transfers count, and settled ones don't.
	P50, P90, P99, Max time.Duration
	// Mismatches lists each account whose balance at the end isn't what the answers imply; empty when the books
	// agree with every answer the clients got.
	Mismatches []string
}

// PerSecond is the run's throughput: finished transfers per second.
func (r Result) PerSecond() float64 { return float64(r.Config.Transfers) / r.Elapsed.Seconds() }

// amount is what each transfer moves, in minor units; funding is what each wallet starts with, enough for every
// transfer to come from it.
const (
	amount  = 10
	funding = 1_000_000_000
)

// retryWait is the client's wait before sending a request again: full jitter, doubling from 5 ms up to 200 ms.
func retryWait(attempt int, rng *mathrand.Rand) time.Duration {
	ceiling := min(200*time.Millisecond, 5*time.Millisecond<<min(attempt, 6))
	return time.Duration(rng.Int64N(int64(ceiling)) + 1)
}

// send sends the request until it is answered or refused: forever after a conflict or an unknown outcome when retry
// is set, once otherwise. It returns the last error and how many times it sent the request again.
func send[T any](ctx context.Context, retry bool, rng *mathrand.Rand, req func() (T, error)) (T, int, error) {
	for attempt := 0; ; attempt++ {
		v, err := req()
		if o := Classify(err); !retry || o == Answered || o == Refused {
			return v, attempt, err
		}
		select {
		case <-ctx.Done():
			return v, attempt, errors.Join(ctx.Err(), err)
		case <-time.After(retryWait(attempt, rng)):
		}
	}
}

// accounts are the run's accounts.
type accounts struct {
	cash, merchant int64
	wallets        []int64
}

// setUp creates the run's accounts and funds each wallet, retrying as cfg says (a setup request that conflicts or
// goes unanswered is always sent again: setup must finish for the run to mean anything).
func setUp(ctx context.Context, c Client, cfg Config) (accounts, error) {
	rng := mathrand.New(mathrand.NewPCG(cfg.Seed, 2))
	open := func(name string, typ ledger.AccountType) (int64, error) {
		id, _, err := send(ctx, true, rng, func() (int64, error) {
			return c.CreateAccount(ctx, ledger.NewAccount{Name: cfg.Prefix + name, Type: typ, Currency: "USD"})
		})
		return id, err
	}
	var a accounts
	var err error
	if a.cash, err = open("cash", ledger.Asset); err != nil {
		return a, fmt.Errorf("create the cash account: %w", err)
	}
	if a.merchant, err = open("merchant", ledger.Liability); err != nil {
		return a, fmt.Errorf("create the merchant account: %w", err)
	}
	a.wallets = make([]int64, cfg.Wallets)
	for i := range a.wallets {
		if a.wallets[i], err = open(fmt.Sprintf("wallet:%d", i), ledger.Liability); err != nil {
			return a, fmt.Errorf("create wallet %d: %w", i, err)
		}
		fund := ledger.Entry{Description: "funding", Postings: []ledger.Posting{
			{AccountID: a.cash, Currency: "USD", Amount: funding},
			{AccountID: a.wallets[i], Currency: "USD", Amount: -funding}}}
		if _, _, err := send(ctx, true, rng, func() (Posted, error) {
			return c.PostEntry(ctx, fmt.Sprintf("%sfund-%d", cfg.Prefix, i), fund)
		}); err != nil {
			return a, fmt.Errorf("fund wallet %d: %w", i, err)
		}
	}
	return a, nil
}

// pad creates cfg.Pad accounts that allow negative balances and cfg.Pad/2 one-cent entries between neighbours,
// cfg.Workers at a time.
func pad(ctx context.Context, c Client, cfg Config) error {
	ids := make([]int64, cfg.Pad)
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make([]error, cfg.Workers)
	work := func(w int, n int, do func(rng *mathrand.Rand, i int) error) {
		rng := mathrand.New(mathrand.NewPCG(cfg.Seed, uint64(3+w)))
		for {
			i := int(next.Add(1)) - 1
			if i >= n || errs[w] != nil {
				return
			}
			errs[w] = do(rng, i)
		}
	}
	for w := range cfg.Workers {
		wg.Go(func() {
			work(w, cfg.Pad, func(rng *mathrand.Rand, i int) (err error) {
				ids[i], _, err = send(ctx, true, rng, func() (int64, error) {
					return c.CreateAccount(ctx, ledger.NewAccount{Name: fmt.Sprintf("%spad:%d", cfg.Prefix, i),
						Type: ledger.Liability, Currency: "USD", AllowNegative: true})
				})
				return err
			})
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("create the padding accounts: %w", err)
	}
	next.Store(0)
	for w := range cfg.Workers {
		wg.Go(func() {
			work(w, cfg.Pad/2, func(rng *mathrand.Rand, i int) error {
				e := ledger.Entry{Description: "padding", Postings: []ledger.Posting{
					{AccountID: ids[2*i], Currency: "USD", Amount: 1}, {AccountID: ids[2*i+1], Currency: "USD", Amount: -1}}}
				_, _, err := send(ctx, true, rng, func() (Posted, error) {
					return c.PostEntry(ctx, fmt.Sprintf("%spad-%d", cfg.Prefix, i), e)
				})
				return err
			})
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("post the padding entries: %w", err)
	}
	return nil
}

// chooser picks transfers' accounts for one worker.
type chooser struct {
	cfg  Config
	acct accounts
	rng  *mathrand.Rand
	zipf *mathrand.Zipf
}

func (ch *chooser) wallet() int {
	if ch.zipf != nil {
		return int(ch.zipf.Uint64())
	}
	return ch.rng.IntN(len(ch.acct.wallets))
}

// transfer returns worker w's i-th transfer.
func (ch *chooser) transfer(w, i int) (from, to int64) {
	switch ch.cfg.Layout {
	case Disjoint:
		from, to = ch.acct.wallets[2*w], ch.acct.wallets[2*w+1]
		if i%2 == 1 {
			from, to = to, from
		}
	case Random:
		x, y := ch.wallet(), ch.wallet()
		for y == x {
			y = ch.wallet()
		}
		from, to = ch.acct.wallets[x], ch.acct.wallets[y]
	case Hot:
		from, to = ch.acct.wallets[ch.wallet()], ch.acct.merchant
	}
	return from, to
}

// worker is one client's share of a run.
type worker struct {
	delta                                       map[int64]int64 // what the transfers answered as posted moved, by account
	touched                                     map[int64]int64 // how many of those transfers posted to each account
	latencies                                   []time.Duration
	unknown                                     []pending // transfers that got no answer, to settle after the run
	posted, replayed, gaveUp, unknowns, retries int
	refused                                     map[string]int
	err                                         error
}

type pending struct {
	key      string
	from, to int64
	began    time.Time
}

func (w *worker) apply(from, to int64) {
	w.delta[from] += amount
	w.delta[to] -= amount
	w.touched[from]++
	w.touched[to]++
}

// Run sets up the run's accounts (and the padding), posts cfg.Transfers transfers from cfg.Workers concurrent
// clients, settles any transfer whose outcome is unknown, and checks every account of the run against what the
// answers say was posted. It fails only when it can't do that: a setup request refused, ctx ended.
func Run(ctx context.Context, c Client, cfg Config) (Result, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	acct, err := setUp(ctx, c, cfg)
	if err != nil {
		return Result{}, err
	}
	if cfg.Pad > 0 {
		if err := pad(ctx, c, cfg); err != nil {
			return Result{}, err
		}
	}

	workers := make([]*worker, cfg.Workers)
	var wg sync.WaitGroup
	start := time.Now()
	for w := range cfg.Workers {
		wk := &worker{delta: map[int64]int64{}, touched: map[int64]int64{}, refused: map[string]int{}}
		workers[w] = wk
		ch := &chooser{cfg: cfg, acct: acct, rng: mathrand.New(mathrand.NewPCG(cfg.Seed, uint64(1000+w)))}
		if cfg.Skew > 1 {
			ch.zipf = mathrand.NewZipf(ch.rng, cfg.Skew, 1, uint64(len(acct.wallets)-1))
		}
		wg.Go(func() {
			// Worker w sends transfers w+1, w+1+Workers, ... in order, each chosen by the worker's own generator, so
			// a transfer's key always names the same transfer: a second run with the same seed and prefix sends the
			// same requests, and every one replays. The waits before sending again draw from another generator, so
			// retries don't change which transfers follow.
			jitter := mathrand.New(mathrand.NewPCG(cfg.Seed, uint64(5000+w)))
			for i := 0; ; i++ {
				n := w + 1 + i*cfg.Workers
				if n > cfg.Transfers {
					return
				}
				from, to := ch.transfer(w, i)
				p := pending{key: fmt.Sprintf("%st%d", cfg.Prefix, n), from: from, to: to, began: time.Now()}
				posted, retries, err := send(ctx, cfg.Retry, jitter, func() (Posted, error) {
					return c.PostEntry(ctx, p.key, entry(from, to))
				})
				wk.retries += retries
				if ctx.Err() != nil {
					wk.err = fmt.Errorf("transfer %s: %w", p.key, errors.Join(ctx.Err(), err))
					return
				}
				switch Classify(err) {
				case Answered:
					wk.answered(posted, p)
				case Refused:
					wk.refused[Reason(err)]++
					wk.latencies = append(wk.latencies, time.Since(p.began))
				case Conflict:
					wk.gaveUp++
					wk.latencies = append(wk.latencies, time.Since(p.began))
				case Unknown:
					wk.unknowns++
					wk.unknown = append(wk.unknown, p)
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := Result{Config: cfg, Elapsed: elapsed, Refused: map[string]int{}}
	rng := mathrand.New(mathrand.NewPCG(cfg.Seed, 4))
	for _, wk := range workers {
		if wk.err != nil {
			return Result{}, wk.err
		}
		// Settle: send each unanswered transfer again with its key until it is answered. A replay means the first
		// request had posted it; a fresh post means it hadn't. Either way it has now posted once.
		for _, p := range wk.unknown {
			posted, retries, err := send(ctx, true, rng, func() (Posted, error) {
				return c.PostEntry(ctx, p.key, entry(p.from, p.to))
			})
			wk.retries += retries + 1
			switch Classify(err) {
			case Answered:
				wk.count(posted, p)
			case Refused:
				wk.refused[Reason(err)]++
			default:
				return Result{}, fmt.Errorf("settle transfer %s: %w", p.key, err)
			}
		}
		res.Posted += wk.posted
		res.Replayed += wk.replayed
		res.GaveUp += wk.gaveUp
		res.Unknown += wk.unknowns
		res.Retries += wk.retries
		for reason, n := range wk.refused {
			res.Refused[reason] += n
		}
	}
	all := slices.Concat(func() [][]time.Duration {
		out := make([][]time.Duration, len(workers))
		for i, wk := range workers {
			out[i] = wk.latencies
		}
		return out
	}()...)
	slices.Sort(all)
	if len(all) > 0 {
		at := func(p float64) time.Duration { return all[int(p*float64(len(all)-1))] }
		res.P50, res.P90, res.P99, res.Max = at(0.50), at(0.90), at(0.99), all[len(all)-1]
	}

	res.Mismatches, err = check(ctx, c, cfg, acct, workers)
	return res, err
}

// answered counts a transfer answered during the run, and its latency.
func (w *worker) answered(posted Posted, p pending) {
	w.count(posted, p)
	w.latencies = append(w.latencies, time.Since(p.began))
}

// count counts an answered transfer and applies it to the worker's deltas. A settled transfer is counted but not
// timed: its time includes the wait for the run to end.
func (w *worker) count(posted Posted, p pending) {
	if posted.Replayed {
		w.replayed++
	} else {
		w.posted++
	}
	w.apply(p.from, p.to)
}

// entry is the transfer of amount from one wallet to another account.
func entry(from, to int64) ledger.Entry {
	return ledger.Entry{Description: "transfer", Postings: []ledger.Posting{
		{AccountID: from, Currency: "USD", Amount: amount}, {AccountID: to, Currency: "USD", Amount: -amount}}}
}

// check compares each of the run's accounts with what the answers imply: cash holds every wallet's funding, each
// wallet its funding moved by the transfers answered as posted, the merchant what it was paid. Versions count
// postings, so each account must also have exactly as many postings as those transfers (and its funding) gave it. A
// transfer posted twice, or answered as posted and lost, shows up as a mismatch, even where balances alone would
// hide it: in the disjoint layout each worker's transfers go back and forth between its two wallets, so a transfer
// posted twice moves the balances by the same amount as one posted twice in the other direction (found in review).
func check(ctx context.Context, c Client, cfg Config, acct accounts, workers []*worker) ([]string, error) {
	want := map[int64]Account{acct.cash: {Balance: int64(len(acct.wallets)) * funding,
		Version: int64(len(acct.wallets))}, acct.merchant: {}}
	for _, w := range acct.wallets {
		want[w] = Account{Balance: -funding, Version: 1}
	}
	for _, wk := range workers {
		for id, d := range wk.delta {
			a := want[id]
			a.Balance += d
			want[id] = a
		}
		for id, n := range wk.touched {
			a := want[id]
			a.Version += n
			want[id] = a
		}
	}
	var mismatches []string
	rng := mathrand.New(mathrand.NewPCG(cfg.Seed, 5))
	for _, id := range slices.Sorted(maps.Keys(want)) {
		got, _, err := send(ctx, true, rng, func() (Account, error) { return c.Account(ctx, id) })
		if err != nil {
			return nil, fmt.Errorf("read account %d: %w", id, err)
		}
		if got != want[id] {
			mismatches = append(mismatches, fmt.Sprintf("account %d: balance %d after %d postings, the answers imply %d "+
				"after %d", id, got.Balance, got.Version, want[id].Balance, want[id].Version))
		}
	}
	return mismatches, nil
}
