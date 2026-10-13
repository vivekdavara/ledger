// Package outbox delivers the ledger's events. Every journal entry has one entry_posted event in the outbox table,
// written by a trigger in the entry's own transaction (migration 0009); a Publisher delivers the events that aren't
// published yet to a Sink and marks them published (DESIGN.md, "Outbox (day 3)").
//
// Delivery is at least once: an event whose delivery succeeded but whose mark didn't commit (a crash, a lost
// connection) is delivered again. Consumers dedupe by the event's id.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EntryPosted is the type of the one kind of event there is so far.
const EntryPosted = "entry_posted"

// Posting is one posting of a posted entry, as recorded.
type Posting struct {
	ID             int64  `json:"posting_id"`
	AccountID      int64  `json:"account_id"`
	Currency       string `json:"currency"`
	Amount         int64  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
	AccountVersion int64  `json:"account_version"`
}

// Event is an outbox event: an entry that was posted, with its postings in account-id order. Amounts are minor units.
type Event struct {
	ID          int64     `json:"id"` // the outbox row's id; consumers dedupe by it
	Type        string    `json:"type"`
	EntryID     int64     `json:"entry_id"`
	Description string    `json:"description"`
	PostedAt    time.Time `json:"posted_at"`
	Postings    []Posting `json:"postings"`
}

// Sink is where events go.
type Sink interface {
	// Deliver delivers events, oldest first. It returns nil only once all of them are delivered. After an error the
	// publisher delivers every one of them again later, so a sink can see an event more than once.
	Deliver(ctx context.Context, events []Event) error
}

// JSONLines is a Sink that writes each event as one line of JSON.
type JSONLines struct{ W io.Writer }

// Deliver writes the events, and stops at the first error.
func (j JSONLines) Deliver(ctx context.Context, events []Event) error {
	for _, e := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := j.W.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// Options configure a Publisher; a zero field means its default.
type Options struct {
	BatchSize      int           // events claimed per batch; default 100
	DeliverTimeout time.Duration // how long a batch's delivery may take; default 10 s
	PollInterval   time.Duration // Run's wait when there was nothing to publish; default 200 ms
	MaxBackoff     time.Duration // Run's longest wait after a failure; default 10 s
}

func (o Options) withDefaults() Options {
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.DeliverTimeout <= 0 {
		o.DeliverTimeout = 10 * time.Second
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 10 * time.Second
	}
	return o
}

// Publisher delivers outbox events to a sink.
type Publisher struct {
	db   *pgxpool.Pool
	sink Sink
	opts Options
}

// New returns a Publisher that delivers db's outbox to sink.
func New(db *pgxpool.Pool, sink Sink, opts Options) *Publisher {
	return &Publisher{db: db, sink: sink, opts: opts.withDefaults()}
}

// maxErrorLength bounds the delivery error kept on an event.
const maxErrorLength = 500

// PublishOnce delivers one batch: it claims up to BatchSize unpublished events, oldest first, skipping events
// another publisher has claimed; delivers them; and marks them published, all in one transaction. It returns how
// many it published. When delivery fails, the events stay unpublished with the attempt recorded, and the error is
// returned. When the mark doesn't commit, the events were delivered but stay unpublished: the next batch delivers
// them again.
//
// The transaction runs at read committed whatever the session's default: at a higher level, an event another
// publisher marked after this transaction's snapshot would be a serialization failure, not a row to skip.
func (p *Publisher) PublishOnce(ctx context.Context) (int, error) {
	tx, err := p.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // after a commit this does nothing

	rows, err := tx.Query(ctx, `SELECT id, entry_id FROM outbox WHERE published_at IS NULL
		ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, p.opts.BatchSize)
	if err != nil {
		return 0, err
	}
	var ids, entryIDs []int64
	var id, entryID int64
	if _, err := pgx.ForEachRow(rows, []any{&id, &entryID}, func() error {
		ids, entryIDs = append(ids, id), append(entryIDs, entryID)
		return nil
	}); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, tx.Commit(ctx)
	}
	events, err := load(ctx, tx, ids, entryIDs)
	if err != nil {
		return 0, err
	}

	deliverCtx, cancel := context.WithTimeout(ctx, p.opts.DeliverTimeout)
	deliverErr := p.sink.Deliver(deliverCtx, events)
	cancel()
	if deliverErr != nil {
		message := deliverErr.Error()
		if len(message) > maxErrorLength {
			message = message[:maxErrorLength]
		}
		// Record the attempt. If that fails too, the events are unpublished all the same, and the delivery's error
		// is the one to report.
		if _, err := tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = ANY($1)`,
			ids, message); err == nil {
			_ = tx.Commit(ctx)
		}
		return 0, fmt.Errorf("deliver %d events from %d: %w", len(ids), ids[0], deliverErr)
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now(), attempts = attempts + 1, last_error = NULL
		WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark %d delivered events published: %w", len(ids), err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("mark %d delivered events published: %w", len(ids), err)
	}
	return len(ids), nil
}

// load builds the events of the claimed outbox rows, in the rows' order.
func load(ctx context.Context, tx pgx.Tx, ids, entryIDs []int64) ([]Event, error) {
	rows, err := tx.Query(ctx, `
		SELECT e.id, e.description, e.created_at,
		       p.id, p.account_id, p.currency, p.amount, p.balance_after, p.account_version
		  FROM journal_entries e JOIN postings p ON p.entry_id = e.id
		 WHERE e.id = ANY($1)
		 ORDER BY e.id, p.account_id`, entryIDs)
	if err != nil {
		return nil, err
	}
	byEntry := make(map[int64]*Event, len(entryIDs))
	var entryID int64
	var description string
	var postedAt time.Time
	var p Posting
	if _, err := pgx.ForEachRow(rows, []any{&entryID, &description, &postedAt,
		&p.ID, &p.AccountID, &p.Currency, &p.Amount, &p.BalanceAfter, &p.AccountVersion}, func() error {
		e, ok := byEntry[entryID]
		if !ok {
			e = &Event{Type: EntryPosted, EntryID: entryID, Description: description, PostedAt: postedAt}
			byEntry[entryID] = e
		}
		e.Postings = append(e.Postings, p)
		return nil
	}); err != nil {
		return nil, err
	}
	events := make([]Event, len(ids))
	for i, id := range ids {
		e, ok := byEntry[entryIDs[i]]
		if !ok {
			// The foreign key and the commit-time checks make this impossible: an event's entry exists and has
			// postings. Delivering an empty event would hide whatever broke that.
			return nil, fmt.Errorf("outbox event %d: entry %d has no postings", id, entryIDs[i])
		}
		events[i] = *e
		events[i].ID = id
	}
	return events, nil
}

// Run publishes until ctx is cancelled, and then returns ctx's error. After a full batch it publishes the next one
// at once; after a short one it waits PollInterval. After a failure it waits too, twice as long each time in a row
// up to MaxBackoff, and logs the failure through onError if that isn't nil.
func (p *Publisher) Run(ctx context.Context, onError func(error)) error {
	backoff := time.Duration(0)
	for {
		n, err := p.PublishOnce(ctx)
		var wait time.Duration
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			if onError != nil {
				onError(err)
			}
			backoff = min(max(2*backoff, p.opts.PollInterval), p.opts.MaxBackoff)
			wait = backoff
		case n == p.opts.BatchSize:
			backoff = 0
			continue
		default:
			backoff = 0
			wait = p.opts.PollInterval
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Dedupe returns events without the ones whose id seen already holds, and adds the rest to seen: what a consumer
// does with at-least-once delivery. It keeps the order.
func Dedupe(seen map[int64]bool, events []Event) []Event {
	return slices.DeleteFunc(slices.Clone(events), func(e Event) bool {
		if seen[e.ID] {
			return true
		}
		seen[e.ID] = true
		return false
	})
}

// ErrOutOfOrder is wrapped by CheckOrder's error.
var ErrOutOfOrder = errors.New("events out of order")

// CheckOrder checks that, account by account, events give each account's postings in version order with none
// missing: 1, 2, 3, ... from the account's first event on. It is the property a single publisher keeps for entries
// the service posted (DESIGN.md, "Outbox (day 3)").
func CheckOrder(events []Event) error {
	last := map[int64]int64{}
	for _, e := range events {
		for _, p := range e.Postings {
			if prev, seen := last[p.AccountID]; seen && p.AccountVersion != prev+1 {
				return fmt.Errorf("%w: account %d's version %d came after %d (event %d)", ErrOutOfOrder,
					p.AccountID, p.AccountVersion, prev, e.ID)
			}
			last[p.AccountID] = p.AccountVersion
		}
	}
	return nil
}
