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
	"strings"
	"time"
	"unicode/utf8"

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

// JSONLines is a Sink that writes each event as one line of JSON. It is not safe for concurrent use.
//
// A write that fails partway through a line leaves the line cut off, and the publisher delivers its events again.
// So the next delivery first ends the cut-off line: a consumer reading lines then sees it on its own, as a line that
// doesn't parse, followed by the events again, whole. Without that, the first event of the retry was glued onto the
// fragment and lost to the consumer, though marked published (found in review on day 3). Consumers skip lines that
// don't parse.
type JSONLines struct {
	W    io.Writer
	torn bool // the last write stopped partway through a line
}

// Deliver writes the events, and stops at the first error.
func (j *JSONLines) Deliver(ctx context.Context, events []Event) error {
	if j.torn {
		if _, err := j.W.Write([]byte{'\n'}); err != nil {
			return err
		}
		j.torn = false
	}
	for _, e := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		line = append(line, '\n')
		if n, err := j.W.Write(line); err != nil {
			j.torn = n > 0 && n < len(line)
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

// maxErrorLength bounds the delivery error kept on an event, in bytes.
const maxErrorLength = 500

// recordable makes a delivery error storable as text: valid UTF-8 with no NUL bytes, which Postgres refuses, and at
// most maxErrorLength bytes, cut where a character starts. Cut at byte 500 alone, an error with a multi-byte
// character across it, or a NUL anywhere, made the record fail, and the attempt went unrecorded (found in review).
func recordable(message string) string {
	message = strings.ReplaceAll(strings.ToValidUTF8(message, "\uFFFD"), "\x00", "")
	if len(message) <= maxErrorLength {
		return message
	}
	cut := maxErrorLength
	for !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut]
}

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
		// Record the attempt. If that fails too, the events are unpublished all the same, and both errors are
		// reported.
		_, err := tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = ANY($1)`,
			ids, recordable(deliverErr.Error()))
		if err == nil {
			err = tx.Commit(ctx)
		}
		if err != nil {
			return 0, fmt.Errorf("deliver %d events from %d: %w (and recording the attempt failed: %v)", len(ids),
				ids[0], deliverErr, err)
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

// markTimeout bounds how long marking a delivered batch may take, once a stop has been asked for.
const markTimeout = 5 * time.Second

// Run publishes until ctx is cancelled, and then returns ctx's error. A stop lets the batch in flight finish,
// delivered and marked, so that a restart doesn't deliver it again; that takes at most DeliverTimeout and 5 s more.
// (Before, a stop between a delivery and its mark cancelled the mark, and ledgerd publish's test failed now and then
// with an event left unpublished.) After a full batch Run publishes the next one at once; after a short one it waits
// PollInterval. After a failure it waits too (nextBackoff), and logs the failure through onError if that isn't nil.
func (p *Publisher) Run(ctx context.Context, onError func(error)) error {
	backoff := time.Duration(0)
	for ctx.Err() == nil {
		batch, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.opts.DeliverTimeout+markTimeout)
		n, err := p.PublishOnce(batch)
		cancel()
		if err != nil && onError != nil {
			onError(err)
		}
		if ctx.Err() != nil {
			break
		}
		var wait time.Duration
		switch {
		case err != nil:
			backoff = p.opts.nextBackoff(backoff)
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
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// nextBackoff is Run's wait after another failure in a row, given the wait after the one before (0 for the first):
// PollInterval, then twice as long each time, up to MaxBackoff.
func (o Options) nextBackoff(last time.Duration) time.Duration {
	return min(max(2*last, o.PollInterval), o.MaxBackoff)
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
