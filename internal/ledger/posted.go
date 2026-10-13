package ledger

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

// PostedPosting is a posting as recorded: with the account's balance after it and its place in the account's
// history (1, 2, 3, ... with no gaps).
type PostedPosting struct {
	ID             int64
	EntryID        int64
	AccountID      int64
	Currency       string
	Amount         int64
	BalanceAfter   int64
	AccountVersion int64
}

// PostedEntry is an entry as recorded, with its postings in account-id order.
type PostedEntry struct {
	ID          int64
	Description string
	CreatedAt   time.Time
	Postings    []PostedPosting
}

// HistoryLine is one line of an account's history: a posting and the entry it belongs to.
type HistoryLine struct {
	Posting     PostedPosting
	Description string
	PostedAt    time.Time
}

// MaxKey is the longest idempotency key, in bytes (keys are ASCII).
const MaxKey = 200

// Errors from posting an entry, besides the rules a ValidationError reports.
var (
	ErrIdempotencyKey = errors.New("invalid idempotency key")
	ErrKeyReused      = errors.New("idempotency key already used for a different request")
	ErrBalanceRange   = errors.New("the balance would leave the range the ledger can store")
	ErrConflict       = errors.New("the ledger was too busy to post the entry; retry with the same idempotency key")
)

// ValidKey checks an idempotency key: 1 to MaxKey visible ASCII characters (0x21 to 0x7E), so a key can travel in
// an HTTP header unchanged. Migration 0008 checks the same rule.
func ValidKey(key string) error {
	if key == "" || len(key) > MaxKey {
		return fmt.Errorf("%w: %d bytes, a key has 1 to %d", ErrIdempotencyKey, len(key), MaxKey)
	}
	for i := 0; i < len(key); i++ {
		if key[i] < '!' || key[i] > '~' {
			return fmt.Errorf("%w: byte %d is %#02x; a key is visible ASCII only", ErrIdempotencyKey, i, key[i])
		}
	}
	return nil
}

// Hash identifies what an entry does, for idempotency: two requests with the same key must have the same hash. It
// covers the description and the postings as a set (each account appears once, so sorting by account id gives one
// order), because the order of the postings in a request doesn't change the entry. Call it on a valid entry: the
// strings are then valid UTF-8, so the JSON encoding is unambiguous.
func (e Entry) Hash() [32]byte {
	postings := slices.Clone(e.Postings)
	slices.SortFunc(postings, func(a, b Posting) int { return cmp.Compare(a.AccountID, b.AccountID) })
	canonical, err := json.Marshal(struct {
		Version     int       `json:"v"`
		Description string    `json:"description"`
		Postings    []Posting `json:"postings"`
	}{1, e.Description, postings})
	if err != nil {
		panic(err) // structs of strings and integers always marshal
	}
	return sha256.Sum256(canonical)
}
