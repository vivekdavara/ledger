// Package ledger holds the journal entry model and the rules an entry must follow before it is posted. The
// database checks the same rules again at commit (migration 0001); this copy exists to reject a bad entry early,
// with an error that says which rule it broke and which posting broke it.
package ledger

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/vivekdavara/ledger/internal/money"
)

const (
	// MinPostings and MaxPostings bound the number of postings in one entry.
	MinPostings = 2
	MaxPostings = 64
	// MaxDescription is the longest description, in characters (what Postgres's char_length counts).
	MaxDescription = 500
)

// Every partial sum of an entry's postings is at most MaxPostings * money.MaxAmount in magnitude. This constant
// stops compiling if a change to either bound would let that overflow an int64, which is why the sums in
// Validate can use plain addition.
const _ = uint64(math.MaxInt64 - MaxPostings*money.MaxAmount)

// Posting is one line of an entry. Amount is in the currency's minor unit; positive is a debit, negative a credit.
type Posting struct {
	AccountID int64
	Currency  string
	Amount    int64
}

// Entry is one business event: postings that are recorded together or not at all.
type Entry struct {
	Description string
	Postings    []Posting
}

// The rules an entry can break. A ValidationError wraps exactly one of them.
var (
	ErrPostingCount     = errors.New("wrong number of postings")
	ErrDescription      = errors.New("invalid description")
	ErrAccountID        = errors.New("invalid account id")
	ErrEntryID          = errors.New("invalid entry id")
	ErrCurrency         = errors.New("unknown currency")
	ErrZeroAmount       = errors.New("zero amount")
	ErrAmountRange      = errors.New("amount out of range")
	ErrDuplicateAccount = errors.New("account appears twice")
	ErrUnbalanced       = errors.New("postings do not sum to zero")
)

// ValidationError says which rule an entry broke and, when one posting is at fault, which one.
type ValidationError struct {
	Rule    error  // one of the Err* values above
	Posting int    // index of the offending posting, or -1 when the entry as a whole is at fault
	Detail  string // human-readable specifics
}

func (e *ValidationError) Error() string {
	if e.Posting >= 0 {
		return fmt.Sprintf("posting %d: %v: %s", e.Posting, e.Rule, e.Detail)
	}
	return fmt.Sprintf("%v: %s", e.Rule, e.Detail)
}

func (e *ValidationError) Unwrap() error { return e.Rule }

// Validate checks an entry against every rule that needs nothing but the entry itself. The rules that need the
// accounts (does the account exist, is it in the posting's currency, does it have the funds) are checked when
// the entry is posted.
func (e Entry) Validate() error {
	if n := len(e.Postings); n < MinPostings || n > MaxPostings {
		return &ValidationError{ErrPostingCount, -1,
			fmt.Sprintf("an entry has %d to %d postings, this one has %d", MinPostings, MaxPostings, n)}
	}
	if err := validDescription(e.Description); err != nil {
		return err
	}
	seen := make(map[int64]int, len(e.Postings))
	sums := make(map[string]int64)
	for i, p := range e.Postings {
		switch {
		case p.AccountID <= 0:
			return &ValidationError{ErrAccountID, i, fmt.Sprintf("account id %d is not positive", p.AccountID)}
		case p.Amount == 0:
			return &ValidationError{ErrZeroAmount, i, "a posting must move money"}
		case p.Amount > money.MaxAmount || p.Amount < -money.MaxAmount:
			return &ValidationError{ErrAmountRange, i,
				fmt.Sprintf("|%d| is more than the maximum of %d minor units", p.Amount, money.MaxAmount)}
		}
		if _, err := money.Lookup(p.Currency); err != nil {
			return &ValidationError{ErrCurrency, i, fmt.Sprintf("%q is not a known currency", p.Currency)}
		}
		if first, dup := seen[p.AccountID]; dup {
			return &ValidationError{ErrDuplicateAccount, i,
				fmt.Sprintf("account %d is already in posting %d; net the two into one posting", p.AccountID, first)}
		}
		seen[p.AccountID] = i
		sums[p.Currency] += p.Amount // can't overflow: see the constant above
	}
	var off []string
	for cur, sum := range sums {
		if sum != 0 {
			off = append(off, fmt.Sprintf("%s by %d", cur, sum))
		}
	}
	if len(off) > 0 {
		sort.Strings(off)
		return &ValidationError{ErrUnbalanced, -1, "off in " + strings.Join(off, ", ")}
	}
	return nil
}

func validDescription(d string) error {
	switch {
	case !utf8.ValidString(d):
		return &ValidationError{ErrDescription, -1, "not valid UTF-8"}
	case strings.ContainsRune(d, 0):
		return &ValidationError{ErrDescription, -1, "contains a NUL character, which Postgres text can't store"}
	case utf8.RuneCountInString(d) > MaxDescription:
		return &ValidationError{ErrDescription, -1,
			fmt.Sprintf("%d characters, the maximum is %d", utf8.RuneCountInString(d), MaxDescription)}
	}
	return nil
}

// Reversed returns the entry that undoes e: the same accounts with every amount negated. History is never edited;
// a mistake is corrected by posting its reversal.
func (e Entry) Reversed(description string) Entry {
	out := Entry{Description: description, Postings: make([]Posting, len(e.Postings))}
	for i, p := range e.Postings {
		p.Amount = -p.Amount
		out.Postings[i] = p
	}
	return out
}
