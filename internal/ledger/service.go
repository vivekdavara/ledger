package ledger

import "context"

// Service is what the REST and gRPC APIs offer, both as thin layers over one implementation (store.Store). Amounts
// are minor units throughout; each API decides how to show them.
type Service interface {
	// CreateAccount creates an account, or returns the existing one with created false if an identical account
	// has the name already.
	CreateAccount(ctx context.Context, a NewAccount) (_ Account, created bool, _ error)
	// Account returns one account with its current balance.
	Account(ctx context.Context, id int64) (Account, error)
	// History returns an account's postings after a version, oldest first.
	History(ctx context.Context, accountID, afterVersion int64, limit int) ([]HistoryLine, error)
	// PostEntry posts an entry once per idempotency key; replayed is true when an earlier request with the key
	// already posted it.
	PostEntry(ctx context.Context, key string, e Entry) (_ PostedEntry, replayed bool, _ error)
	// Entry returns one posted entry, as PostEntry returned it.
	Entry(ctx context.Context, id int64) (PostedEntry, error)
}
