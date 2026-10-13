package store

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
)

// RetryPolicy bounds how PostEntry retries a transaction that Postgres aborted to keep it serializable.
type RetryPolicy struct {
	Attempts  int           // attempts in all; less than 1 means 1
	BaseDelay time.Duration // the first retry waits up to this long; zero means retries don't wait
	MaxDelay  time.Duration // no retry waits longer; less than BaseDelay means BaseDelay
}

// DefaultRetry is the policy New uses.
var DefaultRetry = RetryPolicy{Attempts: 10, BaseDelay: time.Millisecond, MaxDelay: 100 * time.Millisecond}

// delay is how long to wait before attempt n+1: "full jitter", uniform in [0, min(MaxDelay, BaseDelay * 2^(n-1))),
// so transactions that collided once don't collide again in lockstep. The cap doubles until it reaches MaxDelay
// and stops there, so it can't overflow whatever the policy (found in review: a shift overflowed, and
// rand.N(MaxInt64 + 1) panicked).
func (p RetryPolicy) delay(n int) time.Duration {
	ceiling := p.BaseDelay
	for i := 1; i < n && ceiling < p.MaxDelay; i++ {
		if ceiling > p.MaxDelay/2 {
			ceiling = p.MaxDelay
			break
		}
		ceiling *= 2
	}
	ceiling = min(ceiling, p.MaxDelay)
	if ceiling <= 0 {
		return 0
	}
	return rand.N(ceiling)
}

// Store is the ledger service: every operation the REST and gRPC APIs offer, over one connection pool.
type Store struct {
	db      *pgxpool.Pool
	retry   RetryPolicy
	retries atomic.Int64

	// hook, when set by a test, runs at named points of a PostEntry attempt.
	hook func(point string, attempt int)
}

// New returns a Store that posts with DefaultRetry.
func New(db *pgxpool.Pool) *Store { return NewWithRetry(db, DefaultRetry) }

// NewWithRetry returns a Store with its own retry policy.
func NewWithRetry(db *pgxpool.Pool, retry RetryPolicy) *Store {
	retry.Attempts = max(retry.Attempts, 1)
	retry.BaseDelay = max(retry.BaseDelay, 0)
	retry.MaxDelay = max(retry.MaxDelay, retry.BaseDelay)
	return &Store{db: db, retry: retry}
}

// Retries is the number of PostEntry attempts retried so far, after a serialization failure or a deadlock.
func (s *Store) Retries() int64 { return s.retries.Load() }

const accountColumns = `id, name, type, currency, allow_negative, balance, version, created_at`

func scanAccount(row pgx.Row) (ledger.Account, error) {
	var a ledger.Account
	err := row.Scan(&a.ID, &a.Name, &a.Type, &a.Currency, &a.AllowNegative, &a.Balance, &a.Version, &a.CreatedAt)
	return a, err
}

// CreateAccount creates an account with a zero balance. Names are unique, which makes it idempotent: asking again
// for an account that exists with the same type, currency and overdraft setting returns that account with created
// false. A name that exists with anything else different is ErrNameTaken.
func (s *Store) CreateAccount(ctx context.Context, a ledger.NewAccount) (_ ledger.Account, created bool, _ error) {
	if err := a.Validate(); err != nil {
		return ledger.Account{}, false, err
	}
	acct, err := scanAccount(s.db.QueryRow(ctx, `
		INSERT INTO accounts (name, type, currency, allow_negative) VALUES ($1, $2, $3, $4)
		ON CONFLICT ON CONSTRAINT accounts_name_unique DO NOTHING
		RETURNING `+accountColumns, a.Name, a.Type, a.Currency, a.AllowNegative))
	switch {
	case err == nil:
		return acct, true, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return ledger.Account{}, false, translate(err)
	}
	// The name exists: it committed before this statement began, or the statement waited for it to commit.
	acct, err = scanAccount(s.db.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE name = $1`, a.Name))
	if err != nil {
		return ledger.Account{}, false, translate(err)
	}
	if acct.Type != a.Type || acct.Currency != a.Currency || acct.AllowNegative != a.AllowNegative {
		return ledger.Account{}, false, fmt.Errorf("%w: %q is account %d, a %s account in %s with allow_negative %v",
			ledger.ErrNameTaken, a.Name, acct.ID, acct.Type, acct.Currency, acct.AllowNegative)
	}
	return acct, false, nil
}

// validID refuses an account id that no account can have, as a request error rather than "not found": in gRPC an
// unset id arrives as 0.
func validID(id int64) error {
	if id <= 0 {
		return &ledger.ValidationError{Rule: ledger.ErrAccountID, Posting: -1,
			Detail: fmt.Sprintf("account id %d is not positive", id)}
	}
	return nil
}

// Account returns one account, with its balance as of now.
func (s *Store) Account(ctx context.Context, id int64) (ledger.Account, error) {
	if err := validID(id); err != nil {
		return ledger.Account{}, err
	}
	acct, err := scanAccount(s.db.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Account{}, fmt.Errorf("%w: there is no account %d", ledger.ErrAccountNotFound, id)
	}
	return acct, translate(err)
}

// History page sizes.
const (
	DefaultPageSize = 100
	MaxPageSize     = 1000
)

// ErrPage is wrapped by History's errors about its arguments.
var ErrPage = errors.New("invalid page")

// History returns up to limit postings of an account, oldest first, starting after the given version: page 1 is
// afterVersion 0, and each next page starts after the last version of the one before. Versions never change
// and have no gaps, so a page is stable however many postings arrive while a client pages. A limit of 0 means
// DefaultPageSize.
func (s *Store) History(ctx context.Context, accountID, afterVersion int64, limit int) ([]ledger.HistoryLine, error) {
	if err := validID(accountID); err != nil {
		return nil, err
	}
	if limit == 0 {
		limit = DefaultPageSize
	}
	if limit < 0 || limit > MaxPageSize {
		return nil, fmt.Errorf("%w: limit %d, a page has 1 to %d postings", ErrPage, limit, MaxPageSize)
	}
	if afterVersion < 0 {
		return nil, fmt.Errorf("%w: after_version %d is negative", ErrPage, afterVersion)
	}
	// One statement, so the account check and the page come from the same snapshot.
	rows, err := s.db.Query(ctx, `
		SELECT p.id, p.entry_id, p.account_id, p.currency, p.amount, p.balance_after, p.account_version,
		       e.description, e.created_at
		  FROM accounts a
		  LEFT JOIN LATERAL (
		        SELECT * FROM postings
		         WHERE account_id = a.id AND account_version > $2
		         ORDER BY account_version LIMIT $3) p ON true
		  LEFT JOIN journal_entries e ON e.id = p.entry_id
		 WHERE a.id = $1
		 ORDER BY p.account_version`, accountID, afterVersion, limit)
	if err != nil {
		return nil, translate(err)
	}
	var lines []ledger.HistoryLine
	found := false
	for rows.Next() {
		found = true
		var id, entryID, account, amount, after, version *int64
		var currency, description *string
		var postedAt *time.Time
		if err := rows.Scan(&id, &entryID, &account, &currency, &amount, &after, &version, &description,
			&postedAt); err != nil {
			return nil, translate(err)
		}
		if id == nil {
			continue // the account exists but has nothing after afterVersion
		}
		lines = append(lines, ledger.HistoryLine{
			Posting: ledger.PostedPosting{ID: *id, EntryID: *entryID, AccountID: *account, Currency: *currency,
				Amount: *amount, BalanceAfter: *after, AccountVersion: *version},
			Description: *description, PostedAt: *postedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, translate(err)
	}
	if !found {
		return nil, fmt.Errorf("%w: there is no account %d", ledger.ErrAccountNotFound, accountID)
	}
	return lines, nil
}

// PostEntry records an entry atomically and returns it as posted. The key makes it idempotent: a request whose key
// was already used for the same entry returns that entry with replayed true and posts nothing; a key used for a
// different entry is ErrKeyReused. A rejected request leaves no trace, its key included.
//
// Each attempt first looks the key up, outside any transaction, and then posts in one SERIALIZABLE transaction. That
// transaction locks the entry's accounts in id order, so two entries never wait for each other's locks in a cycle,
// and checks the rules that need the accounts while it holds them, to say which posting broke which rule. The
// database checks every rule again. When Postgres aborts an attempt to keep the transactions serializable (40001)
// or to break a deadlock (40P01), or another request commits the same key first (23505), PostEntry retries it under
// the store's RetryPolicy, and returns ErrConflict once the attempts run out.
func (s *Store) PostEntry(ctx context.Context, key string, e ledger.Entry) (
	posted ledger.PostedEntry, replayed bool, err error) {
	if err := ledger.ValidKey(key); err != nil {
		return ledger.PostedEntry{}, false, err
	}
	if err := e.Validate(); err != nil {
		return ledger.PostedEntry{}, false, err
	}
	hash := e.Hash()
	for attempt := 1; ; attempt++ {
		posted, found, err := s.lookUp(ctx, key, hash[:])
		if err != nil || found {
			return posted, found, err
		}
		s.at("looked up", attempt)
		posted, err = s.postOnce(ctx, key, hash[:], e, attempt)
		if !retryable(err) {
			return posted, false, err
		}
		if attempt >= s.retry.Attempts {
			return ledger.PostedEntry{}, false, &conflictError{attempts: attempt, last: err}
		}
		s.retries.Add(1)
		timer := time.NewTimer(s.retry.delay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ledger.PostedEntry{}, false, ctx.Err()
		case <-timer.C:
		}
	}
}

// lookUp finds a key that was used before: the same request gets its entry back (found), a different one is
// ErrKeyReused. It reads outside the posting transaction on purpose. Inside a SERIALIZABLE transaction the read
// takes a predicate lock on where the key would be (its index page, or the whole table while it is small), so every
// concurrent request whose key landed there conflicted with it, even with no account in common (found in review: 16 workers on separate wallets gave 456 retries and 4
// ErrConflicts). Outside, a request racing on the same key is caught by the key's primary key instead (23505) and
// retried. A key commits with its entry and postings and history is append-only, so the entry read here is whole.
func (s *Store) lookUp(ctx context.Context, key string, hash []byte) (ledger.PostedEntry, bool, error) {
	var storedHash []byte
	var entryID int64
	err := s.db.QueryRow(ctx, `SELECT request_hash, entry_id FROM idempotency_keys WHERE key = $1`, key).
		Scan(&storedHash, &entryID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ledger.PostedEntry{}, false, nil
	case err != nil:
		return ledger.PostedEntry{}, false, translate(err)
	case !bytes.Equal(storedHash, hash):
		return ledger.PostedEntry{}, false, fmt.Errorf("%w: key %q was used for a different entry", ledger.ErrKeyReused, key)
	}
	posted, err := loadEntry(ctx, s.db, entryID)
	if err != nil {
		return ledger.PostedEntry{}, false, translate(err)
	}
	return posted, true, nil
}

// conflictError is ErrConflict once the attempts ran out. The last attempt's error stays reachable through
// errors.Is and errors.As, but out of the message, which the APIs show to clients: Postgres's own text has no place
// there.
type conflictError struct {
	attempts int
	last     error
}

func (e *conflictError) Error() string {
	why := "a serialization failure"
	var pgErr *pgconn.PgError
	if errors.As(e.last, &pgErr) {
		switch pgErr.Code {
		case "40P01":
			why = "a deadlock"
		case "23505":
			why = "a concurrent request with the same key"
		}
	}
	return fmt.Sprintf("%v: gave up after %d attempts, the last ended by %s", ledger.ErrConflict, e.attempts, why)
}

func (e *conflictError) Unwrap() []error { return []error{ledger.ErrConflict, e.last} }

func (s *Store) at(point string, attempt int) {
	if s.hook != nil {
		s.hook(point, attempt)
	}
}

func (s *Store) postOnce(ctx context.Context, key string, hash []byte, e ledger.Entry, attempt int) (
	ledger.PostedEntry, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // after a commit this does nothing

	s.at("begin", attempt)

	// 1. Lock the accounts in id order and check what needs them, naming the posting at fault. This first statement
	// also fixes the transaction's snapshot.
	ids := make([]int64, len(e.Postings))
	for i, p := range e.Postings {
		ids[i] = p.AccountID
	}
	slices.Sort(ids)
	rows, err := tx.Query(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	accounts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ledger.Account, error) { return scanAccount(row) })
	if err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	if err := checkAgainstAccounts(e, accounts); err != nil {
		return ledger.PostedEntry{}, err
	}
	s.at("locked", attempt)

	// 2. The entry, its postings in one statement and in account-id order, and the key. The database fills in each
	// posting's balance_after and account_version, and checks the entry as a whole at commit.
	posted := ledger.PostedEntry{Description: e.Description}
	if err := tx.QueryRow(ctx, `INSERT INTO journal_entries (description, posting_count) VALUES ($1, $2)
		RETURNING id, created_at`, e.Description, len(e.Postings)).Scan(&posted.ID, &posted.CreatedAt); err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	sorted := slices.Clone(e.Postings)
	slices.SortFunc(sorted, func(a, b ledger.Posting) int { return cmp.Compare(a.AccountID, b.AccountID) })
	accountIDs, currencies, amounts := make([]int64, len(sorted)), make([]string, len(sorted)), make([]int64, len(sorted))
	for i, p := range sorted {
		accountIDs[i], currencies[i], amounts[i] = p.AccountID, p.Currency, p.Amount
	}
	rows, err = tx.Query(ctx, `
		INSERT INTO postings (entry_id, account_id, currency, amount)
		SELECT $1::bigint, account_id, currency, amount
		  FROM unnest($2::bigint[], $3::text[], $4::bigint[]) WITH ORDINALITY AS p (account_id, currency, amount, n)
		 ORDER BY n
		RETURNING id, entry_id, account_id, currency, amount, balance_after, account_version`,
		posted.ID, accountIDs, currencies, amounts)
	if err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	posted.Postings, err = pgx.CollectRows(rows, scanPosting)
	if err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	slices.SortFunc(posted.Postings, func(a, b ledger.PostedPosting) int { return cmp.Compare(a.AccountID, b.AccountID) })
	if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key, request_hash, entry_id) VALUES ($1, $2, $3)`,
		key, hash, posted.ID); err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	s.at("commit", attempt)
	if err := tx.Commit(ctx); err != nil {
		return ledger.PostedEntry{}, translate(err)
	}
	return posted, nil
}

// checkAgainstAccounts checks the rules that need the accounts: each exists, is in its posting's currency, isn't
// overdrawn by it, and keeps a balance the ledger can store. Each account is in the entry once (Validate), so a
// posting's effect on its account is the whole entry's.
func checkAgainstAccounts(e ledger.Entry, accounts []ledger.Account) error {
	byID := make(map[int64]ledger.Account, len(accounts))
	for _, a := range accounts {
		byID[a.ID] = a
	}
	for i, p := range e.Postings {
		a, ok := byID[p.AccountID]
		cur, _ := money.Lookup(p.Currency) // Validate checked the code
		switch after, fits := money.Add(a.Balance, p.Amount); {
		case !ok:
			return &ledger.ValidationError{Rule: ledger.ErrAccountNotFound, Posting: i,
				Detail: fmt.Sprintf("there is no account %d", p.AccountID)}
		case a.Currency != p.Currency:
			return &ledger.ValidationError{Rule: ledger.ErrCurrencyMismatch, Posting: i,
				Detail: fmt.Sprintf("account %d (%s) is in %s, the posting is in %s", a.ID, a.Name, a.Currency, p.Currency)}
		case a.Overdraws(p.Amount):
			return &ledger.ValidationError{Rule: ledger.ErrInsufficientFunds, Posting: i,
				Detail: fmt.Sprintf("account %d (%s) has %s %s, the posting takes %s", a.ID, a.Name,
					cur.Format(a.Shown()), a.Currency, cur.Format(absolute(p.Amount)))}
		case !fits || after == math.MinInt64:
			return &ledger.ValidationError{Rule: ledger.ErrBalanceRange, Posting: i,
				Detail: fmt.Sprintf("account %d's balance %d moved by %d", a.ID, a.Balance, p.Amount)}
		}
	}
	return nil
}

func absolute(x int64) int64 {
	if x < 0 {
		return -x // a posting is within ±10^15, so this fits
	}
	return x
}

func scanPosting(row pgx.CollectableRow) (ledger.PostedPosting, error) {
	var p ledger.PostedPosting
	err := row.Scan(&p.ID, &p.EntryID, &p.AccountID, &p.Currency, &p.Amount, &p.BalanceAfter, &p.AccountVersion)
	return p, err
}

// queryer is what loadEntry reads through: a pool or a transaction.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func loadEntry(ctx context.Context, db queryer, id int64) (ledger.PostedEntry, error) {
	e := ledger.PostedEntry{ID: id}
	if err := db.QueryRow(ctx, `SELECT description, created_at FROM journal_entries WHERE id = $1`, id).
		Scan(&e.Description, &e.CreatedAt); err != nil {
		return ledger.PostedEntry{}, err
	}
	rows, err := db.Query(ctx, `SELECT id, entry_id, account_id, currency, amount, balance_after, account_version
		FROM postings WHERE entry_id = $1 ORDER BY account_id`, id)
	if err != nil {
		return ledger.PostedEntry{}, err
	}
	e.Postings, err = pgx.CollectRows(rows, scanPosting)
	return e, err
}

// retryable reports whether a failed attempt should run again: Postgres aborted it to keep the transactions
// serializable or to break a deadlock, or another request with the same key committed first (the next attempt
// finds that key and replays it).
func retryable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01" ||
		(pgErr.Code == "23505" && pgErr.ConstraintName == "idempotency_keys_pkey")
}

// translate turns a database rejection into the ledger's error for the rule it enforces, by constraint name
// (DESIGN.md, "Schema"), as a ValidationError about the request as a whole. The service checks those rules before
// the database does, so this is the fallback for a rule the service's check missed. Other errors, retryable ones
// included, pass through unchanged.
func translate(err error) error {
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) {
		return err
	}
	rule := map[string]error{
		"entry_balanced":                 ledger.ErrUnbalanced,
		"entry_posting_count":            ledger.ErrPostingCount,
		"postings_one_per_account":       ledger.ErrDuplicateAccount,
		"postings_amount_range":          ledger.ErrAmountRange,
		"postings_account_currency_fkey": ledger.ErrAccountNotFound,
		"accounts_no_overdraft":          ledger.ErrInsufficientFunds,
		"accounts_balance_negatable":     ledger.ErrBalanceRange,
		"accounts_name_unique":           ledger.ErrNameTaken,
		"idempotency_keys_key_format":    ledger.ErrIdempotencyKey,
	}[pgErr.ConstraintName]
	detail := "refused by the database's " + pgErr.ConstraintName + " check"
	if pgErr.Code == "22003" { // numeric_value_out_of_range: a balance past bigint
		rule, detail = ledger.ErrBalanceRange, "a balance would pass the range of a bigint"
	}
	if rule == nil {
		return err
	}
	// The detail names the constraint (DESIGN.md lists them), not Postgres's message, which the APIs would show.
	return &ledger.ValidationError{Rule: rule, Posting: -1, Detail: detail}
}
