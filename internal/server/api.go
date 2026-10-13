package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/vivekdavara/ledger/internal/apierr"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
	"github.com/vivekdavara/ledger/internal/store"
)

// maxBody bounds a request body. The largest valid entry (64 postings, a 500-character description) is under 8 KB.
const maxBody = 64 << 10

// The REST API's JSON. Amounts are decimal strings in the currency's major unit, exactly as money.Parse and Format
// define them ("20.00", "-19.40", "1500" for JPY): never floats, and exact in any client, JavaScript included.

type accountJSON struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	Currency      string    `json:"currency"`
	AllowNegative bool      `json:"allow_negative"`
	Balance       string    `json:"balance"`       // signed: debits positive (DESIGN.md, "Sign convention")
	ShownBalance  string    `json:"shown_balance"` // positive on the account's normal side
	NormalSide    string    `json:"normal_side"`   // "debit" or "credit"
	Version       int64     `json:"version"`       // the number of postings so far
	CreatedAt     time.Time `json:"created_at"`
}

type newAccountJSON struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Currency      string `json:"currency"`
	AllowNegative bool   `json:"allow_negative"`
}

type postingJSON struct {
	AccountID int64  `json:"account_id"`
	Currency  string `json:"currency"`
	Amount    string `json:"amount"`
}

type entryJSON struct {
	Description string        `json:"description"`
	Postings    []postingJSON `json:"postings"`
}

type postedPostingJSON struct {
	ID             int64  `json:"id"`
	AccountID      int64  `json:"account_id"`
	Currency       string `json:"currency"`
	Amount         string `json:"amount"`
	BalanceAfter   string `json:"balance_after"`
	AccountVersion int64  `json:"account_version"`
}

type postedEntryJSON struct {
	ID          int64               `json:"id"`
	Description string              `json:"description"`
	CreatedAt   time.Time           `json:"created_at"`
	Postings    []postedPostingJSON `json:"postings"`
}

type historyLineJSON struct {
	PostingID      int64     `json:"posting_id"`
	EntryID        int64     `json:"entry_id"`
	Description    string    `json:"description"`
	PostedAt       time.Time `json:"posted_at"`
	Currency       string    `json:"currency"`
	Amount         string    `json:"amount"`
	BalanceAfter   string    `json:"balance_after"`
	AccountVersion int64     `json:"account_version"`
}

type historyJSON struct {
	Postings []historyLineJSON `json:"postings"`
	// NextAfterVersion is set when the page is full: pass it as after_version for the next page.
	NextAfterVersion *int64 `json:"next_after_version,omitempty"`
}

type errorJSON struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
	Posting *int   `json:"posting,omitempty"` // the index of the posting at fault, in the request's order
}

// format renders minor units in the currency's major unit. The currency comes from the database, whose
// currencies table matches the registry (TestCurrenciesMatchTheGoRegistry).
func format(currency string, minor int64) string {
	c, err := money.Lookup(currency)
	if err != nil {
		return strconv.FormatInt(minor, 10) + " (minor units of " + currency + ")"
	}
	return c.Format(minor)
}

func toAccountJSON(a ledger.Account) accountJSON {
	side := "credit"
	if a.Type.DebitNormal() {
		side = "debit"
	}
	return accountJSON{ID: a.ID, Name: a.Name, Type: string(a.Type), Currency: a.Currency,
		AllowNegative: a.AllowNegative, Balance: format(a.Currency, a.Balance),
		ShownBalance: format(a.Currency, a.Shown()), NormalSide: side, Version: a.Version, CreatedAt: a.CreatedAt}
}

func toPostedEntryJSON(e ledger.PostedEntry) postedEntryJSON {
	out := postedEntryJSON{ID: e.ID, Description: e.Description, CreatedAt: e.CreatedAt,
		Postings: make([]postedPostingJSON, len(e.Postings))}
	for i, p := range e.Postings {
		out.Postings[i] = postedPostingJSON{ID: p.ID, AccountID: p.AccountID, Currency: p.Currency,
			Amount: format(p.Currency, p.Amount), BalanceAfter: format(p.Currency, p.BalanceAfter),
			AccountVersion: p.AccountVersion}
	}
	return out
}

// api holds the REST handlers.
type api struct {
	svc ledger.Service
	log *slog.Logger
}

func (a api) routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/accounts", a.createAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", a.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/postings", a.history)
	mux.HandleFunc("POST /v1/entries", a.postEntry)
}

// createAccount answers 201 with a Location for a new account, and 200 with the existing account when an identical
// one has the name already, so a client can retry it freely.
func (a api) createAccount(w http.ResponseWriter, r *http.Request) {
	var req newAccountJSON
	if err := decode(w, r, &req); err != nil {
		a.fail(w, r, err)
		return
	}
	acct, created, err := a.svc.CreateAccount(r.Context(), ledger.NewAccount{Name: req.Name,
		Type: ledger.AccountType(req.Type), Currency: req.Currency, AllowNegative: req.AllowNegative})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		w.Header().Set("Location", fmt.Sprintf("/v1/accounts/%d", acct.ID))
	}
	writeJSON(w, status, toAccountJSON(acct))
}

func (a api) getAccount(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	acct, err := a.svc.Account(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toAccountJSON(acct))
}

// history pages through an account's postings: ?after_version=N (default 0) and ?limit=N (default 100, at most
// 1000).
func (a api) history(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	after, err := queryInt(r, "after_version")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	limit, err := queryInt(r, "limit")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	lines, err := a.svc.History(r.Context(), id, after, int(limit))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	out := historyJSON{Postings: make([]historyLineJSON, len(lines))}
	for i, l := range lines {
		p := l.Posting
		out.Postings[i] = historyLineJSON{PostingID: p.ID, EntryID: p.EntryID, Description: l.Description,
			PostedAt: l.PostedAt, Currency: p.Currency, Amount: format(p.Currency, p.Amount),
			BalanceAfter: format(p.Currency, p.BalanceAfter), AccountVersion: p.AccountVersion}
	}
	pageSize := limit
	if pageSize == 0 {
		pageSize = store.DefaultPageSize
	}
	if n := len(lines); n > 0 && int64(n) == pageSize {
		next := lines[n-1].Posting.AccountVersion
		out.NextAfterVersion = &next
	}
	writeJSON(w, http.StatusOK, out)
}

// postEntry posts an entry. The Idempotency-Key header is required. The answer is 201 for a new entry, and 200 with
// Idempotent-Replayed: true when an earlier request with the key posted it; both carry the entry as posted.
func (a api) postEntry(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if len(r.Header.Values("Idempotency-Key")) != 1 {
		a.fail(w, r, fmt.Errorf("%w: send exactly one Idempotency-Key header", apierr.ErrMalformed))
		return
	}
	var req entryJSON
	if err := decode(w, r, &req); err != nil {
		a.fail(w, r, err)
		return
	}
	e := ledger.Entry{Description: req.Description, Postings: make([]ledger.Posting, len(req.Postings))}
	for i, p := range req.Postings {
		c, err := money.Lookup(p.Currency)
		if err != nil {
			a.fail(w, r, &ledger.ValidationError{Rule: ledger.ErrCurrency, Posting: i,
				Detail: fmt.Sprintf("%q is not a known currency", p.Currency)})
			return
		}
		amount, err := c.Parse(p.Amount)
		if err != nil {
			rule := money.ErrSyntax
			if errors.Is(err, money.ErrRange) {
				rule = money.ErrRange
			}
			a.fail(w, r, &ledger.ValidationError{Rule: rule, Posting: i, Detail: err.Error()})
			return
		}
		e.Postings[i] = ledger.Posting{AccountID: p.AccountID, Currency: p.Currency, Amount: amount}
	}
	posted, replayed, err := a.svc.PostEntry(r.Context(), key, e)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, status, toPostedEntryJSON(posted))
}

// decode reads one JSON object into v, refusing unknown fields, trailing data and bodies over maxBody.
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) && typeErr.Field != "" {
			return fmt.Errorf("%w: %s must be a JSON %s", apierr.ErrMalformed, typeErr.Field, jsonType(typeErr))
		}
		return fmt.Errorf("%w: %v", apierr.ErrMalformed, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: more than one JSON value in the body", apierr.ErrMalformed)
	}
	return nil
}

func jsonType(e *json.UnmarshalTypeError) string {
	switch e.Type.Kind().String() {
	case "string":
		return "string (amounts are decimal strings such as \"12.34\")"
	case "int64", "int":
		return "integer"
	case "bool":
		return "boolean"
	case "slice":
		return "array"
	}
	return e.Type.String()
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, fmt.Errorf("%w: %q is not an account id", apierr.ErrMalformed, r.PathValue("id"))
	}
	return id, nil
}

// queryInt reads an optional non-negative integer parameter; absent is 0.
func queryInt(r *http.Request, name string) (int64, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%w: %s=%q is not a non-negative integer", apierr.ErrMalformed, name, s)
	}
	return n, nil
}

// fail writes err as the API's error JSON, with the status the error table gives it.
func (a api) fail(w http.ResponseWriter, r *http.Request, err error) {
	kind := apierr.Classify(err)
	body := errorBody{Reason: kind.Reason, Message: err.Error()}
	if kind.Redact {
		a.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		body.Message = http.StatusText(kind.HTTP)
	}
	if i := apierr.Posting(err); i >= 0 {
		body.Posting = &i
	}
	writeJSON(w, kind.HTTP, errorJSON{body})
}
