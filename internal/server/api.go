package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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

// RequestTimeout bounds each API request. One stuck behind a lock (a psql session holding an account's row, say)
// fails with 504 deadline_exceeded instead of holding a connection and a database session for as long as the
// client waits. It is below the server's 30 s write timeout, so the client always gets the answer.
const RequestTimeout = 10 * time.Second

// api holds the REST handlers.
type api struct {
	svc     ledger.Service
	log     *slog.Logger
	timeout time.Duration
}

func (a api) routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/accounts", a.bounded(a.createAccount))
	mux.HandleFunc("GET /v1/accounts/{id}", a.bounded(a.getAccount))
	mux.HandleFunc("GET /v1/accounts/{id}/postings", a.bounded(a.history))
	mux.HandleFunc("POST /v1/entries", a.bounded(a.postEntry))
	mux.HandleFunc("GET /v1/entries/{id}", a.bounded(a.getEntry))
}

// bounded gives a handler's request a deadline of a.timeout. The database calls under it are cancelled when it
// passes; a posting whose outcome that leaves unknown is what the idempotency key is for.
func (a api) bounded(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
		defer cancel()
		h(w, r.WithContext(ctx))
	}
}

// createAccount answers 201 with a Location for a new account, and 200 with the existing account when an identical
// one has the name already, so a client can retry it freely.
func (a api) createAccount(w http.ResponseWriter, r *http.Request) {
	var req newAccountJSON
	if err := decode(w, r, &req, accountNames); err != nil {
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
	id, err := pathID(r, "an account id")
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
	id, err := pathID(r, "an account id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	after, err := queryInt(r, "after_version", 64) // versions are int64, as next_after_version is
	if err != nil {
		a.fail(w, r, err)
		return
	}
	limit, err := queryInt(r, "limit", 32) // gRPC's page_size is int32
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
	keys := r.Header.Values("Idempotency-Key")
	switch {
	case len(keys) == 0:
		// The reason gRPC gives for an empty idempotency_key, which it can't tell from a missing one: the APIs gave
		// different reasons for the same mistake until the day-5 review.
		a.fail(w, r, fmt.Errorf("%w: send it in an Idempotency-Key header, and the same one on a retry",
			ledger.ErrIdempotencyKey))
		return
	case len(keys) > 1:
		a.fail(w, r, fmt.Errorf("%w: send exactly one Idempotency-Key header", apierr.ErrMalformed))
		return
	}
	key := keys[0]
	var req entryJSON
	if err := decode(w, r, &req, entryNames); err != nil {
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

// getEntry returns a posted entry in the shape POST /v1/entries answered with, byte for byte.
func (a api) getEntry(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r, "an entry id")
	if err != nil {
		a.fail(w, r, err)
		return
	}
	e, err := a.svc.Entry(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPostedEntryJSON(e))
}

// The JSON field names each request body may use, exactly as spelled.
var (
	accountNames = jsonNames(reflect.TypeFor[newAccountJSON]())
	entryNames   = jsonNames(reflect.TypeFor[entryJSON]())
)

// decode reads one JSON object into v. Beyond what encoding/json checks, it refuses what that package quietly
// accepts (found in review): a body that isn't UTF-8 (it would turn each bad byte into U+FFFD, so two different
// names became one account), a key that appears twice in an object (the last one won), and a key that matches a
// field only when case is ignored ("Name", "AMOUNT"). It also requires a JSON content type, which a cross-site form
// can't send, and answers a body over maxBody with 413.
func decode(w http.ResponseWriter, r *http.Request, v any, names map[string]bool) error {
	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		return fmt.Errorf("%w: send the body as Content-Type: application/json", apierr.ErrMediaType)
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if _, tooLarge := errors.AsType[*http.MaxBytesError](err); tooLarge {
		return fmt.Errorf("%w: a body is at most %d bytes", apierr.ErrTooLarge, maxBody)
	} else if err != nil {
		return fmt.Errorf("%w: reading the body: %v", apierr.ErrMalformed, err)
	}
	if !utf8.Valid(body) {
		return fmt.Errorf("%w: the body is not valid UTF-8", apierr.ErrMalformed)
	}
	if err := strictKeys(body, names); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %s", apierr.ErrMalformed, describe(err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: more than one JSON value in the body", apierr.ErrMalformed)
	}
	return nil
}

// strictKeys walks the body's tokens and refuses a key repeated within one object, or one that isn't a field name
// as spelled. Syntax errors are left for the decoder to report.
func strictKeys(body []byte, names map[string]bool) error {
	type frame struct {
		object    bool
		keys      map[string]bool
		expectKey bool
	}
	var stack []*frame
	valueDone := func() {
		if n := len(stack); n > 0 && stack[n-1].object {
			stack[n-1].expectKey = true
		}
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil // the end, or a syntax error the decoder will report
		}
		switch t := tok.(type) {
		case json.Delim:
			switch t {
			case '{':
				stack = append(stack, &frame{object: true, keys: map[string]bool{}, expectKey: true})
			case '[':
				stack = append(stack, &frame{})
			default: // '}' or ']'
				stack = stack[:len(stack)-1]
				valueDone()
			}
		case string:
			if n := len(stack); n > 0 && stack[n-1].object && stack[n-1].expectKey {
				top := stack[n-1]
				switch {
				case top.keys[t]:
					return fmt.Errorf("%w: %q appears twice in one object", apierr.ErrMalformed, t)
				case !names[t]:
					return fmt.Errorf("%w: unknown field %q (names are case-sensitive)", apierr.ErrMalformed, t)
				}
				top.keys[t], top.expectKey = true, false
				continue
			}
			valueDone()
		default:
			valueDone()
		}
	}
}

// jsonNames collects the JSON names of a struct type's fields, and of the structs inside it.
func jsonNames(t reflect.Type) map[string]bool {
	names := map[string]bool{}
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Slice || t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return
		}
		for f := range t.Fields() {
			if name, _, _ := strings.Cut(f.Tag.Get("json"), ","); name != "" && name != "-" {
				names[name] = true
			}
			walk(f.Type)
		}
	}
	walk(t)
	return names
}

// describe says what was wrong with a body that encoding/json refused, in the API's words rather than Go's.
func describe(err error) string {
	if typeErr, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		if typeErr.Field == "" {
			return "the body must be a JSON object"
		}
		switch typeErr.Type.Kind() {
		case reflect.String:
			return typeErr.Field + " must be a JSON string (amounts are decimal strings such as \"12.34\")"
		case reflect.Int64, reflect.Int:
			return typeErr.Field + " must be a whole number that fits in 64 bits"
		case reflect.Bool:
			return typeErr.Field + " must be true or false"
		case reflect.Slice:
			return typeErr.Field + " must be a JSON array"
		case reflect.Struct:
			return typeErr.Field + " must be a JSON object"
		}
		return typeErr.Field + " has the wrong type"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "the body is empty or cut short"
	}
	return strings.TrimPrefix(err.Error(), "json: ")
}

// pathID reads the account id in the path. Only a non-integer is malformed here; the service refuses an id that
// can't exist (zero or negative) the same way for both APIs.
func pathID(r *http.Request, what string) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q is not %s", apierr.ErrMalformed, r.PathValue("id"), what)
	}
	return id, nil
}

// queryInt reads an optional integer parameter of the given size in bits (the gRPC field's); absent is 0. Only a
// non-integer, or one that doesn't fit that size, is malformed here: the service refuses a negative or too large
// value, the same way for both APIs.
func queryInt(r *http.Request, name string, bits int) (int64, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q is not a whole number", apierr.ErrMalformed, name, s)
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
