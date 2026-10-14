// Package load drives a running ledgerd through its API, as a client would (DESIGN.md, "Load and failure injection
// (day 4)"). Client is the part of the API it uses, over REST or gRPC; Run posts transfers with a chosen contention,
// and reports throughput, latency, and whether the books agree with what the API answered.
package load

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	ledgerv1 "github.com/vivekdavara/ledger/internal/gen/ledger/v1"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Client is what the load generator needs from the ledger's API. REST and GRPC implement it.
type Client interface {
	// CreateAccount creates the account, or finds the identical one by name, and returns its id.
	CreateAccount(ctx context.Context, a ledger.NewAccount) (int64, error)
	// PostEntry posts e under key.
	PostEntry(ctx context.Context, key string, e ledger.Entry) (Posted, error)
	// Account returns an account's stored balance and version.
	Account(ctx context.Context, id int64) (Account, error)
	Close() error
}

// Account is what the generator reads back from an account: its stored balance in minor units (debits positive)
// and its version, the number of postings it has.
type Account struct {
	Balance, Version int64
}

// Posted is the answer to a PostEntry that succeeded.
type Posted struct {
	EntryID  int64
	Replayed bool // an earlier request with the key had posted the entry; nothing was posted this time
}

// Outcome is how a request ended, as its client can tell.
type Outcome int

const (
	// Answered: the request succeeded.
	Answered Outcome = iota
	// Refused: the ledger answered no, and nothing was posted. Sending it again gets the same answer, unless the
	// accounts change meanwhile.
	Refused
	// Conflict: the ledger gave up under contention (conflict, 409, ABORTED), and nothing was posted; the request
	// can be sent again with its key.
	Conflict
	// Unknown: no answer came (the connection failed, the server was down, the deadline passed, or it failed
	// unexpectedly). The entry may or may not have posted; only sending it again with the same key can tell.
	Unknown
)

func (o Outcome) String() string {
	return [...]string{"answered", "refused", "conflict", "unknown"}[o]
}

// APIError is an error answer from the ledger: its reason (insufficient_funds, conflict, ...) and how the API said
// it.
type APIError struct {
	Reason  string
	Status  int        // REST: the HTTP status; 0 over gRPC
	Code    codes.Code // gRPC: the status code; codes.OK over REST
	Message string
}

func (e *APIError) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("%d %s: %s", e.Status, e.Reason, e.Message)
	}
	return fmt.Sprintf("%s %s: %s", e.Code, e.Reason, e.Message)
}

// unknownReasons are the reasons whose request may or may not have taken effect: the ledger's answer says it
// couldn't finish, not that it didn't.
var unknownReasons = map[string]bool{"unavailable": true, "deadline_exceeded": true, "canceled": true, "internal": true}

// Classify says how a request that returned err ended. An error that isn't an answer from the ledger (a refused or
// broken connection, a context that ran out) is Unknown.
func Classify(err error) Outcome {
	var apiErr *APIError
	switch {
	case err == nil:
		return Answered
	case !errors.As(err, &apiErr):
		return Unknown
	case apiErr.Reason == "conflict":
		return Conflict
	case unknownReasons[apiErr.Reason]:
		return Unknown
	}
	return Refused
}

// Reason is the ledger's reason for err, or "no_answer" when it isn't an answer from the ledger.
func Reason(err error) string {
	if apiErr, ok := errors.AsType[*APIError](err); ok {
		return apiErr.Reason
	}
	return "no_answer"
}

// REST is a Client over the REST API.
type REST struct {
	base string
	http *http.Client
}

// NewREST returns a REST client for the API at base (http://host:port), keeping up to conns connections open, one
// per concurrent caller.
func NewREST(base string, conns int) *REST {
	return &REST{base: strings.TrimSuffix(base, "/"), http: &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConns: conns, MaxIdleConnsPerHost: conns, IdleConnTimeout: time.Minute},
	}}
}

func (c *REST) Close() error { c.http.CloseIdleConnections(); return nil }

// do sends a request and decodes a 2xx answer into out; any other status is an *APIError.
func (c *REST) do(ctx context.Context, method, path string, header http.Header, body, out any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return nil, err
	}
	// net/http treats a request with an Idempotency-Key header and a rewindable body as safe to send again, and does
	// so by itself when a reused connection breaks before the answer (go1.27.1: Request.isReplayable,
	// persistConn.shouldRetryRequest). That is safe against this ledger, which dedupes on the key, but then the
	// generator neither sees nor counts the resend (found in review: 40 lost answers, 16 of them resent unseen).
	// Without GetBody the body can't be rewound, so every resend is Run's own.
	req.GetBody = nil
	for k, v := range header {
		req.Header[k] = v
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err // the answer was cut off: unknown
	}
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error struct{ Reason, Message string } `json:"error"`
		}
		if json.Unmarshal(data, &e) != nil || e.Error.Reason == "" {
			// Not the ledger's own error (a proxy's, say). A 5xx says nothing about whether the work was done.
			reason := "http_" + strconv.Itoa(resp.StatusCode)
			if resp.StatusCode >= 500 {
				reason = "unavailable"
			}
			return nil, &APIError{Reason: reason, Status: resp.StatusCode, Message: strings.TrimSpace(string(data))}
		}
		return nil, &APIError{Reason: e.Error.Reason, Status: resp.StatusCode, Message: e.Error.Message}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return nil, fmt.Errorf("decode the answer to %s %s: %w", method, path, err)
		}
	}
	return resp, nil
}

func (c *REST) CreateAccount(ctx context.Context, a ledger.NewAccount) (int64, error) {
	var out struct {
		ID int64 `json:"id"`
	}
	_, err := c.do(ctx, http.MethodPost, "/v1/accounts", nil, map[string]any{"name": a.Name, "type": string(a.Type),
		"currency": a.Currency, "allow_negative": a.AllowNegative}, &out)
	return out.ID, err
}

func (c *REST) PostEntry(ctx context.Context, key string, e ledger.Entry) (Posted, error) {
	type posting struct {
		AccountID int64  `json:"account_id"`
		Currency  string `json:"currency"`
		Amount    string `json:"amount"`
	}
	body := struct {
		Description string    `json:"description"`
		Postings    []posting `json:"postings"`
	}{Description: e.Description, Postings: make([]posting, len(e.Postings))}
	for i, p := range e.Postings {
		c, err := money.Lookup(p.Currency)
		if err != nil {
			return Posted{}, err
		}
		body.Postings[i] = posting{AccountID: p.AccountID, Currency: p.Currency, Amount: c.Format(p.Amount)}
	}
	var out struct {
		ID int64 `json:"id"`
	}
	resp, err := c.do(ctx, http.MethodPost, "/v1/entries", http.Header{"Idempotency-Key": {key}}, body, &out)
	if err != nil {
		return Posted{}, err
	}
	return Posted{EntryID: out.ID, Replayed: resp.Header.Get("Idempotent-Replayed") == "true"}, nil
}

func (c *REST) Account(ctx context.Context, id int64) (Account, error) {
	var out struct {
		Currency string `json:"currency"`
		Balance  string `json:"balance"`
		Version  int64  `json:"version"`
	}
	if _, err := c.do(ctx, http.MethodGet, "/v1/accounts/"+strconv.FormatInt(id, 10), nil, nil, &out); err != nil {
		return Account{}, err
	}
	cur, err := money.Lookup(out.Currency)
	if err != nil {
		return Account{}, err
	}
	balance, err := cur.Parse(out.Balance)
	return Account{Balance: balance, Version: out.Version}, err
}

// GRPC is a Client over the gRPC API.
type GRPC struct {
	conn *grpc.ClientConn
	api  ledgerv1.LedgerServiceClient
}

// callTimeout bounds each gRPC call, as the REST client's timeout bounds each request: a connection that stays open
// but never answers would otherwise hold a call until the run's own deadline.
const callTimeout = 30 * time.Second

// NewGRPC returns a gRPC client for the API at addr (host:port). It reconnects quickly after the server goes away
// (from 50 ms, at most 1 s apart, instead of gRPC's default of 1 s growing to 2 minutes), since a crash test restarts
// the server at once.
func NewGRPC(addr string) (*GRPC, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: time.Second, Backoff: backoff.Config{
			BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second}}))
	if err != nil {
		return nil, err
	}
	return &GRPC{conn: conn, api: ledgerv1.NewLedgerServiceClient(conn)}, nil
}

func (c *GRPC) Close() error { return c.conn.Close() }

// fromStatus turns a gRPC error into an *APIError carrying the ErrorInfo's reason in the REST API's spelling. An
// error with no ErrorInfo (the connection failed, or gRPC itself refused the call) is reasoned from its code.
func fromStatus(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	reason := ""
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			reason = strings.ToLower(info.GetReason())
		}
	}
	if reason == "" {
		switch st.Code() {
		case codes.Unavailable:
			reason = "unavailable"
		case codes.DeadlineExceeded:
			reason = "deadline_exceeded"
		case codes.Canceled:
			reason = "canceled"
		case codes.Aborted:
			reason = "conflict"
		case codes.Internal, codes.Unknown:
			reason = "internal"
		default:
			reason = "grpc_" + strings.ToLower(st.Code().String())
		}
	}
	return &APIError{Reason: reason, Code: st.Code(), Message: st.Message()}
}

var typeToProto = map[ledger.AccountType]ledgerv1.AccountType{
	ledger.Asset:     ledgerv1.AccountType_ACCOUNT_TYPE_ASSET,
	ledger.Liability: ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY,
	ledger.Equity:    ledgerv1.AccountType_ACCOUNT_TYPE_EQUITY,
	ledger.Revenue:   ledgerv1.AccountType_ACCOUNT_TYPE_REVENUE,
	ledger.Expense:   ledgerv1.AccountType_ACCOUNT_TYPE_EXPENSE,
}

func (c *GRPC) CreateAccount(ctx context.Context, a ledger.NewAccount) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	resp, err := c.api.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Name: a.Name, Type: typeToProto[a.Type],
		Currency: a.Currency, AllowNegative: a.AllowNegative})
	if err != nil {
		return 0, fromStatus(err)
	}
	return resp.GetAccount().GetId(), nil
}

func (c *GRPC) PostEntry(ctx context.Context, key string, e ledger.Entry) (Posted, error) {
	req := &ledgerv1.PostEntryRequest{IdempotencyKey: key, Description: e.Description,
		Postings: make([]*ledgerv1.Posting, len(e.Postings))}
	for i, p := range e.Postings {
		req.Postings[i] = &ledgerv1.Posting{AccountId: p.AccountID, Currency: p.Currency, Amount: p.Amount}
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	resp, err := c.api.PostEntry(ctx, req)
	if err != nil {
		return Posted{}, fromStatus(err)
	}
	return Posted{EntryID: resp.GetEntry().GetId(), Replayed: resp.GetReplayed()}, nil
}

func (c *GRPC) Account(ctx context.Context, id int64) (Account, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	resp, err := c.api.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: id})
	if err != nil {
		return Account{}, fromStatus(err)
	}
	return Account{Balance: resp.GetAccount().GetBalance(), Version: resp.GetAccount().GetVersion()}, nil
}
