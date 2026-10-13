// Package apierr is the one table from the ledger's errors to what the APIs say about them: a stable reason, an
// HTTP status and a gRPC code. REST and gRPC both classify through it, so they can't disagree.
package apierr

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
	"github.com/vivekdavara/ledger/internal/store"
	"google.golang.org/grpc/codes"
)

// Kind is how the APIs report one class of error.
type Kind struct {
	Reason string     // stable and machine-readable, such as "insufficient_funds"; clients branch on this
	HTTP   int        // the REST status
	GRPC   codes.Code // the gRPC status code
	// Redact means the error is logged in full and the client sees only the status text: it may name hosts,
	// queries or internals.
	Redact bool
}

// The transport's own refusals: a request the API couldn't read at all (bad JSON, a missing header), one too large
// to read, and a body that isn't JSON.
var (
	ErrMalformed = errors.New("malformed request")
	ErrTooLarge  = errors.New("request too large")
	ErrMediaType = errors.New("unsupported media type")
)

// The request alone is wrong: the same request would fail against any ledger.
var invalid = Kind{"invalid_argument", http.StatusBadRequest, codes.InvalidArgument, false}

// The request is well formed but the ledger's state refuses it.
func refused(reason string, code codes.Code) Kind {
	return Kind{reason, http.StatusUnprocessableEntity, code, false}
}

var (
	// UnknownAccount is a posting naming an account that doesn't exist. It is NotFound in gRPC, but 422 in REST:
	// a 404 from POST /v1/entries would read as "no such endpoint".
	UnknownAccount = refused("unknown_account", codes.NotFound)
	// NotFound is the resource in the request's own path, such as GET /v1/accounts/{id}.
	NotFound = Kind{"not_found", http.StatusNotFound, codes.NotFound, false}
	// Internal is anything not in the table. The client sees no details.
	Internal = Kind{"internal", http.StatusInternalServerError, codes.Internal, true}
)

var table = []struct {
	err  error
	kind Kind
}{
	{ErrMalformed, Kind{"malformed_request", http.StatusBadRequest, codes.InvalidArgument, false}},
	// gRPC refuses an oversized message itself, with RESOURCE_EXHAUSTED and no ErrorInfo; this row is REST's.
	{ErrTooLarge, Kind{"request_too_large", http.StatusRequestEntityTooLarge, codes.ResourceExhausted, false}},
	{ErrMediaType, Kind{"unsupported_media_type", http.StatusUnsupportedMediaType, codes.InvalidArgument, false}},
	{ledger.ErrUnbalanced, Kind{"unbalanced", http.StatusBadRequest, codes.InvalidArgument, false}},
	{ledger.ErrPostingCount, invalid},
	{ledger.ErrDescription, invalid},
	{ledger.ErrAccountID, invalid},
	{ledger.ErrCurrency, invalid},
	{ledger.ErrZeroAmount, invalid},
	{ledger.ErrAmountRange, invalid},
	{ledger.ErrDuplicateAccount, invalid},
	{ledger.ErrAccountName, invalid},
	{ledger.ErrAccountType, invalid},
	{ledger.ErrIdempotencyKey, invalid},
	{store.ErrPage, invalid},
	{money.ErrSyntax, invalid},
	{money.ErrRange, invalid},
	{ledger.ErrCurrencyMismatch, refused("currency_mismatch", codes.FailedPrecondition)},
	{ledger.ErrInsufficientFunds, refused("insufficient_funds", codes.FailedPrecondition)},
	{ledger.ErrBalanceRange, refused("balance_out_of_range", codes.FailedPrecondition)},
	{ledger.ErrKeyReused, refused("idempotency_key_reused", codes.FailedPrecondition)},
	{ledger.ErrNameTaken, Kind{"account_name_taken", http.StatusConflict, codes.AlreadyExists, false}},
	// Retrying the same request with the same key is always safe, so the client should.
	{ledger.ErrConflict, Kind{"conflict", http.StatusConflict, codes.Aborted, false}},
	{context.DeadlineExceeded, Kind{"deadline_exceeded", http.StatusGatewayTimeout, codes.DeadlineExceeded, false}},
	{context.Canceled, Kind{"canceled", 499, codes.Canceled, false}}, // 499: nginx's "client closed request"
}

// unavailable is the database not answering: the request may work on a retry, with the same key.
var unavailable = Kind{"unavailable", http.StatusServiceUnavailable, codes.Unavailable, true}

// Classify says how the APIs report err.
func Classify(err error) Kind {
	// A posting's account comes wrapped in a ValidationError; the request path's account doesn't.
	if errors.Is(err, ledger.ErrAccountNotFound) {
		if _, inEntry := errors.AsType[*ledger.ValidationError](err); inEntry {
			return UnknownAccount
		}
		return NotFound
	}
	for _, row := range table {
		if errors.Is(err, row.err) {
			return row.kind
		}
	}
	if databaseDown(err) {
		return unavailable
	}
	return Internal
}

// databaseDown reports whether err means the database couldn't be reached or dropped the connection: a failed
// connect, a connection exception (SQLSTATE class 08), an operator intervention such as a terminated backend or a
// shutdown (class 57P), or the connection breaking mid-query. A retry with the same key is safe after any of them.
func databaseDown(err error) bool {
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok || pgconn.Timeout(err) {
		return true
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "57P")
	}
	_, netErr := errors.AsType[net.Error](err)
	return netErr || errors.Is(err, io.ErrUnexpectedEOF)
}

// Posting returns the index of the posting an error is about, or -1 if it is about the request as a whole.
func Posting(err error) int {
	if verr, ok := errors.AsType[*ledger.ValidationError](err); ok {
		return verr.Posting
	}
	return -1
}
