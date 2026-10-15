package apierr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/money"
	"github.com/vivekdavara/ledger/internal/store"
	"google.golang.org/grpc/codes"
)

func TestEveryLedgerErrorHasAKind(t *testing.T) {
	cases := []struct {
		err    error
		reason string
		http   int
		grpc   codes.Code
	}{
		{ledger.ErrUnbalanced, "unbalanced", 400, codes.InvalidArgument},
		{ledger.ErrPostingCount, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrDescription, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrAccountID, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrEntryID, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrCurrency, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrZeroAmount, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrAmountRange, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrDuplicateAccount, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrAccountName, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrAccountType, "invalid_argument", 400, codes.InvalidArgument},
		{ledger.ErrIdempotencyKey, "invalid_argument", 400, codes.InvalidArgument},
		{store.ErrPage, "invalid_argument", 400, codes.InvalidArgument},
		{money.ErrSyntax, "invalid_argument", 400, codes.InvalidArgument},
		{money.ErrRange, "invalid_argument", 400, codes.InvalidArgument},
		{ErrMalformed, "malformed_request", 400, codes.InvalidArgument},
		{ledger.ErrAccountNotFound, "not_found", 404, codes.NotFound},
		{ledger.ErrEntryNotFound, "not_found", 404, codes.NotFound},
		{ledger.ErrCurrencyMismatch, "currency_mismatch", 422, codes.FailedPrecondition},
		{ledger.ErrInsufficientFunds, "insufficient_funds", 422, codes.FailedPrecondition},
		{ledger.ErrBalanceRange, "balance_out_of_range", 422, codes.FailedPrecondition},
		{ledger.ErrKeyReused, "idempotency_key_reused", 422, codes.FailedPrecondition},
		{ledger.ErrNameTaken, "account_name_taken", 409, codes.AlreadyExists},
		{ledger.ErrConflict, "conflict", 409, codes.Aborted},
		{context.DeadlineExceeded, "deadline_exceeded", 504, codes.DeadlineExceeded},
		{context.Canceled, "canceled", 499, codes.Canceled},
		{errors.New("something nobody planned for"), "internal", 500, codes.Internal},
	}
	for _, tc := range cases {
		// Wrapped, as the store returns them.
		k := Classify(fmt.Errorf("doing something: %w", tc.err))
		if k.Reason != tc.reason || k.HTTP != tc.http || k.GRPC != tc.grpc {
			t.Errorf("%v: got %+v, want %s %d %v", tc.err, k, tc.reason, tc.http, tc.grpc)
		}
	}
}

func TestAPostingsAccountIsNotTheRequestsResource(t *testing.T) {
	inEntry := &ledger.ValidationError{Rule: ledger.ErrAccountNotFound, Posting: 1, Detail: "there is no account 9"}
	if k := Classify(inEntry); k != UnknownAccount || k.HTTP != http.StatusUnprocessableEntity {
		t.Fatalf("a posting's missing account: %+v", k)
	}
	if Posting(inEntry) != 1 || Posting(errors.New("x")) != -1 {
		t.Fatal("Posting")
	}
	// The store's fallback from the database names no posting, but is still about the entry.
	fallback := &ledger.ValidationError{Rule: ledger.ErrAccountNotFound, Posting: -1, Detail: "the database's check"}
	if k := Classify(fallback); k != UnknownAccount {
		t.Fatalf("the database's fallback: %+v", k)
	}
}

func TestOnlyUnplannedAndUnavailableErrorsAreRedacted(t *testing.T) {
	// A real connection failure: nothing listens on port 1.
	_, down := pgconn.Connect(context.Background(), "postgres://ledger@127.0.0.1:1/ledger?connect_timeout=2")
	if _, ok := errors.AsType[*pgconn.ConnectError](down); !ok {
		t.Fatalf("connecting to port 1 gave %T %v, want a *pgconn.ConnectError", down, down)
	}
	if k := Classify(fmt.Errorf("acquire: %w", down)); k.Reason != "unavailable" || k.HTTP != 503 ||
		k.GRPC != codes.Unavailable || !k.Redact {
		t.Fatalf("database down: %+v", k)
	}
	if !Classify(errors.New("boom")).Redact {
		t.Fatal("an unplanned error isn't redacted")
	}
	for _, row := range table {
		if row.kind.Redact {
			t.Errorf("%v is redacted, but its message is the ledger's own", row.err)
		}
	}
}

func TestALostDatabaseIsUnavailableNotInternal(t *testing.T) {
	for _, err := range []error{
		&pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"},
		&pgconn.PgError{Code: "57P02", Message: "terminating connection due to crash of another server process"},
		&pgconn.PgError{Code: "08006", Message: "connection failure"},
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")},
	} {
		if k := Classify(fmt.Errorf("post: %w", err)); k.Reason != "unavailable" || k.HTTP != 503 || !k.Redact {
			t.Errorf("%v: %+v, want unavailable", err, k)
		}
	}
	// A constraint violation is the ledger's business, not the database being down.
	if k := Classify(&pgconn.PgError{Code: "23514", ConstraintName: "some_check"}); k.Reason != "internal" {
		t.Errorf("an unmapped constraint: %+v", k)
	}
}
