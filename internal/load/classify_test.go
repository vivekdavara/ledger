package load

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/apierr"
	"github.com/vivekdavara/ledger/internal/ledger"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// outcomes is how a client must treat each reason the ledger gives (DESIGN.md, "Errors"). A refusal is final and
// nothing posted; a conflict posted nothing and may be sent again; the rest say the work may or may not have been
// done.
var outcomes = map[string]Outcome{
	"malformed_request": Refused, "unsupported_media_type": Refused, "request_too_large": Refused,
	"invalid_argument": Refused, "unbalanced": Refused, "not_found": Refused, "unknown_account": Refused,
	"currency_mismatch": Refused, "insufficient_funds": Refused, "balance_out_of_range": Refused,
	"idempotency_key_reused": Refused, "account_name_taken": Refused, "method_not_allowed": Refused,
	"conflict":          Conflict,
	"deadline_exceeded": Unknown, "canceled": Unknown, "unavailable": Unknown, "internal": Unknown,
}

// Every error the service can return reaches the client as a reason from apierr's table, and Classify must treat
// each one as outcomes says. With "internal", "deadline_exceeded" or "canceled" missing from unknownReasons they
// were refusals, and no test noticed (found in review).
func TestClassifyTreatsEveryReasonAsItMust(t *testing.T) {
	inEntry := func(rule error) error { return &ledger.ValidationError{Rule: rule, Posting: 0} }
	for _, err := range []error{
		apierr.ErrMalformed, apierr.ErrTooLarge, apierr.ErrMediaType, inEntry(ledger.ErrPostingCount),
		inEntry(ledger.ErrUnbalanced), ledger.ErrAccountNotFound, inEntry(ledger.ErrAccountNotFound),
		inEntry(ledger.ErrCurrencyMismatch), inEntry(ledger.ErrInsufficientFunds), inEntry(ledger.ErrBalanceRange),
		ledger.ErrKeyReused, ledger.ErrNameTaken, ledger.ErrConflict, context.DeadlineExceeded, context.Canceled,
		&pgconn.PgError{Code: "57P01"}, errors.New("anything else"),
	} {
		kind := apierr.Classify(err)
		want, ok := outcomes[kind.Reason]
		if !ok {
			t.Errorf("%v gives reason %q, which the test doesn't know", err, kind.Reason)
			continue
		}
		rest := &APIError{Reason: kind.Reason, Status: kind.HTTP}
		if got := Classify(rest); got != want {
			t.Errorf("%s over REST: %v, want %v", kind.Reason, got, want)
		}
		// The server sends the reason in capitals (grpcapi's status).
		st, _ := status.New(kind.GRPC, "message").WithDetails(&errdetails.ErrorInfo{Reason: strings.ToUpper(kind.Reason)})
		if got := Classify(fromStatus(st.Err())); got != want || Reason(fromStatus(st.Err())) != kind.Reason {
			t.Errorf("%s over gRPC: %v (%s), want %v", kind.Reason, got, Reason(fromStatus(st.Err())), want)
		}
	}
	for reason, want := range outcomes {
		if got := Classify(&APIError{Reason: reason}); got != want {
			t.Errorf("%s: %v, want %v", reason, got, want)
		}
	}
}

// A gRPC error without an ErrorInfo comes from gRPC itself (a broken connection, a deadline, its own size limit),
// and only its code says what happened.
func TestAGRPCErrorWithoutTheLedgersReasonIsReadFromItsCode(t *testing.T) {
	for code, want := range map[codes.Code]struct {
		reason  string
		outcome Outcome
	}{
		codes.Unavailable:       {"unavailable", Unknown},
		codes.DeadlineExceeded:  {"deadline_exceeded", Unknown},
		codes.Canceled:          {"canceled", Unknown},
		codes.Internal:          {"internal", Unknown},
		codes.Unknown:           {"internal", Unknown},
		codes.Aborted:           {"conflict", Conflict},
		codes.ResourceExhausted: {"grpc_resourceexhausted", Refused},
		codes.NotFound:          {"grpc_notfound", Refused},
	} {
		err := fromStatus(status.Error(code, "from gRPC"))
		if Reason(err) != want.reason || Classify(err) != want.outcome {
			t.Errorf("%v: %s, %v; want %s, %v", code, Reason(err), Classify(err), want.reason, want.outcome)
		}
	}
	if err := fromStatus(errors.New("not a status")); Classify(err) != Unknown || Reason(err) != "no_answer" {
		t.Errorf("a plain error: %v, %s", Classify(err), Reason(err))
	}
}
