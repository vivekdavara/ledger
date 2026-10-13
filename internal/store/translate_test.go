package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivekdavara/ledger/internal/ledger"
)

func TestTranslateNamesTheConstraintNotPostgresText(t *testing.T) {
	cases := []struct {
		err  *pgconn.PgError
		rule error
	}{
		{&pgconn.PgError{Code: "23514", ConstraintName: "accounts_no_overdraft",
			Message: `new row for relation "accounts" violates check constraint "accounts_no_overdraft"`},
			ledger.ErrInsufficientFunds},
		{&pgconn.PgError{Code: "23514", ConstraintName: "entry_balanced",
			Message: "journal entry 7 does not balance: off in USD by 1"}, ledger.ErrUnbalanced},
		{&pgconn.PgError{Code: "22003", Message: "bigint out of range"}, ledger.ErrBalanceRange},
	}
	for _, tc := range cases {
		err := translate(tc.err)
		if !errors.Is(err, tc.rule) || strings.Contains(err.Error(), tc.err.Message) {
			t.Errorf("%s: got %q, want %v without Postgres's message", tc.err.ConstraintName, err, tc.rule)
		}
		if tc.err.ConstraintName != "" && !strings.Contains(err.Error(), tc.err.ConstraintName) {
			t.Errorf("%q doesn't name the constraint %s", err, tc.err.ConstraintName)
		}
	}
	// What has no rule passes through unchanged, so retryable errors stay retryable.
	for _, pass := range []*pgconn.PgError{{Code: "40001"}, {Code: "23505", ConstraintName: "idempotency_keys_pkey"},
		{Code: "23514", ConstraintName: "some_other_check"}} {
		if got := translate(pass); got != error(pass) {
			t.Errorf("%s %s: changed to %v", pass.Code, pass.ConstraintName, got)
		}
	}
}
