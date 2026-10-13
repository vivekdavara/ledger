package store

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

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

func TestRetryDelayStaysWithinItsBounds(t *testing.T) {
	policies := []RetryPolicy{
		DefaultRetry,
		{Attempts: 3},                      // no waiting at all
		{BaseDelay: 10 * time.Millisecond}, // MaxDelay is raised to BaseDelay
		{BaseDelay: 0, MaxDelay: 100 * time.Millisecond}, // zero base: no waiting, whatever the cap
		{BaseDelay: time.Millisecond, MaxDelay: math.MaxInt64},
		{BaseDelay: 1<<33 + 1, MaxDelay: time.Minute},
		{BaseDelay: math.MaxInt64, MaxDelay: math.MaxInt64},
		{BaseDelay: -time.Second, MaxDelay: -time.Second},
	}
	for _, p := range policies {
		s := NewWithRetry(nil, p)
		ceiling := s.retry.BaseDelay
		for n := 1; n <= 200; n++ {
			d := s.retry.delay(n) // must not panic
			if d < 0 || d > s.retry.MaxDelay || (s.retry.BaseDelay == 0 && d != 0) || (n == 1 && d > s.retry.BaseDelay) {
				t.Fatalf("%+v: delay(%d) = %v", p, n, d)
			}
			if ceiling < s.retry.MaxDelay/2 {
				ceiling *= 2
			} else {
				ceiling = s.retry.MaxDelay
			}
		}
		_ = ceiling
	}
	// With the default policy the first wait is under 1 ms and the eighth already reaches the 100 ms cap.
	for range 1000 {
		if d := DefaultRetry.delay(1); d >= time.Millisecond {
			t.Fatalf("first retry waited %v", d)
		}
		if d := DefaultRetry.delay(20); d >= 100*time.Millisecond {
			t.Fatalf("twentieth retry waited %v", d)
		}
	}
}

func TestTheChecksRefuseABalanceOfMinusTwoToThe63(t *testing.T) {
	near := ledger.Account{ID: 1, Name: "wallet:big", Type: ledger.Liability, Currency: "USD", AllowNegative: true,
		Balance: math.MinInt64 + 5}
	other := ledger.Account{ID: 2, Name: "fx", Type: ledger.Asset, Currency: "USD", AllowNegative: true}
	entry := func(amount int64) ledger.Entry {
		return ledger.Entry{Postings: []ledger.Posting{{AccountID: 1, Currency: "USD", Amount: amount},
			{AccountID: 2, Currency: "USD", Amount: -amount}}}
	}
	if err := checkAgainstAccounts(entry(-4), []ledger.Account{near, other}); err != nil {
		t.Fatalf("down to -2^63 + 1: %v", err)
	}
	if err := checkAgainstAccounts(entry(-5), []ledger.Account{near, other}); !errors.Is(err, ledger.ErrBalanceRange) {
		t.Fatalf("down to -2^63: %v, want ErrBalanceRange", err)
	}
}
