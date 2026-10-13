package ledger

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/vivekdavara/ledger/internal/money"
)

func TestShownIsPositiveOnTheNormalSide(t *testing.T) {
	cases := []struct {
		typ     AccountType
		balance int64
		shown   int64
	}{
		{Asset, 5000, 5000},       // the platform's bank account holds $50
		{Expense, 120, 120},       // card fees paid
		{Liability, -3000, 3000},  // the platform owes Alice $30
		{Equity, -100000, 100000}, // the owner put in $1,000
		{Revenue, -60, 60},        // fees earned
		{Liability, 25, -25},      // an overdrawn wallet shows as negative
		{Revenue, math.MaxInt64, -math.MaxInt64},
		{Liability, math.MinInt64 + 1, math.MaxInt64}, // the smallest balance 0008 allows
	}
	for _, tc := range cases {
		if got := (Account{Type: tc.typ, Balance: tc.balance}).Shown(); got != tc.shown {
			t.Errorf("%s with balance %d shows %d, want %d", tc.typ, tc.balance, got, tc.shown)
		}
	}
}

func TestEveryTypeHasOneNormalSide(t *testing.T) {
	debit := map[AccountType]bool{Asset: true, Expense: true, Liability: false, Equity: false, Revenue: false}
	if len(AccountTypes) != len(debit) {
		t.Fatalf("AccountTypes has %d types, want %d", len(AccountTypes), len(debit))
	}
	for _, typ := range AccountTypes {
		if !typ.Valid() || typ.DebitNormal() != debit[typ] {
			t.Errorf("%s: valid %v, debit-normal %v", typ, typ.Valid(), typ.DebitNormal())
		}
	}
	for _, typ := range []AccountType{"", "Asset", "assets", "cash"} {
		if typ.Valid() {
			t.Errorf("%q is valid", typ)
		}
	}
}

func TestNewAccountValidate(t *testing.T) {
	ok := NewAccount{Name: "wallet:alice", Type: Liability, Currency: "USD"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("%+v: %v", ok, err)
	}
	longest := NewAccount{Name: strings.Repeat("é", MaxAccountName), Type: Asset, Currency: "JPY"}
	if err := longest.Validate(); err != nil {
		t.Fatalf("a %d-character name: %v", MaxAccountName, err)
	}
	cases := []struct {
		name    string
		account NewAccount
		rule    error
	}{
		{"empty name", NewAccount{Name: "", Type: Asset, Currency: "USD"}, ErrAccountName},
		{"name too long", NewAccount{Name: strings.Repeat("a", MaxAccountName+1), Type: Asset, Currency: "USD"},
			ErrAccountName},
		{"invalid UTF-8", NewAccount{Name: "caf\xe9", Type: Asset, Currency: "USD"}, ErrAccountName},
		{"NUL", NewAccount{Name: "a\x00b", Type: Asset, Currency: "USD"}, ErrAccountName},
		{"no type", NewAccount{Name: "a", Currency: "USD"}, ErrAccountType},
		{"unknown type", NewAccount{Name: "a", Type: "cash", Currency: "USD"}, ErrAccountType},
		{"unknown currency", NewAccount{Name: "a", Type: Asset, Currency: "usd"}, ErrCurrency},
	}
	for _, tc := range cases {
		err := tc.account.Validate()
		var verr *ValidationError
		if !errors.As(err, &verr) || !errors.Is(err, tc.rule) || verr.Posting != -1 {
			t.Errorf("%s: got %v, want a ValidationError for %v", tc.name, err, tc.rule)
		}
	}
}

func TestOverdraws(t *testing.T) {
	cases := []struct {
		name    string
		account Account
		amount  int64
		want    bool
	}{
		{"asset spends what it has", Account{Type: Asset, Balance: 500}, -500, false},
		{"asset spends a cent more", Account{Type: Asset, Balance: 500}, -501, true},
		{"wallet pays what it holds", Account{Type: Liability, Balance: -3000}, 3000, false},
		{"wallet pays a cent more", Account{Type: Liability, Balance: -3000}, 3001, true},
		{"a credit never overdraws a wallet", Account{Type: Liability, Balance: 0}, -1, false},
		{"a debit never overdraws an asset", Account{Type: Asset, Balance: 0}, 1, false},
		{"allowed to go negative", Account{Type: Asset, Balance: 0, AllowNegative: true}, -1, false},
		{"past bigint is not an overdraft", Account{Type: Asset, Balance: math.MaxInt64}, money.MaxAmount, false},
	}
	for _, tc := range cases {
		if got := tc.account.Overdraws(tc.amount); got != tc.want {
			t.Errorf("%s: Overdraws(%d) = %v, want %v", tc.name, tc.amount, got, tc.want)
		}
	}
}
