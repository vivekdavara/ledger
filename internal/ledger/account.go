package ledger

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vivekdavara/ledger/internal/money"
)

// AccountType says what an account represents, and with it the account's normal side (DESIGN.md, "Sign
// convention").
type AccountType string

const (
	Asset     AccountType = "asset"
	Liability AccountType = "liability"
	Equity    AccountType = "equity"
	Revenue   AccountType = "revenue"
	Expense   AccountType = "expense"
)

// AccountTypes lists every type, in the order DESIGN.md's table gives them.
var AccountTypes = []AccountType{Asset, Liability, Equity, Revenue, Expense}

// Valid reports whether t is one of the five types.
func (t AccountType) Valid() bool {
	switch t {
	case Asset, Liability, Equity, Revenue, Expense:
		return true
	}
	return false
}

// DebitNormal reports whether the account's normal side is the debit side (assets and expenses). For the others
// a healthy balance is a credit, which is negative as stored.
func (t AccountType) DebitNormal() bool { return t == Asset || t == Expense }

// MaxAccountName is the longest account name, in characters, as the accounts_name_length CHECK counts them.
const MaxAccountName = 200

// NewAccount is what CreateAccount needs.
type NewAccount struct {
	Name          string
	Type          AccountType
	Currency      string
	AllowNegative bool
}

// Account is an account as stored. Balance is the signed sum of its postings (debits positive) and Version the
// number of postings so far.
type Account struct {
	ID            int64
	Name          string
	Type          AccountType
	Currency      string
	AllowNegative bool
	Balance       int64
	Version       int64
	CreatedAt     time.Time
}

// Shown is the balance as a person reads it: positive on the account's normal side. The negation always fits,
// because migration 0008 keeps every stored balance above math.MinInt64.
func (a Account) Shown() int64 {
	if a.Type.DebitNormal() {
		return a.Balance
	}
	return -a.Balance
}

// Errors about accounts, and the rules for an entry that need the accounts it names. When one posting is at fault
// they come wrapped in a ValidationError that names it.
var (
	ErrAccountName       = errors.New("invalid account name")
	ErrAccountType       = errors.New("unknown account type")
	ErrAccountNotFound   = errors.New("account not found")
	ErrEntryNotFound     = errors.New("entry not found")
	ErrNameTaken         = errors.New("account name taken by a different account")
	ErrCurrencyMismatch  = errors.New("currency does not match the account's")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

// Validate checks a new account against the rules that need nothing but the account itself. The database checks
// the same rules again (migration 0001).
func (a NewAccount) Validate() error {
	n := utf8.RuneCountInString(a.Name)
	switch {
	case a.Name == "":
		return &ValidationError{ErrAccountName, -1, "the name is empty"}
	case !utf8.ValidString(a.Name):
		return &ValidationError{ErrAccountName, -1, "not valid UTF-8"}
	case strings.ContainsRune(a.Name, 0):
		return &ValidationError{ErrAccountName, -1, "contains a NUL character, which Postgres text can't store"}
	case n > MaxAccountName:
		return &ValidationError{ErrAccountName, -1, fmt.Sprintf("%d characters, the maximum is %d", n, MaxAccountName)}
	case !a.Type.Valid():
		return &ValidationError{ErrAccountType, -1, fmt.Sprintf("%q is not one of asset, liability, equity, revenue, expense", a.Type)}
	}
	if _, err := money.Lookup(a.Currency); err != nil {
		return &ValidationError{ErrCurrency, -1, fmt.Sprintf("%q is not a known currency", a.Currency)}
	}
	return nil
}

// Overdraws reports whether moving a protected account's balance by amount takes it below zero on its normal side,
// which the accounts_no_overdraft CHECK forbids. It is false for an account that allows a negative balance.
func (a Account) Overdraws(amount int64) bool {
	if a.AllowNegative {
		return false
	}
	after, ok := money.Add(a.Balance, amount)
	if !ok {
		return false // not an overdraft: the database rejects a balance past bigint for its own reason
	}
	if a.Type.DebitNormal() {
		return after < 0
	}
	return after > 0
}
