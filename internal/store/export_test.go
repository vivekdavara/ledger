package store

import (
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
)

// SetHook lets a test run code at named points of a PostEntry attempt: "looked up" (the key was not found, before
// the transaction), "begin" (the transaction has begun, before its first statement), "locked" (the accounts are
// locked and checked) and "commit" (just before the commit).
func SetHook(s *Store, hook func(point string, attempt int)) { s.hook = hook }

// Delay is the wait before attempt n+1 under the store's normalised policy.
func Delay(s *Store, n int) time.Duration { return s.retry.delay(n) }

// CheckAgainstAccounts is the check PostEntry runs on the locked accounts.
func CheckAgainstAccounts(e ledger.Entry, accounts []ledger.Account) error {
	return checkAgainstAccounts(e, accounts)
}
