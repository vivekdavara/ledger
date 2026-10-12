package ledger

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/vivekdavara/ledger/internal/money"
	"pgregory.net/rapid"
)

func p(account int64, currency string, amount int64) Posting {
	return Posting{AccountID: account, Currency: currency, Amount: amount}
}

func TestValidateAcceptsTheDesignExamples(t *testing.T) {
	examples := map[string]Entry{
		"top-up":  {"Alice tops up $50", []Posting{p(1, "USD", 5000), p(2, "USD", -5000)}},
		"payment": {"Alice pays Bob $20, 3% fee", []Posting{p(2, "USD", 2000), p(3, "USD", -1940), p(4, "USD", -60)}},
		"fx": {"Alice converts $100 to EUR", []Posting{
			p(2, "USD", 10000), p(5, "USD", -10000), p(6, "EUR", 9200), p(7, "EUR", -9200)}},
	}
	for name, e := range examples {
		if err := e.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	many := make([]Posting, MaxPostings+1)
	for i := range many {
		many[i] = p(int64(i+1), "USD", 1)
	}
	many[len(many)-1].Amount = -MaxPostings
	cases := []struct {
		name    string
		entry   Entry
		rule    error
		posting int
	}{
		{"no postings", Entry{}, ErrPostingCount, -1},
		{"one posting", Entry{"", []Posting{p(1, "USD", 0)}}, ErrPostingCount, -1},
		{"65 postings", Entry{"", many}, ErrPostingCount, -1},
		{"zero amount", Entry{"", []Posting{p(1, "USD", 5), p(2, "USD", 0), p(3, "USD", -5)}}, ErrZeroAmount, 1},
		{"account zero", Entry{"", []Posting{p(0, "USD", 5), p(2, "USD", -5)}}, ErrAccountID, 0},
		{"negative account", Entry{"", []Posting{p(1, "USD", 5), p(-2, "USD", -5)}}, ErrAccountID, 1},
		{"lowercase currency", Entry{"", []Posting{p(1, "usd", 5), p(2, "usd", -5)}}, ErrCurrency, 0},
		{"unknown currency", Entry{"", []Posting{p(1, "USD", 5), p(2, "XXX", -5)}}, ErrCurrency, 1},
		{"amount too large", Entry{"", []Posting{p(1, "USD", money.MaxAmount+1), p(2, "USD", -money.MaxAmount-1)}},
			ErrAmountRange, 0},
		{"amount too small", Entry{"", []Posting{p(1, "USD", 5), p(2, "USD", -money.MaxAmount-1)}}, ErrAmountRange, 1},
		{"same account twice", Entry{"", []Posting{p(1, "USD", 5), p(2, "USD", -3), p(1, "USD", -2)}},
			ErrDuplicateAccount, 2},
		{"unbalanced", Entry{"", []Posting{p(1, "USD", 5), p(2, "USD", -4)}}, ErrUnbalanced, -1},
		{"balanced in total but not per currency", Entry{"", []Posting{p(1, "USD", 5), p(2, "EUR", -5)}},
			ErrUnbalanced, -1},
		{"invalid UTF-8", Entry{"caf\xe9", []Posting{p(1, "USD", 5), p(2, "USD", -5)}}, ErrDescription, -1},
		{"NUL", Entry{"a\x00b", []Posting{p(1, "USD", 5), p(2, "USD", -5)}}, ErrDescription, -1},
		{"too long", Entry{strings.Repeat("é", MaxDescription+1), []Posting{p(1, "USD", 5), p(2, "USD", -5)}},
			ErrDescription, -1},
	}
	for _, tc := range cases {
		err := tc.entry.Validate()
		var ve *ValidationError
		if !errors.As(err, &ve) || !errors.Is(err, tc.rule) || ve.Posting != tc.posting {
			t.Errorf("%s: got %v, want rule %v at posting %d", tc.name, err, tc.rule, tc.posting)
		}
	}
}

func TestValidateBoundaries(t *testing.T) {
	limit := make([]Posting, MaxPostings)
	for i := range limit {
		limit[i] = p(int64(i+1), "USD", money.MaxAmount)
	}
	// 32 debits and 32 credits of the maximum: the largest entry there can be, and still exact.
	for i := MaxPostings / 2; i < MaxPostings; i++ {
		limit[i].Amount = -money.MaxAmount
	}
	ok := []Entry{
		{strings.Repeat("é", MaxDescription), []Posting{p(1, "USD", 5), p(2, "USD", -5)}},
		{"", limit},
	}
	for i, e := range ok {
		if err := e.Validate(); err != nil {
			t.Errorf("entry %d: %v", i, err)
		}
	}
}

func TestUnbalancedErrorNamesEachCurrency(t *testing.T) {
	e := Entry{"", []Posting{p(1, "USD", 5), p(2, "USD", -4), p(3, "EUR", 7), p(4, "JPY", -2), p(5, "JPY", 2)}}
	err := e.Validate()
	if want := "off in EUR by 7, USD by 1"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want it to contain %q", err, want)
	}
}

// balancedEntry draws a valid entry: one to three currencies, each with its own postings summing to zero, on
// distinct accounts, 2 to 64 postings in all.
func balancedEntry() *rapid.Generator[Entry] {
	return rapid.Custom(func(t *rapid.T) Entry {
		currencies := rapid.SliceOfNDistinct(rapid.SampledFrom(money.Currencies()), 1, 3,
			func(c money.Currency) string { return c.Code }).Draw(t, "currencies")
		perCurrency := MaxPostings / len(currencies)
		var postings []Posting
		for _, cur := range currencies {
			n := rapid.IntRange(2, perCurrency).Draw(t, "n")
			// Keep each drawn amount small enough that the balancing posting is within bounds too.
			bound := money.MaxAmount / MaxPostings
			amounts := make([]int64, n)
			var sum int64
			for i := range n - 1 {
				a := rapid.Int64Range(-bound, bound).Filter(func(v int64) bool { return v != 0 }).Draw(t, "amount")
				amounts[i] = a
				sum += a
			}
			if sum == 0 {
				// The balancing posting would be zero. Doubling the first amount makes the sum that amount.
				sum += amounts[0]
				amounts[0] *= 2
			}
			amounts[n-1] = -sum
			for _, a := range amounts {
				postings = append(postings, Posting{Currency: cur.Code, Amount: a})
			}
		}
		ids := rapid.SliceOfNDistinct(rapid.Int64Range(1, 1_000_000), len(postings), len(postings),
			rapid.ID[int64]).Draw(t, "accounts")
		for i := range postings {
			postings[i].AccountID = ids[i]
		}
		// Interleave the currencies, so no test depends on postings being grouped.
		perm := rapid.Permutation(postings).Draw(t, "order")
		return Entry{Description: rapid.StringMatching(`[\p{L}\p{N} ]{0,20}`).Draw(t, "description"), Postings: perm}
	})
}

func TestPropertyBalancedEntriesValidate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := balancedEntry().Draw(t, "entry")
		if err := e.Validate(); err != nil {
			t.Fatalf("balanced entry rejected: %v\n%+v", err, e)
		}
	})
}

func TestPropertyOrderNeverMatters(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := balancedEntry().Draw(t, "entry")
		// Break it half the time, so the property covers rejections too.
		if rapid.Bool().Draw(t, "break") {
			i := rapid.IntRange(0, len(e.Postings)-1).Draw(t, "i")
			e.Postings[i].Amount++
			if e.Postings[i].Amount == 0 {
				e.Postings[i].Amount = 2
			}
		}
		shuffled := Entry{e.Description, rapid.Permutation(e.Postings).Draw(t, "shuffled")}
		got, want := shuffled.Validate(), e.Validate()
		if !errors.Is(got, rule(want)) || (got == nil) != (want == nil) {
			t.Fatalf("reordering changed the result: %v, then %v", want, got)
		}
	})
}

func rule(err error) error {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve.Rule
	}
	return err
}

func TestPropertyChangingAnyAmountUnbalances(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := balancedEntry().Draw(t, "entry")
		i := rapid.IntRange(0, len(e.Postings)-1).Draw(t, "i")
		delta := rapid.Int64Range(-1000, 1000).Filter(func(d int64) bool {
			return d != 0 && e.Postings[i].Amount+d != 0
		}).Draw(t, "delta")
		e.Postings[i].Amount += delta
		if err := e.Validate(); !errors.Is(err, ErrUnbalanced) {
			t.Fatalf("posting %d moved by %d: got %v, want ErrUnbalanced", i, delta, err)
		}
	})
}

func TestPropertyMovingAPostingToAnotherCurrencyUnbalances(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := balancedEntry().Draw(t, "entry")
		i := rapid.IntRange(0, len(e.Postings)-1).Draw(t, "i")
		others := slices.DeleteFunc(money.Currencies(), func(c money.Currency) bool {
			return c.Code == e.Postings[i].Currency
		})
		e.Postings[i].Currency = rapid.SampledFrom(others).Draw(t, "to").Code
		if err := e.Validate(); !errors.Is(err, ErrUnbalanced) {
			t.Fatalf("got %v, want ErrUnbalanced", err)
		}
	})
}

func TestPropertyReversalIsValidAndCancelsOut(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		e := balancedEntry().Draw(t, "entry")
		r := e.Reversed("reversal")
		if err := r.Validate(); err != nil {
			t.Fatalf("reversal rejected: %v", err)
		}
		for i := range e.Postings {
			if e.Postings[i].AccountID != r.Postings[i].AccountID || e.Postings[i].Amount+r.Postings[i].Amount != 0 {
				t.Fatalf("posting %d: %+v reversed to %+v", i, e.Postings[i], r.Postings[i])
			}
		}
	})
}

func TestReversedDoesNotAliasTheOriginal(t *testing.T) {
	e := Entry{"x", []Posting{p(1, "USD", 5), p(2, "USD", -5)}}
	r := e.Reversed("undo x")
	r.Postings[0].Amount = 99
	if e.Postings[0].Amount != 5 {
		t.Fatalf("changing the reversal changed the original: %+v", e.Postings[0])
	}
}
