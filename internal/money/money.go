// Package money holds currencies and amounts. An amount is an int64 count of the currency's minor unit (cents for
// USD, yen for JPY, fils for KWD); floats never appear.
package money

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Currency is an ISO 4217 currency. Exponent is the number of digits after the decimal point: 2 for USD, 0 for
// JPY, 3 for KWD.
type Currency struct {
	Code     string
	Exponent int
}

// MaxAmount bounds the magnitude of a single posting, in minor units: 10^15, ten trillion dollars. With at most
// 64 postings per entry no sum can overflow int64, and every amount stays exact in a JSON number read as a
// float64 (2^53 is about 9.007 * 10^15).
const MaxAmount int64 = 1_000_000_000_000_000

// The registry must match the rows migration 0001 seeds into the currencies table; a store test checks it.
var registry = map[string]Currency{
	"CAD": {Code: "CAD", Exponent: 2},
	"CHF": {Code: "CHF", Exponent: 2},
	"EUR": {Code: "EUR", Exponent: 2},
	"GBP": {Code: "GBP", Exponent: 2},
	"INR": {Code: "INR", Exponent: 2},
	"JPY": {Code: "JPY", Exponent: 0},
	"KWD": {Code: "KWD", Exponent: 3},
	"USD": {Code: "USD", Exponent: 2},
}

// ErrUnknownCurrency is returned by Lookup for a code that isn't in the registry.
var ErrUnknownCurrency = errors.New("unknown currency")

// Lookup returns the currency with the given code. Codes are case-sensitive: "usd" is not USD.
func Lookup(code string) (Currency, error) {
	c, ok := registry[code]
	if !ok {
		return Currency{}, fmt.Errorf("%w: %q", ErrUnknownCurrency, code)
	}
	return c, nil
}

// Currencies returns every known currency, sorted by code.
func Currencies() []Currency {
	out := make([]Currency, 0, len(registry))
	for _, c := range registry {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out
}

// ErrSyntax and ErrRange are wrapped by the errors Parse returns.
var (
	ErrSyntax = errors.New("invalid amount")
	ErrRange  = errors.New("amount out of range")
)

// Parse converts a decimal string such as "12.34" or "-0.5" to minor units, exactly. It accepts an optional
// leading minus, at least one digit before the point, and at most Exponent digits after it; "12.345" in USD is
// an error, never a rounding. Signs other than a leading minus, spaces, exponents and separators are rejected.
func (c Currency) Parse(s string) (int64, error) {
	body, neg := strings.CutPrefix(s, "-")
	whole, frac, hasPoint := strings.Cut(body, ".")
	switch {
	case whole == "" || !digitsOnly(whole):
		return 0, fmt.Errorf("%w %q: expected digits before the point", ErrSyntax, s)
	case hasPoint && c.Exponent == 0:
		return 0, fmt.Errorf("%w %q: %s has no minor unit", ErrSyntax, s, c.Code)
	case hasPoint && (frac == "" || !digitsOnly(frac)):
		return 0, fmt.Errorf("%w %q: expected digits after the point", ErrSyntax, s)
	case len(frac) > c.Exponent:
		return 0, fmt.Errorf("%w %q: %s has %d decimal places", ErrSyntax, s, c.Code, c.Exponent)
	}
	digits := whole + frac + strings.Repeat("0", c.Exponent-len(frac))
	if neg {
		digits = "-" + digits
	}
	v, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		// The syntax was checked above, so the only failure left is a value outside int64.
		return 0, fmt.Errorf("%w: %q", ErrRange, s)
	}
	return v, nil
}

// Format renders minor units with exactly Exponent digits after the point: 1234 in USD is "12.34", -5 is
// "-0.05". Parse(Format(x)) == x for every int64 x.
func (c Currency) Format(minor int64) string {
	// The magnitude as a uint64 also covers math.MinInt64, whose negation doesn't fit in an int64.
	mag := uint64(minor)
	if minor < 0 {
		mag = -mag
	}
	digits := strconv.FormatUint(mag, 10)
	if c.Exponent > 0 {
		if len(digits) <= c.Exponent {
			digits = strings.Repeat("0", c.Exponent-len(digits)+1) + digits
		}
		cut := len(digits) - c.Exponent
		digits = digits[:cut] + "." + digits[cut:]
	}
	if minor < 0 {
		return "-" + digits
	}
	return digits
}

// Add returns a + b, and false if the sum doesn't fit in an int64. Go's + wraps silently: four postings of 2^62
// would "sum" to zero.
func Add(a, b int64) (int64, bool) {
	s := a + b
	// Overflow happens exactly when both operands have the same sign and the result has the other one.
	if (a >= 0) == (b >= 0) && (s >= 0) != (a >= 0) {
		return 0, false
	}
	return s, true
}

func digitsOnly(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
