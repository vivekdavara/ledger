package money

import (
	"errors"
	"math"
	"math/big"
	"testing"

	"pgregory.net/rapid"
)

var (
	usd = mustLookup("USD")
	jpy = mustLookup("JPY")
	kwd = mustLookup("KWD")
)

func mustLookup(code string) Currency {
	c, err := Lookup(code)
	if err != nil {
		panic(err)
	}
	return c
}

func TestLookup(t *testing.T) {
	if c, err := Lookup("KWD"); err != nil || c.Exponent != 3 {
		t.Fatalf("Lookup(KWD) = %+v, %v; want exponent 3", c, err)
	}
	for _, code := range []string{"usd", "XXX", "", "US"} {
		if _, err := Lookup(code); !errors.Is(err, ErrUnknownCurrency) {
			t.Errorf("Lookup(%q) error = %v, want ErrUnknownCurrency", code, err)
		}
	}
}

func TestCurrenciesAreSortedAndComplete(t *testing.T) {
	all := Currencies()
	if len(all) != len(registry) {
		t.Fatalf("got %d currencies, want %d", len(all), len(registry))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].Code >= all[i].Code {
			t.Fatalf("not sorted at %d: %s before %s", i, all[i-1].Code, all[i].Code)
		}
	}
}

func TestParse(t *testing.T) {
	cases := []struct {
		cur  Currency
		in   string
		want int64
	}{
		{usd, "0", 0},
		{usd, "12", 1200},
		{usd, "12.3", 1230},
		{usd, "12.34", 1234},
		{usd, "-12.34", -1234},
		{usd, "0.05", 5},
		{usd, "-0.05", -5},
		{usd, "-0", 0},
		{usd, "007.50", 750},
		{usd, "92233720368547758.07", math.MaxInt64},
		{usd, "-92233720368547758.08", math.MinInt64},
		{jpy, "1500", 1500},
		{jpy, "-9223372036854775808", math.MinInt64},
		{kwd, "1.234", 1234},
		{kwd, "1.2", 1200},
	}
	for _, tc := range cases {
		got, err := tc.cur.Parse(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("%s.Parse(%q) = %d, %v; want %d", tc.cur.Code, tc.in, got, err, tc.want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct {
		cur  Currency
		in   string
		want error
	}{
		{usd, "", ErrSyntax},
		{usd, "-", ErrSyntax},
		{usd, ".5", ErrSyntax},
		{usd, "1.", ErrSyntax},
		{usd, "12.345", ErrSyntax}, // too precise for USD: an error, not a rounding
		{usd, "+1", ErrSyntax},
		{usd, "--1", ErrSyntax},
		{usd, " 1", ErrSyntax},
		{usd, "1 ", ErrSyntax},
		{usd, "1,000", ErrSyntax},
		{usd, "1_000", ErrSyntax},
		{usd, "1e3", ErrSyntax},
		{usd, "1.2.3", ErrSyntax},
		{usd, "1.-2", ErrSyntax},
		{usd, "١٢", ErrSyntax}, // Arabic-Indic digits are digits to Unicode, not to a ledger
		{usd, "NaN", ErrSyntax},
		{jpy, "1.0", ErrSyntax}, // JPY has no minor unit
		{kwd, "1.2345", ErrSyntax},
		{usd, "92233720368547758.08", ErrRange},
		{usd, "-92233720368547758.09", ErrRange},
		{jpy, "9223372036854775808", ErrRange},
		{jpy, "99999999999999999999999999", ErrRange},
	}
	for _, tc := range cases {
		got, err := tc.cur.Parse(tc.in)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s.Parse(%q) = %d, %v; want %v", tc.cur.Code, tc.in, got, err, tc.want)
		}
	}
}

func TestFormat(t *testing.T) {
	cases := []struct {
		cur  Currency
		in   int64
		want string
	}{
		{usd, 0, "0.00"},
		{usd, 5, "0.05"},
		{usd, -5, "-0.05"},
		{usd, 50, "0.50"},
		{usd, 1234, "12.34"},
		{usd, -1234, "-12.34"},
		{usd, math.MaxInt64, "92233720368547758.07"},
		{usd, math.MinInt64, "-92233720368547758.08"},
		{jpy, 0, "0"},
		{jpy, -1500, "-1500"},
		{kwd, 1, "0.001"},
		{kwd, -1234, "-1.234"},
	}
	for _, tc := range cases {
		if got := tc.cur.Format(tc.in); got != tc.want {
			t.Errorf("%s.Format(%d) = %q, want %q", tc.cur.Code, tc.in, got, tc.want)
		}
	}
}

func anyCurrency() *rapid.Generator[Currency] {
	return rapid.SampledFrom(Currencies())
}

// Every int64 survives a round trip through its decimal form, in every currency.
func TestFormatParseRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cur := anyCurrency().Draw(t, "currency")
		x := rapid.Int64().Draw(t, "minor")
		s := cur.Format(x)
		got, err := cur.Parse(s)
		if err != nil || got != x {
			t.Fatalf("%s: Parse(Format(%d)) = Parse(%q) = %d, %v", cur.Code, x, s, got, err)
		}
	})
}

// Add agrees with arbitrary-precision addition: it returns the exact sum when it fits, and false when it doesn't.
func TestAddMatchesBigInt(t *testing.T) {
	limit := big.NewInt(math.MaxInt64)
	floor := big.NewInt(math.MinInt64)
	check := func(t interface{ Fatalf(string, ...any) }, a, b int64) {
		exact := new(big.Int).Add(big.NewInt(a), big.NewInt(b))
		fits := exact.Cmp(limit) <= 0 && exact.Cmp(floor) >= 0
		got, ok := Add(a, b)
		if ok != fits || (ok && got != exact.Int64()) {
			t.Fatalf("Add(%d, %d) = %d, %v; exact sum %s", a, b, got, ok, exact)
		}
	}
	// The edges, where wrapping bugs live.
	edges := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, math.MaxInt64 - 1, math.MinInt64 + 1, 1 << 62, -(1 << 62)}
	for _, a := range edges {
		for _, b := range edges {
			check(t, a, b)
		}
	}
	rapid.Check(t, func(t *rapid.T) {
		check(t, rapid.Int64().Draw(t, "a"), rapid.Int64().Draw(t, "b"))
	})
}

func TestAddCatchesTheWrapToZero(t *testing.T) {
	// 2^62 four times is 2^64, which plain int64 addition wraps to exactly 0.
	sum, ok := int64(0), true
	for range 4 {
		if sum, ok = Add(sum, 1<<62); !ok {
			break
		}
	}
	if ok {
		t.Fatalf("four additions of 2^62 reported no overflow (sum %d)", sum)
	}
}

// Parse never panics, and whatever it accepts formats back to a string that parses to the same value.
func FuzzParse(f *testing.F) {
	for _, seed := range []string{"0", "12.34", "-0.05", "1.", ".5", "92233720368547758.08", "1e3", "-", "007.50"} {
		f.Add("USD", seed)
	}
	f.Add("JPY", "1500")
	f.Add("KWD", "1.234")
	f.Fuzz(func(t *testing.T, code, s string) {
		cur, err := Lookup(code)
		if err != nil {
			return
		}
		x, err := cur.Parse(s)
		if err != nil {
			if !errors.Is(err, ErrSyntax) && !errors.Is(err, ErrRange) {
				t.Fatalf("Parse(%q) error %v wraps neither ErrSyntax nor ErrRange", s, err)
			}
			return
		}
		if back, err := cur.Parse(cur.Format(x)); err != nil || back != x {
			t.Fatalf("Parse(%q) = %d, but Format gives %q which parses to %d, %v", s, x, cur.Format(x), back, err)
		}
	})
}
