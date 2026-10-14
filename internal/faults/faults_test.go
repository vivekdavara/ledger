package faults

import (
	"errors"
	"maps"
	"testing"
)

func TestParse(t *testing.T) {
	for spec, want := range map[string]map[string]Rule{
		"":                       {},
		"  ":                     {},
		AfterCommit + ":3":       {AfterCommit: {Nth: 3}},
		AfterCommit + ":5%":      {AfterCommit: {Percent: 5}},
		AfterCommit + ":0.5%":    {AfterCommit: {Percent: 0.5}},
		AfterCommit + ":100%":    {AfterCommit: {Percent: 100}},
		" " + AfterCommit + ":1": {AfterCommit: {Nth: 1}},
		AfterCommit + ":2," + AfterDeliver + ":20%": {AfterCommit: {Nth: 2}, AfterDeliver: {Percent: 20}},
	} {
		got, err := Parse(spec)
		if err != nil || !maps.Equal(got, want) {
			t.Errorf("Parse(%q) = %v, %v; want %v", spec, got, err, want)
		}
	}
}

func TestParseRefusesWhatItCantRead(t *testing.T) {
	for _, spec := range []string{
		AfterCommit,                              // no when
		AfterCommit + ":",                        // empty when
		"store.after-comit:3",                    // misspelled: it would crash nothing
		AfterCommit + ":0",                       // hits count from 1
		AfterCommit + ":-1",                      //
		AfterCommit + ":1.5",                     // not a whole number
		AfterCommit + ":0%",                      // never
		AfterCommit + ":101%",                    // more than always
		AfterCommit + ":NaN%",                    //
		AfterCommit + ":x%",                      //
		AfterCommit + ":1," + AfterCommit + ":2", // twice
		AfterCommit + ":1,",                      // an empty item
	} {
		if rules, err := Parse(spec); err == nil {
			t.Errorf("Parse(%q) = %v, want an error", spec, rules)
		}
	}
}

// The decision itself, for both kinds of rule. Only 100% was tested before, so a percentage read as a fraction
// (5% firing on every hit) passed (found in review).
func TestWhenARuleFires(t *testing.T) {
	third := Rule{Nth: 3}
	for hit := 1; hit <= 5; hit++ {
		if third.fires(hit, 0) != (hit == 3) {
			t.Errorf("Nth 3 on hit %d: %v", hit, third.fires(hit, 0))
		}
	}
	for _, c := range []struct {
		percent, draw float64
		want          bool
	}{
		{5, 0, true}, {5, 0.0499, true}, {5, 0.05, false}, {5, 0.5, false}, {5, 0.999, false},
		{0.5, 0.0049, true}, {0.5, 0.005, false}, {100, 0.9999, true},
	} {
		if got := (Rule{Percent: c.percent}).fires(1, c.draw); got != c.want {
			t.Errorf("%v%% with draw %v: %v, want %v", c.percent, c.draw, got, c.want)
		}
	}
}

func TestRuleStringIsTheSpec(t *testing.T) {
	for _, spec := range []string{"3", "5%", "0.5%"} {
		rules, err := Parse(AfterCommit + ":" + spec)
		if err != nil || rules[AfterCommit].String() != spec {
			t.Errorf("%q: %v, %v", spec, rules, err)
		}
	}
}

func TestArmWithoutTheTagRefusesASpec(t *testing.T) {
	if Enabled {
		t.Skip("built with the faults tag")
	}
	if _, err := Arm(AfterCommit + ":1"); !errors.Is(err, ErrNotBuilt) {
		t.Fatalf("Arm = %v, want ErrNotBuilt", err)
	}
	if rules, err := Arm(""); err != nil || len(rules) != 0 {
		t.Fatalf("Arm(\"\") = %v, %v", rules, err)
	}
	Point(AfterCommit) // does nothing
}
