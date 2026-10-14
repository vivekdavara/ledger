// Package faults holds the points where a test can crash ledgerd on purpose (DESIGN.md, "Load and failure
// injection (day 4)"). A point is a call to Point(name) where a real crash would be interesting: just after a
// posting commits, before its answer is written, or after the outbox's events are delivered, before they are marked.
//
// In a normal build every point does nothing and costs nothing. Built with -tags faults, Arm reads a spec (ledgerd
// takes it from LEDGER_FAULTS) that says which points crash the process, and when:
//
//	store.after-commit:3                    the third time the point is reached
//	store.after-commit:5%                   each time, with probability 5%
//	store.after-commit:3,outbox.after-deliver:20%
//
// The crash is a real one: the process sends itself SIGKILL, so nothing deferred runs and nothing is flushed, and
// the kernel closes its connections, as when the OOM killer or `kill -9` ends it.
package faults

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// The points, by name.
const (
	// BeforeCommit is reached when a posting's transaction has written everything and is about to commit: a crash
	// here leaves nothing posted.
	BeforeCommit = "store.before-commit"
	// AfterCommit is reached just after a posting's transaction commits, before PostEntry returns: a crash here
	// leaves the entry posted and its client without an answer.
	AfterCommit = "store.after-commit"
	// AfterDeliver is reached when the outbox's sink has taken a batch of events and before the batch is marked
	// published: a crash here makes the next publisher deliver the batch again.
	AfterDeliver = "outbox.after-deliver"
)

// Points lists every point, so a misspelled spec is refused instead of quietly crashing nothing.
var Points = []string{BeforeCommit, AfterCommit, AfterDeliver}

// ErrNotBuilt is Arm's error for a non-empty spec in a build without the faults tag: a test that thinks it is
// crashing ledgerd must not pass against a binary that can't.
var ErrNotBuilt = errors.New("this binary was built without -tags faults, so it can't crash on purpose")

// Rule says when one point crashes the process: on its Nth hit, or on each hit with a probability.
type Rule struct {
	Nth     int     // crash on this hit, counting from 1; 0 when Percent is set instead
	Percent float64 // crash on each hit with this probability, in percent: more than 0, at most 100
}

func (r Rule) String() string {
	if r.Nth > 0 {
		return strconv.Itoa(r.Nth)
	}
	return strconv.FormatFloat(r.Percent, 'f', -1, 64) + "%"
}

// Parse reads a spec: comma-separated point:when pairs, where when is a hit number (3) or a percentage (5%). An
// empty spec arms nothing.
func Parse(spec string) (map[string]Rule, error) {
	rules := map[string]Rule{}
	if strings.TrimSpace(spec) == "" {
		return rules, nil
	}
	for _, item := range strings.Split(spec, ",") {
		name, when, ok := strings.Cut(strings.TrimSpace(item), ":")
		switch {
		case !ok || when == "":
			return nil, fmt.Errorf("faults: %q is not point:when, such as %s:3 or %s:5%%", item, AfterCommit, AfterCommit)
		case !slices.Contains(Points, name):
			return nil, fmt.Errorf("faults: unknown point %q (the points are %s)", name, strings.Join(Points, ", "))
		case rules[name] != Rule{}:
			return nil, fmt.Errorf("faults: %s appears twice", name)
		}
		var r Rule
		if p, isPercent := strings.CutSuffix(when, "%"); isPercent {
			percent, err := strconv.ParseFloat(p, 64)
			if err != nil || !(percent > 0 && percent <= 100) {
				return nil, fmt.Errorf("faults: %s:%s: a percentage is more than 0 and at most 100", name, when)
			}
			r.Percent = percent
		} else {
			n, err := strconv.Atoi(when)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("faults: %s:%s: a hit number is a whole number from 1", name, when)
			}
			r.Nth = n
		}
		rules[name] = r
	}
	return rules, nil
}
