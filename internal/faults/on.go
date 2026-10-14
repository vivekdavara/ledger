//go:build faults

package faults

import (
	"math/rand/v2"
	"sync"
	"syscall"
)

// Enabled reports whether this binary was built with the faults tag, and so whether a point can crash it.
const Enabled = true

var (
	mu    sync.Mutex
	armed = map[string]Rule{}
	hits  = map[string]int{}
)

// Arm replaces the armed rules with spec's and starts every point's count again.
func Arm(spec string) (map[string]Rule, error) {
	rules, err := Parse(spec)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	defer mu.Unlock()
	armed, hits = rules, map[string]int{}
	return rules, nil
}

// Point counts a hit of the named point and, when the point's rule says so, crashes the process: it sends itself
// SIGKILL, which can't be caught, and never returns.
func Point(name string) {
	mu.Lock()
	r, ok := armed[name]
	hits[name]++
	crash := ok && (r.Nth == hits[name] || r.Percent > 0 && rand.Float64()*100 < r.Percent)
	mu.Unlock()
	if !crash {
		return
	}
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
	select {} // the signal is on its way; nothing after the point may run
}
