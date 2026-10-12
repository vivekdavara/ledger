package storetest

import (
	"fmt"
	"runtime"
	"testing"
)

// fakeTB records what URL did. Like the real thing, Fatalf and Skipf stop the goroutine that calls them.
type fakeTB struct {
	testing.TB
	fatal, skip string
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.fatal = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (f *fakeTB) Skipf(format string, args ...any) {
	f.skip = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func callURL(f *fakeTB) (url string) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		url = URL(f)
	}()
	<-done
	return url
}

func TestURLSkipsLocallyButFailsInCI(t *testing.T) {
	t.Setenv(EnvURL, "")
	t.Setenv("CI", "")
	local := &fakeTB{}
	callURL(local)
	if local.skip == "" || local.fatal != "" {
		t.Fatalf("without a URL, outside CI: skip %q, fatal %q; want a skip", local.skip, local.fatal)
	}

	t.Setenv("CI", "true")
	ci := &fakeTB{}
	callURL(ci)
	if ci.fatal == "" || ci.skip != "" {
		t.Fatalf("without a URL, in CI: skip %q, fatal %q; want a failure", ci.skip, ci.fatal)
	}

	t.Setenv(EnvURL, "postgres://example/ledger_test")
	set := &fakeTB{}
	if got := callURL(set); got != "postgres://example/ledger_test" || set.fatal != "" || set.skip != "" {
		t.Fatalf("with a URL: got %q, skip %q, fatal %q", got, set.skip, set.fatal)
	}
}
