//go:build faults

package faults

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// The crash can't be watched from inside the process it ends, so these tests run the test binary again as a child
// (with crashChild set), have it hit a point, and look at how it ended.
const crashChild = "FAULTS_TEST_CHILD"

func TestMain(m *testing.M) {
	if spec := os.Getenv(crashChild); spec != "" {
		if _, err := Arm(spec); err != nil {
			os.Exit(3)
		}
		for range 5 {
			Point(AfterCommit)
			os.Stdout.WriteString("survived\n")
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// child runs the test binary with spec armed and returns what it wrote and the signal that ended it, if any.
func child(t *testing.T, spec string) (string, syscall.Signal) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), crashChild+"="+spec)
	// A child that hangs is stopped with SIGTERM, not CommandContext's default SIGKILL, which would pass for a crash.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	out, err := cmd.Output()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return string(out), status.Signal()
		}
		t.Fatalf("child exited with %v", err)
	} else if err != nil {
		t.Fatal(err)
	}
	return string(out), 0
}

func TestTheNthHitIsKilled(t *testing.T) {
	out, sig := child(t, AfterCommit+":3")
	if sig != syscall.SIGKILL || out != "survived\nsurvived\n" {
		t.Fatalf("got %q and signal %v; want two survivals, then SIGKILL", out, sig)
	}
}

func TestAlwaysIsTheFirstHit(t *testing.T) {
	if out, sig := child(t, AfterCommit+":100%"); sig != syscall.SIGKILL || out != "" {
		t.Fatalf("got %q and signal %v", out, sig)
	}
}

func TestAnUnarmedPointDoesNothing(t *testing.T) {
	if out, sig := child(t, AfterDeliver+":1"); sig != 0 || out != "survived\nsurvived\nsurvived\nsurvived\nsurvived\n" {
		t.Fatalf("got %q and signal %v", out, sig)
	}
}
