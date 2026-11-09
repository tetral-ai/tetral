package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestRunnerReapsOnlyOrphansOfOtherNamespaces makes the test process a child
// subreaper with the runner's reaper running. The first process of a new PID
// namespace, orphaned when its outer process is killed, is reaped once it
// exits; a child the process started itself in its own namespace is left
// for its Wait, which gets its exit status.
func TestRunnerReapsOnlyOrphansOfOtherNamespaces(t *testing.T) {
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	own, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reapOrphans(ctx, own)

	outer := exec.Command("unshare", "--user", "--pid", "--fork", "sleep", "2")
	if err := outer.Start(); err != nil {
		t.Fatal(err)
	}
	inner := waitFor(t, "the namespace's first process", func() int { return childOf(outer.Process.Pid) })
	if namespace, err := os.Readlink("/proc/" + strconv.Itoa(inner) + "/ns/pid"); err != nil || namespace == own {
		t.Fatalf("process %d is in PID namespace %q (%v); want one other than %q", inner, namespace, err, own)
	}
	_ = outer.Process.Kill()
	_ = outer.Wait()
	waitFor(t, "the orphan's reaping", func() int {
		if fields := procStatFields(inner); len(fields) > 1 && fields[1] == strconv.Itoa(os.Getpid()) {
			return 0
		}
		return 1
	})

	child := exec.Command("sh", "-c", "exit 3")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the own child's exit", func() int {
		if fields := procStatFields(child.Process.Pid); len(fields) == 0 || fields[0] == "Z" {
			return 1
		}
		return 0
	})
	time.Sleep(200 * time.Millisecond)
	var exit *exec.ExitError
	if err := child.Wait(); !errors.As(err, &exit) || exit.ExitCode() != 3 {
		t.Fatalf("the own child's Wait gave %v; want exit status 3", err)
	}
}

// childOf is a child of parent, 0 while it has none.
func childOf(parent int) int {
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if fields := procStatFields(pid); len(fields) > 1 && fields[1] == strconv.Itoa(parent) {
			return pid
		}
	}
	return 0
}

// waitFor polls found for up to 10 seconds and returns its first nonzero
// value.
func waitFor(t *testing.T, what string, found func() int) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if value := found(); value != 0 {
			return value
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("waited 10 seconds for %s", what)
	return 0
}
