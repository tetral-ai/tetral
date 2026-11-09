package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestLeftoversEndOnlyTheRunsProcesses finds the run's processes in a ps
// listing, a process of a RAM disk that is already detached among them,
// and spares the ones that ran before the lane.
func TestLeftoversEndOnlyTheRunsProcesses(t *testing.T) {
	const (
		entry    = "101 /Users/m/tetral-test-machines/machine.AB/tetral server run --app a"
		detached = "102 /Volumes/tetral-joint-disk5/machine/tetral server run --app a"
		earlier  = "103 /Volumes/tetral-joint-disk4/machine/tetral server run --app a"
		other    = "104 /usr/sbin/sshd -D"
	)
	listing := "  " + entry + "\n  " + detached + "\n  " + earlier + "\n  " + other + "\n"
	got := leftovers(listing, []string{"/Users/m/tetral-test-machines/machine.AB"}, []string{earlier})
	if want := []string{entry, detached}; !slices.Equal(got, want) {
		t.Errorf("leftovers = %q; want %q", got, want)
	}
}

// TestMacLaneGetsCleanupOnlyWithItsBaseline leaves the Mac alone when the
// lane could not list what the Mac held before it, as when SSH drops at any
// of the three listings: the cleanup would otherwise end and remove what
// earlier runs left. A lane that listed all three gets its cleanup.
func TestMacLaneGetsCleanupOnlyWithItsBaseline(t *testing.T) {
	bin := t.TempDir()
	// The fake ssh fails the command that contains FAIL_AT and answers the
	// machine directory listing with the directory alone.
	fake := "#!/bin/sh\nfor last; do :; done\ncase \"$last\" in\n*\"$FAIL_AT\"*) exit 255 ;;\n*'ls -A'*) echo /Users/m/tetral-test-machines ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(fake), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	for _, c := range []struct {
		name    string
		failAt  string
		cleanup bool
	}{
		{"the machine directory listing fails", "ls -A", false},
		{"the disk image listing fails", "hdiutil info", false},
		{"the process listing fails", macProcesses, false},
		{"caffeinate fails after the three listings", "caffeinate", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("FAIL_AT", c.failAt)
			r := &runner{cfg: config{mac: macHost{destination: "mac"}}, started: time.Now(), logOut: io.Discard}
			if err := r.startMacLane(context.Background()); err == nil {
				t.Fatal("startMacLane succeeded")
			}
			if got := r.mac != nil; got != c.cleanup {
				t.Errorf("the lane has a cleanup: %t; want %t", got, c.cleanup)
			}
		})
	}
}

// TestRunDevicesDetachOnlyTheRunsDisks detaches a disk image attached during
// the run only when it shows it is the run's: a RAM disk by its mounted
// tetral-joint volume, an image by its path under one of the run's entries.
func TestRunDevicesDetachOnlyTheRunsDisks(t *testing.T) {
	const entry = "/Users/m/tetral-test-machines/machine.AB"
	before := []string{"/dev/disk4 ram://2097152"}
	// outcome is "detach", "left" (named in the log) or "" (untouched);
	// volume is the volume whose mounts the cleanup then ends.
	for _, c := range []struct {
		name    string
		device  string
		points  []string
		outcome string
		volume  string
	}{
		{"a RAM disk with its tetral-joint volume", "/dev/disk9 ram://2097152", []string{"/Volumes/tetral-joint-disk9"}, "detach", "/Volumes/tetral-joint-disk9"},
		{"a RAM disk with another volume", "/dev/disk9 ram://2097152", []string{"/Volumes/scratch"}, "left", ""},
		{"a RAM disk with another disk's tetral-joint volume", "/dev/disk9 ram://2097152", []string{"/Volumes/tetral-joint-disk4"}, "left", ""},
		{"a RAM disk whose name begins another disk's tetral-joint volume", "/dev/disk9 ram://2097152", []string{"/Volumes/tetral-joint-disk91"}, "left", ""},
		{"a RAM disk with no volume", "/dev/disk9 ram://2097152", nil, "left", ""},
		{"a RAM disk attached before the run", "/dev/disk4 ram://2097152", []string{"/Volumes/tetral-joint-disk4"}, "", ""},
		{"an image under a run's entry", "/dev/disk7 " + entry + "/small.sparseimage", nil, "detach", ""},
		{"an image under an entry whose name extends a run's entry", "/dev/disk7 " + entry + "2/small.sparseimage", nil, "", ""},
		{"an image elsewhere", "/dev/disk7 /Users/m/disk.dmg", nil, "", ""},
	} {
		detach, volumes, left := runDevices(append(slices.Clone(before), c.device), before, []string{entry}, c.points)
		device, _, _ := strings.Cut(c.device, " ")
		outcome := ""
		switch {
		case slices.Contains(detach, device):
			outcome = "detach"
		case slices.Contains(left, device):
			outcome = "left"
		}
		if outcome != c.outcome || strings.Join(volumes, " ") != c.volume {
			t.Errorf("%s: %s %q with volumes %v; want %q with %q", c.name, device, outcome, volumes, c.outcome, c.volume)
		}
	}
}
