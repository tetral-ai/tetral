package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLaneGivesEachJobItsOwnPorts starts a lane's jobs on as many slots at
// once and finds each job's port range in its own environment.
func TestLaneGivesEachJobItsOwnPorts(t *testing.T) {
	const jobs = 6
	dir := t.TempDir()
	script := filepath.Join(dir, "job.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho \"$TETRAL_JOINT_PORTS\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	lane := make([]job, jobs)
	for i := range lane {
		lane[i] = job{name: fmt.Sprintf("j%d", i), rows: []row{{name: fmt.Sprintf("TestA/r%d", i), budget: 1}}, budget: 1}
	}
	r := &runner{
		cfg:     config{out: dir, slots: map[string]int{"linux": jobs}},
		binary:  script,
		logOut:  io.Discard,
		started: time.Now(),
		jobs:    map[string][]job{"linux": lane},
		results: map[string][]jobResult{},
		walls:   map[string]time.Duration{},
	}
	r.lane(context.Background(), "linux")
	for i := range lane {
		out, err := os.ReadFile(filepath.Join(dir, "linux", lane[i].name+".log"))
		if err != nil {
			t.Fatal(err)
		}
		first := firstPort + portsPerJob*i
		if got, want := strings.TrimSpace(string(out)), fmt.Sprintf("%d-%d", first, first+portsPerJob-1); got != want {
			t.Errorf("job %d ran with ports %q; want %q", i, got, want)
		}
	}
}
