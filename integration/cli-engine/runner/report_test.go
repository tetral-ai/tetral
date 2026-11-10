package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestReportExitCode fails the run when a listed row did not pass, a row no
// job ran included, and when a job exits nonzero although each of its rows
// printed PASS, as a test does that fails in its cleanup.
func TestReportExitCode(t *testing.T) {
	const passed = "    --- PASS: TestA/b (1.00s)\n--- FAIL: TestA (1.20s)\n"
	for _, c := range []struct {
		name  string
		noJob bool
		log   string
		exit  int
		want  int
	}{
		{"rows passed and the job exited 0", false, passed, 0, 0},
		{"rows passed and the job exited 1", false, passed, 1, 1},
		{"a row has no result and the job exited 0", false, "ok\n", 0, 1},
		{"no job ran the row", true, "", 0, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "job.log")
			if err := os.WriteFile(log, []byte(c.log), 0o600); err != nil {
				t.Fatal(err)
			}
			b := row{name: "TestA/b", budget: 10}
			results := []jobResult{{job: job{name: "TestA", rows: []row{b}}, exit: c.exit, log: log}}
			if c.noJob {
				results = nil
			}
			r := &runner{
				cfg:      config{lanes: []string{"linux"}, out: dir, slots: map[string]int{"linux": 1}},
				started:  time.Now(),
				logOut:   io.Discard,
				listings: map[string]listing{"linux": {rows: []row{b}}},
				results:  map[string][]jobResult{"linux": results},
				walls:    map[string]time.Duration{},
			}
			if got := r.report(); got != c.want {
				t.Errorf("report returned %d; want %d", got, c.want)
			}
		})
	}
}
