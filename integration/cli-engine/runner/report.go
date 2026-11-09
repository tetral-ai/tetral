package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// networkSampler counts testinfra's Docker networks every 10 s while the
// lanes run, for the peak the report names.
type networkSampler struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	peak   int
	peakAt time.Duration
}

func (s *networkSampler) start(r *runner) {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			out, err := exec.CommandContext(ctx, "docker", "network", "ls", "-q", "--filter", "label=tetral.test.owner=testinfra").Output()
			if err == nil {
				s.mu.Lock()
				if n := len(strings.Fields(string(out))); n > s.peak {
					s.peak, s.peakAt = n, time.Since(r.started)
				}
				s.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *networkSampler) stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
}

// rowResult is one listed row's outcome, read from its job's log.
type rowResult struct {
	lane, result string
	row          row
	seconds      float64
	job          jobResult
}

var testLine = regexp.MustCompile(`^\s*--- (PASS|FAIL|SKIP): (\S+) \(([0-9.]+)s\)`)

// report writes rows.tsv and summary.txt into the run's directory and
// prints the summary: each row's result and time against its budget, each
// lane's wall, the highest budget use, the preflight values and the Docker
// network peak. It returns 1 when a row did not pass or a job exited
// nonzero, as one does when a test fails after its rows passed, and 0
// otherwise.
func (r *runner) report() int {
	var rows []rowResult
	var summary strings.Builder
	code := 0
	for _, machineOS := range r.cfg.lanes {
		r.mu.Lock()
		results := slices.Clone(r.results[machineOS])
		r.mu.Unlock()
		byRow := map[string]jobResult{}
		for _, result := range results {
			for _, row := range result.job.rows {
				byRow[row.name] = result
			}
		}
		var laneRows []rowResult
		for _, row := range r.listings[machineOS].rows {
			result, ok := byRow[row.name]
			rr := rowResult{lane: machineOS, row: row, result: "NONE", job: result}
			if ok {
				rr.result, rr.seconds = rowOutcome(result.log, row.name)
			}
			laneRows = append(laneRows, rr)
		}
		slices.SortFunc(laneRows, func(a, b rowResult) int { return strings.Compare(a.row.name, b.row.name) })
		rows = append(rows, laneRows...)
		passed, highest := 0, rowResult{}
		var failed []string
		for _, rr := range laneRows {
			if rr.result == "PASS" {
				passed++
				if rr.seconds/float64(rr.row.budget) > highest.seconds/float64(max(highest.row.budget, 1)) {
					highest = rr
				}
			} else {
				failed = append(failed, rr.row.name+" "+rr.result)
			}
		}
		if len(failed) > 0 {
			code = 1
		}
		var slotSeconds float64
		for _, result := range results {
			slotSeconds += result.end.Sub(result.start).Seconds()
		}
		fmt.Fprintf(&summary, "%s lane: wall %.1f s; %d rows, %d passed; %d jobs, %.0f job-seconds on %d slots\n", machineOS, r.walls[machineOS].Seconds(), len(laneRows), passed, len(results), slotSeconds, r.cfg.slots[machineOS])
		if highest.row.budget > 0 {
			fmt.Fprintf(&summary, "  highest budget use of a passing row: %s, %.1f s of %d s (%.0f %%)\n", highest.row.name, highest.seconds, highest.row.budget, 100*highest.seconds/float64(highest.row.budget))
		}
		for _, f := range failed {
			fmt.Fprintf(&summary, "  not passed: %s\n", f)
		}
		for _, result := range results {
			if result.exit != 0 {
				code = 1
				fmt.Fprintf(&summary, "  job %s exit %d: %s\n", result.job.name, result.exit, failureLines(readFile(result.log), 3))
			}
		}
	}
	fmt.Fprintf(&summary, "run wall %.1f s\n", r.wall.Seconds())
	fmt.Fprintf(&summary, "preflight: bwrap %s %s; Mac round trip %s; Mac 1-minute load %.2f; Docker networks created %d of %d\n", r.bwrapPath, r.bwrapVersion, r.macRoundTrip.Round(100*time.Microsecond), r.macLoad, r.networksCreated, r.networksWanted)
	r.networks.mu.Lock()
	fmt.Fprintf(&summary, "Docker test networks peak: %d at +%.0f s\n", r.networks.peak, r.networks.peakAt.Seconds())
	r.networks.mu.Unlock()
	var table strings.Builder
	table.WriteString("lane\trow\tresult\tseconds\tbudget\tuse\tjob\tjob seconds\tjob start\n")
	for _, rr := range rows {
		fmt.Fprintf(&table, "%s\t%s\t%s\t%.1f\t%d\t%.0f%%\t%s\t%.1f\t%.1f\n", rr.lane, rr.row.name, rr.result, rr.seconds, rr.row.budget, 100*rr.seconds/float64(rr.row.budget), rr.job.job.name, rr.job.end.Sub(rr.job.start).Seconds(), rr.job.start.Sub(r.started).Seconds())
	}
	for name, data := range map[string]string{"rows.tsv": table.String(), "summary.txt": summary.String()} {
		if err := writeFile(filepath.Join(r.cfg.out, name), []byte(data)); err != nil {
			r.log("%v", err)
		}
	}
	r.log("summary:\n%s", summary.String())
	return code
}

// rowOutcome is the result and time that go test printed for name in log,
// NONE when it printed none.
func rowOutcome(log, name string) (string, float64) {
	file, err := os.Open(log)
	if err != nil {
		return "NONE", 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		if m := testLine.FindStringSubmatch(scanner.Text()); m != nil && m[2] == name {
			seconds, _ := strconv.ParseFloat(m[3], 64)
			return m[1], seconds
		}
	}
	return "NONE", 0
}

// failureLines are the first n lines of output that name a test file's
// line, as go test prints a failure.
func failureLines(output string, n int) string {
	site := regexp.MustCompile(`^\s+\S+_test\.go:\d+: `)
	var lines []string
	for line := range strings.Lines(output) {
		if site.MatchString(line) || strings.Contains(line, "no budget") {
			lines = append(lines, strings.TrimSpace(truncate(line, 300)))
			if len(lines) == n {
				break
			}
		}
	}
	return strings.Join(lines, " | ")
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func readFile(path string) string {
	data, _ := os.ReadFile(path)
	return string(data)
}
