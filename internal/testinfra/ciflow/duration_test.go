// Layer 1: the run duration measure over recorded jobs.
package ciflow

import (
	"strings"
	"testing"
	"time"
)

func TestRunDurationFailsAtTheLimit(t *testing.T) {
	start := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	after := func(seconds int) string { return start.Add(time.Duration(seconds) * time.Second).Format(time.RFC3339) }
	job := func(name string, created, started, completed int) string {
		return `{"name":"` + name + `","created_at":"` + after(created) + `","started_at":"` + after(started) + `","completed_at":"` + after(completed) + `"}`
	}
	list := func(jobs ...string) string { return `{"jobs":[` + strings.Join(jobs, ",") + `]}` }
	for _, row := range []struct {
		name      string
		attempts  []string // the current attempt's list, then the earlier ones
		decided   int
		wantFails string
		wantLines []string
	}{
		{name: "one second under the limit",
			attempts: []string{list(job("Decide", 0, 2, 20), job("Tests", 20, 25, 1700), job("Gate", 1700, 1702, 1799))}, decided: 1799,
			wantLines: []string{"Decide: runner wait 2 s, run 18 s", "Tests: runner wait 5 s, run 1675 s", "run: 1799 s"}},
		{name: "at the limit",
			attempts: []string{list(job("Decide", 0, 2, 20), job("Tests", 20, 25, 1700), job("Gate", 1700, 1702, 1800))}, decided: 1800,
			wantFails: "the run took 1800 s", wantLines: []string{"run: 1800 s"}},
		{name: "runner wait counts",
			attempts: []string{list(job("Decide", 0, 2, 20), job("Tests", 20, 380, 1880), job("Gate", 1880, 1882, 1890))}, decided: 1890,
			wantFails: "the run took 1890 s", wantLines: []string{"Tests: runner wait 360 s, run 1500 s", "run: 1890 s"}},
		{name: "the earliest job need not be listed first",
			attempts: []string{list(job("Tests", 20, 25, 1700), job("Decide", 0, 2, 20))}, decided: 1800,
			wantFails: "the run took 1800 s"},
		// GitHub lists a job the second attempt did not run again with the
		// second attempt's creation time and the times of its first run.
		{name: "the gate run again alone after a run that reached the limit",
			attempts: []string{
				list(job("Decide", 5000, 2, 20), job("Tests", 5000, 25, 1900), job("Gate", 5000, 5002, 5010)),
				list(job("Decide", 0, 2, 20), job("Tests", 20, 25, 1900), job("Gate", 1900, 1902, 1910)),
			}, decided: 5010,
			wantFails: "the run took 5010 s", wantLines: []string{"Decide: runner wait 2 s, run 18 s", "Tests: runner wait 5 s, run 1875 s"}},
		{name: "a job run again in the third attempt beside one kept from the first",
			attempts: []string{
				list(job("Decide", 900, 2, 20), job("Tests", 900, 905, 1200), job("Gate", 1200, 1202, 1210)),
				list(job("Decide", 400, 2, 20), job("Tests", 400, 405, 700), job("Gate", 700, 702, 710)),
				list(job("Decide", 0, 2, 20), job("Tests", 20, 25, 300), job("Gate", 300, 302, 310)),
			}, decided: 1210,
			wantLines: []string{"Decide: runner wait 2 s, run 18 s", "Tests: runner wait 5 s, run 295 s", "run: 1210 s"}},
		{name: "a kept job whose first run no earlier attempt lists",
			attempts: []string{list(job("Decide", 5000, 2, 20), job("Tests", 5000, 5005, 5100))}, decided: 5110,
			wantFails: "no earlier attempt lists the run of Decide"},
		{name: "a job that never started",
			attempts: []string{list(job("Decide", 0, 2, 20), `{"name":"Tests","created_at":"`+after(20)+`","started_at":null,"completed_at":null}`)}, decided: 100,
			wantLines: []string{"Tests: runner wait not started, run not finished", "run: 100 s"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			var attempts [][]JobTimes
			for _, body := range row.attempts {
				jobs, err := ParseJobs([]byte(body), "Gate")
				if err != nil {
					t.Fatal(err)
				}
				attempts = append(attempts, jobs)
			}
			var lines []string
			jobs, err := WithFirstCreations(attempts[0], attempts[1:]...)
			if err == nil {
				lines, err = DurationReport(jobs, start.Add(time.Duration(row.decided)*time.Second))
			}
			if row.wantFails == "" && err != nil || row.wantFails != "" && (err == nil || !strings.Contains(err.Error(), row.wantFails)) {
				t.Fatalf("error = %v, want %q", err, row.wantFails)
			}
			report := strings.Join(lines, "\n") + "\n"
			for _, want := range row.wantLines {
				if !strings.Contains(report, want+"\n") {
					t.Fatalf("report:\n%slacks the line %q", report, want)
				}
			}
			if strings.Contains(report, "Gate:") {
				t.Fatalf("report names the gate job:\n%s", report)
			}
		})
	}
}

func TestParseJobsRefusesAListItCannotMeasure(t *testing.T) {
	for name, body := range map[string]string{
		"no job beside the gate":        `{"jobs":[{"name":"Gate","created_at":"2026-11-12T09:00:00Z"}]}`,
		"a job without a creation time": `{"jobs":[{"name":"Tests"}]}`,
		"not a list of jobs":            `<html>`,
	} {
		t.Run(name, func(t *testing.T) {
			if jobs, err := ParseJobs([]byte(body), "Gate"); err == nil {
				t.Fatalf("ParseJobs gave %+v", jobs)
			}
		})
	}
}
