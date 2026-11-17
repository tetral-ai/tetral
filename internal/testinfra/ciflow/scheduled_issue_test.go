package ciflow

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

type issueAPIStandIn struct {
	jobs   []ScheduledJob
	logs   map[int64]string
	issues []ScheduledIssue
	calls  []string
}

func (s *issueAPIStandIn) Jobs() ([]ScheduledJob, error) { return s.jobs, nil }
func (s *issueAPIStandIn) JobLog(id int64) (string, error) {
	s.calls = append(s.calls, fmt.Sprintf("log %d", id))
	return s.logs[id], nil
}
func (s *issueAPIStandIn) OpenIssues(label string) ([]ScheduledIssue, error) {
	if label != ScheduledRedLabel {
		return nil, fmt.Errorf("listed by label %q", label)
	}
	return s.issues, nil
}
func (s *issueAPIStandIn) EnsureLabel(name string) error {
	s.calls = append(s.calls, "label "+name)
	return nil
}
func (s *issueAPIStandIn) CreateIssue(title, body, label string) error {
	s.calls = append(s.calls, "create "+title+" ["+label+"]\n"+body)
	return nil
}
func (s *issueAPIStandIn) Comment(number int, body string) error {
	s.calls = append(s.calls, fmt.Sprintf("comment %d\n%s", number, body))
	return nil
}
func (s *issueAPIStandIn) CloseIssue(number int) error {
	s.calls = append(s.calls, fmt.Sprintf("close %d", number))
	return nil
}

func TestScheduledRunKeepsItsWorkflowsIssueInStep(t *testing.T) {
	const reporter = "Report a Red Scheduled Run"
	run := ScheduledRun{Workflow: "Scheduled Verification", Schedule: "41 6 * * *", URL: "https://github.com/o/r/actions/runs/7", Commit: "abc123", Reporter: reporter}
	title := ScheduledIssueTitle(run.Workflow, run.Schedule)
	if title != "Scheduled check red on main: Scheduled Verification (41 6 * * *)" {
		t.Fatalf("title %q", title)
	}
	weekly := ScheduledIssue{Number: 5, Title: ScheduledIssueTitle(run.Workflow, "17 7 * * 1")}
	open := []ScheduledIssue{{Number: 3, Title: "Scheduled check red on main: engine-vulncheck (23 6 * * *)"}, {Number: 9, Title: title}}
	failedRun := []ScheduledJob{
		{ID: 1, Name: "Dependency Advisories", Conclusion: "failure"},
		{ID: 2, Name: "Report-only Coverage", Conclusion: "success"},
		{ID: 4, Name: "Compatibility and Repository Health", Conclusion: "failure"},
		{ID: 5, Name: "Repeated Concurrency Owners", Conclusion: "skipped"},
		{ID: 6, Name: reporter, Conclusion: ""},
	}
	logs := map[int64]string{
		1: "2026-11-16T06:41:12.0000000Z Vulnerability #1: GO-2026-4100\n2026-11-16T06:41:12.0000000Z   More info: GO-2026-4100\nGHSA-2c2m-3v8f-9x7q in bun audit\n",
		4: "--- FAIL: TestPostgreSQLRuntimeContextRecovery (3.20s)\n    --- FAIL: TestPostgreSQLRuntimeContextRecovery/resume (1.00s)\n(fail) gateway > routes a stream [12.00ms]\n",
	}
	failedBody := "Scheduled run https://github.com/o/r/actions/runs/7 of Scheduled Verification on commit abc123 failed.\n\n" +
		"- Dependency Advisories: `GO-2026-4100`, `GHSA-2c2m-3v8f-9x7q`\n" +
		"- Compatibility and Repository Health: `TestPostgreSQLRuntimeContextRecovery`, `TestPostgreSQLRuntimeContextRecovery/resume`, `gateway > routes a stream`\n"
	green := []ScheduledJob{
		{ID: 1, Name: "Dependency Advisories", Conclusion: "success"},
		{ID: 5, Name: "Repeated Concurrency Owners", Conclusion: "skipped"},
		{ID: 6, Name: reporter, Conclusion: ""},
	}
	for _, row := range []struct {
		name   string
		jobs   []ScheduledJob
		issues []ScheduledIssue
		did    string
		calls  []string
	}{
		{
			name: "a failed job and no open issue create the issue", jobs: failedRun, issues: open[:1], did: "created",
			calls: []string{"log 1", "log 4", "label " + ScheduledRedLabel, "create " + title + " [" + ScheduledRedLabel + "]\n" + failedBody},
		},
		{
			name: "a failed job and an open issue add one comment", jobs: failedRun, issues: open, did: "commented",
			calls: []string{"log 1", "log 4", "comment 9\n" + failedBody},
		},
		{
			name: "the jobs that ran succeeded, the rest skipped, and an open issue: a comment, then the issue closes", jobs: green, issues: open, did: "closed",
			calls: []string{"comment 9\nScheduled run https://github.com/o/r/actions/runs/7 of Scheduled Verification on commit abc123 succeeded.\n", "close 9"},
		},
		{name: "all jobs succeeded and no open issue: nothing", jobs: green, issues: open[:1], did: "none"},
		{name: "a green run leaves the other schedule's open issue as it is", jobs: green, issues: []ScheduledIssue{weekly}, did: "none"},
		{
			name: "a red run opens its own schedule's issue beside the other schedule's", jobs: failedRun, issues: []ScheduledIssue{weekly}, did: "created",
			calls: []string{"log 1", "log 4", "label " + ScheduledRedLabel, "create " + title + " [" + ScheduledRedLabel + "]\n" + failedBody},
		},
		{
			name: "none failed but one cancelled: nothing", issues: open, did: "none",
			jobs: []ScheduledJob{{ID: 1, Name: "Dependency Advisories", Conclusion: "success"}, {ID: 2, Name: "Report-only Coverage", Conclusion: "cancelled"}},
		},
		{
			name: "every job skipped: nothing", issues: open, did: "none",
			jobs: []ScheduledJob{{ID: 5, Name: "Repeated Concurrency Owners", Conclusion: "skipped"}, {ID: 6, Name: reporter}},
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			api := &issueAPIStandIn{jobs: row.jobs, logs: logs, issues: row.issues}
			did, err := ReportScheduledRun(api, run)
			if err != nil || did != row.did || !slices.Equal(api.calls, row.calls) {
				t.Fatalf("did %q, %v, calls:\n%s\nwant %q, calls:\n%s", did, err, strings.Join(api.calls, "\n--\n"), row.did, strings.Join(row.calls, "\n--\n"))
			}
		})
	}
}

func TestJobCauseNamesWhatTheLogReports(t *testing.T) {
	var many strings.Builder
	var listed []string
	for index := range maxCauses + 2 {
		fmt.Fprintf(&many, "--- FAIL: TestCase%d (0.00s)\n", index)
		if index < maxCauses {
			listed = append(listed, fmt.Sprintf("`TestCase%d`", index))
		}
	}
	// cargo-deny's report of an advisory, as job 129304031156 logged it.
	cargoDeny := strings.Join([]string{
		"2026-11-09T16:52:53.2109131Z error[vulnerability]: `libsqlite3-sys` via C SQLite CVE-2026-119375",
		"2026-11-09T16:52:53.2117912Z     ├ ID: RUSTSEC-2026-0459",
		"2026-11-09T16:52:53.2118955Z     ├ Advisory: https://rustsec.org/advisories/RUSTSEC-2026-0459",
		"2026-11-09T16:52:53.2144132Z     ├ Announcement: https://nvd.nist.gov/vuln/detail/CVE-2026-119375",
		"2026-11-09T16:52:53.2145542Z     ├ Solution: Upgrade to >=0.39.0 (try `cargo update -p libsqlite3-sys`)",
	}, "\n")
	for _, row := range []struct{ name, log, want string }{
		{name: "a run that failed before its tests", log: "verification apparatus-failed: enumerate fast Go evidence: go list: exit status 1\n", want: "`enumerate fast Go evidence: go list: exit status 1`"},
		{name: "a RustSec advisory that also names a CVE", log: cargoDeny, want: "`RUSTSEC-2026-0459`"},
		{name: "a log with none", log: "Process completed with exit code 1.\n", want: "no cause recorded"},
		{name: "more causes than the list holds", log: many.String(), want: strings.Join(listed, ", ") + " and 2 more"},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := JobCause(row.log); got != row.want {
				t.Fatalf("JobCause = %q; want %q", got, row.want)
			}
		})
	}
}
