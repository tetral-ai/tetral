package ciflow

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

type issueAPIStandIn struct {
	jobs        []ScheduledJob
	logs        map[int64]string
	annotations map[int64][]ScheduledAnnotation
	// unreadable fails the reads of these jobs' logs and annotations.
	unreadable map[int64]bool
	issues     []ScheduledIssue
	calls      []string
}

func (s *issueAPIStandIn) Jobs() ([]ScheduledJob, error) { return s.jobs, nil }
func (s *issueAPIStandIn) JobLog(id int64) (string, error) {
	s.calls = append(s.calls, fmt.Sprintf("log %d", id))
	if s.unreadable[id] {
		return "", fmt.Errorf("read the log of job %d: 404 Not Found", id)
	}
	return s.logs[id], nil
}
func (s *issueAPIStandIn) Annotations(id int64) ([]ScheduledAnnotation, error) {
	s.calls = append(s.calls, fmt.Sprintf("annotations %d", id))
	if s.unreadable[id] {
		return nil, fmt.Errorf("HTTP 403: Resource not accessible by integration")
	}
	return s.annotations[id], nil
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
	title := ScheduledIssueTitle("Scheduled Verification", "41 6 * * *")
	if title != "Scheduled check red on main: Scheduled Verification (41 6 * * *)" {
		t.Fatalf("title %q", title)
	}
	weekly := ScheduledIssue{Number: 5, Title: ScheduledIssueTitle("Scheduled Verification", "17 7 * * 1")}
	open := []ScheduledIssue{{Number: 3, Title: "Scheduled check red on main: engine-vulncheck (23 6 * * *)"}, {Number: 9, Title: title}}
	current := []string{title, weekly.Title, open[0].Title}
	run := ScheduledRun{Workflow: "Scheduled Verification", Schedule: "41 6 * * *", URL: "https://github.com/o/r/actions/runs/7", Commit: "abc123", Reporter: reporter, DeclaredTitles: current}
	// A workflow's cron was edited: no run carries the old one any more.
	removed := ScheduledIssue{Number: 4, Title: ScheduledIssueTitle("Scheduled Verification", "41 5 * * *")}
	coverage := ScheduledJob{ID: 2, Name: "Report-only Coverage", Conclusion: "cancelled"}
	// The annotations GitHub gave job 132777784768 of run 43702901197.
	timedOut := []ScheduledAnnotation{
		{Message: "The job has exceeded the maximum execution time of 95m0s"},
		{Message: "The operation was canceled."},
		{Message: "The ubuntu-latest label will migrate to Ubuntu 26 beginning October 19, 2026."},
	}
	cancelledByHand := []ScheduledAnnotation{{Message: "The operation was canceled."}}
	failedRun := []ScheduledJob{
		{ID: 1, Name: "Dependency Advisories", Conclusion: "failure"},
		{ID: 2, Name: "Report-only Coverage", Conclusion: "success"},
		{ID: 4, Name: "Compatibility and Repository Health", Conclusion: "failure"},
		{ID: 5, Name: "Repeated Concurrency Owners", Conclusion: "skipped"},
		{ID: 6, Name: reporter, Conclusion: ""},
	}
	logs := map[int64]string{
		1: "2026-11-16T06:41:12.0000000Z Vulnerability #1: GO-2026-4100\n2026-11-16T06:41:12.0000000Z   More info: GO-2026-4100\nGHSA-2c2m-3v8f-9x7q in bun audit\n",
		4: "--- FAIL: TestPostgreSQLRuntimeContextRecovery (3.20s)\n    --- FAIL: TestPostgreSQLRuntimeContextRecovery/resume (1.00s)\n",
	}
	failedHeader := "Scheduled run https://github.com/o/r/actions/runs/7 of Scheduled Verification on commit abc123 failed.\n\n"
	failedBody := failedHeader +
		"- Dependency Advisories: `GO-2026-4100`, `GHSA-2c2m-3v8f-9x7q`\n" +
		"- Compatibility and Repository Health: `TestPostgreSQLRuntimeContextRecovery`, `TestPostgreSQLRuntimeContextRecovery/resume`\n"
	green := []ScheduledJob{
		{ID: 1, Name: "Dependency Advisories", Conclusion: "success"},
		{ID: 5, Name: "Repeated Concurrency Owners", Conclusion: "skipped"},
		{ID: 6, Name: reporter, Conclusion: ""},
	}
	for _, row := range []struct {
		name        string
		jobs        []ScheduledJob
		issues      []ScheduledIssue
		annotations []ScheduledAnnotation
		unreadable  map[int64]bool
		current     []string
		did, err    string
		calls       []string
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
			name: "a job ended at its time limit and no open issue create the issue, naming the limit", issues: open[:1], annotations: timedOut, did: "created",
			jobs: []ScheduledJob{{ID: 1, Name: "Dependency Advisories", Conclusion: "success"}, coverage},
			calls: []string{"annotations 2", "label " + ScheduledRedLabel, "create " + title + " [" + ScheduledRedLabel + "]\n" + failedHeader +
				"- Report-only Coverage: `The job has exceeded the maximum execution time of 95m0s`\n"},
		},
		{
			name: "none failed but one cancelled by hand: nothing", issues: open, annotations: cancelledByHand, did: "none",
			jobs:  []ScheduledJob{{ID: 1, Name: "Dependency Advisories", Conclusion: "success"}, coverage},
			calls: []string{"annotations 2"},
		},
		{
			name: "a cancelled job whose annotations cannot be read is reported with the read error", issues: open, unreadable: map[int64]bool{2: true}, did: "commented",
			jobs: []ScheduledJob{{ID: 1, Name: "Dependency Advisories", Conclusion: "success"}, coverage},
			calls: []string{"annotations 2", "comment 9\n" + failedHeader +
				"- Report-only Coverage: cancelled, and its annotations could not be read: HTTP 403: Resource not accessible by integration\n"},
		},
		{
			name: "a failed job whose log cannot be read is reported with the read error beside the others", jobs: failedRun, issues: open[:1], unreadable: map[int64]bool{1: true}, did: "created",
			calls: []string{"log 1", "log 4", "label " + ScheduledRedLabel, "create " + title + " [" + ScheduledRedLabel + "]\n" + failedHeader +
				"- Dependency Advisories: its log could not be read: read the log of job 1: 404 Not Found\n" +
				"- Compatibility and Repository Health: `TestPostgreSQLRuntimeContextRecovery`, `TestPostgreSQLRuntimeContextRecovery/resume`\n"},
		},
		{
			name: "the open issue of a schedule no workflow declares closes, whatever the run", jobs: green, issues: []ScheduledIssue{removed, weekly}, did: "none",
			calls: []string{"comment 4\nNo workflow of this repository declares this schedule any more, so no run will report on it.\n", "close 4"},
		},
		{
			name: "every job skipped: nothing", issues: open, did: "none",
			jobs: []ScheduledJob{{ID: 5, Name: "Repeated Concurrency Owners", Conclusion: "skipped"}, {ID: 6, Name: reporter}},
		},
		{
			name: "a run whose own schedule the workflows do not declare touches no issue", jobs: failedRun, issues: []ScheduledIssue{removed}, current: []string{weekly.Title},
			err: `the repository's workflows declare no schedule titled "` + title + `"`,
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			api := &issueAPIStandIn{jobs: row.jobs, logs: logs, annotations: map[int64][]ScheduledAnnotation{2: row.annotations}, unreadable: row.unreadable, issues: row.issues}
			run := run
			if row.current != nil {
				run.DeclaredTitles = row.current
			}
			did, err := ReportScheduledRun(api, run)
			failure := ""
			if err != nil {
				failure = err.Error()
			}
			if failure != row.err || did != row.did || !slices.Equal(api.calls, row.calls) {
				t.Fatalf("did %q, %q, calls:\n%s\nwant %q, %q, calls:\n%s", did, failure, strings.Join(api.calls, "\n--\n"), row.did, row.err, strings.Join(row.calls, "\n--\n"))
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
