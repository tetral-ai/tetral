package ciflow

import (
	"fmt"
	"regexp"
	"strings"
)

// ScheduledRedLabel marks the one open issue per workflow and schedule that
// says that schedule's run on main is red.
const ScheduledRedLabel = "scheduled-check-red"

// ScheduledJob is a job of a scheduled run as the GitHub API lists it.
type ScheduledJob struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

// ScheduledIssue is an open issue as the GitHub API lists it.
type ScheduledIssue struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
}

// ScheduledIssueAPI is what the report reads from and writes to GitHub.
type ScheduledIssueAPI interface {
	// Jobs lists the jobs of the run's attempt, the reporting job included.
	Jobs() ([]ScheduledJob, error)
	JobLog(id int64) (string, error)
	OpenIssues(label string) ([]ScheduledIssue, error)
	EnsureLabel(name string) error
	CreateIssue(title, body, label string) error
	Comment(number int, body string) error
	CloseIssue(number int) error
}

// ScheduledRun names the run the report is about.
type ScheduledRun struct {
	Workflow, URL, Commit string
	// Schedule is the cron expression that started the run. A workflow's
	// schedules can run different jobs, so each has its own issue.
	Schedule string
	// Reporter is the reporting job's own name, left out of the jobs it reads.
	Reporter string
}

// ScheduledIssueTitle is the exact title of the issue of a workflow's
// schedule.
func ScheduledIssueTitle(workflow, schedule string) string {
	return "Scheduled check red on main: " + workflow + " (" + schedule + ")"
}

// ReportScheduledRun keeps the issue of a workflow's schedule in step with
// its scheduled run: a run with a failed job opens the issue or comments on the open one;
// a run whose jobs all succeeded comments on the open issue and closes it. A
// skipped job did not run (each schedule runs only its own jobs) and counts
// for neither; a run with a cancelled job and none failed changes nothing,
// and so does a green run with no open issue. It returns what it did.
func ReportScheduledRun(api ScheduledIssueAPI, run ScheduledRun) (string, error) {
	jobs, err := api.Jobs()
	if err != nil {
		return "", fmt.Errorf("list the run's jobs: %w", err)
	}
	var failed []ScheduledJob
	ran, cancelled := 0, false
	for _, job := range jobs {
		switch {
		case job.Name == run.Reporter, job.Conclusion == "skipped":
		case job.Conclusion == "success":
			ran++
		case job.Conclusion == "failure" || job.Conclusion == "timed_out":
			failed = append(failed, job)
		default:
			cancelled = true
		}
	}
	title := ScheduledIssueTitle(run.Workflow, run.Schedule)
	open, err := api.OpenIssues(ScheduledRedLabel)
	if err != nil {
		return "", fmt.Errorf("list the open issues: %w", err)
	}
	number := 0
	for _, issue := range open {
		if issue.Title == title {
			number = issue.Number
			break
		}
	}
	switch {
	case len(failed) > 0:
		body := fmt.Sprintf("Scheduled run %s of %s on commit %s failed.\n", run.URL, run.Workflow, run.Commit)
		for _, job := range failed {
			log, err := api.JobLog(job.ID)
			if err != nil {
				return "", fmt.Errorf("read the log of job %s: %w", job.Name, err)
			}
			body += fmt.Sprintf("\n- %s: %s", job.Name, JobCause(log))
		}
		body += "\n"
		if number != 0 {
			return "commented", api.Comment(number, body)
		}
		if err := api.EnsureLabel(ScheduledRedLabel); err != nil {
			return "", fmt.Errorf("create the label: %w", err)
		}
		return "created", api.CreateIssue(title, body, ScheduledRedLabel)
	case cancelled || ran == 0 || number == 0:
		return "none", nil
	default:
		if err := api.Comment(number, fmt.Sprintf("Scheduled run %s of %s on commit %s succeeded.\n", run.URL, run.Workflow, run.Commit)); err != nil {
			return "", err
		}
		return "closed", api.CloseIssue(number)
	}
}

var (
	logTimestamp  = regexp.MustCompile(`^\d{4}-\d\d-\d\dT[\d:.]+Z `)
	failedGoTest  = regexp.MustCompile(`--- FAIL: (\S+)`)
	failedBunTest = regexp.MustCompile(`^\(fail\) (.+?)(?: \[[\d.]+m?s\])?$`)
	// The runner prints a failed step's diagnostic lines behind this prefix.
	excerptPrefix = regexp.MustCompile(`^\|\s?`)
	// The IDs the advisory producers report: govulncheck's GO-, bun audit's
	// GHSA- and cargo-deny's RUSTSEC-. cargo-deny also names a CVE in an
	// advisory's title and announcement; the advisory's own ID is the cause.
	advisory       = regexp.MustCompile(`\b(?:GO-\d{4}-\d{4,}|GHSA(?:-[2-9cfghjmpqrvwx]{4}){3}|RUSTSEC-\d{4}-\d{4})\b`)
	apparatusCause = regexp.MustCompile(`verification apparatus-failed: (.+)`)
)

const maxCauses = 20

// JobCause is what a failed job's log names as its cause: the advisories it
// reports, the tests that failed, or the error of a run that failed before
// its tests; "no cause recorded" when the log names none of these.
func JobCause(log string) string {
	var causes []string
	seen := map[string]bool{}
	add := func(cause string) {
		cause = strings.ReplaceAll(strings.TrimSpace(cause), "`", "'")
		if cause != "" && !seen[cause] {
			seen[cause] = true
			causes = append(causes, cause)
		}
	}
	for _, line := range strings.Split(log, "\n") {
		line = logTimestamp.ReplaceAllString(strings.TrimRight(line, "\r"), "")
		for _, id := range advisory.FindAllString(line, -1) {
			add(id)
		}
		if match := failedGoTest.FindStringSubmatch(line); match != nil {
			add(match[1])
		}
		if match := failedBunTest.FindStringSubmatch(excerptPrefix.ReplaceAllString(strings.TrimSpace(line), "")); match != nil {
			add(match[1])
		}
		if match := apparatusCause.FindStringSubmatch(line); match != nil {
			add(match[1])
		}
	}
	if len(causes) == 0 {
		return "no cause recorded"
	}
	more := ""
	if len(causes) > maxCauses {
		more = fmt.Sprintf(" and %d more", len(causes)-maxCauses)
		causes = causes[:maxCauses]
	}
	return "`" + strings.Join(causes, "`, `") + "`" + more
}
