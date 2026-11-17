package ciflow

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const ScheduledRedLabel = "scheduled-check-red"

type ScheduledJob struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
}

type ScheduledIssue struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
}

type ScheduledAnnotation struct {
	Message string `json:"message"`
}

type ScheduledIssueAPI interface {
	Jobs() ([]ScheduledJob, error)
	JobLog(id int64) (string, error)
	Annotations(id int64) ([]ScheduledAnnotation, error)
	OpenIssues(label string) ([]ScheduledIssue, error)
	EnsureLabel(name string) error
	CreateIssue(title, body, label string) error
	Comment(number int, body string) error
	CloseIssue(number int) error
}

type ScheduledRun struct {
	Workflow, URL, Commit string
	Schedule              string
	Reporter              string
	DeclaredTitles        []string
}

func ScheduledIssueTitle(workflow, schedule string) string {
	return scheduledIssueTitlePrefix + workflow + " (" + schedule + ")"
}

const scheduledIssueTitlePrefix = "Scheduled check red on main: "

// ReportScheduledRun opens, comments on or closes the issue of the run's
// schedule and returns which. Every run also closes the open issues of
// schedules that no workflow declares any more.
func ReportScheduledRun(api ScheduledIssueAPI, run ScheduledRun) (string, error) {
	title := ScheduledIssueTitle(run.Workflow, run.Schedule)
	if !slices.Contains(run.DeclaredTitles, title) {
		return "", fmt.Errorf("the repository's workflows declare no schedule titled %q", title)
	}
	jobs, err := api.Jobs()
	if err != nil {
		return "", fmt.Errorf("list the run's jobs: %w", err)
	}
	var failed []string
	ran, cancelled := 0, false
	for _, job := range jobs {
		switch {
		case job.Name == run.Reporter, job.Conclusion == "skipped":
		case job.Conclusion == "success":
			ran++
		case job.Conclusion == "failure" || job.Conclusion == "timed_out":
			cause := "its log could not be read: "
			if log, err := api.JobLog(job.ID); err != nil {
				cause += oneLine(err.Error())
			} else {
				cause = JobCause(log)
			}
			failed = append(failed, job.Name+": "+cause)
		case job.Conclusion == "cancelled":
			annotations, err := api.Annotations(job.ID)
			if err != nil {
				// Without its annotations a time limit cannot be told from a
				// cancel, and a red run must not go unreported.
				failed = append(failed, job.Name+": cancelled, and its annotations could not be read: "+oneLine(err.Error()))
				continue
			}
			if limit := timeLimitAnnotation(annotations); limit != "" {
				failed = append(failed, job.Name+": "+limit)
				continue
			}
			cancelled = true
		default:
			cancelled = true
		}
	}
	open, err := api.OpenIssues(ScheduledRedLabel)
	if err != nil {
		return "", fmt.Errorf("list the open issues: %w", err)
	}
	number := 0
	for _, issue := range open {
		switch {
		case issue.Title == title:
			number = issue.Number
		case strings.HasPrefix(issue.Title, scheduledIssueTitlePrefix) && !slices.Contains(run.DeclaredTitles, issue.Title):
			if err := api.Comment(issue.Number, "No workflow of this repository declares this schedule any more, so no run will report on it.\n"); err != nil {
				return "", err
			}
			if err := api.CloseIssue(issue.Number); err != nil {
				return "", err
			}
		}
	}
	switch {
	case len(failed) > 0:
		body := fmt.Sprintf("Scheduled run %s of %s on commit %s failed.\n", run.URL, run.Workflow, run.Commit)
		for _, line := range failed {
			body += "\n- " + line
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

// GitHub's annotation on a job it ended at its timeout-minutes; the job's log
// holds only the line a manual cancel also writes.
const timeLimitMessage = "The job has exceeded the maximum execution time"

func timeLimitAnnotation(annotations []ScheduledAnnotation) string {
	for _, annotation := range annotations {
		if strings.HasPrefix(annotation.Message, timeLimitMessage) {
			return "`" + strings.ReplaceAll(oneLine(annotation.Message), "`", "'") + "`"
		}
	}
	return ""
}

func oneLine(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

var (
	logTimestamp  = regexp.MustCompile(`^\d{4}-\d\d-\d\dT[\d:.]+Z `)
	failedGoTest  = regexp.MustCompile(`--- FAIL: (\S+)`)
	failedBunTest = regexp.MustCompile(`^\(fail\) (.+?)(?: \[[\d.]+m?s\])?$`)
	excerptPrefix = regexp.MustCompile(`^\|\s?`)
	// The IDs the advisory producers report: govulncheck's GO-, bun audit's
	// GHSA- and cargo-deny's RUSTSEC-. cargo-deny also names a CVE in an
	// advisory's title and announcement; the advisory's own ID is the cause.
	advisory       = regexp.MustCompile(`\b(?:GO-\d{4}-\d{4,}|GHSA(?:-[2-9cfghjmpqrvwx]{4}){3}|RUSTSEC-\d{4}-\d{4})\b`)
	apparatusCause = regexp.MustCompile(`verification apparatus-failed: (.+)`)
)

const maxCauses = 20

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
