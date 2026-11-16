package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/testinfra/ciflow"
)

// ghIssueAPI reads and writes GitHub through gh, which takes the repository
// from GH_REPO and the token from GH_TOKEN, and reads job logs with both.
type ghIssueAPI struct {
	run     string
	attempt int
}

func (a ghIssueAPI) Jobs() ([]ciflow.ScheduledJob, error) {
	body, err := readAPI(fmt.Sprintf("repos/{owner}/{repo}/actions/runs/%s/attempts/%d/jobs?per_page=100", a.run, a.attempt))
	if err != nil {
		return nil, err
	}
	var listed struct {
		Jobs []ciflow.ScheduledJob `json:"jobs"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, err
	}
	return listed.Jobs, nil
}

// JobLog reads the log over HTTP, not through gh: gh refuses to print a
// response with terminal escape sequences, which a job's log holds, unless
// given a flag older releases do not know.
func (ghIssueAPI) JobLog(id int64) (string, error) {
	api := os.Getenv("GITHUB_API_URL")
	if api == "" {
		api = "https://api.github.com"
	}
	// The address is the workflow's own API and repository, set by the runner.
	//nolint:gosec
	request, err := http.NewRequest(http.MethodGet, api+"/repos/"+os.Getenv("GH_REPO")+"/actions/jobs/"+strconv.FormatInt(id, 10)+"/logs", nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+os.Getenv("GH_TOKEN"))
	request.Header.Set("Accept", "application/vnd.github+json")
	// The API answers with a redirect to the log's storage, which the client
	// follows without the token.
	//nolint:gosec
	response, err := (&http.Client{Timeout: time.Minute}).Do(request)
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("read the log of job %d: %s", id, response.Status)
	}
	return string(body), nil
}

func (ghIssueAPI) OpenIssues(label string) ([]ciflow.ScheduledIssue, error) {
	body, err := readAPI("repos/{owner}/{repo}/issues?state=open&per_page=100&labels=" + url.QueryEscape(label))
	if err != nil {
		return nil, err
	}
	var listed []struct {
		ciflow.ScheduledIssue
		PullRequest json.RawMessage `json:"pull_request"`
	}
	if err := json.Unmarshal(body, &listed); err != nil {
		return nil, err
	}
	var issues []ciflow.ScheduledIssue
	for _, issue := range listed {
		if issue.PullRequest == nil {
			issues = append(issues, issue.ScheduledIssue)
		}
	}
	return issues, nil
}

func (ghIssueAPI) EnsureLabel(name string) error {
	if _, err := readAPI("repos/{owner}/{repo}/labels/" + url.PathEscape(name)); err == nil {
		return nil
	}
	return writeAPI("POST", "repos/{owner}/{repo}/labels", map[string]string{"name": name, "color": "b60205", "description": "A scheduled workflow is red on main"})
}

func (ghIssueAPI) CreateIssue(title, body, label string) error {
	return writeAPI("POST", "repos/{owner}/{repo}/issues", map[string]any{"title": title, "body": body, "labels": []string{label}})
}

func (ghIssueAPI) Comment(number int, body string) error {
	return writeAPI("POST", fmt.Sprintf("repos/{owner}/{repo}/issues/%d/comments", number), map[string]string{"body": body})
}

func (ghIssueAPI) CloseIssue(number int) error {
	return writeAPI("PATCH", fmt.Sprintf("repos/{owner}/{repo}/issues/%d", number), map[string]string{"state": "closed", "state_reason": "completed"})
}

func writeAPI(method, path string, fields any) error {
	body, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	//nolint:gosec // The method and path are this file's own.
	command := exec.Command("gh", "api", "--method", method, path, "--input", "-")
	command.Stdin = bytes.NewReader(body)
	command.Stdout, command.Stderr = nil, os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	return nil
}
