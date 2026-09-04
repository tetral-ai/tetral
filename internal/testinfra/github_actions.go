package testinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

const githubActionsAppID int64 = 15368

type githubAPIClient interface {
	JSON(context.Context, string, any) error
}

type commandGitHubClient struct{}

func (commandGitHubClient) JSON(ctx context.Context, endpoint string, value any) error {
	// Endpoints are assembled by the read-only CI gate and health collectors.
	//nolint:gosec
	body, err := exec.CommandContext(ctx, "gh", "api", endpoint).Output()
	if err != nil {
		return fmt.Errorf("read GitHub API %s: %w", endpoint, err)
	}
	if err := json.Unmarshal(body, value); err != nil {
		return fmt.Errorf("decode GitHub API response for %s: %w", endpoint, err)
	}
	return nil
}

type githubWorkflowRun struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Path         string    `json:"path"`
	HeadSHA      string    `json:"head_sha"`
	RunAttempt   int       `json:"run_attempt"`
	CheckSuiteID int64     `json:"check_suite_id"`
	CreatedAt    time.Time `json:"created_at"`
	RunStartedAt time.Time `json:"run_started_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type workflowJob struct {
	ID          int64             `json:"id"`
	Name        string            `json:"name"`
	Status      string            `json:"status"`
	Conclusion  string            `json:"conclusion"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	Steps       []workflowJobStep `json:"steps"`
}

type workflowJobStep struct {
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type workflowCheck struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	HeadSHA    string `json:"head_sha"`
	AppID      int64  `json:"app_id"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}
