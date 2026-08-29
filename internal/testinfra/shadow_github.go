package testinfra

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ghShadowClient interface {
	JSON(context.Context, string, any) error
	Bytes(context.Context, string) ([]byte, error)
}

type commandGHClient struct{}

func (commandGHClient) JSON(ctx context.Context, endpoint string, value any) error {
	body, err := (commandGHClient{}).Bytes(ctx, endpoint)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, value); err != nil {
		return fmt.Errorf("decode GitHub API response for %s: %w", endpoint, err)
	}
	return nil
}

func (commandGHClient) Bytes(ctx context.Context, endpoint string) ([]byte, error) {
	// Endpoint shapes are assembled exclusively by this read-only collector.
	//nolint:gosec
	command := exec.CommandContext(ctx, "gh", "api", endpoint)
	body, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("read GitHub API %s: %w", endpoint, err)
	}
	return body, nil
}

func CollectLiveShadowSnapshot(ctx context.Context, repository string, pullRequest int) (ShadowSnapshot, error) {
	return collectShadowSnapshot(ctx, commandGHClient{}, repository, pullRequest)
}

func collectShadowSnapshot(ctx context.Context, client ghShadowClient, repository string, pullRequest int) (ShadowSnapshot, error) {
	var pull struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			SHA string `json:"sha"`
		} `json:"base"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/pulls/%d", repository, pullRequest), &pull); err != nil {
		return ShadowSnapshot{}, err
	}
	var runList struct {
		Runs []githubWorkflowRun `json:"workflow_runs"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/runs?event=pull_request&head_sha=%s&per_page=100", repository, pull.Head.SHA), &runList); err != nil {
		return ShadowSnapshot{}, err
	}
	legacyRun, err := selectWorkflowRun(runList.Runs, "engine-ci")
	if err != nil {
		return ShadowSnapshot{}, err
	}
	shadowRun, err := selectWorkflowRun(runList.Runs, "Pull Request Verification")
	if err != nil {
		return ShadowSnapshot{}, err
	}
	legacyExecution, err := collectWorkflowExecution(ctx, client, repository, legacyRun)
	if err != nil {
		return ShadowSnapshot{}, err
	}
	shadowExecution, err := collectWorkflowExecution(ctx, client, repository, shadowRun)
	if err != nil {
		return ShadowSnapshot{}, err
	}
	legacyMetadata, _, err := collectRunArtifacts(ctx, client, repository, legacyRun.ID)
	if err != nil {
		return ShadowSnapshot{}, err
	}
	_, shadowResults, err := collectRunArtifacts(ctx, client, repository, shadowRun.ID)
	if err != nil {
		return ShadowSnapshot{}, err
	}
	if len(legacyMetadata) == 0 || len(shadowResults) == 0 {
		return ShadowSnapshot{}, fmt.Errorf("shadow collection is missing legacy metadata or new results")
	}
	legacyExecution.WorkflowSourceSHA = legacyMetadata[0].WorkflowSourceSHA
	shadowExecution.WorkflowSourceSHA = shadowResults[0].Execution.WorkflowSourceSHA
	legacyExecution.WorkflowBlobSHA, err = readWorkflowBlobSHA(ctx, client, repository, legacyExecution.Path, legacyExecution.WorkflowSourceSHA)
	if err != nil {
		return ShadowSnapshot{}, err
	}
	shadowExecution.WorkflowBlobSHA, err = readWorkflowBlobSHA(ctx, client, repository, shadowExecution.Path, shadowExecution.WorkflowSourceSHA)
	if err != nil {
		return ShadowSnapshot{}, err
	}
	snapshot := ShadowSnapshot{
		Repository: repository, PullRequest: pullRequest, EventHeadSHA: pull.Head.SHA, EventBaseSHA: pull.Base.SHA,
		TestMergeSHA: shadowResults[0].Execution.TestMergeSHA, CollectedAt: time.Now().UTC(),
		Legacy: legacyExecution, Shadow: shadowExecution, LegacyMetadata: legacyMetadata, ShadowResults: shadowResults,
	}
	return snapshot, nil
}

func readWorkflowBlobSHA(ctx context.Context, client ghShadowClient, repository, path, sourceSHA string) (string, error) {
	path = strings.SplitN(path, "@", 2)[0]
	var content struct {
		SHA string `json:"sha"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/contents/%s?ref=%s", repository, path, sourceSHA), &content); err != nil {
		return "", err
	}
	if content.SHA == "" {
		return "", fmt.Errorf("workflow %s has no source blob identity", path)
	}
	return content.SHA, nil
}

type githubWorkflowRun struct {
	ID                 int64     `json:"id"`
	Name               string    `json:"name"`
	Path               string    `json:"path"`
	HeadSHA            string    `json:"head_sha"`
	RunAttempt         int       `json:"run_attempt"`
	CheckSuiteID       int64     `json:"check_suite_id"`
	Status             string    `json:"status"`
	Conclusion         string    `json:"conclusion"`
	CreatedAt          time.Time `json:"created_at"`
	RunStartedAt       time.Time `json:"run_started_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	PreviousAttemptURL *string   `json:"previous_attempt_url"`
}

func selectWorkflowRun(runs []githubWorkflowRun, name string) (githubWorkflowRun, error) {
	var selected githubWorkflowRun
	for _, run := range runs {
		if run.Name != name {
			continue
		}
		if selected.ID == 0 || run.ID > selected.ID || run.ID == selected.ID && run.RunAttempt > selected.RunAttempt {
			selected = run
		}
	}
	if selected.ID == 0 || selected.Status != "completed" || selected.Conclusion == "" {
		return githubWorkflowRun{}, fmt.Errorf("workflow %q has no completed run for the PR head", name)
	}
	return selected, nil
}

func collectWorkflowExecution(ctx context.Context, client ghShadowClient, repository string, run githubWorkflowRun) (ShadowWorkflowExecution, error) {
	var jobList struct {
		Jobs []ShadowJob `json:"jobs"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d/jobs?per_page=100", repository, run.ID, run.RunAttempt), &jobList); err != nil {
		return ShadowWorkflowExecution{}, err
	}
	var suite struct {
		HeadSHA string `json:"head_sha"`
		App     struct {
			ID int64 `json:"id"`
		} `json:"app"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/check-suites/%d", repository, run.CheckSuiteID), &suite); err != nil {
		return ShadowWorkflowExecution{}, err
	}
	var checkList struct {
		CheckRuns []struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			HeadSHA    string `json:"head_sha"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			App        struct {
				ID int64 `json:"id"`
			} `json:"app"`
		} `json:"check_runs"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/check-suites/%d/check-runs?per_page=100", repository, run.CheckSuiteID), &checkList); err != nil {
		return ShadowWorkflowExecution{}, err
	}
	checks := make([]ShadowCheck, 0, len(checkList.CheckRuns))
	for _, item := range checkList.CheckRuns {
		checks = append(checks, ShadowCheck{ID: item.ID, Name: item.Name, HeadSHA: item.HeadSHA, AppID: item.App.ID, Status: item.Status, Conclusion: item.Conclusion})
	}
	rerunOf := int64(0)
	if run.RunAttempt > 1 && run.PreviousAttemptURL != nil {
		rerunOf = run.ID
	}
	return ShadowWorkflowExecution{
		Name: run.Name, Path: run.Path, RunID: run.ID, RunAttempt: run.RunAttempt, RerunOf: rerunOf,
		CheckSuiteID: run.CheckSuiteID, CheckHeadSHA: suite.HeadSHA, SourceAppID: suite.App.ID,
		CreatedAt: run.CreatedAt, StartedAt: run.RunStartedAt, CompletedAt: run.UpdatedAt, Jobs: jobList.Jobs, Checks: checks,
	}, nil
}

func collectRunArtifacts(ctx context.Context, client ghShadowClient, repository string, runID int64) ([]LegacyWorkflowMetadata, []Result, error) {
	var list struct {
		Artifacts []struct {
			ID      int64  `json:"id"`
			Name    string `json:"name"`
			Expired bool   `json:"expired"`
		} `json:"artifacts"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/runs/%d/artifacts?per_page=100", repository, runID), &list); err != nil {
		return nil, nil, err
	}
	var metadata []LegacyWorkflowMetadata
	var results []Result
	for _, artifact := range list.Artifacts {
		if artifact.Expired || !strings.HasPrefix(artifact.Name, "legacy-metadata-") && !strings.HasPrefix(artifact.Name, "pr-evidence-") {
			continue
		}
		body, err := client.Bytes(ctx, fmt.Sprintf("repos/%s/actions/artifacts/%d/zip", repository, artifact.ID))
		if err != nil {
			return nil, nil, err
		}
		archive, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return nil, nil, fmt.Errorf("decode artifact %d: %w", artifact.ID, err)
		}
		for _, file := range archive.File {
			if !strings.HasSuffix(file.Name, "metadata.json") && !strings.HasSuffix(file.Name, "result.json") {
				continue
			}
			reader, err := file.Open()
			if err != nil {
				return nil, nil, err
			}
			content, readErr := io.ReadAll(io.LimitReader(reader, 16<<20))
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil {
				return nil, nil, fmt.Errorf("read artifact %d", artifact.ID)
			}
			if strings.HasSuffix(file.Name, "metadata.json") {
				var item LegacyWorkflowMetadata
				if err := json.Unmarshal(content, &item); err != nil {
					return nil, nil, fmt.Errorf("decode legacy metadata: %w", err)
				}
				metadata = append(metadata, item)
			} else {
				var item Result
				if err := json.Unmarshal(content, &item); err != nil {
					return nil, nil, fmt.Errorf("decode shadow result: %w", err)
				}
				results = append(results, item)
			}
		}
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].Producer < metadata[j].Producer })
	sort.Slice(results, func(i, j int) bool { return results[i].Execution.Producer < results[j].Execution.Producer })
	return metadata, results, nil
}

func ParseRunID(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}
