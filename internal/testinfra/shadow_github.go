package testinfra

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
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
	return collectShadowSnapshot(ctx, commandGHClient{}, repository, pullRequest, ShadowCollectionOptions{})
}

type ShadowCollectionOptions struct {
	ForkPendingCapture []byte
	AgreedIssueNumber  int
	AgreementCommentID int64
}

type githubPullSnapshot struct {
	Head struct {
		SHA  string `json:"sha"`
		Repo struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
	Base struct {
		SHA string `json:"sha"`
	} `json:"base"`
	AuthorAssociation string     `json:"author_association"`
	ChangedFiles      int        `json:"changed_files"`
	State             string     `json:"state"`
	ClosedAt          time.Time  `json:"closed_at"`
	MergedAt          *time.Time `json:"merged_at"`
}

func CollectLiveShadowSnapshotWithOptions(ctx context.Context, repository string, pullRequest int, options ShadowCollectionOptions) (ShadowSnapshot, error) {
	return collectShadowSnapshot(ctx, commandGHClient{}, repository, pullRequest, options)
}

func collectShadowSnapshot(ctx context.Context, client ghShadowClient, repository string, pullRequest int, options ShadowCollectionOptions) (ShadowSnapshot, error) {
	var pull githubPullSnapshot
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/pulls/%d", repository, pullRequest), &pull); err != nil {
		return ShadowSnapshot{}, err
	}
	var files []struct {
		Filename string `json:"filename"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/pulls/%d/files?per_page=100", repository, pullRequest), &files); err != nil {
		return ShadowSnapshot{}, err
	}
	if pull.ChangedFiles != len(files) {
		return ShadowSnapshot{}, fmt.Errorf("pull request changed-file capture is incomplete: got %d of %d", len(files), pull.ChangedFiles)
	}
	changedPaths := make([]string, 0, len(files))
	for _, file := range files {
		changedPaths = append(changedPaths, file.Filename)
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
		Repository: repository, PullRequest: pullRequest, HeadRepository: pull.Head.Repo.FullName,
		AuthorAssociation: pull.AuthorAssociation, ChangedPaths: changedPaths,
		EventHeadSHA: pull.Head.SHA, EventBaseSHA: pull.Base.SHA,
		TestMergeSHA: shadowResults[0].Execution.TestMergeSHA, CollectedAt: time.Now().UTC(),
		Legacy: legacyExecution, Shadow: shadowExecution, LegacyMetadata: legacyMetadata, ShadowResults: shadowResults,
	}
	if pull.Head.Repo.FullName != repository {
		snapshot.ForkApproval, err = collectForkApproval(ctx, client, repository, pullRequest, pull, shadowRun, options)
		if err != nil {
			return ShadowSnapshot{}, err
		}
	}
	return snapshot, nil
}

func collectForkApproval(ctx context.Context, client ghShadowClient, repository string, pullRequest int, pull githubPullSnapshot, shadowRun githubWorkflowRun, options ShadowCollectionOptions) (*ShadowForkApproval, error) {
	if len(options.ForkPendingCapture) == 0 || options.AgreedIssueNumber <= 0 || options.AgreementCommentID <= 0 {
		return nil, fmt.Errorf("external fork collection requires pending-run, agreed-Issue, and agreement-comment evidence")
	}
	var pending githubWorkflowRun
	if err := json.Unmarshal(options.ForkPendingCapture, &pending); err != nil {
		return nil, fmt.Errorf("decode fork pending-run capture: %w", err)
	}
	if pending.ID != shadowRun.ID || pending.HeadSHA != pull.Head.SHA || pending.RunAttempt != shadowRun.RunAttempt || pending.Status != "action_required" || pending.Conclusion != "" || pending.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("fork pending-run capture does not match the exact shadow run")
	}
	var approvals []struct {
		State string `json:"state"`
		User  struct {
			ID int64 `json:"id"`
		} `json:"user"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/runs/%d/approvals", repository, shadowRun.ID), &approvals); err != nil {
		return nil, err
	}
	var approvalActorID int64
	for _, approval := range approvals {
		if approval.State == "approved" && approval.User.ID > 0 {
			approvalActorID = approval.User.ID
			break
		}
	}
	if approvalActorID == 0 {
		return nil, fmt.Errorf("fork workflow has no approved review-history entry")
	}
	var issue struct {
		Number int    `json:"number"`
		State  string `json:"state"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/issues/%d", repository, options.AgreedIssueNumber), &issue); err != nil {
		return nil, err
	}
	var comment struct {
		ID                int64     `json:"id"`
		IssueURL          string    `json:"issue_url"`
		AuthorAssociation string    `json:"author_association"`
		Body              string    `json:"body"`
		CreatedAt         time.Time `json:"created_at"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/issues/comments/%d", repository, options.AgreementCommentID), &comment); err != nil {
		return nil, err
	}
	expectedIssueURL := fmt.Sprintf("https://api.github.com/repos/%s/issues/%d", repository, options.AgreedIssueNumber)
	if issue.Number != options.AgreedIssueNumber || comment.ID != options.AgreementCommentID || comment.IssueURL != expectedIssueURL ||
		strings.TrimSpace(comment.Body) == "" || !maintainerAssociation(comment.AuthorAssociation) || comment.CreatedAt.IsZero() {
		return nil, fmt.Errorf("fork Issue agreement evidence is incomplete")
	}
	cleanupState := ""
	if pull.State == "closed" && !pull.ClosedAt.IsZero() {
		cleanupState = "closed"
		if pull.MergedAt != nil {
			cleanupState = "merged"
		}
	}
	if cleanupState == "" || !pull.ClosedAt.After(pending.UpdatedAt) {
		return nil, fmt.Errorf("fork pull request cleanup is not complete")
	}
	approvalBody, _ := json.Marshal(approvals)
	agreementBody, _ := json.Marshal(struct {
		Issue   any `json:"issue"`
		Comment any `json:"comment"`
	}{Issue: issue, Comment: comment})
	cleanupBody, _ := json.Marshal(struct {
		PullRequest int       `json:"pull_request"`
		HeadSHA     string    `json:"head_sha"`
		State       string    `json:"state"`
		ClosedAt    time.Time `json:"closed_at"`
	}{PullRequest: pullRequest, HeadSHA: pull.Head.SHA, State: cleanupState, ClosedAt: pull.ClosedAt})
	return &ShadowForkApproval{
		HeadSHA: pull.Head.SHA, RunID: shadowRun.ID, PendingStatus: pending.Status, PendingObservedAt: pending.UpdatedAt,
		PendingCaptureSHA256: digestBytes(options.ForkPendingCapture), ApprovalState: "approved", ApprovalActorID: approvalActorID,
		ApprovalCaptureSHA256: digestBytes(approvalBody), AgreedIssueNumber: issue.Number, AgreementCommentID: comment.ID,
		AgreementCaptureSHA256: digestBytes(agreementBody), CleanupState: cleanupState, CleanupObservedAt: pull.ClosedAt,
		CleanupCaptureSHA256: digestBytes(cleanupBody),
	}, nil
}

func maintainerAssociation(value string) bool {
	return value == "OWNER" || value == "MEMBER" || value == "COLLABORATOR"
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest[:])
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
