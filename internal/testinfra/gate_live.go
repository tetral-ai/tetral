package testinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

type GateLiveFacts struct {
	CurrentHeadSHA string
	CurrentBaseSHA string
	RunID          int64
	RunAttempt     int
	RunName        string
	RunPath        string
	RunHeadSHA     string
	CheckSuiteID   int64
	CheckHeadSHA   string
	SourceAppID    int64
	GateChecks     []ShadowCheck
}

func VerifyUpstreamNeeds(body string, expectedJobs []string) error {
	var needs map[string]struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal([]byte(body), &needs); err != nil {
		return fmt.Errorf("decode Merge Gate dependencies: %w", err)
	}
	if len(needs) != len(expectedJobs) {
		return fmt.Errorf("merge gate dependency result set is incomplete")
	}
	for _, job := range expectedJobs {
		result, exists := needs[job]
		if !exists || result.Result != "success" {
			return fmt.Errorf("upstream job %q concluded %q", job, result.Result)
		}
	}
	return nil
}

func PRUpstreamJobs() []string {
	return []string{
		"repository-integrity", "go-static-analysis", "go-race", "agent-runtime", "provider-gateway",
		"protocol-sdk", "deployment-definitions", "sandbox-image", "dependency-security",
	}
}

func ReadLiveGateFacts(ctx context.Context, repository string, pullRequest int, runID int64, runAttempt int) (GateLiveFacts, error) {
	return readLiveGateFacts(ctx, commandGHClient{}, repository, pullRequest, runID, runAttempt)
}

func readLiveGateFacts(ctx context.Context, client ghShadowClient, repository string, pullRequest int, runID int64, runAttempt int) (GateLiveFacts, error) {
	var pull struct {
		Head struct {
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			SHA string `json:"sha"`
		} `json:"base"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/pulls/%d", repository, pullRequest), &pull); err != nil {
		return GateLiveFacts{}, err
	}
	var run githubWorkflowRun
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/actions/runs/%d/attempts/%d", repository, runID, runAttempt), &run); err != nil {
		return GateLiveFacts{}, err
	}
	var suite struct {
		HeadSHA string `json:"head_sha"`
		App     struct {
			ID int64 `json:"id"`
		} `json:"app"`
	}
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/check-suites/%d", repository, run.CheckSuiteID), &suite); err != nil {
		return GateLiveFacts{}, err
	}
	var list struct {
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
	if err := client.JSON(ctx, fmt.Sprintf("repos/%s/check-suites/%d/check-runs?per_page=100", repository, run.CheckSuiteID), &list); err != nil {
		return GateLiveFacts{}, err
	}
	var gateChecks []ShadowCheck
	for _, check := range list.CheckRuns {
		if check.Name == "Merge Gate" {
			gateChecks = append(gateChecks, ShadowCheck{ID: check.ID, Name: check.Name, HeadSHA: check.HeadSHA, AppID: check.App.ID, Status: check.Status, Conclusion: check.Conclusion})
		}
	}
	return GateLiveFacts{
		CurrentHeadSHA: pull.Head.SHA, CurrentBaseSHA: pull.Base.SHA,
		RunID: run.ID, RunAttempt: run.RunAttempt, RunName: run.Name, RunPath: run.Path, RunHeadSHA: run.HeadSHA,
		CheckSuiteID: run.CheckSuiteID, CheckHeadSHA: suite.HeadSHA, SourceAppID: suite.App.ID, GateChecks: gateChecks,
	}, nil
}

func VerifyLiveGateFacts(expectation GateExpectation, facts GateLiveFacts) error {
	runID, err := strconv.ParseInt(expectation.RunID, 10, 64)
	if err != nil {
		return fmt.Errorf("expected run ID is malformed")
	}
	attempt, err := strconv.Atoi(expectation.RunAttempt)
	if err != nil {
		return fmt.Errorf("expected run attempt is malformed")
	}
	if facts.CurrentHeadSHA != expectation.EventHeadSHA || facts.CurrentBaseSHA != expectation.EventBaseSHA {
		return fmt.Errorf("pull request head or base changed after this run started")
	}
	if facts.RunID != runID || facts.RunAttempt != attempt || facts.RunName != expectation.Workflow ||
		facts.RunPath != ".github/workflows/pull-request-verification.yml" || facts.RunHeadSHA != expectation.EventHeadSHA {
		return fmt.Errorf("current workflow run identity does not match")
	}
	if facts.CheckSuiteID <= 0 || facts.CheckHeadSHA != expectation.RequiredCheckSHA || facts.SourceAppID != githubActionsAppID {
		return fmt.Errorf("current check suite carrier does not match")
	}
	if len(facts.GateChecks) != 1 {
		return fmt.Errorf("merge gate check identity is missing or duplicated")
	}
	check := facts.GateChecks[0]
	if check.HeadSHA != expectation.RequiredCheckSHA || check.AppID != githubActionsAppID || check.Status != "in_progress" && check.Status != "completed" {
		return fmt.Errorf("merge gate check carrier does not match")
	}
	return nil
}
