package testinfra

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const githubActionsAppID int64 = 15368

type ShadowSnapshot struct {
	Repository        string                   `json:"repository"`
	PullRequest       int                      `json:"pull_request"`
	HeadRepository    string                   `json:"head_repository"`
	AuthorAssociation string                   `json:"author_association"`
	ChangedPaths      []string                 `json:"changed_paths"`
	ForkApproval      *ShadowForkApproval      `json:"fork_approval,omitempty"`
	EventHeadSHA      string                   `json:"event_head_sha"`
	EventBaseSHA      string                   `json:"event_base_sha"`
	TestMergeSHA      string                   `json:"test_merge_sha"`
	CollectedAt       time.Time                `json:"collected_at"`
	Legacy            ShadowWorkflowExecution  `json:"legacy"`
	Shadow            ShadowWorkflowExecution  `json:"shadow"`
	LegacyMetadata    []LegacyWorkflowMetadata `json:"legacy_metadata"`
	ShadowResults     []Result                 `json:"shadow_results"`
}

type ShadowForkApproval struct {
	PendingObservedAt time.Time `json:"pending_observed_at"`
	ApprovedAt        time.Time `json:"approved_at"`
}

type ShadowWorkflowExecution struct {
	Name              string        `json:"name"`
	Path              string        `json:"path"`
	RunID             int64         `json:"run_id"`
	RunAttempt        int           `json:"run_attempt"`
	RerunOf           int64         `json:"rerun_of,omitempty"`
	CheckSuiteID      int64         `json:"check_suite_id"`
	CheckHeadSHA      string        `json:"check_head_sha"`
	SourceAppID       int64         `json:"source_app_id"`
	WorkflowSourceSHA string        `json:"workflow_source_sha"`
	WorkflowBlobSHA   string        `json:"workflow_blob_sha"`
	CreatedAt         time.Time     `json:"created_at"`
	StartedAt         time.Time     `json:"started_at"`
	CompletedAt       time.Time     `json:"completed_at"`
	Jobs              []ShadowJob   `json:"jobs"`
	Checks            []ShadowCheck `json:"checks"`
}

type ShadowJob struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type ShadowCheck struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	HeadSHA    string `json:"head_sha"`
	AppID      int64  `json:"app_id"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

type LegacyWorkflowMetadata struct {
	Schema            string `json:"schema"`
	Repository        string `json:"repository"`
	Producer          string `json:"producer"`
	CheckedOutSHA     string `json:"checked_out_sha"`
	EventHeadSHA      string `json:"event_head_sha"`
	EventBaseSHA      string `json:"event_base_sha"`
	TestMergeSHA      string `json:"test_merge_sha"`
	WorkflowSourceSHA string `json:"workflow_source_sha"`
	Workflow          string `json:"workflow"`
	RunID             string `json:"run_id"`
	RunAttempt        string `json:"run_attempt"`
	Job               string `json:"job"`
	StartedAt         string `json:"started_at"`
	CompletedAt       string `json:"completed_at"`
}

type ShadowLedgerRow struct {
	Repository          string                  `json:"repository"`
	PullRequest         int                     `json:"pull_request"`
	HeadRepository      string                  `json:"head_repository"`
	AuthorAssociation   string                  `json:"author_association"`
	ChangeClasses       []string                `json:"change_classes"`
	ForkApproval        *ShadowForkApproval     `json:"fork_approval,omitempty"`
	EventHeadSHA        string                  `json:"event_head_sha"`
	EventBaseSHA        string                  `json:"event_base_sha"`
	TestMergeSHA        string                  `json:"test_merge_sha"`
	RequiredCheckSHA    string                  `json:"required_check_sha"`
	WorkflowSourceSHA   string                  `json:"workflow_source_sha"`
	LegacyRunID         int64                   `json:"legacy_run_id"`
	LegacyRunAttempt    int                     `json:"legacy_run_attempt"`
	ShadowRunID         int64                   `json:"shadow_run_id"`
	ShadowRunAttempt    int                     `json:"shadow_run_attempt"`
	LegacyDuration      time.Duration           `json:"legacy_duration_ns"`
	ShadowDuration      time.Duration           `json:"shadow_duration_ns"`
	LegacyQueueTime     time.Duration           `json:"legacy_queue_time_ns"`
	ShadowQueueTime     time.Duration           `json:"shadow_queue_time_ns"`
	LegacyRunnerMinutes float64                 `json:"legacy_runner_minutes"`
	ShadowRunnerMinutes float64                 `json:"shadow_runner_minutes"`
	LegacyConclusion    string                  `json:"legacy_conclusion"`
	ShadowConclusion    string                  `json:"shadow_conclusion"`
	LegacyExecution     ShadowWorkflowExecution `json:"legacy_execution"`
	ShadowExecution     ShadowWorkflowExecution `json:"shadow_execution"`
	ShadowResults       []Result                `json:"shadow_results"`
	SnapshotDigest      string                  `json:"snapshot_digest"`
	CollectedAt         time.Time               `json:"collected_at"`
}

var legacyShadowProducers = map[string]string{
	"go-static":                 "go-static",
	"go-test-0":                 "go-test (0)",
	"go-test-1":                 "go-test (1)",
	"go-test-2":                 "go-test (2)",
	"go-test-3":                 "go-test (3)",
	"protocol":                  "protocol",
	"agent-runtime-ts":          "agent-runtime-ts",
	"gateway-ts":                "gateway-ts",
	"k8s-manifests":             "k8s-manifests",
	"helm-chart":                "helm-chart",
	"security":                  "security",
	"sandbox-local-image-smoke": "sandbox-local-image-smoke",
}

var legacyProducerJobIDs = map[string]string{
	"go-static": "go-static", "go-test-0": "go-test", "go-test-1": "go-test",
	"go-test-2": "go-test", "go-test-3": "go-test", "protocol": "protocol",
	"agent-runtime-ts": "agent-runtime-ts", "gateway-ts": "gateway-ts",
	"k8s-manifests": "k8s-manifests", "helm-chart": "helm-chart", "security": "security",
	"sandbox-local-image-smoke": "sandbox-local-image-smoke",
}

var shadowProducerJobs = map[string]string{
	"repository":    "Repository Integrity",
	"go-static":     "Go Static Analysis",
	"go-0":          "Go Race (shard 0)",
	"go-1":          "Go Race (shard 1)",
	"go-2":          "Go Race (shard 2)",
	"go-3":          "Go Race (shard 3)",
	"runtime":       "Agent Runtime",
	"gateway":       "Provider Gateway",
	"protocol":      "Protocol and SDK Compatibility",
	"deployment":    "Deployment Definitions",
	"sandbox-image": "Sandbox Image",
	"security":      "Dependency Security",
}

func NormalizeShadowSnapshot(snapshot ShadowSnapshot) (ShadowLedgerRow, error) {
	if snapshot.Repository == "" || snapshot.PullRequest <= 0 || snapshot.HeadRepository == "" || snapshot.AuthorAssociation == "" ||
		len(snapshot.ChangedPaths) == 0 || snapshot.EventHeadSHA == "" || snapshot.EventBaseSHA == "" || snapshot.TestMergeSHA == "" {
		return ShadowLedgerRow{}, fmt.Errorf("shadow snapshot has an incomplete PR revision tuple")
	}
	if snapshot.HeadRepository != snapshot.Repository && !validForkApproval(snapshot.ForkApproval) {
		return ShadowLedgerRow{}, fmt.Errorf("external fork snapshot lacks pending and approval evidence")
	}
	if snapshot.Legacy.Name != "engine-ci" || snapshot.Legacy.Path != ".github/workflows/engine-ci.yml" ||
		snapshot.Shadow.Name != "Pull Request Verification" || snapshot.Shadow.Path != ".github/workflows/pull-request-verification.yml" {
		return ShadowLedgerRow{}, fmt.Errorf("shadow snapshot has missing legacy or new workflow")
	}
	if err := validateShadowExecution(snapshot.Legacy, snapshot.TestMergeSHA); err != nil {
		return ShadowLedgerRow{}, fmt.Errorf("legacy execution: %w", err)
	}
	if err := validateShadowExecution(snapshot.Shadow, snapshot.TestMergeSHA); err != nil {
		return ShadowLedgerRow{}, fmt.Errorf("shadow execution: %w", err)
	}
	if snapshot.Legacy.WorkflowSourceSHA == "" || snapshot.Legacy.WorkflowSourceSHA != snapshot.Shadow.WorkflowSourceSHA {
		return ShadowLedgerRow{}, fmt.Errorf("workflow source revisions are missing or incomparable")
	}
	if snapshot.Legacy.WorkflowBlobSHA == "" || snapshot.Shadow.WorkflowBlobSHA == "" {
		return ShadowLedgerRow{}, fmt.Errorf("workflow source blob identities are missing")
	}
	if err := validateLegacyMetadata(snapshot); err != nil {
		return ShadowLedgerRow{}, err
	}
	requiredCheckSHA, err := validateShadowResults(snapshot)
	if err != nil {
		return ShadowLedgerRow{}, err
	}
	snapshotDigest, err := shadowSnapshotDigest(snapshot)
	if err != nil {
		return ShadowLedgerRow{}, err
	}
	legacyDuration, legacyQueue, legacyMinutes := shadowExecutionMetrics(snapshot.Legacy, mapValues(legacyShadowProducers))
	shadowJobs := mapValues(shadowProducerJobs)
	shadowJobs = append(shadowJobs, "Merge Gate")
	shadowDuration, shadowQueue, shadowMinutes := shadowExecutionMetrics(snapshot.Shadow, shadowJobs)
	return ShadowLedgerRow{
		Repository: snapshot.Repository, PullRequest: snapshot.PullRequest,
		HeadRepository: snapshot.HeadRepository, AuthorAssociation: snapshot.AuthorAssociation,
		ChangeClasses: classifyShadowChanges(snapshot.ChangedPaths), ForkApproval: snapshot.ForkApproval,
		EventHeadSHA: snapshot.EventHeadSHA, EventBaseSHA: snapshot.EventBaseSHA,
		TestMergeSHA: snapshot.TestMergeSHA, RequiredCheckSHA: requiredCheckSHA,
		WorkflowSourceSHA: snapshot.Shadow.WorkflowSourceSHA,
		LegacyRunID:       snapshot.Legacy.RunID, LegacyRunAttempt: snapshot.Legacy.RunAttempt,
		ShadowRunID: snapshot.Shadow.RunID, ShadowRunAttempt: snapshot.Shadow.RunAttempt,
		LegacyDuration: legacyDuration, ShadowDuration: shadowDuration,
		LegacyQueueTime: legacyQueue, ShadowQueueTime: shadowQueue,
		LegacyRunnerMinutes: legacyMinutes, ShadowRunnerMinutes: shadowMinutes,
		LegacyConclusion: aggregateConclusion(snapshot.Legacy.Jobs),
		ShadowConclusion: aggregateConclusion(snapshot.Shadow.Jobs),
		LegacyExecution:  snapshot.Legacy,
		ShadowExecution:  snapshot.Shadow,
		ShadowResults:    snapshot.ShadowResults,
		SnapshotDigest:   snapshotDigest,
		CollectedAt:      snapshot.CollectedAt,
	}, nil
}

func classifyShadowChanges(paths []string) []string {
	classes := map[string]bool{}
	for _, path := range paths {
		path = filepath.ToSlash(strings.TrimSpace(path))
		if strings.HasPrefix(path, "services/agent-runtime/") || strings.HasPrefix(path, "services/gateway/") {
			classes["runtime-or-gateway"] = true
		}
		if strings.Contains(path, "/proto/") || strings.Contains(path, "/gen/") || strings.HasSuffix(path, ".proto") {
			classes["protocol-generated"] = true
		}
		if strings.HasPrefix(path, ".github/") || strings.HasPrefix(path, "internal/testinfra/") || path == "Makefile" {
			classes["ci-test-infrastructure"] = true
		}
		if strings.HasSuffix(path, ".go") && (strings.HasPrefix(path, "services/bridge/") || strings.HasPrefix(path, "internal/storage/") ||
			strings.HasPrefix(path, "internal/queue/") || strings.HasPrefix(path, "internal/sessionevent/") || strings.HasPrefix(path, "internal/files/")) {
			classes["database-heavy-go"] = true
		}
	}
	result := make([]string, 0, len(classes))
	for class := range classes {
		result = append(result, class)
	}
	sort.Strings(result)
	return result
}

func shadowSnapshotDigest(snapshot ShadowSnapshot) (string, error) {
	body, err := json.Marshal(snapshot)
	if err != nil {
		return "", fmt.Errorf("encode immutable shadow snapshot: %w", err)
	}
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func validateShadowExecution(execution ShadowWorkflowExecution, testMergeSHA string) error {
	if execution.RunID <= 0 || execution.RunAttempt <= 0 || execution.CheckSuiteID <= 0 {
		return fmt.Errorf("run identity is incomplete")
	}
	if execution.RunAttempt == 1 && execution.RerunOf != 0 || execution.RunAttempt > 1 && execution.RerunOf <= 0 {
		return fmt.Errorf("rerun ancestry is inconsistent")
	}
	if execution.SourceAppID != githubActionsAppID || execution.CheckHeadSHA != testMergeSHA {
		return fmt.Errorf("check carrier or source App does not match")
	}
	if execution.CreatedAt.IsZero() || execution.StartedAt.Before(execution.CreatedAt) || !execution.CompletedAt.After(execution.StartedAt) {
		return fmt.Errorf("workflow duration is not comparable")
	}
	jobNames := map[string]bool{}
	jobIDs := map[int64]bool{}
	for _, job := range execution.Jobs {
		if job.ID <= 0 || jobIDs[job.ID] || job.Name == "" || jobNames[job.Name] {
			return fmt.Errorf("job identity is missing or duplicated")
		}
		jobIDs[job.ID], jobNames[job.Name] = true, true
		if job.Status != "completed" || job.Conclusion == "" || job.StartedAt.IsZero() || job.CompletedAt.Before(job.StartedAt) {
			return fmt.Errorf("job %q has an incomparable result", job.Name)
		}
	}
	checkNames := map[string]bool{}
	checkIDs := map[int64]bool{}
	for _, check := range execution.Checks {
		if check.ID <= 0 || checkIDs[check.ID] || check.Name == "" || checkNames[check.Name] {
			return fmt.Errorf("check identity is missing or duplicated")
		}
		checkIDs[check.ID], checkNames[check.Name] = true, true
		if check.HeadSHA != testMergeSHA || check.AppID != execution.SourceAppID || check.Status != "completed" || check.Conclusion == "" {
			return fmt.Errorf("check %q has the wrong carrier or result", check.Name)
		}
	}
	return nil
}

func validateLegacyMetadata(snapshot ShadowSnapshot) error {
	metadata := map[string]LegacyWorkflowMetadata{}
	jobs := jobNameSet(snapshot.Legacy.Jobs)
	checks := checkNameSet(snapshot.Legacy.Checks)
	for _, item := range snapshot.LegacyMetadata {
		if _, exists := metadata[item.Producer]; exists {
			return fmt.Errorf("duplicate legacy metadata producer %q", item.Producer)
		}
		metadata[item.Producer] = item
	}
	for producer, jobName := range legacyShadowProducers {
		item, exists := metadata[producer]
		if !exists || !jobs[jobName] || !checks[jobName] {
			return fmt.Errorf("missing legacy job or metadata for %q", producer)
		}
		if item.Schema != "tetral.ci-legacy-sidecar/v1" || item.Repository != snapshot.Repository ||
			item.CheckedOutSHA != snapshot.TestMergeSHA || item.EventHeadSHA != snapshot.EventHeadSHA ||
			item.EventBaseSHA != snapshot.EventBaseSHA || item.TestMergeSHA != snapshot.TestMergeSHA ||
			item.WorkflowSourceSHA != snapshot.Legacy.WorkflowSourceSHA || item.Workflow != snapshot.Legacy.Name ||
			item.RunID != strconv.FormatInt(snapshot.Legacy.RunID, 10) || item.RunAttempt != strconv.Itoa(snapshot.Legacy.RunAttempt) ||
			item.Job != legacyProducerJobIDs[producer] || !validMetadataInterval(item.StartedAt, item.CompletedAt) {
			return fmt.Errorf("legacy metadata %q has a mismatched execution envelope", producer)
		}
	}
	return nil
}

func validMetadataInterval(start, end string) bool {
	started, startErr := time.Parse(time.RFC3339, start)
	completed, endErr := time.Parse(time.RFC3339, end)
	return startErr == nil && endErr == nil && !completed.Before(started)
}

func validateShadowResults(snapshot ShadowSnapshot) (string, error) {
	expected, err := LoadPRProducers()
	if err != nil {
		return "", err
	}
	results := map[string]Result{}
	for _, result := range snapshot.ShadowResults {
		producer := result.Execution.Producer
		if _, exists := results[producer]; exists {
			return "", fmt.Errorf("duplicate shadow result producer %q", producer)
		}
		results[producer] = result
	}
	jobs := jobNameSet(snapshot.Shadow.Jobs)
	checks := checkNameSet(snapshot.Shadow.Checks)
	requiredCheckSHA := ""
	for _, producer := range expected {
		result, exists := results[producer]
		jobName := shadowProducerJobs[producer]
		if !exists || !jobs[jobName] || !checks[jobName] {
			return "", fmt.Errorf("missing shadow job or result for %q", producer)
		}
		if requiredCheckSHA == "" {
			requiredCheckSHA = result.Execution.RequiredCheckSHA
		}
		expectation := GateExpectation{
			Repository: snapshot.Repository, EventHeadSHA: snapshot.EventHeadSHA, EventBaseSHA: snapshot.EventBaseSHA,
			TestMergeSHA: snapshot.TestMergeSHA, RequiredCheckSHA: requiredCheckSHA,
			WorkflowSourceSHA: snapshot.Shadow.WorkflowSourceSHA, Workflow: snapshot.Shadow.Name,
			RunID: strconv.FormatInt(snapshot.Shadow.RunID, 10), RunAttempt: strconv.Itoa(snapshot.Shadow.RunAttempt),
			ProducerJobs: PRProducerJobs(),
		}
		envelopeOnly := result
		envelopeOnly.Status = "pass"
		if reason := gateResultFailure(envelopeOnly, expectation); reason != "" {
			return "", fmt.Errorf("shadow result %q: %s", producer, reason)
		}
	}
	if len(results) != len(expected) || requiredCheckSHA != snapshot.Shadow.CheckHeadSHA {
		return "", fmt.Errorf("shadow result set or required check carrier is inconsistent")
	}
	return requiredCheckSHA, nil
}

func jobNameSet(jobs []ShadowJob) map[string]bool {
	result := map[string]bool{}
	for _, job := range jobs {
		result[job.Name] = true
	}
	return result
}

func checkNameSet(checks []ShadowCheck) map[string]bool {
	result := map[string]bool{}
	for _, check := range checks {
		result[check.Name] = true
	}
	return result
}

func shadowExecutionMetrics(execution ShadowWorkflowExecution, includedNames []string) (time.Duration, time.Duration, float64) {
	included := map[string]bool{}
	for _, name := range includedNames {
		included[name] = true
	}
	var earliest, latest time.Time
	var total time.Duration
	for _, job := range execution.Jobs {
		if !included[job.Name] {
			continue
		}
		if earliest.IsZero() || job.StartedAt.Before(earliest) {
			earliest = job.StartedAt
		}
		if latest.IsZero() || job.CompletedAt.After(latest) {
			latest = job.CompletedAt
		}
		total += job.CompletedAt.Sub(job.StartedAt)
	}
	return latest.Sub(earliest), earliest.Sub(execution.CreatedAt), total.Minutes()
}

func mapValues(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func aggregateConclusion(jobs []ShadowJob) string {
	for _, job := range jobs {
		if job.Conclusion != "success" {
			return job.Conclusion
		}
	}
	return "success"
}

func LegacyShadowProducers() []string {
	result := make([]string, 0, len(legacyShadowProducers))
	for producer := range legacyShadowProducers {
		result = append(result, producer)
	}
	sort.Strings(result)
	return result
}
