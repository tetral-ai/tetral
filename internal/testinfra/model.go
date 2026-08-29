package testinfra

import "time"

type Profile string

const (
	ProfileFast     Profile = "fast"
	ProfileAffected Profile = "affected"
	ProfileFull     Profile = "full"
)

type Revision struct {
	Head              string   `json:"head"`
	ResolvedBaseTip   string   `json:"resolved_base_tip,omitempty"`
	ComparisonCommit  string   `json:"comparison_commit,omitempty"`
	WorktreeDirty     bool     `json:"worktree_dirty"`
	ChangedPaths      []string `json:"changed_paths,omitempty"`
	FullFallbackCause string   `json:"full_fallback_cause,omitempty"`
}

type Plan struct {
	Profile      Profile     `json:"profile"`
	Revision     Revision    `json:"revision"`
	Selections   []Selection `json:"selections"`
	Excluded     []Exclusion `json:"excluded,omitempty"`
	Dependencies []string    `json:"dependencies"`
	CreatedAt    time.Time   `json:"created_at"`
}

type Exclusion struct {
	Group       string `json:"group"`
	Package     string `json:"package,omitempty"`
	Runnable    string `json:"runnable,omitempty"`
	Capability  string `json:"capability"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason"`
}

type Selection struct {
	Group        string   `json:"group"`
	Reason       string   `json:"reason"`
	Packages     []string `json:"packages,omitempty"`
	Tests        []string `json:"tests,omitempty"`
	Command      []string `json:"command,omitempty"`
	WorkingDir   string   `json:"working_dir,omitempty"`
	Mode         string   `json:"mode"`
	Dependencies []string `json:"dependencies,omitempty"`
}

type Result struct {
	Plan         Plan                 `json:"plan"`
	Execution    ExecutionEnvelope    `json:"execution"`
	StartedAt    time.Time            `json:"started_at"`
	FinishedAt   time.Time            `json:"finished_at"`
	Status       string               `json:"status"`
	Dependencies []DependencyEvidence `json:"dependencies,omitempty"`
	Steps        []StepResult         `json:"steps"`
	Setup        time.Duration        `json:"setup_elapsed_ns"`
	Teardown     time.Duration        `json:"teardown_elapsed_ns"`
}

// ExecutionEnvelope identifies the exact local or CI process that produced a
// result. GitHub check identities are joined from the API by the shadow
// collector because they are not exposed to the running job environment.
type ExecutionEnvelope struct {
	Repository        string `json:"repository,omitempty"`
	EventHeadSHA      string `json:"event_head_sha,omitempty"`
	EventBaseSHA      string `json:"event_base_sha,omitempty"`
	TestMergeSHA      string `json:"test_merge_sha,omitempty"`
	RequiredCheckSHA  string `json:"required_check_sha,omitempty"`
	WorkflowSourceSHA string `json:"workflow_source_sha,omitempty"`
	Workflow          string `json:"workflow,omitempty"`
	RunID             string `json:"run_id,omitempty"`
	RunAttempt        string `json:"run_attempt,omitempty"`
	Job               string `json:"job,omitempty"`
	Producer          string `json:"producer,omitempty"`
}

type DependencyEvidence struct {
	Name     string `json:"name"`
	Source   string `json:"source"`
	Identity string `json:"identity"`
	Version  string `json:"version,omitempty"`
	RunID    string `json:"run_id,omitempty"`
}

type StepResult struct {
	Group        string        `json:"group"`
	Command      []string      `json:"command"`
	WorkingDir   string        `json:"working_dir"`
	Status       string        `json:"status"`
	Elapsed      time.Duration `json:"elapsed_ns"`
	FirstFailure string        `json:"first_failure,omitempty"`
	Artifact     string        `json:"artifact,omitempty"`
}
