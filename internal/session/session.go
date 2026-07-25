// Package session owns the durable Session control-plane domain and store.
//
// OWNS:
//   - Rows in sessions, session_threads, and session_resources.
//   - The three status axes below plus the config_generation, approval_mode, and
//     installed_tools_json columns on the session row.
//   - Resource bounds (maxFileResources, maxMemoryResources) and signed page tokens.
//   - Enqueue of the runtime_config_update queue job on a runtime-visible config change.
//
// STATE MACHINE (three independent status axes on a session/thread):
//
//	LifecycleState — written by this package's store; new rows are created active:
//	  active -> archiving -> archived   ArchiveSession; the archived step stamps
//	                                    archived_at exactly once (COALESCE)
//	  (non-deleted, non-archiving) -> deleted   DeleteSession (sets
//	                                    delete_cleanup_id)
//	  admitted and deleted are also defined values. A running session cannot be
//	  archived; an archiving session cannot be deleted (409 conflict, a
//	  transition is already in progress); running/rescheduling sessions cannot
//	  be deleted.
//
//	Status (idle/running/rescheduling/terminated) — written by the runtime bridge
//	(package bridge), NOT by this package, which only seeds idle at
//	create:
//	  (idle|running) -> rescheduling    markPublicSessionReschedulingTx
//	  rescheduling   -> running         markPublicSessionRunningTx
//	  rescheduling   -> idle            markPublicSessionIdleTx
//	  (non-terminated) -> terminated    CommitRuntimeTermination (terminal)
//	  the running/idle marks never overwrite a terminated row (WHERE status <> 'terminated').
//
//	ThreadStatus — 7 internal values (running, idle, requires_action,
//	closed_for_runtime, rescheduling, terminated, failed) projected to the 4-value
//	public Status by publicThreadStatus (fold table at that function).
//
// INVARIANTS:
//   - config_generation starts at 1 and advances only on a runtime-visible config
//     change (approval_mode or the Tools/MCPServers overlay); each advance enqueues
//     exactly one runtime_config_update job.
//   - archived_at is set exactly once, when LifecycleState reaches archived.
//   - The public Status column is authored by the runtime bridge; this package seeds
//     it to idle at create and never advances it thereafter.
//
// UPDATE-WITH:
//   - postgresql_store.go (CreateSession, ArchiveSession, DeleteSession, UpdateSession).
//   - services/bridge/bridge_api_events.go (markPublicSessionRunningTx,
//     markPublicSessionIdleTx, markPublicSessionReschedulingTx).
//   - services/bridge/runtime_termination.go (appendRuntimeTerminatedStatusTx).
package session

import (
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type Status string

const (
	StatusIdle         Status = "idle"
	StatusRunning      Status = "running"
	StatusTerminated   Status = "terminated"
	StatusRescheduling Status = "rescheduling"
)

type ThreadRole string

const (
	ThreadRoleMain             ThreadRole = "main"
	ThreadRoleSubagent         ThreadRole = "subagent"
	ThreadRoleApprovalReviewer ThreadRole = "approval_reviewer"
)

type ThreadVisibility string

const (
	ThreadVisibilityPublic   ThreadVisibility = "public"
	ThreadVisibilityInternal ThreadVisibility = "internal"
)

type ThreadStatus string

const (
	ThreadStatusRunning          ThreadStatus = "running"
	ThreadStatusIdle             ThreadStatus = "idle"
	ThreadStatusRequiresAction   ThreadStatus = "requires_action"
	ThreadStatusClosedForRuntime ThreadStatus = "closed_for_runtime"
	ThreadStatusRescheduling     ThreadStatus = "rescheduling"
	ThreadStatusTerminated       ThreadStatus = "terminated"
	ThreadStatusFailed           ThreadStatus = "failed"
)

type LifecycleState string

const (
	LifecycleStateAdmitted  LifecycleState = "admitted"
	LifecycleStateActive    LifecycleState = "active"
	LifecycleStateArchiving LifecycleState = "archiving"
	LifecycleStateArchived  LifecycleState = "archived"
	LifecycleStateDeleted   LifecycleState = "deleted"
)

type ResourceType string

const (
	ResourceTypeFile             ResourceType = "file"
	ResourceTypeMemoryStore      ResourceType = "memory_store"
	ResourceTypeGitHubRepository ResourceType = "github_repository"
)

type ApprovalMode string

const (
	ApprovalModeFullAccess     ApprovalMode = "full_access"
	ApprovalModeAskForApproval ApprovalMode = "ask_for_approval"
	ApprovalModeApproveForMe   ApprovalMode = "approve_for_me"
)

type Session struct {
	ID                 string
	Type               string
	WorkspaceID        workspace.ID
	Title              *string
	Metadata           map[string]string
	Status             Status
	LifecycleState     LifecycleState
	ConfigGeneration   int64
	ApprovalMode       ApprovalMode
	RuntimeAgentConfig *RuntimeAgentConfig
	ArchivedAt         *time.Time
	AgentID            string
	AgentVersionID     string
	AgentVersion       int
	EnvironmentID      string
	VaultIDs           []string
	Usage              Usage
	RunningSince       *time.Time
	ActiveSecondsTotal float64
	TerminatedAt       *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Resources          []*Resource
}

func normalizeApprovalMode(mode ApprovalMode) (ApprovalMode, error) {
	if mode == "" {
		return ApprovalModeAskForApproval, nil
	}
	if err := validateSessionApprovalMode(mode); err != nil {
		return "", err
	}
	return mode, nil
}

func validateSessionApprovalMode(mode ApprovalMode) error {
	switch mode {
	case ApprovalModeFullAccess, ApprovalModeAskForApproval, ApprovalModeApproveForMe:
		return nil
	default:
		return &ValidationError{Message: "approval_mode must be one of full_access, ask_for_approval, approve_for_me"}
	}
}

type Thread struct {
	ID              string
	WorkspaceID     workspace.ID
	SessionID       string
	ParentThreadID  *string
	Role            ThreadRole
	Visibility      ThreadVisibility
	Status          ThreadStatus
	AgentType       string
	Title           *string
	TaskName        *string
	IsTrunk         bool
	StorageSequence int64
	CreatedAt       time.Time
	LastActiveAt    time.Time
	ClosedAt        *time.Time
	ArchivedAt      *time.Time
	UpdatedAt       time.Time
	Usage           Usage
}

type Resource struct {
	ID                string
	SessionID         string
	WorkspaceID       workspace.ID
	StorageSequence   int64
	Type              ResourceType
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DeleteRequestedAt *time.Time
	DetachedAt        *time.Time

	File             *FileResource
	MemoryStore      *MemoryStoreResource
	GitHubRepository *GitHubRepositoryResource
}

type FileResource struct {
	SourceFileID string
	FileID       string
	MountPath    string
}

type MemoryStoreResource struct {
	MemoryStoreID string
	Access        string
	Instructions  string
	Name          string
	Description   string
	MountPath     string
}

type GitHubRepositoryResource struct {
	URL                         string
	MountPath                   string
	CheckoutType                string
	CheckoutRef                 string
	AuthorizationTokenEncrypted []byte `json:"-"`
}

type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

type ValidationError struct {
	Message string
	Detail  string
}

func (e *ValidationError) Error() string { return e.Message }

type ConflictError struct {
	Message        string
	InvalidRequest bool
}

func (e *ConflictError) Error() string { return e.Message }

type PermissionError struct {
	Message string
}

func (e *PermissionError) Error() string { return e.Message }

func validateStatus(status Status) error {
	switch status {
	case StatusIdle, StatusRunning, StatusTerminated, StatusRescheduling:
		return nil
	default:
		return &ValidationError{Message: fmt.Sprintf("invalid session status %q", status)}
	}
}

func validateThreadRole(role ThreadRole) error {
	switch role {
	case ThreadRoleMain, ThreadRoleSubagent, ThreadRoleApprovalReviewer:
		return nil
	default:
		return &ValidationError{Message: fmt.Sprintf("invalid session thread role %q", role)}
	}
}

func validateThreadVisibility(visibility ThreadVisibility) error {
	switch visibility {
	case ThreadVisibilityPublic, ThreadVisibilityInternal:
		return nil
	default:
		return &ValidationError{Message: fmt.Sprintf("invalid session thread visibility %q", visibility)}
	}
}

func validateThreadStatus(status ThreadStatus) error {
	switch status {
	case ThreadStatusRunning, ThreadStatusIdle, ThreadStatusRequiresAction, ThreadStatusClosedForRuntime, ThreadStatusRescheduling, ThreadStatusTerminated, ThreadStatusFailed:
		return nil
	default:
		return &ValidationError{Message: fmt.Sprintf("invalid session thread status %q", status)}
	}
}

// publicThreadStatus collapses the 7 internal ThreadStatus values to the 4-value
// public Status. The fold is many-to-one: three distinct internal states all
// surface publicly as idle, so the public value alone cannot recover the internal
// one. The internal meanings that justify the fold:
//
//	internal ThreadStatus   meaning                                   public Status
//	---------------------   -------                                   -------------
//	running                 runtime is actively progressing it        running
//	rescheduling            runtime binding/rebinding in progress     rescheduling
//	terminated              ended normally                            terminated
//	failed                  ended in error                            terminated
//	idle                    admitted, no runtime activity             idle
//	requires_action         paused pending a required action          idle
//	closed_for_runtime      detached from its runtime pod             idle
//
// The internal value is seeded idle here (CreatePrimaryThread) and advanced by the
// runtime bridge (markPublicSessionRunningTx/IdleTx/ReschedulingTx,
// runtime_termination). Reader of the fold: assembleThreadResponse.
// UPDATE-WITH: session.go (validateThreadStatus), service.go (assembleThreadResponse).
func publicThreadStatus(status ThreadStatus) (Status, error) {
	if err := validateThreadStatus(status); err != nil {
		return "", err
	}
	switch status {
	case ThreadStatusRunning:
		return StatusRunning, nil
	case ThreadStatusRescheduling:
		return StatusRescheduling, nil
	case ThreadStatusTerminated, ThreadStatusFailed:
		return StatusTerminated, nil
	default:
		return StatusIdle, nil
	}
}

func validateLifecycleState(state LifecycleState) error {
	switch state {
	case LifecycleStateAdmitted, LifecycleStateActive, LifecycleStateArchiving, LifecycleStateArchived, LifecycleStateDeleted:
		return nil
	default:
		return &ValidationError{Message: fmt.Sprintf("invalid session lifecycle state %q", state)}
	}
}
