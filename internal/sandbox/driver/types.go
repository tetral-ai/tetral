package driver

import (
	"context"
	"io"
	"time"
)

const RuntimeUser = "daytona"

type ToolExecutor interface {
	CheckHealth(context.Context, ToolTarget) error
	RunTool(context.Context, ToolInvocation) (ToolExecution, error)
	ReadCommandResult(context.Context, CommandReference) (CommandResult, error)
	SendCommandInput(context.Context, CommandInput) (CommandResult, error)
	CancelCommand(context.Context, CommandCancel) (CommandResult, error)
}

type OutputCapturer interface {
	CaptureOutputs(context.Context, OutputCaptureTarget) (OutputCaptureScan, error)
}

type MemoryProjectionRefresher interface {
	RefreshMemoryProjection(context.Context, MemoryProjectionRefresh) error
}

type PreparationCommandRunner interface {
	RunPreparationCommand(context.Context, PreparationCommandTarget, string, map[string]string, time.Duration) error
}

type PreparationFileStager interface {
	StagePreparationFile(context.Context, PreparationCommandTarget, string, io.Reader) error
}

type ToolTarget struct {
	WorkspaceID       string
	SessionID         string
	SessionThreadID   string
	BindingID         string
	BindingGeneration int64
	SandboxID         string
	ProviderSandboxID string
	ResourceRootsJSON string
}

type PreparationCommandTarget struct {
	ProviderSandboxID string
}

type ToolInvocation struct {
	Target               ToolTarget
	ToolUseEventID       string
	ToolName             string
	InputJSON            string
	ApprovalDecisionJSON string
}

type ToolExecution struct {
	ResultJSON            string
	BackgroundTask        *BackgroundTask
	ForegroundObservation *ForegroundCommandObservation
}

// ForegroundCommandObservation is the durable continuation for a foreground
// helper command that detached before reaching a terminal state. It contains
// only provider recovery identity and bounded output-aggregation state; it
// never authorizes submitting the original command again.
type ForegroundCommandObservation struct {
	Reference CommandReference                 `json:"reference"`
	Stdout    ForegroundStreamObservationState `json:"stdout"`
	Stderr    ForegroundStreamObservationState `json:"stderr"`
	Limits    ForegroundObservationLimits      `json:"limits"`
}

type ForegroundObservationLimits struct {
	VisibleBytes int `json:"visible_bytes"`
	VisibleLines int `json:"visible_lines"`
}

type ForegroundStreamObservationState struct {
	Head          []byte `json:"head,omitempty"`
	Tail          []byte `json:"tail,omitempty"`
	CapturedBytes int64  `json:"captured_bytes"`
	TotalBytes    int64  `json:"total_bytes"`
	TotalLines    int64  `json:"total_lines"`
	Truncated     bool   `json:"truncated"`
}

// PreparedToolExecution is the opaque, repeatable Daytona payload-staging
// result. Creating it never invokes the user-authored tool command; callers
// must cross their durable submission fence before ExecutePreparedTool.
type PreparedToolExecution struct {
	target            ToolTarget
	process           daytonaProcess
	toolUseEventID    string
	helperCommand     string
	payloadPath       string
	pollUntilTerminal bool
	visibleBytes      int
	visibleLines      int
	immediateResult   *ToolExecution
}

func (p PreparedToolExecution) ImmediateResult() *ToolExecution {
	if p.immediateResult == nil {
		return nil
	}
	result := *p.immediateResult
	return &result
}

type BackgroundTask struct {
	TaskID                      string
	SourceToolUseEventID        string
	ProviderSessionID           string
	ProviderCommandID           string
	ProviderCommandMetadataJSON string
}

type CommandReference struct {
	Target          ToolTarget
	Task            BackgroundTask
	ToolUseEventID  string
	MaxOutputTokens int
}

type CommandInput struct {
	CommandReference
	InputJSON string
}

type CommandCancel struct {
	CommandReference
	Reason string
}

type CommandResult struct {
	ResultJSON     string
	TerminalStatus string
}

type OutputCaptureTarget struct {
	WorkspaceID       string
	SessionID         string
	SessionThreadID   string
	BindingID         string
	BindingGeneration int64
	SandboxID         string
	ProviderSandboxID string
	MaxFiles          int
	MaxFileBytes      int64
	MaxTotalBytes     int64
}

type OutputCaptureScan struct {
	Files                []OutputCaptureFile
	Truncated            bool
	UnrepresentableNames int
	Records              []OutputCaptureScanRecord
}

type OutputCaptureScanRecord struct {
	ParentPath string
	Reason     string
	Count      int
}

type OutputCaptureFile struct {
	SourcePath string
	Kind       string
	LinkCount  uint64
	SizeBytes  int64
	SHA256     string
	MIMEType   string
	Skipped    bool
	SkipReason string
	Open       func(context.Context) (io.ReadCloser, error)
}

type MemoryProjectionRefresh struct {
	Target     ToolTarget
	MountPaths []string
	Ops        []MemoryProjectionOp
}

type MemoryProjectionOp struct {
	Kind          string
	RelativePath  string
	Content       string
	ContentSHA256 string
}
