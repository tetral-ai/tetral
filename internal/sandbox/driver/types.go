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
	ResultJSON     string
	BackgroundTask *BackgroundTask
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
