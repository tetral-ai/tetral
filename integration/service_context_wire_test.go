package integration

import (
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/runtimeconfig"
)

// These observation types decode the published cold-context wire. They contain
// no serializer or acceptance logic; compositions assert their returned facts.
type bridgeLoadAgent struct {
	ID         string          `json:"id"`
	Version    int64           `json:"version"`
	ConfigHash string          `json:"configHash"`
	Config     json.RawMessage `json:"config"`
}

type bridgeLoadContextAgentMail struct {
	DeliveryID string `json:"deliveryId"`
	Content    string `json:"content"`
}

type bridgeLoadContextAttachmentOrigin struct {
	Transient  *bridgeLoadContextTransientAttachment `json:"transient,omitempty"`
	FileBacked *bridgeLoadContextFileAttachment      `json:"fileBacked,omitempty"`
}

type bridgeLoadContextFailure struct {
	ErrorType   string `json:"errorType"`
	RetryStatus string `json:"retryStatus"`
}

type bridgeLoadContextFileAttachment struct {
	SourceEventID string `json:"sourceEventId"`
	FileID        string `json:"fileId"`
}

type bridgeLoadContextIdle struct {
	StopReason string `json:"stopReason"`
}

type bridgeLoadContextMCPManifest struct {
	MCPServerName      string                     `json:"mcpServerName"`
	ManifestETag       string                     `json:"manifestETag,omitempty"`
	ManifestGeneration int64                      `json:"manifestGeneration"`
	Readiness          string                     `json:"readiness"`
	Diagnostic         *string                    `json:"diagnostic"`
	Tools              []bridgeLoadContextMCPTool `json:"tools"`
}

type bridgeLoadContextMCPTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type bridgeLoadContextPayload struct {
	ContextEntries           []bridgeRuntimeContextEntry          `json:"contextEntries"`
	OpenRequestDraft         *bridgeRuntimeOpenRequestDraft       `json:"openRequestDraft"`
	TurnFacts                bridgeLoadContextTurnFacts           `json:"turnFacts"`
	ThreadContextPrefix      *bridgeLoadContextThreadPrefix       `json:"threadContextPrefix"`
	Thread                   bridgeLoadContextThread              `json:"thread"`
	RuntimeConfig            bridgeLoadContextRuntimeConfig       `json:"runtimeConfig"`
	MCPManifests             []bridgeLoadContextMCPManifest       `json:"mcpManifests"`
	PendingToolUses          []bridgeLoadContextPendingTool       `json:"pendingToolUses"`
	PendingSandboxExecutions []bridgeLoadContextSandboxExecution  `json:"pendingSandboxExecutions"`
	PendingAttachments       []bridgeLoadContextPendingAttachment `json:"pendingAttachments"`
	PendingAgentMail         []bridgeLoadContextAgentMail         `json:"pendingAgentMail"`
}

type bridgeLoadContextPendingAttachment struct {
	Origin   bridgeLoadContextAttachmentOrigin `json:"origin"`
	Mime     string                            `json:"mime"`
	Filename string                            `json:"filename"`
}

type bridgeLoadContextPendingTool struct {
	ToolUseEventID  string          `json:"toolUseEventId"`
	ModelRequestID  string          `json:"modelRequestId"`
	ModelToolCallID string          `json:"modelToolCallId"`
	ToolName        string          `json:"toolName"`
	Input           json.RawMessage `json:"input"`
	Decision        *string         `json:"decision,omitempty"`
	DenyMessage     *string         `json:"denyMessage,omitempty"`
	Status          string          `json:"status"`
}

type bridgeLoadContextProviderRetention struct {
	Disposition              string   `json:"disposition"`
	AssistantMessageSequence *int64   `json:"assistantMessageSequence,omitempty"`
	ToolUseEventIDs          []string `json:"toolUseEventIds"`
	RepairEventIDs           []string `json:"repairEventIds"`
}

type bridgeLoadContextRepairFact struct {
	RepairKey       string `json:"repairKey"`
	RepairEventID   string `json:"repairEventId"`
	EventSequence   int64  `json:"eventSequence"`
	ModelRequestID  string `json:"modelRequestId"`
	ModelToolCallID string `json:"modelToolCallId"`
	ToolName        string `json:"toolName"`
}

type bridgeLoadContextRequestEnd struct {
	RequestStartEventID      string                              `json:"requestStartEventId"`
	IsError                  bool                                `json:"isError"`
	ErrorKind                *string                             `json:"errorKind,omitempty"`
	ProviderContextRetention bridgeLoadContextProviderRetention  `json:"providerContextRetention"`
	Reschedule               *bridgeLoadContextRequestReschedule `json:"reschedule,omitempty"`
}

type bridgeLoadContextRequestReschedule struct {
	Attempt            int64  `json:"attempt"`
	EffectiveDeadline  string `json:"effectiveDeadline"`
	ProviderAttempts   int64  `json:"providerAttempts"`
	CompactionAttempts int64  `json:"compactionAttempts"`
}

type bridgeLoadContextRequestStart struct {
	RequestKind                   string `json:"requestKind"`
	ContextThroughMessageSequence int64  `json:"contextThroughMessageSequence"`
}

type bridgeLoadContextRuntimeConfig struct {
	ConfigGeneration           int64                       `json:"configGeneration"`
	ApprovalMode               string                      `json:"approvalMode"`
	System                     *string                     `json:"system"`
	MemoryStores               []runtimeconfig.MemoryStore `json:"memoryStores"`
	Agent                      bridgeLoadAgent             `json:"agent"`
	Environment                bridgeLoadEnv               `json:"environment"`
	ToolPolicy                 map[string]any              `json:"toolPolicy"`
	Skills                     json.RawMessage             `json:"skills"`
	SkillsIndex                json.RawMessage             `json:"skillsIndex"`
	InstalledTools             json.RawMessage             `json:"installedTools"`
	ProviderRescheduleBudget   int64                       `json:"providerRescheduleBudget"`
	CompactionRescheduleBudget int64                       `json:"compactionRescheduleBudget"`
}

type bridgeLoadContextSandboxExecution struct {
	ToolUseEventID  string          `json:"toolUseEventId"`
	ModelRequestID  string          `json:"modelRequestId"`
	ModelToolCallID string          `json:"modelToolCallId"`
	ToolName        string          `json:"toolName"`
	Input           json.RawMessage `json:"input"`
	ExecutionState  string          `json:"executionState"`
}

type bridgeLoadContextThread struct {
	ParentThreadID *string `json:"parentThreadId"`
	ParentTaskName *string `json:"parentTaskName"`
	Role           string  `json:"role"`
	Visibility     string  `json:"visibility"`
	TaskName       *string `json:"taskName"`
	AgentType      string  `json:"agentType"`
	Status         string  `json:"status"`
}

type bridgeLoadContextThreadPrefix struct {
	ChildThreadID         string                      `json:"childThreadId"`
	ParentThreadID        string                      `json:"parentThreadId"`
	ParentBoundaryEventID string                      `json:"parentBoundaryEventId"`
	Entries               []bridgeRuntimeContextEntry `json:"entries"`
}

type bridgeLoadContextToolResult struct {
	ModelToolCallID string `json:"modelToolCallId,omitempty"`
	ToolName        string `json:"toolName,omitempty"`
	Outcome         string `json:"outcome,omitempty"`
	RepairKey       string `json:"repairKey,omitempty"`
}

type bridgeLoadContextToolUse struct {
	ModelToolCallID string `json:"modelToolCallId"`
	ToolName        string `json:"toolName"`
}

type bridgeLoadContextTransientAttachment struct {
	AttachmentRef string `json:"attachmentRef"`
	SourcePath    string `json:"sourcePath,omitempty"`
	PageRange     string `json:"pageRange,omitempty"`
	Detail        string `json:"detail,omitempty"`
}

type bridgeLoadContextTurnEvent struct {
	EventID        string                         `json:"eventId"`
	EventSequence  int64                          `json:"eventSequence"`
	Type           string                         `json:"type"`
	ModelRequestID *string                        `json:"modelRequestId,omitempty"`
	RequestStart   *bridgeLoadContextRequestStart `json:"requestStart,omitempty"`
	RequestEnd     *bridgeLoadContextRequestEnd   `json:"requestEnd,omitempty"`
	ToolUse        *bridgeLoadContextToolUse      `json:"toolUse,omitempty"`
	ToolResult     *bridgeLoadContextToolResult   `json:"toolResult,omitempty"`
	Idle           *bridgeLoadContextIdle         `json:"idle,omitempty"`
	Failure        *bridgeLoadContextFailure      `json:"failure,omitempty"`
}

type bridgeLoadContextTurnFacts struct {
	Events          []bridgeLoadContextTurnEvent  `json:"events"`
	InternalRepairs []bridgeLoadContextRepairFact `json:"internalRepairs"`
}

type bridgeLoadEnv struct {
	ID                string          `json:"id"`
	CurrentGeneration int64           `json:"currentGeneration"`
	Config            json.RawMessage `json:"config"`
}

type bridgeRuntimeContextEntry struct {
	MessageSequence int64             `json:"messageSequence"`
	ContextKind     string            `json:"contextKind"`
	Parts           []json.RawMessage `json:"parts"`
}

type bridgeRuntimeOpenRequestDraft struct {
	ModelRequestID  string            `json:"modelRequestId"`
	MessageSequence int64             `json:"messageSequence"`
	Parts           []json.RawMessage `json:"parts"`
}
