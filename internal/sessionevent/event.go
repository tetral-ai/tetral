package sessionevent

import (
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const IDPrefix = "sevt_"
const RuntimeInputIDPrefix = "rin_"

const EventTypeUserMessage = "user.message"
const EventTypeUserInterrupt = "user.interrupt"
const EventTypeUserToolConfirmation = "user.tool_confirmation"

const RuntimeInputKindMessages = "messages"
const RuntimeInputKindInterruptControl = "interrupt_control"
const RuntimeInputKindToolConfirmation = "tool_confirmation"

const ContentBlockTypeText = "text"
const ContentBlockTypeImage = "image"
const ContentBlockTypeDocument = "document"

const ContentSourceTypeFile = "file"

// MaxProviderRequestAttachments bounds the file-backed media references one
// events request may admit. The front-door check rejects an over-limit request
// at admission with a 400 invalid_request_error, never a silent later drop.
// The literal 32 is one shared bound with three copies that must stay equal:
// this events-admission front door, the provider-request assembly cap in the
// gateway protocol, and the runtime pending-attachment window. At runtime
// lowering, transient (tool-produced) and file-backed origins count toward that
// shared bound together. 32 is narrower than the upstream per-request image
// allowance of 600 images for this context class (100 for 200k-context models).
// UPDATE-WITH: services/gateway/packages/protocol/src/bounds.ts,
// services/agent-runtime/packages/core/src/session/session-state.ts.
const MaxProviderRequestAttachments = 32

const ToolConfirmationResultAllow = "allow"
const ToolConfirmationResultDeny = "deny"

const MaxEventsPerRequest = 32

type Limits struct {
	MaxEventsPerRequest int
}

func DefaultLimits() Limits {
	return Limits{
		MaxEventsPerRequest: MaxEventsPerRequest,
	}
}

func NewEventID() string {
	return id.New(IDPrefix)
}

func NewRuntimeInputID() string {
	return id.New(RuntimeInputIDPrefix)
}

type ContentSource struct {
	Type   string `json:"type"`
	FileID string `json:"file_id"`
}

type ContentBlock struct {
	Type   string         `json:"type"`
	Text   string         `json:"text,omitempty"`
	Source *ContentSource `json:"source,omitempty"`
}

func (b ContentBlock) MarshalJSON() ([]byte, error) {
	switch b.Type {
	case ContentBlockTypeText:
		return json.Marshal(struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{Type: b.Type, Text: b.Text})
	case ContentBlockTypeImage, ContentBlockTypeDocument:
		return json.Marshal(struct {
			Type   string         `json:"type"`
			Source *ContentSource `json:"source"`
		}{Type: b.Type, Source: b.Source})
	default:
		type contentBlockAlias ContentBlock
		return json.Marshal(contentBlockAlias(b))
	}
}

type TextContentBlock = ContentBlock

type IncomingEvent struct {
	Type            string         `json:"type"`
	Content         []ContentBlock `json:"content,omitempty"`
	SessionThreadID string         `json:"session_thread_id,omitempty"`
	ToolUseID       string         `json:"tool_use_id,omitempty"`
	Result          string         `json:"result,omitempty"`
	DenyMessage     string         `json:"deny_message,omitempty"`
}

type AppendRequest struct {
	Events []IncomingEvent `json:"events"`
}

type Event struct {
	ID                   string
	WorkspaceID          workspace.ID
	SessionID            string
	ThreadID             string
	Sequence             int64
	Type                 string
	Payload              json.RawMessage
	PreparationAttemptID string
	CreatedAt            time.Time
	ProcessedAt          *time.Time
}

type AppendResult struct {
	Data []*Event
}

func RuntimeInputPartitionKey(workspaceID workspace.ID, sessionID string) string {
	return queue.FormatSessionPartitionKey(workspaceID, sessionID)
}

func RuntimeInputDedupeKey(workspaceID workspace.ID, sessionID string, runtimeInputID string) string {
	return queue.FormatRuntimeInputDedupeKey(workspaceID, sessionID, runtimeInputID)
}
