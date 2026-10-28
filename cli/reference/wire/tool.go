package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"unicode"
)

// Byte limits of the tool messages, which a ready declares as
// TransportLimits.
const (
	MetadataJSONBytes           = 65_536
	ToolInputJSONBytes          = 4_194_304
	ExecuteSSEJSONBytes         = 4_259_840
	StdinUTF8Bytes              = 32_768
	StdinSSEJSONBytes           = 262_144
	DefaultMessageJSONBytes     = 65_536
	HelperResultJSONBytes       = 262_144
	DriveSyncJSONBytes          = 65_536
	OrdinaryToolStatusJSONBytes = 327_680
	DriveSyncReportJSONBytes    = 131_072
	ViewImageDecodedBytes       = 10_485_760
	MediaToolStatusJSONBytes    = 14_680_064
	MCPCallSSEJSONBytes         = 131_072
	DriveSyncListedMax          = 100
)

// toolSchemaVersion is the `schema_version` of a result envelope.
const toolSchemaVersion = 1

// SandboxPolicy is the permissions of one call. It is closed at every level,
// so a misspelled member is refused rather than read as a grant.
type SandboxPolicy struct {
	_             Closed
	SchemaVersion int64            `json:"schema_version" wire:"const=1"`
	Filesystem    FilesystemPolicy `json:"filesystem"`
	Network       NetworkPolicy    `json:"network"`
}

func (SandboxPolicy) MaxBytes() int { return DefaultMessageJSONBytes }

type FilesystemPolicy struct {
	_     Closed
	Rules []FilesystemRule `json:"rules"`
}

type FilesystemRule struct {
	_      Closed
	Path   AbsolutePath     `json:"path"`
	Access FilesystemAccess `json:"access"`
}

type FilesystemAccess string

func (access *FilesystemAccess) check() error {
	return oneOf(string(*access), "deny", "read", "read_write")
}

// NetworkPolicy is one of NetworkDisabled, NetworkDirect and NetworkManaged,
// named by the member `mode`.
type NetworkPolicy struct{ Mode networkMode }

type networkMode interface{ variant }

type NetworkDisabled struct{ _ Closed }

type NetworkDirect struct{ _ Closed }

type NetworkManaged struct {
	_     Closed
	Rules ManagedNetworkRules `json:"rules"`
}

func (*NetworkDisabled) variantName() string { return "disabled" }
func (*NetworkDirect) variantName() string   { return "direct" }
func (*NetworkManaged) variantName() string  { return "managed" }

func (policy *NetworkPolicy) decodeTree(tree any) (err error) {
	policy.Mode, err = decodeVariant[networkMode](tree, "mode", &NetworkDisabled{}, &NetworkDirect{}, &NetworkManaged{})
	return err
}

func (policy NetworkPolicy) MarshalJSON() ([]byte, error) { return encodeVariant("mode", policy.Mode) }

type ManagedNetworkRules struct {
	_      Closed
	Allow  []NetworkRule   `json:"allow"`
	Deny   []NetworkRule   `json:"deny"`
	Listen []NetworkListen `json:"listen"`
}

type NetworkRule struct {
	_           Closed
	Destination NetworkDestination `json:"destination"`
	Transport   Transport          `json:"transport"`
	Ports       []PortRange        `json:"ports" wire:"min=1"`
}

type NetworkListen struct {
	_         Closed
	Address   NonEmpty    `json:"address"`
	Transport Transport   `json:"transport"`
	Ports     []PortRange `json:"ports" wire:"min=1"`
}

type PortRange struct {
	_    Closed
	From int64 `json:"from" wire:"min=1,max=65535"`
	To   int64 `json:"to" wire:"min=1,max=65535"`
}

// NetworkDestination is a host name or a CIDR block.
type NetworkDestination struct {
	_     Closed
	Kind  string   `json:"kind"`
	Value NonEmpty `json:"value"`
}

func (destination *NetworkDestination) check() error {
	return oneOf(destination.Kind, "host", "cidr")
}

type Transport string

func (transport *Transport) check() error { return oneOf(string(*transport), "tcp", "udp") }

// VisibleLimits are the visible-output limits an instruction asks for.
type VisibleLimits struct {
	VisibleBytes Maybe[int64] `json:"visible_bytes,omitzero"`
	VisibleLines Maybe[int64] `json:"visible_lines,omitzero"`
}

// ToolInput is the input of one tool: ExecInput, EditInput or ViewImageInput.
type ToolInput interface{ variant }

type ExecInput struct {
	Cmd            Command                  `json:"cmd"`
	Cwd            Maybe[string]            `json:"cwd,omitzero"`
	Env            Maybe[map[string]string] `json:"env,omitzero"`
	WaitMs         Maybe[int64]             `json:"wait_ms,omitzero"`
	OnWaitExpiry   Maybe[OnWaitExpiry]      `json:"on_wait_expiry,omitzero"`
	TaskLifetimeMs Maybe[int64]             `json:"task_lifetime_ms,omitzero"`
	TaskID         Maybe[ID]                `json:"task_id,omitzero"`
}

// Command is an exec_command command: text with a character that is not
// white space.
type Command string

func (command *Command) check() error {
	for _, character := range string(*command) {
		if !unicode.IsSpace(character) {
			return nil
		}
	}
	return want(false, "a command with a character that is not white space")
}

type OnWaitExpiry string

func (expiry *OnWaitExpiry) check() error { return oneOf(string(*expiry), "", "kill", "detach") }

// ToolPath is a path a tool input names: nonempty, without U+0000.
type ToolPath = ID

type EditInput struct {
	Path       ToolPath      `json:"path"`
	OldString  NonEmpty      `json:"old_string"`
	NewString  Maybe[string] `json:"new_string,omitzero"`
	ReplaceAll Maybe[bool]   `json:"replace_all,omitzero"`
}

type ViewImageInput struct {
	Path ToolPath `json:"path"`
}

func (*ExecInput) variantName() string      { return "exec_command" }
func (*EditInput) variantName() string      { return "edit" }
func (*ViewImageInput) variantName() string { return "view_image" }

// ExecutePayload is a tool.execute's payload: one tool call with its input
// and its permissions.
type ExecutePayload struct {
	SessionID      ID
	BindingID      ID
	ToolUseEventID ID
	TaskID         ID
	Input          ToolInput
	SandboxPolicy  SandboxPolicy
	Limits         Opt[VisibleLimits]
}

type executeFields struct {
	SessionID      ID                 `json:"session_id"`
	BindingID      ID                 `json:"binding_id"`
	ToolUseEventID ID                 `json:"tool_use_event_id"`
	TaskID         ID                 `json:"task_id"`
	Tool           string             `json:"tool"`
	Input          any                `json:"input"`
	SandboxPolicy  SandboxPolicy      `json:"sandbox_policy"`
	Limits         Opt[VisibleLimits] `json:"limits,omitzero"`
}

func (payload *ExecutePayload) decodeTree(tree any) error {
	var fields executeFields
	if err := decodeAt(&fields, tree, ""); err != nil {
		return err
	}
	inputs := map[string]ToolInput{"exec_command": &ExecInput{}, "edit": &EditInput{}, "view_image": &ViewImageInput{}}
	input, known := inputs[fields.Tool]
	if !known {
		return fmt.Errorf("/tool: unknown tool %q", fields.Tool)
	}
	if _, isObject := fields.Input.(map[string]any); !isObject {
		return errors.New("/input: want an object")
	}
	if err := decodeAt(input, fields.Input, "/input"); err != nil {
		return err
	}
	*payload = ExecutePayload{
		SessionID: fields.SessionID, BindingID: fields.BindingID, ToolUseEventID: fields.ToolUseEventID,
		TaskID: fields.TaskID, Input: input, SandboxPolicy: fields.SandboxPolicy, Limits: fields.Limits,
	}
	return nil
}

func (payload ExecutePayload) MarshalJSON() ([]byte, error) {
	return json.Marshal(executeFields{
		SessionID: payload.SessionID, BindingID: payload.BindingID, ToolUseEventID: payload.ToolUseEventID,
		TaskID: payload.TaskID, Tool: payload.Input.variantName(), Input: payload.Input,
		SandboxPolicy: payload.SandboxPolicy, Limits: payload.Limits,
	})
}

type StdinPayload struct {
	SessionID      ID                 `json:"session_id"`
	BindingID      ID                 `json:"binding_id"`
	ToolUseEventID ID                 `json:"tool_use_event_id"`
	TaskID         ID                 `json:"task_id"`
	Chars          string             `json:"chars"`
	WriteSeq       Opt[int64]         `json:"write_seq,omitzero" wire:"min=1"`
	WaitMs         Maybe[int64]       `json:"wait_ms,omitzero"`
	Limits         Opt[VisibleLimits] `json:"limits,omitzero"`
}

func (payload *StdinPayload) check() error {
	return rule(payload.Chars == "" || payload.WriteSeq.Set, "a nonempty tool.stdin without write_seq")
}

type CancelPayload struct {
	SessionID      ID                 `json:"session_id"`
	BindingID      ID                 `json:"binding_id"`
	ToolUseEventID ID                 `json:"tool_use_event_id"`
	TaskID         ID                 `json:"task_id"`
	Reason         Maybe[string]      `json:"reason,omitzero"`
	Limits         Opt[VisibleLimits] `json:"limits,omitzero"`
}

// MCPCallPayload is an mcp.call's payload. Engine never sends it permissions
// or limits.
type MCPCallPayload struct {
	SessionID      ID       `json:"session_id"`
	BindingID      ID       `json:"binding_id"`
	ToolUseEventID ID       `json:"tool_use_event_id"`
	TaskID         ID       `json:"task_id"`
	Server         NonEmpty `json:"server"`
	Tool           NonEmpty `json:"tool"`
	Arguments      Object   `json:"arguments"`
	SandboxPolicy  Absent   `json:"sandbox_policy,omitzero"`
	Limits         Absent   `json:"limits,omitzero"`
}

// toolInstructionLimit is the encoded size limit of a tool instruction type.
func toolInstructionLimit(kind string) (int, bool) {
	switch kind {
	case "tool.execute":
		return ExecuteSSEJSONBytes, true
	case "tool.stdin":
		return StdinSSEJSONBytes, true
	case "tool.cancel":
		return DefaultMessageJSONBytes, true
	case "mcp.call":
		return MCPCallSSEJSONBytes, true
	}
	return 0, false
}

type StreamSnapshot struct {
	Text          string `json:"text"`
	TotalBytes    int64  `json:"total_bytes" wire:"min=0"`
	TotalLines    int64  `json:"total_lines" wire:"min=0"`
	ReturnedBytes int64  `json:"returned_bytes" wire:"min=0"`
	Truncated     bool   `json:"truncated"`
}

type ExecResult struct {
	TaskID       Nullable[ID]     `json:"task_id"`
	ExitCode     Nullable[int64]  `json:"exit_code"`
	Signal       Nullable[string] `json:"signal"`
	TimedOut     bool             `json:"timed_out"`
	Cancelled    Opt[bool]        `json:"cancelled,omitzero"`
	WasRunning   Opt[bool]        `json:"was_running,omitzero"`
	WrittenBytes Opt[int64]       `json:"written_bytes,omitzero" wire:"min=0"`
	Stdout       StreamSnapshot   `json:"stdout"`
	Stderr       StreamSnapshot   `json:"stderr"`
	DurationMs   int64            `json:"duration_ms" wire:"min=0"`
}

type EditResult struct {
	Replacements int64 `json:"replacements" wire:"min=0"`
	BytesWritten int64 `json:"bytes_written" wire:"min=0"`
}

type MediaResult struct {
	Mime       ImageMime `json:"mime"`
	SizeBytes  int64     `json:"size_bytes" wire:"min=0"`
	DataBase64 string    `json:"data_base64"`
}

type ImageMime string

func (mime *ImageMime) check() error {
	return oneOf(string(*mime), "image/png", "image/jpeg", "image/gif", "image/webp")
}

// MCPResult is an MCP tools/call result as the CLI forwards it, without
// `_meta`.
type MCPResult struct {
	Content           []Object    `json:"content"`
	StructuredContent Opt[Object] `json:"structuredContent,omitzero"`
	IsError           Opt[bool]   `json:"isError,omitzero"`
	Meta              Absent      `json:"_meta,omitzero"`
}

type ToolError struct {
	Kind    NonEmpty    `json:"kind"`
	Message string      `json:"message"`
	Path    Opt[string] `json:"path,omitzero"`
	Detail  Opt[Object] `json:"detail,omitzero"`
}

// checkMCPError holds an mcp result's error kind to the detail it requires.
func (toolError *ToolError) checkMCPError() error {
	detail := toolError.Detail.Value
	integer := func(name string, minimum int64) bool {
		number, ok := detail[name].(json.Number)
		if !ok {
			return false
		}
		value, err := number.Int64()
		return err == nil && value >= minimum
	}
	switch toolError.Kind {
	case "mcp_error":
		_, isText := detail["message"].(string)
		return want(integer("code", -1<<63) && isText, "an mcp_error detail with code and message")
	case "cancelled":
		_, isFlag := detail["sent"].(bool)
		return want(isFlag, "a cancelled detail with sent")
	case "mcp_result_too_large":
		return want(integer("media_bytes", 0) && integer("envelope_bytes", 0) && integer("report_bytes", 0),
			"an mcp_result_too_large detail with its byte counts")
	case "mcp_timeout", "mcp_server_exited", "mcp_invalid_result":
		return nil
	}
	return want(false, "an error kind of an mcp result")
}

type Metrics struct {
	WallTimeMs  int64 `json:"wall_time_ms" wire:"min=0"`
	QueuedMs    int64 `json:"queued_ms" wire:"min=0"`
	DriveWaitMs int64 `json:"drive_wait_ms" wire:"min=0"`
	RunMs       int64 `json:"run_ms" wire:"min=0"`
	SyncMs      int64 `json:"sync_ms" wire:"min=0"`
}

// HelperEnvelope is the result envelope of one tool call. Result holds an
// *ExecResult, *EditResult, *MediaResult or *MCPResult where the tool,
// status and truncated name one, and the object as read otherwise.
type HelperEnvelope struct {
	Tool      string
	Status    string
	Truncated bool
	Error     Nullable[ToolError]
	Result    any
	Metrics   Opt[Metrics]
	DriveSync Opt[DriveSyncOutcome]
}

type envelopeFields struct {
	SchemaVersion int64                 `json:"schema_version" wire:"const=1"`
	Tool          string                `json:"tool"`
	Status        string                `json:"status"`
	Truncated     bool                  `json:"truncated"`
	Error         Nullable[ToolError]   `json:"error"`
	Result        Object                `json:"result"`
	Metrics       Opt[Metrics]          `json:"metrics,omitzero"`
	DriveSync     Opt[DriveSyncOutcome] `json:"drive_sync,omitzero"`
}

func (envelope *HelperEnvelope) decodeTree(tree any) error {
	var fields envelopeFields
	if err := decodeAt(&fields, tree, ""); err != nil {
		return err
	}
	if err := oneOf(fields.Tool, "exec_command", "edit", "view_image", "mcp", "stdin", "cancel"); err != nil {
		return fmt.Errorf("/tool: %w", err)
	}
	if err := oneOf(fields.Status, "success", "error", "running"); err != nil {
		return fmt.Errorf("/status: %w", err)
	}
	if (fields.Status == "error") != fields.Error.Valid {
		return errors.New("status error without an error, or an error without status error")
	}
	runs := slices.Contains([]string{"exec_command", "stdin", "cancel"}, fields.Tool)
	if fields.Status == "running" && !runs {
		return errors.New("status running for a tool that runs no process")
	}
	if fields.Tool == "mcp" && (fields.Truncated || fields.DriveSync.Set) {
		return errors.New("an mcp result that is truncated or has drive_sync")
	}
	complete := fields.Status == "success" && !fields.Truncated
	var typed any
	switch {
	case runs && fields.Status != "error":
		typed = &ExecResult{}
	case fields.Tool == "edit" && complete:
		typed = &EditResult{}
	case fields.Tool == "view_image" && complete:
		typed = &MediaResult{}
	case fields.Tool == "mcp" && fields.Status == "success":
		typed = &MCPResult{}
	case fields.Tool == "mcp":
		if len(fields.Result) > 0 {
			return errors.New("an mcp error with a nonempty result")
		}
		if err := fields.Error.Value.checkMCPError(); err != nil {
			return fmt.Errorf("/error: %w", err)
		}
	}
	var result any = fields.Result
	if typed != nil {
		if err := decodeAt(typed, map[string]any(fields.Result), "/result"); err != nil {
			return err
		}
		result = typed
	}
	*envelope = HelperEnvelope{
		Tool: fields.Tool, Status: fields.Status, Truncated: fields.Truncated, Error: fields.Error,
		Result: result, Metrics: fields.Metrics, DriveSync: fields.DriveSync,
	}
	return nil
}

func (envelope HelperEnvelope) MarshalJSON() ([]byte, error) {
	fields := struct {
		SchemaVersion int64                 `json:"schema_version"`
		Tool          string                `json:"tool"`
		Status        string                `json:"status"`
		Truncated     bool                  `json:"truncated"`
		Error         Nullable[ToolError]   `json:"error"`
		Result        any                   `json:"result"`
		Metrics       Opt[Metrics]          `json:"metrics,omitzero"`
		DriveSync     Opt[DriveSyncOutcome] `json:"drive_sync,omitzero"`
	}{toolSchemaVersion, envelope.Tool, envelope.Status, envelope.Truncated, envelope.Error,
		envelope.Result, envelope.Metrics, envelope.DriveSync}
	return json.Marshal(fields)
}

// ProtocolError is a report's error, its code one of the report's codes.
type ProtocolError struct {
	Code    NonEmpty `json:"code"`
	Message string   `json:"message"`
}

var (
	rejectionCodes = []string{"invalid_input", "task_conflict", "unsupported_policy", "sandbox_unavailable",
		"binding_unavailable", "resource_exhausted", "mcp_server_unavailable"}
	controlErrorCodes = []string{"unsupported_control", "task_not_found", "write_conflict", "invalid_input",
		"result_unavailable", "stdin_closed", "resource_exhausted"}
	protocolErrorCodes = []string{"message_conflict", "invalid_message"}
)

// ReportHeader is the identities every tool-status report carries.
type ReportHeader struct {
	ProtocolVersion  int64 `json:"protocol_version" wire:"const=1"`
	ConnectionID     ID    `json:"connection_id"`
	MessageID        ID    `json:"message_id"`
	SessionID        ID    `json:"session_id"`
	BindingID        ID    `json:"binding_id"`
	ToolUseEventID   ID    `json:"tool_use_event_id"`
	TaskID           ID    `json:"task_id"`
	RequestMessageID ID    `json:"request_message_id"`
}

// ToolStatus is a tool-status report. An execution report has Phase
// `running` or `terminal` with ReportSeq and Result, or `rejected` with
// Error; a control report has no phase and a Result, an Error or both.
type ToolStatus struct {
	ReportHeader
	Kind      string              `json:"kind"`
	Phase     Opt[string]         `json:"phase,omitzero"`
	ReportSeq Opt[int64]          `json:"report_seq,omitzero" wire:"min=1"`
	Result    Opt[HelperEnvelope] `json:"result,omitzero"`
	Error     Opt[ProtocolError]  `json:"error,omitzero"`
}

func (ToolStatus) MaxBytes() int { return MediaToolStatusJSONBytes }

func (report *ToolStatus) check() error {
	switch {
	case report.Kind == "execution" && report.Phase.Set && report.Phase.Value != "rejected":
		if err := oneOf(report.Phase.Value, "running", "terminal"); err != nil {
			return fmt.Errorf("/phase: %w", err)
		}
		if !report.ReportSeq.Set || !report.Result.Set || report.Error.Set {
			return errors.New("a result report without report_seq or result, or with an error")
		}
		if report.Phase.Value == "terminal" && !report.Result.Value.Metrics.Set {
			return errors.New("a terminal result without metrics")
		}
		return nil
	case report.Kind == "execution" && report.Phase.Set:
		if report.ReportSeq.Set || report.Result.Set || !report.Error.Set {
			return errors.New("a rejection without an error, or with report_seq or result")
		}
		return want(slices.Contains(rejectionCodes, string(report.Error.Value.Code)), "a rejection code")
	case report.Kind == "execution":
		return errors.New("an execution report without a phase")
	case report.Kind == "control" && !report.Phase.Set:
		if report.ReportSeq.Set {
			return errors.New("a control report with report_seq")
		}
		if !report.Result.Set && !report.Error.Set {
			return errors.New("a control report with neither result nor error")
		}
		if report.Error.Set && !slices.Contains(controlErrorCodes, string(report.Error.Value.Code)) {
			return want(false, "a control error code")
		}
		return nil
	case report.Kind == "control":
		return errors.New("a control report with a phase")
	}
	return oneOf(report.Kind, "execution", "control")
}

// DriveSyncReport is the drive outcome of a sync point that no tool-status
// report carries.
type DriveSyncReport struct {
	ProtocolVersion int64            `json:"protocol_version" wire:"const=1"`
	ConnectionID    ID               `json:"connection_id"`
	MessageID       ID               `json:"message_id"`
	SessionID       ID               `json:"session_id"`
	BindingID       ID               `json:"binding_id"`
	TaskID          Opt[ID]          `json:"task_id,omitzero"`
	DriveSync       DriveSyncOutcome `json:"drive_sync"`
}

func (DriveSyncReport) MaxBytes() int { return DriveSyncReportJSONBytes }

// DriveSyncOutcome is what one sync point did to the drives; each list holds
// at most 100 entries, and its `_total` counts all of them when more exist.
type DriveSyncOutcome struct {
	_                Closed
	Committed        int64                    `json:"committed" wire:"min=0"`
	Deleted          int64                    `json:"deleted" wire:"min=0"`
	Rejected         []DriveRejection         `json:"rejected" wire:"max=100"`
	RejectedTotal    Opt[int64]               `json:"rejected_total,omitzero" wire:"min=1"`
	NotSynced        []DriveNotSynced         `json:"not_synced" wire:"max=100"`
	NotSyncedTotal   Opt[int64]               `json:"not_synced_total,omitzero" wire:"min=1"`
	Downloading      Opt[[]DriveDownload]     `json:"downloading,omitzero" wire:"max=100"`
	DownloadingTotal Opt[int64]               `json:"downloading_total,omitzero" wire:"min=1"`
	Unmounted        Opt[[]DriveMountFailure] `json:"unmounted,omitzero"`
}

func (outcome *DriveSyncOutcome) check() error {
	reports := outcome.Committed > 0 || outcome.Deleted > 0 || len(outcome.Rejected) > 0 ||
		len(outcome.NotSynced) > 0 || len(outcome.Downloading.Value) > 0 || outcome.RejectedTotal.Set ||
		outcome.NotSyncedTotal.Set || outcome.DownloadingTotal.Set || len(outcome.Unmounted.Value) > 0
	return want(reports, "a drive_sync outcome that reports something")
}

type DriveRejection struct {
	_                Closed
	Path             AbsolutePath     `json:"path"`
	Change           string           `json:"change"`
	Reason           string           `json:"reason"`
	Rule             Opt[NonEmpty]    `json:"rule,omitzero"`
	CurrentVersionID Opt[ID]          `json:"current_version_id,omitzero"`
	CurrentWriter    Opt[DriveWriter] `json:"current_writer,omitzero"`
}

func (rejection *DriveRejection) check() error {
	if err := oneOf(rejection.Change, "write", "delete"); err != nil {
		return err
	}
	switch rejection.Reason {
	case "path_rule", "limit":
		return want(rejection.Rule.Set, "a rule with a path_rule or limit rejection")
	case "conflict":
		return want(rejection.CurrentVersionID.Set == rejection.CurrentWriter.Set,
			"both or neither of current_version_id and current_writer")
	}
	return oneOf(rejection.Reason, "conflict", "path_rule", "limit")
}

type DriveNotSynced struct {
	_      Closed
	Path   AbsolutePath `json:"path"`
	Reason string       `json:"reason"`
}

func (entry *DriveNotSynced) check() error {
	return oneOf(entry.Reason, "git_repository", "symlink", "special_file", "uploading", "upload_pending")
}

type DriveDownload struct {
	_               Closed
	Path            AbsolutePath `json:"path"`
	DownloadedBytes int64        `json:"downloaded_bytes" wire:"min=0"`
	SizeBytes       int64        `json:"size_bytes" wire:"min=0"`
}

type DriveMountFailure struct {
	_       Closed
	DriveID ID       `json:"drive_id"`
	Error   NonEmpty `json:"error"`
}

// DriveWriter is who wrote a drive version: one of APIActor, UserActor,
// ServiceAccountActor and SessionActor, named by the member `type`.
type DriveWriter struct{ Actor driveActor }

type driveActor interface{ variant }

type APIActor struct {
	_        Closed
	APIKeyID ID `json:"api_key_id"`
}

type UserActor struct {
	_      Closed
	UserID ID `json:"user_id"`
}

type ServiceAccountActor struct {
	_                Closed
	ServiceAccountID ID `json:"service_account_id"`
}

type SessionActor struct {
	_          Closed
	SessionID  ID `json:"session_id"`
	ComputerID ID `json:"computer_id"`
}

func (*APIActor) variantName() string            { return "api_actor" }
func (*UserActor) variantName() string           { return "user_actor" }
func (*ServiceAccountActor) variantName() string { return "service_account_actor" }
func (*SessionActor) variantName() string        { return "session_actor" }

func (writer *DriveWriter) decodeTree(tree any) (err error) {
	writer.Actor, err = decodeVariant[driveActor](tree, "type",
		&APIActor{}, &UserActor{}, &ServiceAccountActor{}, &SessionActor{})
	return err
}

func (writer DriveWriter) MarshalJSON() ([]byte, error) { return encodeVariant("type", writer.Actor) }

type Acknowledgement struct {
	ConnectionID ID   `json:"connection_id"`
	MessageID    ID   `json:"message_id"`
	Accepted     bool `json:"accepted" wire:"const=true"`
}

func (Acknowledgement) MaxBytes() int { return DefaultMessageJSONBytes }

type ProtocolErrorReport struct {
	ProtocolVersion  int64         `json:"protocol_version" wire:"const=1"`
	ConnectionID     ID            `json:"connection_id"`
	MessageID        ID            `json:"message_id"`
	RequestMessageID Opt[ID]       `json:"request_message_id,omitzero"`
	Error            ProtocolError `json:"error"`
}

func (ProtocolErrorReport) MaxBytes() int { return DefaultMessageJSONBytes }

func (report *ProtocolErrorReport) check() error {
	return want(slices.Contains(protocolErrorCodes, string(report.Error.Code)), "a protocol error code")
}

// TransportLimits are the byte limits a ready declares, each fixed to its
// value.
type TransportLimits struct {
	_                           Closed
	MetadataJSONBytes           int64 `json:"metadata_json_bytes" wire:"const=65536"`
	ToolInputJSONBytes          int64 `json:"tool_input_json_bytes" wire:"const=4194304"`
	ExecuteSSEJSONBytes         int64 `json:"execute_sse_json_bytes" wire:"const=4259840"`
	StdinUTF8Bytes              int64 `json:"stdin_utf8_bytes" wire:"const=32768"`
	StdinSSEJSONBytes           int64 `json:"stdin_sse_json_bytes" wire:"const=262144"`
	DefaultMessageJSONBytes     int64 `json:"default_message_json_bytes" wire:"const=65536"`
	HelperResultJSONBytes       int64 `json:"helper_result_json_bytes" wire:"const=262144"`
	DriveSyncJSONBytes          int64 `json:"drive_sync_json_bytes" wire:"const=65536"`
	OrdinaryToolStatusJSONBytes int64 `json:"ordinary_tool_status_json_bytes" wire:"const=327680"`
	DriveSyncReportJSONBytes    int64 `json:"drive_sync_report_json_bytes" wire:"const=131072"`
	ViewImageDecodedBytes       int64 `json:"view_image_decoded_bytes" wire:"const=10485760"`
	MediaToolStatusJSONBytes    int64 `json:"media_tool_status_json_bytes" wire:"const=14680064"`
}

type ManagedNetworkCapability struct {
	_          Closed
	Transports []Transport `json:"transports" wire:"unique"`
	Listen     bool        `json:"listen"`
}

// ToolExecutionCapability is the `tool_execution` fragment of a ready.
type ToolExecutionCapability struct {
	SchemaVersion         int64                         `json:"schema_version" wire:"const=1"`
	Tools                 []ExecutionTool               `json:"tools" wire:"unique"`
	Controls              []Control                     `json:"controls" wire:"unique"`
	SandboxPolicyVersions []SandboxPolicyVersion        `json:"sandbox_policy_versions" wire:"unique"`
	NetworkModes          []NetworkMode                 `json:"network_modes" wire:"unique"`
	ManagedNetwork        Opt[ManagedNetworkCapability] `json:"managed_network,omitzero"`
	Limits                TransportLimits               `json:"limits"`
}

type ExecutionTool string

func (tool *ExecutionTool) check() error {
	return oneOf(string(*tool), "exec_command", "edit", "view_image")
}

type Control string

func (control *Control) check() error { return oneOf(string(*control), "stdin", "cancel") }

type SandboxPolicyVersion int64

func (version *SandboxPolicyVersion) check() error {
	return want(*version == 1, "sandbox policy version 1")
}

type NetworkMode string

func (mode *NetworkMode) check() error { return oneOf(string(*mode), "disabled", "direct", "managed") }

func (capability *ToolExecutionCapability) check() error {
	return want(slices.Contains(capability.NetworkModes, "managed") == capability.ManagedNetwork.Set,
		"managed_network exactly when the managed network mode is declared")
}

// checkDeclarations holds a ready or a ReadyExtension to declaring tools,
// local MCP or both, each with the controls it needs; localMCP is whether it
// declares local MCP.
func (capability *ToolExecutionCapability) checkDeclarations(localMCP bool) error {
	if err := rule(len(capability.Tools) > 0 || localMCP, "tool_execution without tools or local_mcp"); err != nil {
		return err
	}
	cancel := slices.Contains(capability.Controls, "cancel")
	if err := rule(len(capability.Tools) == 0 || cancel, "every tool requires cancel"); err != nil {
		return err
	}
	if err := rule(!slices.Contains(capability.Tools, "exec_command") ||
		slices.Contains(capability.Controls, "stdin"), "exec_command requires stdin"); err != nil {
		return err
	}
	return rule(!localMCP || cancel, "local MCP requires cancel")
}

type LocalMCPCapability struct {
	SchemaVersion int64 `json:"schema_version" wire:"const=1"`
}

// ReadyExtension is the tool execution part of a ready's capabilities.
type ReadyExtension struct {
	ToolExecution ToolExecutionCapability `json:"tool_execution"`
	LocalMCP      Opt[LocalMCPCapability] `json:"local_mcp,omitzero"`
}

func (ReadyExtension) MaxBytes() int { return DefaultMessageJSONBytes }

func (extension *ReadyExtension) check() error {
	return extension.ToolExecution.checkDeclarations(extension.LocalMCP.Set)
}
