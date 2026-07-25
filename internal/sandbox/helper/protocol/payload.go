package protocol

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	SchemaVersion = 1

	ToolStatusSuccess = "success"
	ToolStatusError   = "error"
	ToolStatusRunning = "running"

	RootModeRead      = "read"
	RootModeReadWrite = "read_write"
)

type ErrorKind = string

const (
	ErrorKindInvalidInput      ErrorKind = "invalid_input"
	ErrorKindUnsupportedFormat ErrorKind = "unsupported_format"
	ErrorKindNotFound          ErrorKind = "not_found"
	ErrorKindIsDirectory       ErrorKind = "is_directory"
	ErrorKindNotADirectory     ErrorKind = "not_a_directory"
	ErrorKindPathEscape        ErrorKind = "path_escape"
	ErrorKindForbiddenPath     ErrorKind = "forbidden_path"
	ErrorKindTooLarge          ErrorKind = "too_large"
	ErrorKindBinaryFile        ErrorKind = "binary_file"
	ErrorKindPDFPageOutOfRange ErrorKind = "pdf_page_out_of_range"
	ErrorKindPDFCorrupt        ErrorKind = "pdf_corrupt"
	ErrorKindPDFEncrypted      ErrorKind = "pdf_encrypted"
	ErrorKindMatchNotFound     ErrorKind = "match_not_found"
	ErrorKindMatchAmbiguous    ErrorKind = "match_ambiguous"
	ErrorKindPatchParse        ErrorKind = "patch_parse"
	ErrorKindPatchVerify       ErrorKind = "patch_verify"
	ErrorKindWriteFailed       ErrorKind = "write_failed"
	ErrorKindExecSpawnFailed   ErrorKind = "exec_spawn_failed"
	ErrorKindTimeout           ErrorKind = "timeout"
	ErrorKindTaskNotFound      ErrorKind = "task_not_found"
	ErrorKindTaskExited        ErrorKind = "task_exited"
	ErrorKindTaskLost          ErrorKind = "task_lost"
	ErrorKindStdinClosed       ErrorKind = "stdin_closed"
	ErrorKindTaskLimit         ErrorKind = "task_limit"
	ErrorKindHelperFailure     ErrorKind = "helper_failure"
)

// AllErrorKinds is the one shared error enum spanning every tool, and it is
// stable API for the Bridge driver: adding a kind is a contract change, and
// downstream formatters map each kind to family-specific model text. Keep this
// slice equal to the ErrorKind* set above.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/health/health.go (helper_failure)
//   - internal/sandbox/helper/internal/cli/cli.go (envelope emission)
var AllErrorKinds = []ErrorKind{
	ErrorKindInvalidInput,
	ErrorKindUnsupportedFormat,
	ErrorKindNotFound,
	ErrorKindIsDirectory,
	ErrorKindNotADirectory,
	ErrorKindPathEscape,
	ErrorKindForbiddenPath,
	ErrorKindTooLarge,
	ErrorKindBinaryFile,
	ErrorKindPDFPageOutOfRange,
	ErrorKindPDFCorrupt,
	ErrorKindPDFEncrypted,
	ErrorKindMatchNotFound,
	ErrorKindMatchAmbiguous,
	ErrorKindPatchParse,
	ErrorKindPatchVerify,
	ErrorKindWriteFailed,
	ErrorKindExecSpawnFailed,
	ErrorKindTimeout,
	ErrorKindTaskNotFound,
	ErrorKindTaskExited,
	ErrorKindTaskLost,
	ErrorKindStdinClosed,
	ErrorKindTaskLimit,
	ErrorKindHelperFailure,
}

type Payload struct {
	SchemaVersion  int             `json:"schema_version"`
	Tool           string          `json:"tool"`
	ToolUseEventID string          `json:"tool_use_event_id"`
	WorkspaceRoot  string          `json:"workspace_root"`
	Roots          []Root          `json:"roots"`
	Limits         Limits          `json:"limits"`
	Input          json.RawMessage `json:"input"`
}

func DecodePayload(body []byte) (Payload, error) {
	var payload Payload
	if err := json.Unmarshal(body, &payload); err != nil {
		return Payload{}, err
	}
	if err := ValidatePayload(payload); err != nil {
		return Payload{}, err
	}
	return payload, nil
}

func ValidatePayload(payload Payload) error {
	switch {
	case payload.SchemaVersion != SchemaVersion:
		return errors.New("unsupported payload schema_version")
	case payload.Tool == "":
		return errors.New("payload tool is required")
	case payload.ToolUseEventID == "":
		return errors.New("payload tool_use_event_id is required")
	case payload.WorkspaceRoot == "":
		return errors.New("payload workspace_root is required")
	case len(payload.Roots) == 0:
		return errors.New("payload roots are required")
	case len(payload.Input) == 0 || string(payload.Input) == "null":
		return errors.New("payload input is required")
	}
	for _, root := range payload.Roots {
		if root.Path == "" {
			return errors.New("payload root path is required")
		}
		if root.Mode != RootModeRead && root.Mode != RootModeReadWrite {
			return errors.New("payload root mode is invalid")
		}
	}
	return nil
}

type Root struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
}

type Limits struct {
	VisibleBytes int `json:"visible_bytes"`
	VisibleLines int `json:"visible_lines"`
}

type ToolError struct {
	Kind    ErrorKind      `json:"kind"`
	Message string         `json:"message"`
	Path    string         `json:"path,omitempty"`
	Detail  map[string]any `json:"detail,omitempty"`
}

type Metrics struct {
	WallTimeMS int64 `json:"wall_time_ms"`
}

func NewToolResultEnvelope(tool string, result any, truncated bool, startedAt time.Time) (Envelope, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		SchemaVersion: SchemaVersion,
		Tool:          tool,
		Status:        ToolStatusSuccess,
		Truncated:     boolPtr(truncated),
		Error:         nil,
		Result:        encoded,
		Metrics:       metricsSince(startedAt),
	}, nil
}

func NewToolErrorEnvelope(tool string, toolError ToolError, result any, truncated bool, startedAt time.Time) (Envelope, error) {
	encoded := json.RawMessage(`{}`)
	if result != nil {
		var err error
		encoded, err = json.Marshal(result)
		if err != nil {
			return Envelope{}, err
		}
	}
	return Envelope{
		SchemaVersion: SchemaVersion,
		Tool:          tool,
		Status:        ToolStatusError,
		Truncated:     boolPtr(truncated),
		Error:         &toolError,
		Result:        encoded,
		Metrics:       metricsSince(startedAt),
	}, nil
}

func metricsSince(startedAt time.Time) *Metrics {
	if startedAt.IsZero() {
		return nil
	}
	return &Metrics{WallTimeMS: time.Since(startedAt).Milliseconds()}
}

func boolPtr(value bool) *bool {
	return &value
}
