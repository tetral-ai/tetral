package eventwire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"unicode/utf8"
)

// These protocol bounds are checked against the Gateway's canonical
// protocol/src/preview-limits.json by the cross-language contract guard.
const MaxPreviewFrameBytes = 256 * 1024
const MaxPreviewEventIdentities = 4096

type PreviewFrame struct {
	Version                  int     `json:"version"`
	WorkspaceID              string  `json:"workspace_id"`
	SessionID                string  `json:"session_id"`
	ThreadID                 string  `json:"thread_id"`
	ModelRequestID           string  `json:"model_request_id"`
	ModelRequestStartEventID string  `json:"model_request_start_event_id"`
	RequestKind              string  `json:"request_kind"`
	Kind                     string  `json:"kind"`
	EventType                string  `json:"event_type,omitempty"`
	EventID                  string  `json:"event_id,omitempty"`
	PreviewSequence          *int64  `json:"preview_sequence,omitempty"`
	Text                     *string `json:"text,omitempty"`
}

func PreviewSubject(workspaceID, sessionID string) string {
	return "preview.v1." + base64.RawURLEncoding.EncodeToString([]byte(workspaceID)) + "." + base64.RawURLEncoding.EncodeToString([]byte(sessionID))
}

// DecodePreviewFrame enforces one strict, bounded private vocabulary before
// admission. JSON replacement of isolated surrogate halves is never a prefix.
func DecodePreviewFrame(subject string, data []byte) (PreviewFrame, error) {
	invalid := errors.New("invalid preview frame")
	if len(data) > MaxPreviewFrameBytes || !utf8.Valid(data) || !validJSONUnicode(data) {
		return PreviewFrame{}, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return PreviewFrame{}, invalid
	}
	fields := map[string]struct{}{}
	var frame PreviewFrame
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return PreviewFrame{}, invalid
		}
		name, ok := key.(string)
		if !ok {
			return PreviewFrame{}, invalid
		}
		if _, ok := fields[name]; ok {
			return PreviewFrame{}, invalid
		}
		var target any
		switch name {
		case "version":
			target = &frame.Version
		case "workspace_id":
			target = &frame.WorkspaceID
		case "session_id":
			target = &frame.SessionID
		case "thread_id":
			target = &frame.ThreadID
		case "model_request_id":
			target = &frame.ModelRequestID
		case "model_request_start_event_id":
			target = &frame.ModelRequestStartEventID
		case "request_kind":
			target = &frame.RequestKind
		case "kind":
			target = &frame.Kind
		case "event_type":
			target = &frame.EventType
		case "event_id":
			target = &frame.EventID
		case "preview_sequence":
			target = &frame.PreviewSequence
		case "text":
			target = &frame.Text
		default:
			return PreviewFrame{}, invalid
		}
		if decoder.Decode(target) != nil {
			return PreviewFrame{}, invalid
		}
		fields[name] = struct{}{}
	}
	if _, err := decoder.Token(); err != nil {
		return PreviewFrame{}, invalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return PreviewFrame{}, invalid
	}
	if frame.Version != 1 || frame.RequestKind != "agent_provider_request" || !validIdentity(frame.WorkspaceID) || !validIdentity(frame.SessionID) || !validIdentity(frame.ThreadID) || !validIdentity(frame.ModelRequestID) || !validIdentity(frame.ModelRequestStartEventID) || subject != PreviewSubject(frame.WorkspaceID, frame.SessionID) {
		return PreviewFrame{}, invalid
	}
	switch frame.Kind {
	case "request_open":
		if len(fields) != 8 {
			return PreviewFrame{}, invalid
		}
	case "event_start", "event_delta":
		if !validIdentity(frame.EventID) || (frame.EventType != "agent.message" && frame.EventType != "agent.thinking") || frame.PreviewSequence == nil || *frame.PreviewSequence < 0 || *frame.PreviewSequence > 9007199254740991 {
			return PreviewFrame{}, invalid
		}
		if frame.Kind == "event_start" {
			if len(fields) != 11 || *frame.PreviewSequence != 0 {
				return PreviewFrame{}, invalid
			}
		} else {
			if len(fields) != 12 || frame.EventType != "agent.message" || *frame.PreviewSequence == 0 || frame.Text == nil || *frame.Text == "" {
				return PreviewFrame{}, invalid
			}
		}
	default:
		return PreviewFrame{}, invalid
	}
	return frame, nil
}

func validIdentity(value string) bool {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

// validJSONUnicode inspects escapes rather than the decoded replacement value.
func validJSONUnicode(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		i++
		for i < len(data) && data[i] != '"' {
			if data[i] != '\\' {
				i++
				continue
			}
			i++
			if i >= len(data) {
				return false
			}
			if data[i] != 'u' {
				i++
				continue
			}
			if i+4 >= len(data) {
				return false
			}
			n, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
			if err != nil {
				return false
			}
			i += 5
			if n >= 0xD800 && n <= 0xDBFF {
				if i+5 >= len(data) || data[i] != '\\' || data[i+1] != 'u' {
					return false
				}
				low, err := strconv.ParseUint(string(data[i+2:i+6]), 16, 16)
				if err != nil || low < 0xDC00 || low > 0xDFFF {
					return false
				}
				i += 6
			} else if n >= 0xDC00 && n <= 0xDFFF {
				return false
			}
		}
	}
	return true
}

// PublicPreviewEncodedBytes computes the exact public wrapper size before any
// encoded output allocation. JSON string escaping matches Go's default HTML-
// escaping rules. It does not retain/copy text or private scope fields.
func PublicPreviewEncodedBytes(frame PreviewFrame) (int, error) {
	if !utf8.ValidString(frame.EventID) {
		return 0, errors.New("invalid preview identity")
	}
	switch frame.Kind {
	case "event_start":
		if frame.EventType != "agent.message" && frame.EventType != "agent.thinking" {
			return 0, errors.New("unsupported preview event")
		}
		return len(`{"type":"event_start","event":{"id":,"type":}}`) + quotedJSONBytes(frame.EventID) + quotedJSONBytes(frame.EventType), nil
	case "event_delta":
		if frame.EventType != "agent.message" || frame.Text == nil || !utf8.ValidString(*frame.Text) {
			return 0, errors.New("unsupported preview delta")
		}
		return len(`{"delta":{"content":{"text":,"type":"text"},"index":0,"type":"content_delta"},"event_id":,"type":"event_delta"}`) + quotedJSONBytes(*frame.Text) + quotedJSONBytes(frame.EventID), nil
	default:
		return 0, errors.New("unsupported preview kind")
	}
}

// MarshalPreviewEvent allocates only its exact reserved output. Separate from
// durable row projection, the two fixed SDK wrappers contain no durable metadata
// or internal scope/sequence fields. No intermediate encoded buffer is retained.
func MarshalPreviewEvent(frame PreviewFrame) ([]byte, error) {
	size, err := PublicPreviewEncodedBytes(frame)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, size)
	if frame.Kind == "event_start" {
		data = append(data, `{"type":"event_start","event":{"id":`...)
		data = appendQuotedJSON(data, frame.EventID)
		data = append(data, `,"type":`...)
		data = appendQuotedJSON(data, frame.EventType)
		data = append(data, "}}"...)
	} else {
		data = append(data, `{"delta":{"content":{"text":`...)
		data = appendQuotedJSON(data, *frame.Text)
		data = append(data, `,"type":"text"},"index":0,"type":"content_delta"},"event_id":`...)
		data = appendQuotedJSON(data, frame.EventID)
		data = append(data, `,"type":"event_delta"}`...)
	}
	return data, nil
}

func quotedJSONBytes(value string) int {
	size := 2
	for _, r := range value {
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			size += 2
		case '<', '>', '&', '\u2028', '\u2029':
			size += 6
		default:
			if r < 0x20 {
				size += 6
			} else {
				size += utf8.RuneLen(r)
			}
		}
	}
	return size
}
func appendQuotedJSON(data []byte, value string) []byte {
	const hex = "0123456789abcdef"
	data = append(data, '"')
	for i := 0; i < len(value); i++ {
		b := value[i]
		switch b {
		case '"', '\\':
			data = append(data, '\\', b)
		case '\b':
			data = append(data, `\b`...)
		case '\f':
			data = append(data, `\f`...)
		case '\n':
			data = append(data, `\n`...)
		case '\r':
			data = append(data, `\r`...)
		case '\t':
			data = append(data, `\t`...)
		default:
			if b < 0x20 || b == '<' || b == '>' || b == '&' {
				data = append(data, '\\', 'u', '0', '0', hex[b>>4], hex[b&15])
			} else if b == 0xe2 && i+2 < len(value) && value[i+1] == 0x80 && (value[i+2] == 0xa8 || value[i+2] == 0xa9) {
				data = append(data, '\\', 'u', '2', '0', '2', hex[value[i+2]&15])
				i += 2
			} else {
				data = append(data, b)
			}
		}
	}
	return append(data, '"')
}
