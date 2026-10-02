package queue

import (
	"bytes"
	"encoding/json"
	"io"
)

// RuntimeRecoveryPayload has exactly one reviewed source. Event and handoff
// identities cannot alias each other or disappear during decode.
type RuntimeRecoveryPayload struct {
	SessionID       string `json:"session_id"`
	SessionThreadID string `json:"session_thread_id"`
	SourceEventID   string `json:"source_event_id,omitempty"`
	HandoffID       string `json:"handoff_id,omitempty"`
}

func DecodeRuntimeRecoveryPayload(raw []byte) (RuntimeRecoveryPayload, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return RuntimeRecoveryPayload{}, &ValidationError{Message: "runtime recovery payload is invalid JSON"}
	}
	_, eventPresent := fields["source_event_id"]
	_, handoffPresent := fields["handoff_id"]
	if eventPresent == handoffPresent {
		return RuntimeRecoveryPayload{}, &ValidationError{Message: "runtime recovery requires exactly one source"}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var payload RuntimeRecoveryPayload
	if err := decoder.Decode(&payload); err != nil {
		return RuntimeRecoveryPayload{}, &ValidationError{Message: "runtime recovery payload fields are invalid"}
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return RuntimeRecoveryPayload{}, &ValidationError{Message: "runtime recovery payload has trailing data"}
	}
	if payload.SessionID == "" || payload.SessionThreadID == "" || (eventPresent && payload.SourceEventID == "") || (handoffPresent && payload.HandoffID == "") {
		return RuntimeRecoveryPayload{}, &ValidationError{Message: "runtime recovery payload is missing identity"}
	}
	return payload, nil
}
