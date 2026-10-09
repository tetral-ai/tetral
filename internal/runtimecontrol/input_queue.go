package runtimecontrol

// InputQueuePayload is the durable Queue wire shared by control birth and custody handoff.
type InputQueuePayload struct {
	WorkspaceID     string   `json:"workspace_id"`
	SessionID       string   `json:"session_id"`
	SessionThreadID string   `json:"session_thread_id"`
	RuntimeInputID  string   `json:"runtime_input_id"`
	EventIDs        []string `json:"event_ids"`
	SequenceFrom    int64    `json:"sequence_from"`
	SequenceTo      int64    `json:"sequence_to"`
	InputKind       string   `json:"input_kind"`
}
