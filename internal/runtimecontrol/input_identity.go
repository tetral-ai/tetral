package runtimecontrol

// InputIdentity is the durable reference used by both Bridge admission and
// Runner reconciliation to lock and validate existing Runtime Inbox custody.
// Queue leasing and disposition remain Runner-owned; callers check the exact
// Queue lease in their enclosing transaction where that authority is required.
type InputIdentity struct {
	WorkspaceID     string
	SessionID       string
	SessionThreadID string
	RuntimeInputID  string
	InputKind       string
	EventIDs        []string
	SequenceFrom    int64
	SequenceTo      int64
}
