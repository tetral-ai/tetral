package runtimecontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Dispatch record types, the downstream tool instruction each record issues.
const (
	DispatchExecute = "tool.execute"
	DispatchStdin   = "tool.stdin"
	DispatchCancel  = "tool.cancel"
)

// Dispatch record states.
const (
	DispatchWaitingInstance    = "waiting_instance"
	DispatchWaitingPreparation = "waiting_preparation"
	DispatchReady              = "ready"
	DispatchReported           = "reported"
	DispatchEnded              = "ended"
)

// Causes of a turn-end cancel and the unknown-outcome text of each.
const (
	CancelTurnEnd     = "turn_end"
	CancelInterrupt   = "interrupt"
	CancelTermination = "termination"

	MessageOutcomeUnknownTurnEnd     = "Tool execution outcome is unknown because the turn ended."
	MessageOutcomeUnknownInterrupt   = "Tool execution outcome is unknown because the Thread was interrupted."
	MessageOutcomeUnknownTermination = "Tool execution outcome is unknown because the runtime terminated."
)

// Admission outcomes besides an error.
const (
	AdmissionAdmitted            = "admitted"
	AdmissionDuplicate           = "duplicate"
	AdmissionTooLarge            = "too_large"
	AdmissionComputerNotAttached = "computer_not_attached"
)

// Limits a too_large admission names.
const (
	LimitInput    = "input"
	LimitMetadata = "metadata"
	LimitChars    = "chars"
	LimitStdin    = "stdin"
)

// Every generated identity has a fixed length, so an instruction measured
// with these values at admission is never longer when it is issued.
var (
	longestConnectionID = "conn_" + strings.Repeat("0", 32)
	longestBindingID    = "bind_" + strings.Repeat("0", 16)
	longestTraceparent  = "00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-00"
	longestWriteSeq     = int64(1<<63 - 1)
)

// ComputerRouting is the part of admission that internal/computer writes.
// That package imports this one, so Bridge passes its implementation in.
type ComputerRouting interface {
	// EnsureRequest joins the open request of an attached cloud computer
	// without a usable instance, or records one; fresh reports a request
	// recorded by this call.
	EnsureRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (requestID string, fresh bool, err error)
	// IssuePreparation issues a session.prepare for the computer on its
	// current instance's online connection.
	IssuePreparation(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (issued bool, err error)
	// PrepareAgain invalidates the computer's failed binding there and
	// issues a new session.prepare.
	PrepareAgain(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (issued bool, err error)
}

// ToolCallAdmission is one AdmitToolCall: input_json is the tool input the
// Runtime mapped, sandbox_policy_json the call's complete policy.
type ToolCallAdmission struct {
	Key               ToolCallKey
	ModelToolCallID   string
	ComputerID        string
	Tool              string
	InputJSON         string
	SandboxPolicyJSON string
	Limits            protocol.Optional[protocol.VisibleLimits]
	// Traceparent is the admitting turn's traceparent, "" without one.
	Traceparent string
}

// AdmissionOutcome is what an admission or a control answers: the outcome,
// the call's task_id, and the limit a too_large outcome exceeded.
type AdmissionOutcome struct {
	Outcome    string
	TaskID     string
	Limit      string
	LimitBytes int
}

// Dispatch is one session_tool_dispatches row.
type Dispatch struct {
	WorkspaceID     workspace.ID
	SessionID       string
	MessageID       string
	Type            string
	SessionThreadID string
	ToolUseEventID  string
	TaskID          string
	ComputerID      string
	InstanceID      string
	ConnectionID    string
	BindingID       string
	RequestID       string
	PayloadJSON     string
	State           string
	LastWriteSeq    int64
	// TraceContext is the admitting turn's traceparent; every instruction of
	// the record carries it, whoever issues it.
	TraceContext string
}

// key is the tool result row of the call that issued d.
func (d Dispatch) key() ToolCallKey {
	return ToolCallKey{WorkspaceID: d.WorkspaceID, SessionID: d.SessionID, SessionThreadID: d.SessionThreadID, ToolUseEventID: d.ToolUseEventID}
}

const dispatchColumns = `workspace_id,session_id,message_id,type,session_thread_id,tool_use_event_id,task_id,computer_id,
	COALESCE(instance_id,''),COALESCE(connection_id,''),COALESCE(binding_id,''),COALESCE(request_id,''),payload_json,state,
	COALESCE(last_write_seq,0),COALESCE(trace_context,'')`

func scanDispatch(row interface{ Scan(...any) error }) (Dispatch, error) {
	var d Dispatch
	var ws string
	err := row.Scan(&ws, &d.SessionID, &d.MessageID, &d.Type, &d.SessionThreadID, &d.ToolUseEventID, &d.TaskID, &d.ComputerID,
		&d.InstanceID, &d.ConnectionID, &d.BindingID, &d.RequestID, &d.PayloadJSON, &d.State, &d.LastWriteSeq, &d.TraceContext)
	d.WorkspaceID = workspace.ID(ws)
	return d, err
}

func queryDispatches(ctx context.Context, tx *dbconnect.Tx, query string, args ...any) ([]Dispatch, error) {
	rows, err := tx.Query(ctx, `SELECT `+dispatchColumns+` FROM session_tool_dispatches `+query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var dispatches []Dispatch
	for rows.Next() {
		d, err := scanDispatch(rows)
		if err != nil {
			return nil, err
		}
		dispatches = append(dispatches, d)
	}
	return dispatches, rows.Err()
}

// machineCall is a machine-bound call's tool result row.
type machineCall struct {
	Kind           string
	IdentityHash   string
	TaskID         string
	ExecutionState string
	ResultJSON     string
}

func readMachineCallTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey, lock bool) (machineCall, bool, error) {
	query := `SELECT tool_kind,normalized_input_hash,COALESCE(task_id,''),COALESCE(execution_state,''),COALESCE(result_json,'')
		  FROM session_runtime_tool_results
		 WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`
	if lock {
		query += ` FOR UPDATE`
	}
	var call machineCall
	err := tx.QueryRow(ctx, query, string(key.WorkspaceID), key.SessionID, key.SessionThreadID, key.ToolUseEventID).
		Scan(&call.Kind, &call.IdentityHash, &call.TaskID, &call.ExecutionState, &call.ResultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return machineCall{}, false, nil
	}
	return call, err == nil, err
}

// admittedCall is an admission's validated request: the tool input and the
// policy as the compact encodings that are measured, stored and issued.
type admittedCall struct {
	input    protocol.RawObject
	policy   protocol.RawObject
	limits   protocol.Optional[protocol.VisibleLimits]
	identity string
}

// removedInputKeys are the model-facing fields Engine removes before
// dispatch; a mapped input that still holds one is refused.
var removedInputKeys = []string{"computer_id", "sandbox_permissions", "justification"}

func validateToolCall(a ToolCallAdmission) (admittedCall, error) {
	if a.ComputerID == "" {
		return admittedCall{}, status.Error(codes.InvalidArgument, "computer_id is required")
	}
	input, err := protocol.CompactToolInput(a.Tool, []byte(a.InputJSON))
	if err != nil {
		// The decoder's text can quote the model's input, which no record holds.
		return admittedCall{}, status.Error(codes.InvalidArgument, "tool input does not match the tool's input schema")
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(input, &members); err != nil {
		return admittedCall{}, status.Error(codes.InvalidArgument, "tool input is invalid")
	}
	for _, key := range removedInputKeys {
		if _, found := members[key]; found {
			return admittedCall{}, status.Errorf(codes.InvalidArgument, "tool input must not carry %s", key)
		}
	}
	policy, _, err := protocol.CompactSandboxPolicy([]byte(a.SandboxPolicyJSON))
	if err != nil {
		return admittedCall{}, status.Errorf(codes.InvalidArgument, "sandbox policy is invalid: %v", err)
	}
	limits := ""
	if a.Limits.Set {
		limits = string(protocol.Encode(a.Limits.Value))
	}
	return admittedCall{input: input, policy: policy, limits: a.Limits,
		identity: StableRuntimeID(ToolKindMachineTool, a.ComputerID, a.Tool, string(input), string(policy), limits)}, nil
}

// AdmitToolCallTx records one machine-bound call and routes it, steps 2 to 6
// of AdmitToolCall; Bridge has verified the scope, the process, the thread
// fence and the durable route first. It returns a gRPC InvalidArgument or
// AlreadyExists status for a refused request.
func AdmitToolCallTx(ctx context.Context, tx *dbconnect.Tx, a ToolCallAdmission, routing ComputerRouting) (AdmissionOutcome, error) {
	ws := a.Key.WorkspaceID
	call, invalid := validateToolCall(a)
	existing, found, err := readMachineCallTx(ctx, tx, a.Key, true)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if found {
		if invalid != nil || existing.Kind != ToolKindMachineTool || existing.IdentityHash != call.identity {
			return AdmissionOutcome{}, status.Error(codes.AlreadyExists, "tool use id conflicts with an admitted call")
		}
		return AdmissionOutcome{Outcome: AdmissionDuplicate, TaskID: existing.TaskID}, nil
	}
	if invalid != nil {
		return AdmissionOutcome{}, invalid
	}
	taskID, messageID := id.New("task_"), id.New("msg_")
	payload := protocol.ExecuteInstruction{SessionID: a.Key.SessionID, ToolUseEventID: a.Key.ToolUseEventID, TaskID: taskID,
		Tool: a.Tool, Input: call.input, SandboxPolicy: call.policy, Limits: call.limits}
	if len(call.input) > protocol.ToolInputJSONBytes {
		return AdmissionOutcome{Outcome: AdmissionTooLarge, Limit: LimitInput, LimitBytes: protocol.ToolInputJSONBytes}, nil
	}
	measured := payload
	measured.BindingID, measured.Input = longestBindingID, protocol.RawObject("{}")
	if executeMetadata := len(protocol.Encode(protocol.Downstream{ConnectionID: longestConnectionID, MessageID: messageID,
		Payload: measured, Traceparent: protocol.Some(longestTraceparent)})); executeMetadata > protocol.MetadataJSONBytes {
		return AdmissionOutcome{Outcome: AdmissionTooLarge, Limit: LimitMetadata, LimitBytes: protocol.MetadataJSONBytes}, nil
	}
	computer, attached, err := lockComputerTx(ctx, tx, ws, a.Key.SessionID, a.ComputerID)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if !attached {
		return AdmissionOutcome{Outcome: AdmissionComputerNotAttached}, nil
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, model_tool_call_id,
			execution_state, task_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,'machine_tool',$5,$6,$7,'committed',$8,'pending',$9,clock_timestamp(),clock_timestamp())`,
		string(ws), a.Key.SessionID, a.Key.SessionThreadID, a.Key.ToolUseEventID,
		call.identity, a.Tool, string(call.input), a.ModelToolCallID, taskID); err != nil {
		return AdmissionOutcome{}, err
	}
	admitted := AdmissionOutcome{Outcome: AdmissionAdmitted, TaskID: taskID}
	record := Dispatch{WorkspaceID: ws, SessionID: a.Key.SessionID, MessageID: messageID, Type: DispatchExecute,
		SessionThreadID: a.Key.SessionThreadID, ToolUseEventID: a.Key.ToolUseEventID, TaskID: taskID, ComputerID: a.ComputerID,
		PayloadJSON: string(protocol.Encode(payload)), TraceContext: a.Traceparent}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	reviewer, err := isApprovalReviewerThreadTx(ctx, tx, a.Key)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	var connection instance.Connection
	online := false
	if computer.instanceID != "" {
		if connection, online, err = instance.ReadOnlineConnection(ctx, tx, ws, computer.instanceID, now); err != nil {
			return AdmissionOutcome{}, err
		}
	}
	switch {
	case !online && (computer.kind != computerKindCloud || reviewer):
		return admitted, settleNotExecutedTx(ctx, tx, a.Key, CodeComputerOffline, MessageComputerOffline)
	case !online:
		requestID, _, err := routing.EnsureRequest(ctx, tx, ws, a.Key.SessionID, a.ComputerID)
		if err != nil {
			return AdmissionOutcome{}, err
		}
		record.State, record.RequestID = DispatchWaitingInstance, requestID
		return admitted, insertDispatchTx(ctx, tx, record)
	}
	binding, err := activeBindingTx(ctx, tx, ws, a.ComputerID, connection.ID)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	record.State, record.InstanceID, record.ConnectionID = DispatchWaitingPreparation, connection.InstanceID, connection.ID
	issued := true
	switch binding.state {
	case "prepared":
		if err := insertDispatchTx(ctx, tx, record); err != nil {
			return AdmissionOutcome{}, err
		}
		return admitted, releaseDispatchTx(ctx, tx, record, binding.id, connection)
	case "failed":
		issued, err = routing.PrepareAgain(ctx, tx, ws, a.Key.SessionID, a.ComputerID)
	case "":
		issued, err = routing.IssuePreparation(ctx, tx, ws, a.Key.SessionID, a.ComputerID)
	}
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if !issued {
		// The connection ended after it was read; routing again finds the
		// computer offline.
		return AdmissionOutcome{}, errRouteChanged
	}
	return admitted, insertDispatchTx(ctx, tx, record)
}

// errRouteChanged aborts an admission whose route changed while it ran; the
// caller answers it as unavailable and the Runtime retries.
var errRouteChanged = errors.New("the computer's route changed during admission")

// IsRouteChanged reports an admission aborted by errRouteChanged.
func IsRouteChanged(err error) bool { return errors.Is(err, errRouteChanged) }

const computerKindCloud = "cloud"

// lockedComputer is a computer's route as its lock holder reads it:
// instanceID is the current instance of a cloud computer and the instance of
// a user computer.
type lockedComputer struct {
	kind              string
	instanceID        string
	currentInstanceID string
}

// lockComputerTx locks the attached computer computerID of sessionID FOR NO
// KEY UPDATE, the lock every admission, release, control and waiting-record
// writer takes; attached is false for a computer that is not attached.
func lockComputerTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (lockedComputer, bool, error) {
	var kind, association string
	var instanceID, currentInstanceID sql.NullString
	err := tx.QueryRow(ctx, `SELECT kind,association_state,instance_id,current_instance_id FROM computers
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 FOR NO KEY UPDATE`, string(ws), sessionID, computerID).
		Scan(&kind, &association, &instanceID, &currentInstanceID)
	if errors.Is(err, sql.ErrNoRows) {
		return lockedComputer{}, false, nil
	}
	if err != nil {
		return lockedComputer{}, false, err
	}
	computer := lockedComputer{kind: kind, instanceID: instanceID.String, currentInstanceID: currentInstanceID.String}
	if kind == computerKindCloud {
		computer.instanceID = currentInstanceID.String
	}
	return computer, association == "attached", nil
}

func isApprovalReviewerThreadTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey) (bool, error) {
	var role string
	if err := tx.QueryRow(ctx, `SELECT role FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		string(key.WorkspaceID), key.SessionID, key.SessionThreadID).Scan(&role); err != nil {
		return false, err
	}
	return role == "approval_reviewer", nil
}

type activeBinding struct{ id, state string }

// activeBindingTx reads the computer's preparing, prepared or failed binding
// on connectionID; an empty state means none.
func activeBindingTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, computerID, connectionID string) (activeBinding, error) {
	var binding activeBinding
	err := tx.QueryRow(ctx, `SELECT binding_id,state FROM computer_bindings WHERE workspace_id=$1 AND computer_id=$2 AND connection_id=$3
		AND state IN ('preparing','prepared','failed')`, string(ws), computerID, connectionID).Scan(&binding.id, &binding.state)
	if errors.Is(err, sql.ErrNoRows) {
		return activeBinding{}, nil
	}
	return binding, err
}

func insertDispatchTx(ctx context.Context, tx *dbconnect.Tx, d Dispatch) error {
	var lastWriteSeq any
	if d.Type == DispatchExecute {
		lastWriteSeq = d.LastWriteSeq
	}
	_, err := tx.Exec(ctx, `INSERT INTO session_tool_dispatches (workspace_id,session_id,message_id,type,session_thread_id,tool_use_event_id,
		task_id,computer_id,instance_id,connection_id,binding_id,request_id,payload_json,state,last_write_seq,trace_context)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),NULLIF($10,''),NULLIF($11,''),NULLIF($12,''),$13,$14,$15,NULLIF($16,''))`,
		string(d.WorkspaceID), d.SessionID, d.MessageID, d.Type, d.SessionThreadID, d.ToolUseEventID, d.TaskID, d.ComputerID,
		d.InstanceID, d.ConnectionID, d.BindingID, d.RequestID, d.PayloadJSON, d.State, lastWriteSeq, d.TraceContext)
	return err
}

func setDispatchStateTx(ctx context.Context, tx *dbconnect.Tx, d Dispatch, state string) error {
	_, err := tx.Exec(ctx, `UPDATE session_tool_dispatches SET state=$2,updated_at=clock_timestamp() WHERE message_id=$1`, d.MessageID, state)
	return err
}

// ReleaseDispatchTx moves a tool.execute record waiting for preparation on
// connection to ready on the prepared binding bindingID and issues its
// instruction, while the caller holds the computer's lock. A record outside
// the connection's capability intersection is not sent: it ends and its call
// settles not executed, capability_missing.
func ReleaseDispatchTx(ctx context.Context, tx *dbconnect.Tx, d Dispatch, bindingID string, connection instance.Connection) error {
	return releaseDispatchTx(ctx, tx, d, bindingID, connection)
}

func releaseDispatchTx(ctx context.Context, tx *dbconnect.Tx, d Dispatch, bindingID string, connection instance.Connection) error {
	payload, err := protocol.DecodeExecuteInstruction([]byte(d.PayloadJSON))
	if err != nil {
		return err
	}
	if message, err := capabilityGap(connection, payload); err != nil {
		return err
	} else if message != "" {
		if err := setDispatchStateTx(ctx, tx, d, DispatchEnded); err != nil {
			return err
		}
		return settleNotExecutedTx(ctx, tx, d.key(), CodeCapabilityMissing, message)
	}
	payload.BindingID = bindingID
	frozen := string(protocol.Encode(payload))
	if _, err := tx.Exec(ctx, `UPDATE session_tool_dispatches SET state='ready',instance_id=$2,connection_id=$3,binding_id=$4,payload_json=$5,
		updated_at=clock_timestamp() WHERE message_id=$1`, d.MessageID, connection.InstanceID, connection.ID, bindingID, frozen); err != nil {
		return err
	}
	_, err = instance.IssueInstruction(workload.ContextWithTraceparent(ctx, d.TraceContext), tx, d.WorkspaceID, connection, payload, d.MessageID)
	return err
}

// capabilityGap is the not-executed message of an instruction the
// connection's latest accepted ready does not support, "" when it does.
func capabilityGap(connection instance.Connection, payload protocol.ExecuteInstruction) (string, error) {
	if len(connection.Capabilities) == 0 {
		return MessageToolMissing, nil
	}
	capabilities, err := protocol.DecodeCapabilities(connection.Capabilities)
	if err != nil {
		return "", err
	}
	tools := capabilities.ToolExecution
	if !tools.Set || !slices.Contains(tools.Value.Tools, payload.Tool) {
		return MessageToolMissing, nil
	}
	policy, err := protocol.DecodeSandboxPolicy(payload.SandboxPolicy)
	if err != nil {
		return "", err
	}
	if !slices.Contains(tools.Value.SandboxPolicyVersions, 1) || !slices.Contains(tools.Value.NetworkModes, policy.Network.Mode) {
		return MessagePolicyUnenforceable, nil
	}
	return "", nil
}

// AwaitPreparationTx moves every tool.execute record waiting on requestID to
// waiting for preparation on connection, the connection whose accepted ready
// fulfilled the request. Its caller is internal/computer's fulfilment.
func AwaitPreparationTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID, requestID string, connection instance.Connection) error {
	if _, _, err := lockComputerTx(ctx, tx, ws, sessionID, computerID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE session_tool_dispatches SET state='waiting_preparation',instance_id=$3,connection_id=$4,updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND request_id=$2 AND state='waiting_instance'`, string(ws), requestID, connection.InstanceID, connection.ID)
	return err
}

// ReleasePreparedTx releases every tool.execute record of computerID waiting
// for preparation on connection, now that binding bindingID there is
// prepared. Its caller is internal/computer's preparation report writer.
func ReleasePreparedTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID, bindingID string, connection instance.Connection) error {
	if _, _, err := lockComputerTx(ctx, tx, ws, sessionID, computerID); err != nil {
		return err
	}
	waiting, err := queryDispatches(ctx, tx, `WHERE workspace_id=$1 AND connection_id=$2 AND computer_id=$3 AND state='waiting_preparation'
		ORDER BY created_at,message_id FOR UPDATE`, string(ws), connection.ID, computerID)
	if err != nil {
		return err
	}
	for _, d := range waiting {
		if err := releaseDispatchTx(ctx, tx, d, bindingID, connection); err != nil {
			return err
		}
	}
	return nil
}

// EndPreparationWaitersTx ends every tool.execute record of computerID
// waiting for preparation on connectionID and settles each call not
// executed with code and message, a failed preparation's error. Its caller
// is internal/computer's preparation report writer.
func EndPreparationWaitersTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID, connectionID, code, message string) error {
	if _, _, err := lockComputerTx(ctx, tx, ws, sessionID, computerID); err != nil {
		return err
	}
	return endWaitingTx(ctx, tx, code, message, `WHERE workspace_id=$1 AND connection_id=$2 AND computer_id=$3 AND state='waiting_preparation'
		ORDER BY created_at,message_id FOR UPDATE`, string(ws), connectionID, computerID)
}

// EndRequestWaitersTx ends every tool.execute record waiting on requestID, a
// request of kind new or wake that failed or expired, and settles each call
// not executed: machine_not_provided for new, machine_not_started for wake.
// Its callers are internal/computer's fail and expiry writers.
func EndRequestWaitersTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID, requestID, kind string) error {
	if _, _, err := lockComputerTx(ctx, tx, ws, sessionID, computerID); err != nil {
		return err
	}
	code, message := CodeMachineNotProvided, MessageMachineNotProvided
	if kind == "wake" {
		code, message = CodeMachineNotStarted, MessageMachineNotStarted
	}
	return endWaitingTx(ctx, tx, code, message, `WHERE workspace_id=$1 AND request_id=$2 AND state='waiting_instance'
		ORDER BY created_at,message_id FOR UPDATE`, string(ws), requestID)
}

// EndConnectionCallsTx is the tool part of connectionID's end, run in the
// transaction that ends it, which holds the runtime mutation locks of the
// Sessions ConnectionCallSessionsTx returns: every record
// issued on the connection without a stored final report ends, and its call,
// when still pending, settles as "computer offline, result unknown"; every
// call waiting for a preparation report on it ends not executed with
// "computer offline"; and every background command of it still running ends
// unknown_outcome (endConnectionTasksTx). Like every record writer it locks
// the records' computers first, in id order.
func EndConnectionCallsTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string, now time.Time) error {
	if _, err := tx.Exec(ctx, `SELECT 1 FROM computers WHERE workspace_id=$1 AND id IN (SELECT computer_id FROM session_tool_dispatches
		WHERE workspace_id=$1 AND connection_id=$2 AND state IN ('ready','waiting_preparation'))
		ORDER BY id FOR NO KEY UPDATE`, string(ws), connectionID); err != nil {
		return err
	}
	issued, err := queryDispatches(ctx, tx, `WHERE workspace_id=$1 AND connection_id=$2 AND state='ready'
		ORDER BY created_at,message_id FOR UPDATE`, string(ws), connectionID)
	if err != nil {
		return err
	}
	for _, d := range issued {
		if d.Type != DispatchCancel {
			if _, err := settleUnknownOutcomeIfPendingTx(ctx, tx, d.key(), MessageComputerOfflineResultUnknown); err != nil {
				return err
			}
		}
		if err := setDispatchStateTx(ctx, tx, d, DispatchEnded); err != nil {
			return err
		}
	}
	if err := endWaitingTx(ctx, tx, CodeComputerOffline, MessageComputerOffline, `WHERE workspace_id=$1 AND connection_id=$2 AND state='waiting_preparation'
		ORDER BY created_at,message_id FOR UPDATE`, string(ws), connectionID); err != nil {
		return err
	}
	return endConnectionTasksTx(ctx, tx, ws, connectionID, now)
}

func endWaitingTx(ctx context.Context, tx *dbconnect.Tx, code, message, query string, args ...any) error {
	waiting, err := queryDispatches(ctx, tx, query, args...)
	if err != nil {
		return err
	}
	for _, d := range waiting {
		if err := setDispatchStateTx(ctx, tx, d, DispatchEnded); err != nil {
			return err
		}
		if err := settleNotExecutedTx(ctx, tx, d.key(), code, message); err != nil {
			return err
		}
	}
	return nil
}

// ToolControlAdmission is one AdmitToolControl, the model's write_stdin to
// task TaskID.
type ToolControlAdmission struct {
	Key             ToolCallKey
	ModelToolCallID string
	TaskID          string
	Chars           string
	WaitMS          protocol.Nullable[int64]
	Limits          protocol.Optional[protocol.VisibleLimits]
}

func (c ToolControlAdmission) identity() string {
	waitMS, limits := "", ""
	if c.WaitMS.Present {
		waitMS = strconv.FormatInt(c.WaitMS.Value, 10)
	}
	if c.Limits.Set {
		limits = string(protocol.Encode(c.Limits.Value))
	}
	return StableRuntimeID(ToolKindMachineControl, c.TaskID, c.Chars, waitMS, limits)
}

// AdmitToolControlTx records one write_stdin and routes it on its task's
// recorded route, steps 1 to 4 of AdmitToolControl after Bridge verified the
// scope and the durable route.
func AdmitToolControlTx(ctx context.Context, tx *dbconnect.Tx, c ToolControlAdmission) (AdmissionOutcome, error) {
	ws := c.Key.WorkspaceID
	existing, found, err := readMachineCallTx(ctx, tx, c.Key, true)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if found {
		if existing.Kind != ToolKindMachineControl || existing.IdentityHash != c.identity() {
			return AdmissionOutcome{}, status.Error(codes.AlreadyExists, "tool use id conflicts with an admitted control")
		}
		return AdmissionOutcome{Outcome: AdmissionDuplicate, TaskID: existing.TaskID}, nil
	}
	executes, err := queryDispatches(ctx, tx, `WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND type='tool.execute'`,
		string(ws), c.Key.SessionID, c.TaskID)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if len(executes) != 1 {
		return AdmissionOutcome{}, status.Error(codes.InvalidArgument, "task_id names no machine tool call of the Session")
	}
	if !utf8.ValidString(c.Chars) {
		return AdmissionOutcome{}, status.Error(codes.InvalidArgument, "chars must be UTF-8")
	}
	if len(c.Chars) > protocol.StdinUTF8Bytes {
		return AdmissionOutcome{Outcome: AdmissionTooLarge, Limit: LimitChars, LimitBytes: protocol.StdinUTF8Bytes}, nil
	}
	payload := protocol.StdinPayload{SessionID: c.Key.SessionID, ToolUseEventID: c.Key.ToolUseEventID, TaskID: c.TaskID,
		Chars: c.Chars, WaitMS: c.WaitMS, Limits: c.Limits}
	messageID := id.New("msg_")
	measured := payload
	measured.BindingID = longestBindingID
	if c.Chars != "" {
		measured.WriteSeq = protocol.Some(longestWriteSeq)
	}
	if size := len(protocol.Encode(protocol.Downstream{ConnectionID: longestConnectionID, MessageID: messageID, Payload: measured,
		Traceparent: protocol.Some(longestTraceparent)})); size > protocol.StdinSSEJSONBytes {
		return AdmissionOutcome{Outcome: AdmissionTooLarge, Limit: LimitStdin, LimitBytes: protocol.StdinSSEJSONBytes}, nil
	}
	computer, attached, err := lockComputerTx(ctx, tx, ws, c.Key.SessionID, executes[0].ComputerID)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	controlInput, err := MarshalDataJSON(struct {
		TaskID string `json:"task_id"`
		Chars  string `json:"chars"`
	}{c.TaskID, c.Chars})
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, model_tool_call_id,
			execution_state, task_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,'machine_control',$5,'write_stdin',$6,'committed',$7,'pending',$8,clock_timestamp(),clock_timestamp())`,
		string(ws), c.Key.SessionID, c.Key.SessionThreadID, c.Key.ToolUseEventID,
		c.identity(), controlInput, c.ModelToolCallID, c.TaskID); err != nil {
		return AdmissionOutcome{}, err
	}
	admitted := AdmissionOutcome{Outcome: AdmissionAdmitted, TaskID: c.TaskID}
	execute, err := lockDispatchTx(ctx, tx, executes[0].MessageID)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if c.Chars != "" {
		if err := tx.QueryRow(ctx, `UPDATE session_tool_dispatches SET last_write_seq=last_write_seq+1,updated_at=clock_timestamp()
			WHERE message_id=$1 RETURNING last_write_seq`, execute.MessageID).Scan(&execute.LastWriteSeq); err != nil {
			return AdmissionOutcome{}, err
		}
		payload.WriteSeq = protocol.Some(execute.LastWriteSeq)
	}
	connection, routed, err := controlRouteTx(ctx, tx, execute, computer, attached)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if !routed {
		return admitted, settleNotExecutedTx(ctx, tx, c.Key, CodeConnectionEnded, MessageConnectionEnded)
	}
	payload.BindingID = execute.BindingID
	record := Dispatch{WorkspaceID: ws, SessionID: c.Key.SessionID, MessageID: messageID, Type: DispatchStdin,
		SessionThreadID: c.Key.SessionThreadID, ToolUseEventID: c.Key.ToolUseEventID, TaskID: c.TaskID, ComputerID: execute.ComputerID,
		InstanceID: execute.InstanceID, ConnectionID: execute.ConnectionID, BindingID: execute.BindingID,
		PayloadJSON: string(protocol.Encode(payload)), State: DispatchReady, TraceContext: execute.TraceContext}
	issued, err := issueControlTx(ctx, tx, record, connection, payload)
	if err != nil {
		return AdmissionOutcome{}, err
	}
	if !issued {
		return admitted, settleNotExecutedTx(ctx, tx, c.Key, CodeConnectionEnded, MessageConnectionEnded)
	}
	return admitted, nil
}

func lockDispatchTx(ctx context.Context, tx *dbconnect.Tx, messageID string) (Dispatch, error) {
	return scanDispatch(tx.QueryRow(ctx, `SELECT `+dispatchColumns+` FROM session_tool_dispatches WHERE message_id=$1 FOR UPDATE`, messageID))
}

// controlRouteTx reads the connection a control of execute's task goes to:
// the task's recorded connection while it is still its instance's live
// connection and that instance is still the computer's current instance.
func controlRouteTx(ctx context.Context, tx *dbconnect.Tx, execute Dispatch, computer lockedComputer, attached bool) (instance.Connection, bool, error) {
	if !attached || execute.BindingID == "" || computer.instanceID != execute.InstanceID {
		return instance.Connection{}, false, nil
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return instance.Connection{}, false, err
	}
	return instance.ReadLiveConnection(ctx, tx, execute.WorkspaceID, execute.InstanceID, execute.ConnectionID, now)
}

// issueControlTx stores a ready control record and issues its instruction;
// issued is false, with nothing stored, when the connection ended after the
// route was read.
func issueControlTx(ctx context.Context, tx *dbconnect.Tx, record Dispatch, connection instance.Connection, payload protocol.DownstreamPayload) (bool, error) {
	if err := insertDispatchTx(ctx, tx, record); err != nil {
		return false, err
	}
	_, err := instance.IssueInstruction(workload.ContextWithTraceparent(ctx, record.TraceContext), tx, record.WorkspaceID, connection, payload, record.MessageID)
	var stale *instance.StaleConnectionError
	if errors.As(err, &stale) {
		return false, setDispatchStateTx(ctx, tx, record, DispatchEnded)
	}
	return err == nil, err
}

// CancelToolCallTx is the one writer that cancels a machine-bound call when
// its turn ends (CancelTurnEnd), its thread is interrupted (CancelInterrupt)
// or its runtime terminates (CancelTermination), settling it through
// SettleToolCallTx: a call never sent ends not executed with nothing sent; a
// sent one gets its task's single tool.cancel and settles as an unknown
// outcome; a pending write_stdin gets no tool.cancel. A call that is not
// pending changes nothing. It reports whether it settled the call.
func CancelToolCallTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey, cause string) (bool, error) {
	unknown := MessageOutcomeUnknownTurnEnd
	switch cause {
	case CancelInterrupt:
		unknown = MessageOutcomeUnknownInterrupt
	case CancelTermination:
		unknown = MessageOutcomeUnknownTermination
	}
	call, found, err := readMachineCallTx(ctx, tx, key, false)
	if err != nil || !found || call.ExecutionState != "pending" {
		return false, err
	}
	if call.Kind == ToolKindMachineControl {
		return settleUnknownOutcomeIfPendingTx(ctx, tx, key, unknown)
	}
	if call.Kind != ToolKindMachineTool {
		return false, status.Error(codes.FailedPrecondition, "tool use is not a machine-bound call")
	}
	executes, err := queryDispatches(ctx, tx, `WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		AND type='tool.execute'`, string(key.WorkspaceID), key.SessionID, key.SessionThreadID, key.ToolUseEventID)
	if err != nil {
		return false, err
	}
	if len(executes) != 1 {
		return false, errors.New("a pending machine tool call has no dispatch record")
	}
	computer, attached, err := lockComputerTx(ctx, tx, key.WorkspaceID, key.SessionID, executes[0].ComputerID)
	if err != nil {
		return false, err
	}
	if call, _, err = readMachineCallTx(ctx, tx, key, true); err != nil || call.ExecutionState != "pending" {
		return false, err
	}
	execute, err := lockDispatchTx(ctx, tx, executes[0].MessageID)
	if err != nil {
		return false, err
	}
	switch execute.State {
	case DispatchWaitingInstance, DispatchWaitingPreparation:
		if err := setDispatchStateTx(ctx, tx, execute, DispatchEnded); err != nil {
			return false, err
		}
		return true, settleNotExecutedTx(ctx, tx, key, CodeCancelledBeforeStart, MessageCancelledBeforeStart)
	case DispatchReady:
		if err := cancelTaskTx(ctx, tx, execute, computer, attached); err != nil {
			return false, err
		}
	}
	return settleUnknownOutcomeIfPendingTx(ctx, tx, key, unknown)
}

func settleUnknownOutcomeIfPendingTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey, message string) (bool, error) {
	record, err := UnknownOutcomeSettlement(message)
	if err != nil {
		return false, err
	}
	return SettleToolCallTx(ctx, tx, key, record)
}

// CancelRunningTaskTx gives the task of key's tool.execute record its single
// tool.cancel, for a call whose running report was stored but not committed
// by the Runtime when its thread was interrupted or terminated. Its callers
// are Bridge's interrupt closeout and a Runtime termination.
func CancelRunningTaskTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey) error {
	executes, err := queryDispatches(ctx, tx, `WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		AND type='tool.execute'`, string(key.WorkspaceID), key.SessionID, key.SessionThreadID, key.ToolUseEventID)
	if err != nil || len(executes) != 1 {
		return err
	}
	computer, attached, err := lockComputerTx(ctx, tx, key.WorkspaceID, key.SessionID, executes[0].ComputerID)
	if err != nil {
		return err
	}
	execute, err := lockDispatchTx(ctx, tx, executes[0].MessageID)
	if err != nil {
		return err
	}
	return cancelTaskTx(ctx, tx, execute, computer, attached)
}

// cancelTaskTx inserts the task's one tool.cancel record, on the original
// route, when the task has none yet, and issues it when the route still
// passes the control check; otherwise the record ends unsent. The caller
// holds the computer's lock and the tool.execute record's lock.
func cancelTaskTx(ctx context.Context, tx *dbconnect.Tx, execute Dispatch, computer lockedComputer, attached bool) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_tool_dispatches WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND type='tool.cancel')`,
		string(execute.WorkspaceID), execute.SessionID, execute.TaskID).Scan(&exists); err != nil || exists {
		return err
	}
	payload := protocol.CancelPayload{SessionID: execute.SessionID, BindingID: execute.BindingID, ToolUseEventID: id.New("tcan_"), TaskID: execute.TaskID}
	record := Dispatch{WorkspaceID: execute.WorkspaceID, SessionID: execute.SessionID, MessageID: id.New("msg_"), Type: DispatchCancel,
		SessionThreadID: execute.SessionThreadID, ToolUseEventID: payload.ToolUseEventID, TaskID: execute.TaskID, ComputerID: execute.ComputerID,
		InstanceID: execute.InstanceID, ConnectionID: execute.ConnectionID, BindingID: execute.BindingID,
		PayloadJSON: string(protocol.Encode(payload)), State: DispatchReady, TraceContext: execute.TraceContext}
	connection, routed, err := controlRouteTx(ctx, tx, execute, computer, attached)
	if err != nil {
		return err
	}
	if !routed {
		record.State = DispatchEnded
		return insertDispatchTx(ctx, tx, record)
	}
	_, err = issueControlTx(ctx, tx, record, connection, payload)
	return err
}
