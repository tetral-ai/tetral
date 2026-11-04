package runtimecontrol

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Tool kinds of a machine-bound call's session_runtime_tool_results row: a
// call of exec_command, edit or view_image, and a write_stdin.
const (
	ToolKindMachineTool    = "machine_tool"
	ToolKindMachineControl = "machine_control"
)

// Codes and messages of the calls Engine settles as not executed itself.
const (
	CodeComputerOffline      = "computer_offline"
	CodeMachineNotProvided   = "machine_not_provided"
	CodeMachineNotStarted    = "machine_not_started"
	CodeConnectionEnded      = "connection_ended"
	CodeCancelledBeforeStart = "cancelled_before_start"
	CodeCapabilityMissing    = "capability_missing"

	MessageComputerOffline      = "computer offline"
	MessageMachineNotProvided   = "No machine could be provided for the computer."
	MessageMachineNotStarted    = "The computer's machine could not be started."
	MessageConnectionEnded      = "The command's machine connection has ended; the command cannot be controlled."
	MessageCancelledBeforeStart = "Tool execution was cancelled before it started."
	MessageToolMissing          = "The computer's CLI does not provide this tool."
	MessagePolicyUnenforceable  = "The computer's CLI cannot enforce this call's sandbox policy."
)

// ToolCallKey names one machine-bound call's tool result row.
type ToolCallKey struct {
	WorkspaceID     workspace.ID
	SessionID       string
	SessionThreadID string
	ToolUseEventID  string
}

// NotExecutedSettlement is the settlement record of a call Engine confirmed
// not executed.
func NotExecutedSettlement(code, message string) (string, error) {
	return MarshalDataJSON(struct {
		Settlement string `json:"settlement"`
		Code       string `json:"code"`
		Message    string `json:"message"`
	}{"not_executed", code, message})
}

// UnknownOutcomeSettlement is the settlement record of a call that may have
// run.
func UnknownOutcomeSettlement(message string) (string, error) {
	return MarshalDataJSON(struct {
		Settlement string `json:"settlement"`
		Message    string `json:"message"`
	}{"unknown_outcome", message})
}

// SettleToolCallTx is the one writer that moves a machine_tool or
// machine_control row from pending to terminal_unconsumed with its
// settlement record. A row that is not pending changes nothing, so every
// call settles once; it reports whether this call settled the row.
func SettleToolCallTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey, record string) (bool, error) {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state='terminal_unconsumed', result_json=$5, updated_at=clock_timestamp()
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		    AND tool_kind IN ('machine_tool','machine_control') AND execution_state='pending'`,
		string(key.WorkspaceID), key.SessionID, key.SessionThreadID, key.ToolUseEventID, record)
	if err != nil {
		return false, err
	}
	return RowsAffected(result), nil
}

// settleNotExecutedTx settles key not executed with code and message.
func settleNotExecutedTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey, code, message string) error {
	record, err := NotExecutedSettlement(code, message)
	if err != nil {
		return err
	}
	_, err = SettleToolCallTx(ctx, tx, key, record)
	return err
}

// ConsumeToolCallTx moves key's settled machine_tool or machine_control row
// to consumed by terminalEventID, the result event written for the call, so no
// later writer delivers the call again.
func ConsumeToolCallTx(ctx context.Context, tx *dbconnect.Tx, key ToolCallKey, terminalEventID, reason string) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state='consumed', consumed_by_terminal_event_id=$5, consumption_reason=$6, updated_at=clock_timestamp()
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		    AND tool_kind IN ('machine_tool','machine_control') AND execution_state='terminal_unconsumed'`,
		string(key.WorkspaceID), key.SessionID, key.SessionThreadID, key.ToolUseEventID, terminalEventID, reason)
	if err != nil {
		return err
	}
	if !RowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "machine tool call is not settled")
	}
	return nil
}
