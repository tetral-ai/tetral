package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tasks protocol-family boundary.

// CommitTaskNotificationResult settles a background task's late notification and
// is also the read/replay side of the pre-inbox task-notification exhaustion
// fence. A task_notification can fail delivery before its session_runtime_inbox
// row ever exists, so ordinary inbox finalization can never fire for it; the
// delivery path records "notification delivery exhausted" instead, in the same
// bridge_operations record family this call reads (the two dedupe surfaces are
// one surface). Semantics of that fence, as observed here:
//
//   - The exhausted fact is recorded ORTHOGONALLY to the task row's
//     compare-and-set status column — it never writes status or terminal_event_id,
//     so a still-running task stays running and a later legitimate poll completion
//     can still settle the real result through settleBackgroundTaskTx.
//   - Its emission carries exactly one session.error (unknown_error,
//     retry_status = exhausted).
//   - A later redelivery of the same runtime_input_id observes the stored
//     disposition and returns it without re-emitting (the exhausted arm below).
//   - A notification whose task row is absent, already terminal, or
//     cancelled_by_cleanup keeps a silent stale-ACK; the fence applies only while
//     the task row exists and is running.
func (s *PostgreSQLBridgeAPIStore) CommitTaskNotificationResult(ctx context.Context, request *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error) {
	if request.GetRuntimeInputId() == "" || request.GetTaskId() == "" || request.GetResultJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid task notification result request")
	}
	resultJSON := stripInternalProviderFields(request.GetResultJson())
	if !json.Valid([]byte(resultJSON)) {
		return nil, status.Error(codes.InvalidArgument, "task notification result must be JSON")
	}
	terminalStatus, err := terminalStatusFromResultJSON(resultJSON)
	if err != nil {
		return nil, err
	}
	taskID, sourceToolUseEventID, err := taskNotificationResultIdentity(resultJSON)
	if err != nil {
		return nil, err
	}
	if taskID != request.GetTaskId() {
		return nil, status.Error(codes.InvalidArgument, "task notification result task id mismatch")
	}
	resultJSON, err = canonicalTaskNotificationPayloadJSON(request.GetTaskId(), sourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := request.GetTaskId() + ":" + request.GetRuntimeInputId()
	requestHash := bridgeRequestHash(bridgeOpCommitTaskNotificationResult, key, resultJSON)
	now := s.now()
	var ack *bridgev1.BridgeWriteAck
	var runtimeMessageJSON string
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_task_notification_result", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpCommitTaskNotificationResult, key); err != nil {
			return err
		} else if ok {
			if taskNotificationOperationIsExhausted(existing.ResultJSON) {
				ack = rejectedAck(defaultString(existing.ErrorCode, "task_notification_delivery_exhausted"))
				return nil
			}
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "task notification idempotency conflict")
			}
			if existing.AckStatus == bridgeAckRejected {
				ack = rejectedAck(defaultString(existing.ErrorCode, "task_notification_stale"))
				return nil
			}
			runtimeMessageJSON, err = taskNotificationRuntimeMessageFromOperationResultTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetTaskId(),
				sourceToolUseEventID,
				existing.ResultJSON,
			)
			if err != nil {
				return err
			}
			ack = duplicateAck(request.GetRuntimeInputId(), "")
			return nil
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'committed',
			        committed_at = COALESCE(committed_at, $8),
			        updated_at = $8
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND runtime_input_id = $4
			    AND binding_id = $5
			    AND binding_generation = $6
			    AND target_pod_uid = $7
			    AND input_kind = 'task_notification'
			    AND status IN ('delivering', 'accepted', 'committed')`,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(),
			request.GetRuntimeInputId(),
			request.GetScope().GetBinding().GetBindingId(),
			request.GetScope().GetBinding().GetBindingGeneration(),
			request.GetScope().GetBinding().GetTargetPodUid(),
			now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return status.Error(codes.FailedPrecondition, "task notification input is not deliverable")
		}
		settled, committedRuntimeMessageJSON, err := settleBackgroundTaskTx(ctx, tx, request.GetScope(), request.GetTaskId(), sourceToolUseEventID, terminalStatus, resultJSON, now)
		if err != nil {
			return err
		}
		if !settled {
			resultJSON, err := marshalBridgeJSON(map[string]any{
				"runtime_input_id": request.GetRuntimeInputId(),
				"task_id":          request.GetTaskId(),
				"status":           "stale",
			})
			if err != nil {
				return err
			}
			if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
				Operation:      bridgeOpCommitTaskNotificationResult,
				IdempotencyKey: key,
				RequestHash:    requestHash,
				AckStatus:      bridgeAckRejected,
				RuntimeInputID: sql.NullString{String: request.GetRuntimeInputId(), Valid: true},
				ErrorCode:      sql.NullString{String: "task_notification_stale", Valid: true},
				ResultJSON:     resultJSON,
				Now:            now,
			}); err != nil {
				return err
			}
			ack = rejectedAck("task_notification_stale")
			return nil
		}
		runtimeMessageJSON = committedRuntimeMessageJSON
		operationResultJSON, err := taskNotificationOperationResultJSON(resultJSON, runtimeMessageJSON)
		if err != nil {
			return err
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation:      bridgeOpCommitTaskNotificationResult,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			RuntimeInputID: sql.NullString{String: request.GetRuntimeInputId(), Valid: true},
			ResultJSON:     operationResultJSON,
			Now:            now,
		}); err != nil {
			return err
		}
		ack = committedAck(request.GetRuntimeInputId(), "")
		return nil
	}); err != nil {
		return nil, err
	}
	return &bridgev1.CommitTaskNotificationResultResponse{Ack: ack, RuntimeMessageJson: runtimeMessageJSON}, nil
}

func (s *PostgreSQLBridgeAPIStore) ReadCommandResult(ctx context.Context, request *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error) {
	if request.GetScope() == nil || request.GetTaskId() == "" || request.GetToolUseEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid read command result request")
	}
	operation := bridgeOpReadCommandResult
	maxOutputTokens := positiveInt32(request.GetMaxOutputTokens())
	requestID, key, payloadHashPart := readCommandResultOwnerIdentity(
		request.GetToolUseEventId(),
		request.GetTaskId(),
		request.GetDeferTerminalSettlement(),
		maxOutputTokens,
	)
	scope := copyRuntimeScopeWithRequestID(request.GetScope(), requestID)
	options := commandOperationOptions{ClaimDrainingRead: true, DeferTerminalSettlement: request.GetDeferTerminalSettlement()}
	options.RequireTaskRowForReplay = true
	options.MaxOutputTokens = maxOutputTokens
	options.ToolUseEventID = request.GetToolUseEventId()
	result, err := s.runCommandOperation(ctx, scope, operation, key, payloadHashPart, request.GetTaskId(), options, func(reference SandboxCommandReference, _ string) (SandboxCommandResult, error) {
		if s.SandboxToolExecutor == nil {
			return SandboxCommandResult{ResultJSON: mustMarshalToolRuntimeError("sandbox_helper_unavailable", "sandbox helper command polling is not installed", true)}, nil
		}
		return s.SandboxToolExecutor.ReadCommandResult(ctx, reference)
	})
	if err != nil {
		return nil, err
	}
	return &bridgev1.ReadCommandResultResponse{Ack: committedAck("", ""), ResultJson: result.ResultJSON}, nil
}

func (s *PostgreSQLBridgeAPIStore) SendCommandInput(ctx context.Context, request *bridgev1.SendCommandInputRequest) (*bridgev1.SendCommandInputResponse, error) {
	if request.GetScope().GetRequestId() == "" || request.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid send command input request")
	}
	if strings.TrimSpace(request.GetInputJson()) == "" {
		return nil, status.Error(codes.InvalidArgument, "empty command input must use read command result")
	}
	if request.GetInputJson() != "" && !json.Valid([]byte(request.GetInputJson())) {
		return nil, status.Error(codes.InvalidArgument, "command input must be JSON")
	}
	if commandInputCharsEmpty(request.GetInputJson()) {
		return nil, status.Error(codes.InvalidArgument, "empty command chars must use read command result")
	}
	maxOutputTokens := positiveInt32(request.GetMaxOutputTokens())
	key := request.GetTaskId() + ":" + request.GetScope().GetRequestId()
	result, err := s.runCommandOperation(ctx, request.GetScope(), bridgeOpSendCommandInput, key, commandPayloadHashPart(request.GetTaskId(), request.GetInputJson(), maxOutputTokens), request.GetTaskId(), commandOperationOptions{AllocateWriteSeq: true, InputJSON: request.GetInputJson(), MaxOutputTokens: maxOutputTokens, ToolUseEventID: request.GetToolUseEventId()}, func(reference SandboxCommandReference, inputJSON string) (SandboxCommandResult, error) {
		if s.SandboxToolExecutor == nil {
			return SandboxCommandResult{ResultJSON: mustMarshalToolRuntimeError("sandbox_helper_unavailable", "sandbox helper command input is not installed", true)}, nil
		}
		return s.SandboxToolExecutor.SendCommandInput(ctx, SandboxCommandInput{SandboxCommandReference: reference, InputJSON: inputJSON})
	})
	if err != nil {
		return nil, err
	}
	return &bridgev1.SendCommandInputResponse{Ack: committedAck("", ""), ResultJson: result.ResultJSON, WriteSeq: result.WriteSeq}, nil
}

func (s *PostgreSQLBridgeAPIStore) CancelCommand(ctx context.Context, request *bridgev1.CancelCommandRequest) (*bridgev1.CancelCommandResponse, error) {
	if request.GetScope().GetRequestId() == "" || request.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid cancel command request")
	}
	result, err := s.runCommandOperation(ctx, request.GetScope(), bridgeOpCancelCommand, request.GetScope().GetRequestId()+":"+request.GetTaskId(), request.GetTaskId()+":"+request.GetReason(), request.GetTaskId(), commandOperationOptions{ToolUseEventID: request.GetToolUseEventId()}, func(reference SandboxCommandReference, _ string) (SandboxCommandResult, error) {
		if s.SandboxToolExecutor == nil {
			return SandboxCommandResult{ResultJSON: mustMarshalToolRuntimeError("sandbox_helper_unavailable", "sandbox helper command cancellation is not installed", true)}, nil
		}
		result, err := s.SandboxToolExecutor.CancelCommand(ctx, SandboxCommandCancel{SandboxCommandReference: reference, Reason: request.GetReason()})
		if result.ResultJSON == "" || !json.Valid([]byte(result.ResultJSON)) {
			result.ResultJSON = mustMarshalToolRuntimeError("sandbox_helper_protocol_error", "sandbox helper returned an invalid command result", false)
		}
		return result, err
	})
	if err != nil {
		return nil, err
	}
	return &bridgev1.CancelCommandResponse{Ack: committedAck("", ""), ResultJson: result.ResultJSON}, nil
}

func commandInputCharsEmpty(inputJSON string) bool {
	var payload struct {
		Chars *string `json:"chars"`
	}
	if err := json.Unmarshal([]byte(inputJSON), &payload); err != nil {
		return false
	}
	return payload.Chars != nil && *payload.Chars == ""
}

func (s *PostgreSQLBridgeAPIStore) runCommandOperation(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	operation string,
	key string,
	payloadHashPart string,
	taskID string,
	options commandOperationOptions,
	execute func(SandboxCommandReference, string) (SandboxCommandResult, error),
) (commandOperationResult, error) {
	now := s.now()
	requestHash := bridgeRequestHash(operation, key, payloadHashPart)
	var result commandOperationResult
	var reference SandboxCommandReference
	var terminal bool
	var preparedInputJSON string
	var pendingOperation bool
	if err := s.withScopeTx(ctx, scope, "agentruntimebridge."+operation, func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		taskLoaded := false
		if options.AllocateWriteSeq {
			loaded, terminalResultJSON, err := loadBackgroundTaskTx(ctx, tx, scope, taskID)
			if err != nil {
				return err
			}
			reference = loaded
			reference.MaxOutputTokens = options.MaxOutputTokens
			reference.ToolUseEventID = options.ToolUseEventID
			result.ResultJSON = terminalResultJSON
			terminal = terminalResultJSON != ""
			taskLoaded = true
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, scope, operation, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "command operation idempotency conflict")
			}
			if options.RequireTaskRowForReplay {
				if err := ensureBackgroundTaskScopeTx(ctx, tx, scope, taskID); err != nil {
					return err
				}
			}
			if options.AllocateWriteSeq {
				result.WriteSeq = existing.StdinWriteSeq.Int64
				if inputJSON, ok := pendingCommandInput(existing.ResultJSON); ok {
					preparedInputJSON = inputJSON
					pendingOperation = true
					if result.WriteSeq <= 0 {
						result.WriteSeq = commandInputWriteSeq(inputJSON)
					}
				} else {
					result.ResultJSON = existing.ResultJSON
					return nil
				}
			} else if options.ClaimDrainingRead && pendingCommandRead(existing.ResultJSON) {
				pendingOperation = true
			} else {
				result.ResultJSON = existing.ResultJSON
				return nil
			}
		}
		if !taskLoaded {
			loaded, terminalResultJSON, err := loadBackgroundTaskTx(ctx, tx, scope, taskID)
			if err != nil {
				return err
			}
			reference = loaded
			reference.MaxOutputTokens = options.MaxOutputTokens
			reference.ToolUseEventID = options.ToolUseEventID
			result.ResultJSON = terminalResultJSON
			terminal = terminalResultJSON != ""
		}
		if terminal {
			if pendingOperation {
				return updateBridgeOperationResultTx(ctx, tx, scope, operation, key, result.ResultJSON, sql.NullInt64{Int64: result.WriteSeq, Valid: result.WriteSeq > 0}, now)
			}
			return insertBridgeOperationTx(ctx, tx, scope, bridgeOperationInsert{
				Operation:      operation,
				IdempotencyKey: key,
				RequestHash:    requestHash,
				AckStatus:      bridgeAckCommitted,
				StdinWriteSeq:  sql.NullInt64{Int64: result.WriteSeq, Valid: result.WriteSeq > 0},
				ResultJSON:     result.ResultJSON,
				Now:            now,
			})
		}
		// A running task remains controlled through its stored launch sandbox and
		// provider command identity. The session's current sandbox may already
		// have rotated or been released after the helper started.
		reference.OwnerRequestID = scope.GetRequestId()
		if options.AllocateWriteSeq && !pendingOperation {
			allocatedWriteSeq, err := allocateBackgroundTaskStdinWriteSequenceTx(ctx, tx, scope, taskID)
			if err != nil {
				return err
			}
			result.WriteSeq = allocatedWriteSeq
			preparedInputJSON, err = injectCommandInputWriteSeq(options.InputJSON, result.WriteSeq)
			if err != nil {
				return err
			}
			return insertBridgeOperationTx(ctx, tx, scope, bridgeOperationInsert{
				Operation:      operation,
				IdempotencyKey: key,
				RequestHash:    requestHash,
				AckStatus:      bridgeAckCommitted,
				ResultJSON:     pendingCommandInputJSON(preparedInputJSON),
				StdinWriteSeq:  sql.NullInt64{Int64: result.WriteSeq, Valid: true},
				Now:            now,
			})
		}
		if options.ClaimDrainingRead && !pendingOperation {
			return insertBridgeOperationTx(ctx, tx, scope, bridgeOperationInsert{
				Operation:      operation,
				IdempotencyKey: key,
				RequestHash:    requestHash,
				AckStatus:      bridgeAckCommitted,
				ResultJSON:     pendingCommandReadJSON(),
				Now:            now,
			})
		}
		return nil
	}); err != nil {
		return commandOperationResult{}, err
	}
	if result.ResultJSON != "" {
		return result, nil
	}
	if terminal {
		return result, nil
	}
	commandResult, err := execute(reference, preparedInputJSON)
	if err != nil {
		commandResult = SandboxCommandResult{ResultJSON: sandboxHelperExecutionErrorResult(err, "sandbox helper command operation failed")}
	}
	if commandResult.ResultJSON == "" || !json.Valid([]byte(commandResult.ResultJSON)) {
		commandResult.ResultJSON = mustMarshalToolRuntimeError("sandbox_helper_protocol_error", "sandbox helper returned an invalid command result", false)
	}
	commandResult.ResultJSON = stripInternalProviderFields(commandResult.ResultJSON)
	if commandResult.TerminalStatus != "" {
		commandResult.TerminalStatus = normalizeBackgroundTaskTerminalStatus(commandResult.TerminalStatus)
		if commandResult.TerminalStatus == "" {
			return commandOperationResult{}, status.Error(codes.Internal, "sandbox helper returned invalid terminal task status")
		}
	}
	result.ResultJSON = commandResult.ResultJSON
	if err := s.withScopeTx(ctx, scope, "agentruntimebridge."+operation, func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		resolveTaskWinner := func() error {
			reference, winningResultJSON, err := loadBackgroundTaskTx(ctx, tx, scope, taskID)
			if err != nil {
				return err
			}
			if winningResultJSON != "" {
				result.ResultJSON = winningResultJSON
				return nil
			}
			if commandResult.TerminalStatus != "" && !options.DeferTerminalSettlement {
				result.ResultJSON, err = settleBackgroundCommandResultTx(
					ctx, tx, scope, taskID, reference.Task.SourceToolUseEventID,
					commandResult.TerminalStatus, commandResult.ResultJSON, now,
				)
				return err
			}
			return nil
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, scope, operation, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "command operation idempotency conflict")
			}
			if options.AllocateWriteSeq {
				if result.WriteSeq <= 0 {
					result.WriteSeq = existing.StdinWriteSeq.Int64
				}
				if _, pending := pendingCommandInput(existing.ResultJSON); pending {
					if err := resolveTaskWinner(); err != nil {
						return err
					}
					return updateBridgeOperationResultTx(ctx, tx, scope, operation, key, result.ResultJSON, sql.NullInt64{Int64: result.WriteSeq, Valid: result.WriteSeq > 0}, now)
				}
			} else if options.ClaimDrainingRead && pendingCommandRead(existing.ResultJSON) {
				if err := resolveTaskWinner(); err != nil {
					return err
				}
				return updateBridgeOperationResultTx(ctx, tx, scope, operation, key, result.ResultJSON, sql.NullInt64{}, now)
			}
			result.ResultJSON = existing.ResultJSON
			return nil
		}
		if err := resolveTaskWinner(); err != nil {
			return err
		}
		return insertBridgeOperationTx(ctx, tx, scope, bridgeOperationInsert{
			Operation:      operation,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			StdinWriteSeq:  sql.NullInt64{Int64: result.WriteSeq, Valid: result.WriteSeq > 0},
			ResultJSON:     result.ResultJSON,
			Now:            now,
		})
	}); err != nil {
		return commandOperationResult{}, err
	}
	return result, nil
}

type commandOperationOptions struct {
	DeferTerminalSettlement bool
	AllocateWriteSeq        bool
	ClaimDrainingRead       bool
	InputJSON               string
	MaxOutputTokens         int
	ToolUseEventID          string
	RequireTaskRowForReplay bool
}

type commandOperationResult struct {
	ResultJSON string
	WriteSeq   int64
}

func positiveInt32(value int32) int {
	if value <= 0 {
		return 0
	}
	return int(value)
}

func commandPayloadHashPart(taskID string, inputJSON string, maxOutputTokens int) string {
	if inputJSON == "" && maxOutputTokens <= 0 {
		return taskID
	}
	if maxOutputTokens <= 0 {
		return taskID + ":" + inputJSON
	}
	return taskID + ":" + inputJSON + ":max_output_tokens=" + strconv.Itoa(maxOutputTokens)
}

func readCommandResultOwnerIdentity(sourceToolUseEventID string, taskID string, deferTerminalSettlement bool, maxOutputTokens int) (string, string, string) {
	digest := sha256.Sum256([]byte("command-followup:" + sourceToolUseEventID))
	requestID := "req_" + hex.EncodeToString(digest[:])[:32]
	payload := "defer_terminal_settlement=" + strconv.FormatBool(deferTerminalSettlement) +
		";max_output_tokens=" + strconv.Itoa(maxOutputTokens)
	key := requestID + ":" + taskID + ":" + bridgeRequestHash(payload)[:32]
	return requestID, key, payload
}

func copyRuntimeScopeWithRequestID(scope *bridgev1.RuntimeScope, requestID string) *bridgev1.RuntimeScope {
	if scope == nil {
		return nil
	}
	result := proto.Clone(scope).(*bridgev1.RuntimeScope)
	result.RequestId = requestID
	return result
}

func injectCommandInputWriteSeq(inputJSON string, writeSeq int64) (string, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(inputJSON), &payload); err != nil {
		return "", status.Error(codes.InvalidArgument, "command input must be JSON object")
	}
	if payload == nil {
		return "", status.Error(codes.InvalidArgument, "command input must be JSON object")
	}
	payload["write_seq"] = writeSeq
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func pendingCommandInputJSON(inputJSON string) string {
	body, err := json.Marshal(map[string]any{
		"_tetral_pending_command_input": true,
		"input_json":                    inputJSON,
	})
	if err != nil {
		return `{"_tetral_pending_command_input":true,"input_json":"{}"}`
	}
	return string(body)
}

func pendingCommandReadJSON() string {
	return `{"_tetral_pending_command_read":true}`
}

func pendingCommandRead(resultJSON string) bool {
	var payload struct {
		Pending bool `json:"_tetral_pending_command_read"`
	}
	return json.Unmarshal([]byte(resultJSON), &payload) == nil && payload.Pending
}

func pendingCommandInput(resultJSON string) (string, bool) {
	var payload struct {
		Pending   bool   `json:"_tetral_pending_command_input"`
		InputJSON string `json:"input_json"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &payload); err != nil {
		return "", false
	}
	return payload.InputJSON, payload.Pending && payload.InputJSON != ""
}

func commandInputWriteSeq(inputJSON string) int64 {
	var payload map[string]any
	if err := json.Unmarshal([]byte(inputJSON), &payload); err != nil {
		return 0
	}
	return metadataInt64(payload["write_seq"])
}

func metadataInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		if typed > 0 {
			return int64(typed)
		}
	case json.Number:
		if value, err := typed.Int64(); err == nil && value > 0 {
			return value
		}
	}
	return 0
}

func loadBackgroundTaskTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string) (SandboxCommandReference, string, error) {
	row := tx.QueryRow(ctx,
		`SELECT t.session_thread_id, t.source_tool_use_event_id, t.binding_id, t.sandbox_id,
		        t.provider_session_id, t.provider_command_id, t.provider_command_metadata_json, t.status, t.terminal_event_id,
		        COALESCE(p.resource_roots_json, '')
		   FROM session_background_tasks t
		   LEFT JOIN LATERAL (
		     SELECT resource_roots_json
			       FROM session_preparations p
			      WHERE p.workspace_id = t.workspace_id
			        AND p.session_id = t.session_id
			        AND p.superseded_at IS NULL
			      ORDER BY p.created_at DESC, p.preparation_attempt_id DESC
			      LIMIT 1
		   ) p ON TRUE
		  WHERE t.workspace_id = $1
		    AND t.session_id = $2
		    AND t.task_id = $3
		  FOR UPDATE OF t`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		taskID,
	)
	var threadID string
	var toolUseEventID string
	var bindingID string
	var sandboxID string
	var providerSessionID string
	var providerCommandID string
	var metadataJSON string
	var taskStatus string
	var terminalEventID sql.NullString
	var resourceRootsJSON string
	if err := row.Scan(&threadID, &toolUseEventID, &bindingID, &sandboxID, &providerSessionID, &providerCommandID, &metadataJSON, &taskStatus, &terminalEventID, &resourceRootsJSON); dbconnect.IsNoRows(err) {
		return SandboxCommandReference{}, "", status.Error(codes.NotFound, "background task not found")
	} else if err != nil {
		return SandboxCommandReference{}, "", err
	}
	if threadID != scope.GetSessionThreadId() || bindingID != scope.GetBinding().GetBindingId() {
		return SandboxCommandReference{}, "", status.Error(codes.FailedPrecondition, "background task binding is stale")
	}
	if !json.Valid([]byte(defaultString(metadataJSON, "{}"))) {
		return SandboxCommandReference{}, "", status.Error(codes.Internal, "background task metadata is invalid")
	}
	reference := SandboxCommandReference{
		Target: SandboxToolTarget{
			WorkspaceID:       scope.GetWorkspaceId(),
			SessionID:         scope.GetSessionId(),
			SessionThreadID:   scope.GetSessionThreadId(),
			BindingID:         bindingID,
			BindingGeneration: scope.GetBinding().GetBindingGeneration(),
			SandboxID:         sandboxID,
			ProviderSandboxID: providerSessionID,
			ResourceRootsJSON: resourceRootsJSON,
		},
		Task: SandboxBackgroundTask{
			TaskID:                      taskID,
			SourceToolUseEventID:        toolUseEventID,
			ProviderSessionID:           providerSessionID,
			ProviderCommandID:           providerCommandID,
			ProviderCommandMetadataJSON: defaultString(metadataJSON, "{}"),
		},
	}
	if taskStatus != "running" {
		terminalResultJSON, err := backgroundTaskTerminalResultJSONTx(ctx, tx, scope, taskID, terminalEventID)
		if err != nil {
			return SandboxCommandReference{}, "", err
		}
		return reference, terminalResultJSON, nil
	}
	return reference, "", nil
}

func allocateBackgroundTaskStdinWriteSequenceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string) (int64, error) {
	row := tx.QueryRow(ctx,
		`UPDATE session_background_tasks
		    SET stdin_write_sequence = stdin_write_sequence + 1
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		    AND status = 'running'
		RETURNING stdin_write_sequence`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		taskID,
	)
	var sequence int64
	if err := row.Scan(&sequence); dbconnect.IsNoRows(err) {
		return 0, status.Error(codes.FailedPrecondition, "background task is not running")
	} else if err != nil {
		return 0, err
	}
	return sequence, nil
}

func ensureBackgroundTaskScopeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string) error {
	row := tx.QueryRow(ctx,
		`SELECT session_thread_id, binding_id
		   FROM session_background_tasks
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND task_id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		taskID,
	)
	var threadID string
	var bindingID string
	if err := row.Scan(&threadID, &bindingID); dbconnect.IsNoRows(err) {
		return status.Error(codes.NotFound, "background task not found")
	} else if err != nil {
		return err
	}
	if threadID != scope.GetSessionThreadId() || bindingID != scope.GetBinding().GetBindingId() {
		return status.Error(codes.FailedPrecondition, "background task binding is stale")
	}
	return nil
}

func backgroundTaskTerminalResultJSONTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, terminalEventID sql.NullString) (string, error) {
	if !terminalEventID.Valid || terminalEventID.String == "" {
		return "", status.Error(codes.Internal, "background task terminal result is missing")
	}
	var payloadJSON string
	err := tx.QueryRow(ctx,
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4
		    AND type = 'runtime_notification'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		terminalEventID.String,
	).Scan(&payloadJSON)
	if dbconnect.IsNoRows(err) {
		return "", status.Error(codes.Internal, "background task terminal event is missing")
	}
	if err != nil {
		return "", err
	}
	var payload struct {
		TaskID string          `json:"task_id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.TaskID != taskID || len(payload.Result) == 0 || !json.Valid(payload.Result) {
		return "", status.Error(codes.Internal, "background task terminal result is invalid")
	}
	return string(payload.Result), nil
}

// settleBackgroundTaskTx is the shared compare-and-set for three of the four
// settlement callers — normal poll (ReadCommandResult), sandbox
// task_notification, and manual CancelCommand. Its UPDATE ... WHERE status =
// 'running' admits a single writer, so losers acknowledge or drop as stale and
// never write a second terminal result. Lifecycle cleanup does not route through
// here: it settles a running task to cancelled_by_cleanup through its own
// status = 'running' compare-and-set (settleCleanupBackgroundTasksTx in
// runtime_session_cleanup.go, and the pod-lost path in runtime_pod_lost.go). The
// single-winner invariant holds across all four callers because every settlement
// is a compare-and-set gated on status = 'running'.
//
//	session_background_tasks.status
//	running                one non-terminal start state; the only CAS source
//	completed              natural process completion
//	failed                 process failure
//	cancelled              manual CancelCommand only
//	cancelled_by_cleanup   lifecycle cleanup only; nothing else settles it
//	expired                helper lifetime-timeout (timed_out) fact
//	stale                  reserved for superseded / losing CAS attempts;
//	                        never a terminal outcome of the task's own process
//
// Terminal facts map to statuses one-to-one, so the settling caller's terminal
// fact fixes the status.
func settleBackgroundTaskTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string, now time.Time) (bool, string, error) {
	if !validBackgroundTaskTerminalStatus(terminalStatus) {
		return false, "", status.Error(codes.Internal, "background task terminal status is invalid")
	}
	if sourceToolUseEventID == "" {
		return false, "", status.Error(codes.Internal, "background task source identity is invalid")
	}
	err := tx.QueryRow(ctx,
		`UPDATE session_background_tasks
		    SET status = $7,
		        terminal_at = COALESCE(terminal_at, $8),
		        updated_at = $8
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		    AND source_tool_use_event_id = $5
		    AND binding_id = $6
		    AND status = 'running'
		  RETURNING source_tool_use_event_id`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		taskID,
		sourceToolUseEventID,
		scope.GetBinding().GetBindingId(),
		terminalStatus,
		now,
	).Scan(&sourceToolUseEventID)
	if dbconnect.IsNoRows(err) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	eventID, runtimeMessageJSON, err := insertRuntimeNotificationTx(ctx, tx, scope, taskID, sourceToolUseEventID, terminalStatus, resultJSON, now)
	if err != nil {
		return false, "", err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_background_tasks
		    SET terminal_event_id = COALESCE(terminal_event_id, $7),
		        updated_at = $8
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		    AND source_tool_use_event_id = $5
		    AND binding_id = $6`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		taskID,
		sourceToolUseEventID,
		scope.GetBinding().GetBindingId(),
		eventID,
		now,
	)
	if err != nil {
		return false, "", err
	}
	if !rowsAffected(result) {
		return false, "", status.Error(codes.Internal, "background task terminal event fence failed")
	}
	return true, runtimeMessageJSON, nil
}

func settleBackgroundCommandResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, sourceToolUseEventID string, terminalStatus string, helperResultJSON string, now time.Time) (string, error) {
	settled, _, err := settleBackgroundTaskTx(ctx, tx, scope, taskID, sourceToolUseEventID, terminalStatus, helperResultJSON, now)
	if err != nil {
		return "", err
	}
	if settled {
		return helperResultJSON, nil
	}
	_, winningResultJSON, err := loadBackgroundTaskTx(ctx, tx, scope, taskID)
	if err != nil {
		return "", err
	}
	if winningResultJSON == "" {
		return "", status.Error(codes.Internal, "background task terminal CAS lost without a durable winner")
	}
	return winningResultJSON, nil
}

func validBackgroundTaskTerminalStatus(status string) bool {
	switch status {
	case "completed", "failed", "cancelled", "expired", "cancelled_by_cleanup", "stale":
		return true
	default:
		return false
	}
}

func terminalStatusFromResultJSON(resultJSON string) (string, error) {
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &payload); err != nil {
		return "", status.Error(codes.InvalidArgument, "task notification result must be JSON")
	}
	switch payload.Status {
	case "completed", "failed", "cancelled", "expired":
		return payload.Status, nil
	default:
		return "", status.Error(codes.InvalidArgument, "task notification result status is invalid")
	}
}

func insertRuntimeNotificationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string, now time.Time) (string, string, error) {
	notificationJSON, err := runtimeNotificationJSON(taskID, sourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return "", "", err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'runtime_notification', $6, 'internal', false, $6, $7, $7)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		notificationJSON,
		now,
	); err != nil {
		return "", "", err
	}
	if err := projectRuntimeEventTx(ctx, tx, scope, runtimeEventProjection{
		EventID:        eventID,
		EventType:      "runtime_notification",
		PayloadJSON:    notificationJSON,
		ProjectionJSON: notificationJSON,
	}, now); err != nil {
		return "", "", err
	}
	runtimeMessageJSON, err := readRuntimeNotificationMessageTx(ctx, tx, scope, eventID)
	if err != nil {
		return "", "", err
	}
	return eventID, runtimeMessageJSON, nil
}

func readRuntimeNotificationMessageTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string) (string, error) {
	var dataJSON string
	err := tx.QueryRow(ctx,
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND source_event_id = $4
		    AND kind = 'runtime_notification'
		  LIMIT 1`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
	).Scan(&dataJSON)
	if dbconnect.IsNoRows(err) {
		return "", status.Error(codes.Internal, "runtime notification projection is missing")
	}
	return dataJSON, err
}

func runtimeNotificationJSON(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (string, error) {
	canonicalResultJSON, err := canonicalTaskNotificationPayloadJSON(taskID, sourceToolUseEventID, terminalStatus, stripInternalProviderFields(resultJSON))
	if err != nil {
		canonicalResultJSON = `{"status":"failed","error_code":"invalid_result_json"}`
	}
	result := json.RawMessage(canonicalResultJSON)
	return marshalBridgeJSON(map[string]any{
		"type":                     "runtime_notification",
		"task_id":                  taskID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"status":                   runtimeTaskNotificationStatus(terminalStatus),
		"result":                   result,
	})
}

func taskNotificationOperationResultJSON(resultJSON string, runtimeMessageJSON string) (string, error) {
	if !json.Valid([]byte(resultJSON)) || !json.Valid([]byte(runtimeMessageJSON)) {
		return "", status.Error(codes.Internal, "task notification operation result is invalid")
	}
	return marshalBridgeJSON(map[string]any{
		"result_json":          json.RawMessage(resultJSON),
		"runtime_message_json": json.RawMessage(runtimeMessageJSON),
	})
}

func taskNotificationRuntimeMessageFromOperationResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, sourceToolUseEventID string, resultJSON string) (string, error) {
	var result struct {
		RuntimeMessageJSON json.RawMessage `json:"runtime_message_json"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return "", status.Error(codes.Internal, "task notification operation result is invalid")
	}
	if len(result.RuntimeMessageJSON) > 0 {
		if !json.Valid(result.RuntimeMessageJSON) {
			return "", status.Error(codes.Internal, "task notification runtime message projection is invalid")
		}
		return string(result.RuntimeMessageJSON), nil
	}
	var terminalEventID sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		    AND source_tool_use_event_id = $5
		    AND binding_id = $6
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		taskID,
		sourceToolUseEventID,
		scope.GetBinding().GetBindingId(),
	).Scan(&terminalEventID)
	if dbconnect.IsNoRows(err) {
		return "", status.Error(codes.Internal, "task notification operation projection source is missing")
	}
	if err != nil {
		return "", err
	}
	if !terminalEventID.Valid || terminalEventID.String == "" {
		return "", status.Error(codes.Internal, "task notification terminal event projection is missing")
	}
	return readRuntimeNotificationMessageTx(ctx, tx, scope, terminalEventID.String)
}

func taskNotificationResultIdentity(resultJSON string) (string, string, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resultJSON), &object); err != nil || object == nil {
		return "", "", status.Error(codes.InvalidArgument, "task notification result must be a JSON object")
	}
	taskID, err := requiredNonEmptyStringRaw(object["task_id"], "task notification result task_id")
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	sourceToolUseEventID, err := requiredNonEmptyStringRaw(object["source_tool_use_event_id"], "task notification result source_tool_use_event_id")
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, err.Error())
	}
	return taskID, sourceToolUseEventID, nil
}

func canonicalTaskNotificationPayloadJSON(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (string, error) {
	value, err := canonicalTaskNotificationPayloadValue(taskID, sourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func canonicalTaskNotificationPayloadValue(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (map[string]any, error) {
	if taskID == "" || sourceToolUseEventID == "" {
		return nil, errors.New("task notification source identity is incomplete")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resultJSON), &object); err != nil || object == nil {
		return nil, errors.New("task notification result must be a JSON object")
	}
	factObject := object
	if rawResult, ok := object["result"]; ok {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(rawResult, &nested); err == nil && nested != nil {
			factObject = nested
		}
	}
	canonical := map[string]any{
		"task_id":                  taskID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"status":                   runtimeTaskNotificationStatus(terminalStatus),
	}
	if canonical["status"] == "" {
		return nil, errors.New("task notification result status is invalid")
	}
	if raw, ok := factObject["exit_code"]; ok {
		value, err := nullableIntegerRaw(raw, "task notification result exit_code")
		if err != nil {
			return nil, err
		}
		canonical["exit_code"] = value
	}
	for _, field := range []string{"stdout", "stderr"} {
		raw, ok := factObject[field]
		if !ok {
			return nil, errors.New("task notification result " + field + " is required")
		}
		stream, err := canonicalTaskNotificationStream(raw, "task notification result "+field)
		if err != nil {
			return nil, err
		}
		canonical[field] = stream
	}
	if err := fitTaskNotificationPayload(canonical); err != nil {
		return nil, err
	}
	return canonical, nil
}

func fitTaskNotificationPayload(payload map[string]any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(encoded) <= runtimeTaskNotificationPayloadMaxBytes {
		return nil
	}
	stdout, stdoutOK := payload["stdout"].(map[string]any)
	stderr, stderrOK := payload["stderr"].(map[string]any)
	if !stdoutOK || !stderrOK {
		return errors.New("task notification streams are invalid")
	}
	stdoutText, stdoutOK := stdout["text"].(string)
	stderrText, stderrOK := stderr["text"].(string)
	if !stdoutOK || !stderrOK {
		return errors.New("task notification stream text is invalid")
	}
	stdoutBytes := len([]byte(stdoutText))
	stderrBytes := len([]byte(stderrText))
	bestStdout, bestStderr := "", ""
	bestFound := false
	for low, high := 0, stdoutBytes+stderrBytes; low <= high; {
		candidateBytes := low + (high-low)/2
		stdoutBudget, stderrBudget := splitTaskNotificationBudget(candidateBytes, stdoutBytes, stderrBytes)
		candidateStdout := taskNotificationHeadTail(stdoutText, stdoutBudget)
		candidateStderr := taskNotificationHeadTail(stderrText, stderrBudget)
		stdout["text"] = candidateStdout
		stderr["text"] = candidateStderr
		candidate, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		if len(candidate) <= runtimeTaskNotificationPayloadMaxBytes {
			bestStdout, bestStderr, bestFound = candidateStdout, candidateStderr, true
			low = candidateBytes + 1
		} else {
			high = candidateBytes - 1
		}
	}
	if !bestFound {
		return errors.New("task notification metadata exceeds runtime payload limit")
	}
	stdout["text"] = bestStdout
	stderr["text"] = bestStderr
	if bestStdout != stdoutText {
		stdout["truncated"] = true
	}
	if bestStderr != stderrText {
		stderr["truncated"] = true
	}
	return nil
}

func splitTaskNotificationBudget(total int, stdoutBytes int, stderrBytes int) (int, int) {
	visibleBytes := stdoutBytes + stderrBytes
	if visibleBytes == 0 || total <= 0 {
		return 0, 0
	}
	stdoutBudget := total * stdoutBytes / visibleBytes
	if stdoutBytes > 0 && stdoutBudget == 0 {
		stdoutBudget = 1
	}
	if stdoutBudget > total {
		stdoutBudget = total
	}
	return stdoutBudget, total - stdoutBudget
}

func taskNotificationHeadTail(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(value)) <= maxBytes {
		return value
	}
	headBudget := maxBytes / 2
	tailBudget := maxBytes - headBudget
	headEnd := 0
	for index := range value {
		if index > headBudget {
			break
		}
		headEnd = index
	}
	if headBudget >= len(value) {
		headEnd = len(value)
	}
	tailStart := len(value)
	used := 0
	for tailStart > headEnd {
		_, size := utf8.DecodeLastRuneInString(value[:tailStart])
		if size == 0 || used+size > tailBudget {
			break
		}
		used += size
		tailStart -= size
	}
	return value[:headEnd] + value[tailStart:]
}

func canonicalTaskNotificationStream(raw json.RawMessage, field string) (map[string]any, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New(field + " must be a JSON object")
	}
	text, err := requiredStringRaw(object["text"], field+" text")
	if err != nil {
		return nil, err
	}
	truncated, err := requiredBoolRaw(object["truncated"], field+" truncated")
	if err != nil {
		return nil, err
	}
	canonical := map[string]any{
		"text":      text,
		"truncated": truncated,
	}
	for _, optional := range []struct {
		output string
		input  string
	}{
		{output: "original_bytes", input: "original_bytes"},
		{output: "original_bytes", input: "total_bytes"},
		{output: "original_lines", input: "original_lines"},
		{output: "original_lines", input: "total_lines"},
	} {
		if _, exists := canonical[optional.output]; exists {
			continue
		}
		rawOptional, ok := object[optional.input]
		if !ok {
			continue
		}
		value, err := nullableIntegerRaw(rawOptional, field+" "+optional.input)
		if err != nil {
			return nil, err
		}
		canonical[optional.output] = value
	}
	return canonical, nil
}

func requiredNonEmptyStringRaw(raw json.RawMessage, field string) (string, error) {
	value, err := requiredStringRaw(raw, field)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", errors.New(field + " is required")
	}
	return value, nil
}

func requiredStringRaw(raw json.RawMessage, field string) (string, error) {
	var value string
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &value) != nil {
		return "", errors.New(field + " must be a string")
	}
	return value, nil
}

func requiredBoolRaw(raw json.RawMessage, field string) (bool, error) {
	var value bool
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &value) != nil {
		return false, errors.New(field + " must be a boolean")
	}
	return value, nil
}

func nullableIntegerRaw(raw json.RawMessage, field string) (any, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	var value int64
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return nil, errors.New(field + " must be an integer or null")
	}
	return value, nil
}

func insertBackgroundTaskTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, target SandboxToolTarget, toolUseEventID string, task SandboxBackgroundTask, now time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_background_tasks (
			workspace_id, session_id, session_thread_id, task_id, source_tool_use_event_id,
			binding_id, sandbox_id, provider_session_id, provider_command_id,
			provider_command_metadata_json, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'running', $11, $11)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		task.TaskID,
		toolUseEventID,
		target.BindingID,
		target.SandboxID,
		task.ProviderSessionID,
		task.ProviderCommandID,
		defaultString(task.ProviderCommandMetadataJSON, "{}"),
		now,
	)
	return err
}

func validateSandboxBackgroundTask(task SandboxBackgroundTask) error {
	if task.TaskID == "" || task.ProviderSessionID == "" || task.ProviderCommandID == "" {
		return status.Error(codes.Internal, "sandbox helper returned incomplete background task metadata")
	}
	metadataJSON := defaultString(task.ProviderCommandMetadataJSON, "{}")
	if len([]byte(metadataJSON)) > providerCommandMetadataMaxBytes {
		return status.Error(codes.Internal, "sandbox helper returned oversized background task metadata")
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil || metadata == nil {
		return status.Error(codes.Internal, "sandbox helper returned invalid background task metadata object")
	}
	return nil
}
