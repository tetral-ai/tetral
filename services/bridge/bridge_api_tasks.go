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
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tasks protocol-family boundary.

// CommitTaskNotificationResult authors the conversation projection for a
// terminal background task. Sandbox Service owns terminal task settlement and
// the task-notification inbox birth; Bridge verifies the Runtime declaration
// against that stored terminal result before committing one event and message.
func (s *PostgreSQLBridgeAPIStore) CommitTaskNotificationResult(ctx context.Context, request *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error) {
	if request.GetRuntimeInputId() == "" || request.GetTaskId() == "" || request.GetResultJson() == "" || request.GetDraft() == nil {
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
	sourceID := stableRuntimeID("task_notification", request.GetRuntimeInputId(), request.GetTaskId())
	if err := validateTaskNotificationDraft(request, sourceID, resultJSON); err != nil {
		return nil, err
	}
	declarationDigest, err := taskNotificationDeclarationDigest(request, resultJSON)
	if err != nil {
		return nil, err
	}
	now := s.now()
	var ack *bridgev1.BridgeWriteAck
	var receipt *bridgev1.DeclarationReceipt
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_task_notification_result", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpCommitTaskNotificationResult, key); err != nil {
			return err
		} else if ok {
			if existing.AckStatus == bridgeAckRejected {
				ack = rejectedAck(defaultString(existing.ErrorCode, "task_notification_stale"))
				return nil
			}
		}
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitTaskNotificationResult,
			"task_notification",
			sourceID,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest != declarationDigest || existing.ReceiptJSON == "" {
				return status.Error(codes.AlreadyExists, "task notification idempotency conflict")
			}
			receipt, err = unmarshalDeclarationReceipt(existing.ReceiptJSON)
			if err != nil {
				return status.Error(codes.FailedPrecondition, "task notification receipt is invalid")
			}
			ack = duplicateAck(sourceID, "")
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockThreadMutationOnlyTx(ctx, tx, request.GetScope()); err != nil {
			return err
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
		settled, committedReceipt, err := commitTaskNotificationDeclarationTx(
			ctx,
			tx,
			request,
			sourceID,
			sourceToolUseEventID,
			terminalStatus,
			resultJSON,
			now,
		)
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
				RequestHash:    declarationDigest,
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
		receipt = committedReceipt
		receipt.DeclarationDigest = declarationDigest
		receiptJSON, err := marshalDeclarationReceipt(receipt)
		if err != nil {
			return err
		}
		if err := insertTaskNotificationDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			sourceID,
			request.GetRuntimeInputId(),
			declarationDigest,
			receiptJSON,
			now,
		); err != nil {
			return err
		}
		ack = committedAck(sourceID, "")
		return nil
	}); err != nil {
		return nil, err
	}
	if receipt == nil {
		return &bridgev1.CommitTaskNotificationResultResponse{Ack: ack}, nil
	}
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpCommitTaskNotificationResult,
		"task_notification",
		sourceID,
		declarationDigest,
		ack,
		observation,
	)
	return &bridgev1.CommitTaskNotificationResultResponse{
		Ack: ack,
		Declaration: &bridgev1.DeclarationResponse{
			Receipts:                  []*bridgev1.DeclarationReceipt{receipt},
			ObservedBindingId:         observation.BindingID,
			ObservedBindingGeneration: observation.BindingGeneration,
			ApplicationDisposition:    observation.Disposition,
		},
	}, nil
}

func (s *PostgreSQLBridgeAPIStore) ReadCommandResult(ctx context.Context, request *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error) {
	if request.GetScope() == nil || request.GetTaskId() == "" || request.GetToolUseEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid read command result request")
	}
	maxOutputTokens := positiveInt32(request.GetMaxOutputTokens())
	requestID, _, _ := readCommandResultOwnerIdentity(
		request.GetToolUseEventId(),
		request.GetTaskId(),
		request.GetDeferTerminalSettlement(),
		maxOutputTokens,
	)
	scope := copyRuntimeScopeWithRequestID(request.GetScope(), requestID)
	inputJSON, err := marshalBridgeJSON(map[string]any{
		"task_id": request.GetTaskId(), "max_output_tokens": maxOutputTokens,
		"defer_terminal_settlement": request.GetDeferTerminalSettlement(),
	})
	if err != nil {
		return nil, err
	}
	result, err := s.acceptAndAwaitBackgroundCommand(ctx, scope, request.GetTaskId(), request.GetToolUseEventId(), "poll", inputJSON, maxOutputTokens)
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
	result, err := s.acceptAndAwaitBackgroundCommand(ctx, request.GetScope(), request.GetTaskId(), request.GetToolUseEventId(), "stdin", request.GetInputJson(), maxOutputTokens)
	if err != nil {
		return nil, err
	}
	return &bridgev1.SendCommandInputResponse{Ack: committedAck("", ""), ResultJson: result.ResultJSON, WriteSeq: result.WriteSeq}, nil
}

func (s *PostgreSQLBridgeAPIStore) CancelCommand(ctx context.Context, request *bridgev1.CancelCommandRequest) (*bridgev1.CancelCommandResponse, error) {
	if request.GetScope().GetRequestId() == "" || request.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid cancel command request")
	}
	result, err := s.acceptAndAwaitBackgroundCancel(ctx, request.GetScope(), request.GetTaskId(), request.GetToolUseEventId(), request.GetReason())
	if err != nil {
		return nil, err
	}
	return &bridgev1.CancelCommandResponse{Ack: committedAck("", ""), ResultJson: result.ResultJSON}, nil
}

func (s *PostgreSQLBridgeAPIStore) acceptAndAwaitBackgroundCommand(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	taskID string,
	toolUseEventID string,
	kind string,
	inputJSON string,
	maxOutputTokens int,
) (commandOperationResult, error) {
	if kind != "poll" && kind != "stdin" {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background command kind is invalid")
	}
	canonicalInput, inputHash, err := canonicalBackgroundCommandInput(inputJSON)
	if err != nil {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background command input must be JSON")
	}
	requestID := scope.GetRequestId()
	if requestID == "" || toolUseEventID == "" {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background command identity is incomplete")
	}
	result := commandOperationResult{}
	now := s.now()
	err = s.withScopeTx(ctx, scope, "agentruntimebridge.accept_background_command", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		if err := lockRuntimeMutationSessionTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId()); err != nil {
			return err
		}
		if err := lockThreadMutationOnlyTx(ctx, tx, scope); err != nil {
			return err
		}
		terminalResult, err := loadBackgroundTaskForCommandAcceptanceTx(ctx, tx, scope, taskID)
		if err != nil {
			return err
		}
		var existingKind, existingState, existingRequestID, existingTaskID, existingInputHash, existingInputJSON string
		var existingResult sql.NullString
		var existingWriteSeq sql.NullInt64
		err = tx.QueryRow(ctx, `SELECT background_operation_kind, background_operation_state,
			background_request_id, background_task_id, normalized_input_hash, input_json,
			result_json, background_write_sequence
			FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
			FOR UPDATE`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID).Scan(
			&existingKind, &existingState, &existingRequestID, &existingTaskID, &existingInputHash, &existingInputJSON,
			&existingResult, &existingWriteSeq,
		)
		if err == nil {
			if existingKind != kind || existingRequestID != requestID || existingTaskID != taskID || existingInputHash != inputHash || existingInputJSON != canonicalInput {
				return status.Error(codes.AlreadyExists, "background command idempotency conflict")
			}
			result.WriteSeq = existingWriteSeq.Int64
			if existingState == "terminal" && existingResult.Valid {
				result.ResultJSON = existingResult.String
			}
			return nil
		}
		if !dbconnect.IsNoRows(err) {
			return err
		}
		if err := verifyBackgroundCommandToolUseTx(ctx, tx, scope, toolUseEventID); err != nil {
			return err
		}
		var writeSequence int64
		if terminalResult == "" && kind == "stdin" {
			writeSequence, err = allocateBackgroundTaskWriteSequenceTx(ctx, tx, scope, taskID, now)
			if err != nil {
				return err
			}
			result.WriteSeq = writeSequence
		}
		state := "pending"
		var storedResult any
		var digest any
		if terminalResult != "" {
			state = "terminal"
			storedResult = terminalResult
			digest = bridgeRequestHash(terminalResult)
			result.ResultJSON = terminalResult
		}
		if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json, result_digest,
			background_operation_kind, background_operation_state, background_request_id,
			background_task_id, background_max_output_tokens, background_write_sequence,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,'sandbox_background',$5,'write_stdin',$6,'committed',$7,$8,
			$9,$10,$11,$12,$13,$14,$15,$15)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
			inputHash, canonicalInput, storedResult, digest, kind, state, requestID, taskID,
			maxOutputTokens, nullablePositiveInt64(writeSequence), now,
		); err != nil {
			return err
		}
		if state == "terminal" {
			return nil
		}
		return enqueueBackgroundCommandTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), taskID, requestID, now)
	})
	if err != nil || result.ResultJSON != "" {
		return result, err
	}
	return s.waitForBackgroundCommandResult(ctx, scope, toolUseEventID)
}

func (s *PostgreSQLBridgeAPIStore) acceptAndAwaitBackgroundCancel(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	taskID string,
	toolUseEventID string,
	reason string,
) (commandOperationResult, error) {
	requestID := scope.GetRequestId() + ":cancel"
	if scope.GetRequestId() == "" || toolUseEventID == "" {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background cancellation identity is incomplete")
	}
	inputJSON, err := marshalBridgeJSON(map[string]string{"reason": reason})
	if err != nil {
		return commandOperationResult{}, err
	}
	var immediate string
	now := s.now()
	err = s.withScopeTx(ctx, scope, "agentruntimebridge.accept_background_cancel", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		if err := lockRuntimeMutationSessionTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId()); err != nil {
			return err
		}
		var rowTaskID, state string
		var resultJSON, cancelRequestID, cancelResult sql.NullString
		err := tx.QueryRow(ctx, `SELECT background_task_id, background_operation_state, result_json,
			background_cancel_request_id, background_cancel_result_json
			FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			  AND tool_use_event_id=$4 AND tool_kind='sandbox_background' FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
		).Scan(&rowTaskID, &state, &resultJSON, &cancelRequestID, &cancelResult)
		if dbconnect.IsNoRows(err) {
			return status.Error(codes.FailedPrecondition, "background command acceptance is not durable")
		}
		if err != nil {
			return err
		}
		if rowTaskID != taskID {
			return status.Error(codes.AlreadyExists, "background cancellation task mismatch")
		}
		if cancelRequestID.Valid {
			if cancelRequestID.String != requestID {
				return status.Error(codes.AlreadyExists, "background cancellation idempotency conflict")
			}
			if cancelResult.Valid {
				immediate = cancelResult.String
			}
			return nil
		}
		if state == "terminal" && resultJSON.Valid {
			immediate = resultJSON.String
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE session_runtime_tool_results
			SET cancel_requested_at=$5, cancel_state='pending', background_cancel_request_id=$6,
			    background_cancel_input_json=$7, updated_at=$5
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
			now, requestID, inputJSON); err != nil {
			return err
		}
		return enqueueBackgroundCommandTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), taskID, requestID, now)
	})
	if err != nil || immediate != "" {
		return commandOperationResult{ResultJSON: immediate}, err
	}
	return s.waitForBackgroundCancelResult(ctx, scope, toolUseEventID)
}

func loadBackgroundTaskForCommandAcceptanceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string) (string, error) {
	var threadID, statusValue string
	var terminalResult sql.NullString
	if err := tx.QueryRow(ctx, `SELECT session_thread_id, status, terminal_result_json
		FROM session_background_tasks WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), taskID).Scan(&threadID, &statusValue, &terminalResult); dbconnect.IsNoRows(err) {
		return "", status.Error(codes.NotFound, "background task not found")
	} else if err != nil {
		return "", err
	}
	if threadID != scope.GetSessionThreadId() {
		return "", status.Error(codes.FailedPrecondition, "background task thread is stale")
	}
	if statusValue != "running" {
		if !terminalResult.Valid || terminalResult.String == "" {
			return "", status.Error(codes.Internal, "background task terminal result is missing")
		}
		return terminalResult.String, nil
	}
	return "", nil
}

func allocateBackgroundTaskWriteSequenceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, now time.Time) (int64, error) {
	var writeSequence int64
	if err := tx.QueryRow(ctx, `UPDATE session_background_tasks
		SET stdin_write_sequence=stdin_write_sequence+1, updated_at=$4
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND status='running'
		RETURNING stdin_write_sequence`, scope.GetWorkspaceId(), scope.GetSessionId(), taskID, now.UTC()).Scan(&writeSequence); dbconnect.IsNoRows(err) {
		return 0, status.Error(codes.FailedPrecondition, "background task is not running")
	} else if err != nil {
		return 0, err
	}
	return writeSequence, nil
}

func verifyBackgroundCommandToolUseTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string) error {
	var eventType string
	if err := tx.QueryRow(ctx, `SELECT type FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4 FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID).Scan(&eventType); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "durable background command tool use is missing")
	} else if err != nil {
		return err
	}
	if eventType != "agent.tool_use" {
		return status.Error(codes.FailedPrecondition, "durable background command identity is invalid")
	}
	return nil
}

func enqueueBackgroundCommandTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, taskID string, requestID string, now time.Time) error {
	payload, err := json.Marshal(map[string]string{"workspace_id": workspaceID, "session_id": sessionID, "task_id": taskID, "request_id": requestID})
	if err != nil {
		return err
	}
	ws := workspace.ID(workspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: ws, Kind: queue.KindSandboxBackgroundCommand,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(ws, sessionID, taskID),
		DedupeKey:      queue.FormatSandboxBackgroundCommandDedupeKey(ws, sessionID, taskID, requestID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: 5, Now: now,
	})
	return err
}

func (s *PostgreSQLBridgeAPIStore) waitForBackgroundCommandResult(ctx context.Context, scope *bridgev1.RuntimeScope, toolUseEventID string) (commandOperationResult, error) {
	return s.waitForBackgroundResult(ctx, scope, toolUseEventID, false)
}

func (s *PostgreSQLBridgeAPIStore) waitForBackgroundCancelResult(ctx context.Context, scope *bridgev1.RuntimeScope, toolUseEventID string) (commandOperationResult, error) {
	return s.waitForBackgroundResult(ctx, scope, toolUseEventID, true)
}

func (s *PostgreSQLBridgeAPIStore) waitForBackgroundResult(ctx context.Context, scope *bridgev1.RuntimeScope, toolUseEventID string, cancellation bool) (commandOperationResult, error) {
	ticker := time.NewTicker(runToolResultPollInterval)
	defer ticker.Stop()
	for {
		var result commandOperationResult
		var terminal bool
		err := s.withScopeReadOnlyTx(ctx, scope, "agentruntimebridge.await_background_command", func(tx *dbconnect.Tx) error {
			var operationState string
			var resultJSON, cancelResult sql.NullString
			var writeSequence sql.NullInt64
			err := tx.QueryRow(ctx, `SELECT background_operation_state, result_json,
				background_cancel_result_json, background_write_sequence
				FROM session_runtime_tool_results
				WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
				  AND tool_kind='sandbox_background'`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID).Scan(
				&operationState, &resultJSON, &cancelResult, &writeSequence)
			if err != nil {
				return err
			}
			result.WriteSeq = writeSequence.Int64
			if cancellation && cancelResult.Valid {
				result.ResultJSON = cancelResult.String
				terminal = true
			} else if !cancellation && operationState == "terminal" && resultJSON.Valid {
				result.ResultJSON = resultJSON.String
				terminal = true
			}
			return nil
		})
		if err != nil {
			return commandOperationResult{}, err
		}
		if terminal {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return commandOperationResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func canonicalBackgroundCommandInput(inputJSON string) (string, string, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(inputJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil || value == nil {
		return "", "", errors.New("background command input must be JSON")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(canonical)
	return string(canonical), hex.EncodeToString(digest[:]), nil
}

func nullablePositiveInt64(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
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

func validBackgroundTaskTerminalStatus(status string) bool {
	switch status {
	case "completed", "failed", "cancelled", "expired", "unknown_outcome", "cancelled_by_cleanup", "stale":
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
	case "completed", "failed", "cancelled", "expired", "unknown_outcome":
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
	if err := insertSessionMessageProjectionTx(
		ctx,
		tx,
		scope,
		eventID,
		"runtime_notification",
		notificationJSON,
		now,
	); err != nil {
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

func validateTaskNotificationDraft(
	request *bridgev1.CommitTaskNotificationResultRequest,
	sourceID string,
	resultJSON string,
) error {
	draft := request.GetDraft()
	if draft == nil ||
		draft.GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TASK_NOTIFICATION ||
		draft.GetOrdinal() != 0 ||
		draft.GetSourceKind() != "task_notification" ||
		draft.GetSourceId() != sourceID ||
		draft.GetSourceEventId() != "" ||
		len(draft.GetParts()) != 1 {
		return status.Error(codes.InvalidArgument, "task notification draft identity is invalid")
	}
	expectedMessageID := stableRuntimeID(
		"runtime_message_draft",
		request.GetScope().GetWorkspaceId(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
		"task_notification",
		sourceID,
		runtimeDraftKindToken(draft.GetDraftKind()),
		"0",
	)
	if draft.GetRuntimeLocalId() != expectedMessageID {
		return status.Error(codes.InvalidArgument, "task notification draft id is invalid")
	}
	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil ||
		len(messageInfo) != 3 ||
		messageInfo["role"] != "user" ||
		messageInfo["origin"] != "runtime" ||
		messageInfo["status"] != "completed" {
		return status.Error(codes.InvalidArgument, "task notification draft message is invalid")
	}
	part := draft.GetParts()[0]
	if part == nil ||
		part.GetPartKind() != "text" ||
		part.GetOrdinal() != 0 ||
		part.GetRuntimeLocalPartId() != stableRuntimeID("runtime_message_part_draft", expectedMessageID, "text", "0") {
		return status.Error(codes.InvalidArgument, "task notification draft part identity is invalid")
	}
	var partInfo map[string]any
	if err := json.Unmarshal([]byte(part.GetPartJson()), &partInfo); err != nil ||
		len(partInfo) != 4 ||
		partInfo["type"] != "text" ||
		partInfo["text"] != resultJSON ||
		partInfo["truncated"] != false ||
		partInfo["status"] != "completed" {
		return status.Error(codes.InvalidArgument, "task notification draft part is invalid")
	}
	return nil
}

func commitTaskNotificationDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitTaskNotificationResultRequest,
	sourceID string,
	sourceToolUseEventID string,
	terminalStatus string,
	resultJSON string,
	now time.Time,
) (bool, *bridgev1.DeclarationReceipt, error) {
	scope := request.GetScope()
	var storedStatus, storedSourceToolUseEventID, storedResultJSON string
	var terminalEventID sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT status, source_tool_use_event_id, terminal_result_json, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		request.GetTaskId(),
	).Scan(&storedStatus, &storedSourceToolUseEventID, &storedResultJSON, &terminalEventID)
	if dbconnect.IsNoRows(err) || terminalEventID.Valid {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	if !validBackgroundTaskTerminalStatus(storedStatus) || storedSourceToolUseEventID != sourceToolUseEventID || storedResultJSON == "" {
		return false, nil, status.Error(codes.FailedPrecondition, "background task terminal result is invalid")
	}
	expectedResultJSON, err := canonicalTaskNotificationPayloadJSON(request.GetTaskId(), storedSourceToolUseEventID, storedStatus, storedResultJSON)
	if err != nil || expectedResultJSON != resultJSON {
		return false, nil, status.Error(codes.FailedPrecondition, "task notification result does not match durable task result")
	}
	notificationJSON, err := runtimeNotificationJSON(
		request.GetTaskId(),
		sourceToolUseEventID,
		terminalStatus,
		resultJSON,
	)
	if err != nil {
		return false, nil, err
	}
	eventID := id.New("evt_")
	eventSequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return false, nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'runtime_notification', $6, 'internal', false, $7, $6, $8, $8)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		eventSequence,
		notificationJSON,
		sourceID,
		now,
	); err != nil {
		return false, nil, err
	}
	messageStamp, err := upsertRuntimeOutputDraftTx(
		ctx,
		tx,
		scope,
		sourceID,
		"task_notification",
		eventID,
		"",
		0,
		request.GetDraft(),
		runtimeOutputWritePolicy{AllowNewParts: true},
		now,
	)
	if err != nil {
		return false, nil, err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_background_tasks
		    SET terminal_event_id = $6,
		        updated_at = $7
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		    AND source_tool_use_event_id = $5
		    AND terminal_event_id IS NULL`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		request.GetTaskId(),
		sourceToolUseEventID,
		eventID,
		now,
	)
	if err != nil {
		return false, nil, err
	}
	if !rowsAffected(result) {
		return false, nil, status.Error(codes.Internal, "background task terminal event fence failed")
	}
	return true, &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpCommitTaskNotificationResult,
		SourceKind:      "task_notification",
		SourceId:        sourceID,
		Events: []*bridgev1.DurableEventStamp{{
			SessionThreadId: scope.GetSessionThreadId(),
			SourceEventId:   sourceID,
			EventId:         eventID,
			EventSequence:   eventSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}},
		Messages: []*bridgev1.DurableMessageStamp{messageStamp},
	}, nil
}

func insertTaskNotificationDeclarationOperationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sourceID string,
	runtimeInputID string,
	declarationDigest string,
	receiptJSON string,
	now time.Time,
) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind,
			idempotency_key, request_hash, declaration_digest, receipt_json,
			ack_status, runtime_input_id, result_json, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, 'task_notification', $5, $6, $6, $7, $8, $9, '{}', $10, $10
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		bridgeOpCommitTaskNotificationResult,
		sourceID,
		declarationDigest,
		receiptJSON,
		bridgeAckCommitted,
		runtimeInputID,
		now,
	)
	return err
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
