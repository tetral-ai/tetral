package agentruntimebridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const childInterruptRequestedEventType = "agent.thread_interrupt_requested"

type childInterruptEventPayload struct {
	Type                 string `json:"type"`
	SourceToolUseEventID string `json:"source_tool_use_event_id"`
	ControlOperationID   string `json:"control_operation_id"`
	RootChildThreadID    string `json:"root_child_thread_id"`
	Action               string `json:"action"`
	IncludeDescendants   bool   `json:"include_descendants"`
	TargetThreadID       string `json:"target_thread_id"`
	Disposition          string `json:"disposition"`
	RequestedAt          string `json:"requested_at"`
	RuntimeInputID       string `json:"runtime_input_id,omitempty"`
}

type childControlCommand struct {
	scope                *bridgev1.RuntimeScope
	sourceToolUseEventID string
	controlOperationID   string
	rootChildThreadID    string
	action               bridgev1.ChildControlAction
	includeDescendants   bool
}

func (s *PostgreSQLBridgeAPIStore) AdmitChildInterrupt(ctx context.Context, request *bridgev1.AdmitChildInterruptRequest) (*bridgev1.AdmitChildInterruptResponse, error) {
	ctx = withInterruptBarrierBirth(ctx)
	if request.GetScope() == nil || request.GetSourceToolUseEventId() == "" {
		err := status.Error(codes.InvalidArgument, "child interrupt scope and source identity are required")
		logActorBoundaryRejected(s.Logger, request.GetScope(), "admit_child_interrupt", request.GetSourceToolUseEventId(), "validate", err)
		return nil, err
	}
	if err := validateRuntimeScope(request.GetScope()); err != nil {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "admit_child_interrupt", request.GetSourceToolUseEventId(), "validate", err)
		return nil, err
	}
	now := s.now()
	var targets []*bridgev1.ChildInterruptTarget
	var command childControlCommand
	duplicate := false
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.admit_child_interrupt", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var err error
		command, err = deriveChildControlCommandTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId())
		if err != nil {
			return err
		}
		stored, operationID, ok, err := readChildInterruptCensusBySourceTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId())
		if err != nil {
			return err
		}
		if ok {
			command.controlOperationID = operationID
			if err := validateStoredChildInterruptRequestTx(ctx, tx, command.scope, command.sourceToolUseEventID, command.rootChildThreadID, command.action, command.includeDescendants); err != nil {
				return err
			}
			targets, duplicate = stored, true
			return nil
		}
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId()); err != nil {
			return err
		}
		command.controlOperationID = id.New("ctrl_")
		targetIDs, err := childInterruptTargetIDsTx(ctx, tx, command)
		if err != nil {
			return err
		}
		for _, targetID := range targetIDs {
			if overlap, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), targetID); err != nil {
				return err
			} else if overlap {
				return status.Error(codes.FailedPrecondition, "child_control_in_progress")
			}
		}
		for _, targetID := range targetIDs {
			target, err := insertChildInterruptTargetTx(ctx, tx, command, targetID, now)
			if err != nil {
				return err
			}
			targets = append(targets, target)
		}
		return deferTargetTaskNotificationsTx(ctx, tx, request.GetScope(), targetIDs, now)
	})
	if err != nil {
		logThreadControlFailure(s.Logger, request.GetScope(), request.GetSourceToolUseEventId(), "admission", err)
		return nil, err
	}
	logThreadControlEvent(s.Logger, "thread_interrupt_admitted", request.GetScope(), command.rootChildThreadID, command.controlOperationID, len(targets), 0)
	if duplicate {
		return &bridgev1.AdmitChildInterruptResponse{Outcome: &bridgev1.AdmitChildInterruptResponse_Duplicate{Duplicate: &bridgev1.AdmitChildInterruptDuplicate{ControlOperationId: command.controlOperationID}}}, nil
	}
	return &bridgev1.AdmitChildInterruptResponse{Outcome: &bridgev1.AdmitChildInterruptResponse_Committed{Committed: &bridgev1.AdmitChildInterruptCommitted{ControlOperationId: command.controlOperationID}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) AwaitChildInterrupt(ctx context.Context, request *bridgev1.AwaitChildInterruptRequest) (*bridgev1.AwaitChildInterruptResponse, error) {
	if request.GetScope() == nil || request.GetControlOperationId() == "" {
		err := status.Error(codes.InvalidArgument, "child interrupt scope and control operation are required")
		logActorBoundaryRejected(s.Logger, request.GetScope(), "await_child_interrupt", request.GetControlOperationId(), "validate", err)
		return nil, err
	}
	var outcomes []*bridgev1.ChildInterruptTargetOutcome
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.await_child_interrupt", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		command, err := deriveChildControlCommandByOperationTx(ctx, tx, request.GetScope(), request.GetControlOperationId())
		if err != nil {
			return err
		}
		stored, ok, err := readChildInterruptCensusTx(ctx, tx, request.GetScope(), request.GetControlOperationId())
		if err != nil {
			return err
		}
		if !ok {
			return status.Error(codes.FailedPrecondition, "child interrupt census is missing or conflicting")
		}
		if err := validateStoredChildInterruptRequestTx(ctx, tx, command.scope, command.sourceToolUseEventID, command.rootChildThreadID, command.action, command.includeDescendants); err != nil {
			return err
		}
		if terminal, err := childControlSourceTerminalTx(ctx, tx, request.GetScope(), command.sourceToolUseEventID); err != nil {
			return err
		} else if terminal {
			return status.Error(codes.FailedPrecondition, "child_control_source_terminal")
		}
		pending := false
		for _, target := range stored {
			outcome, targetPending, err := childInterruptOutcomeTx(ctx, tx, request.GetScope(), target)
			if err != nil {
				return err
			}
			pending = pending || targetPending
			outcomes = append(outcomes, outcome)
		}
		if pending {
			return status.Error(codes.DeadlineExceeded, "child interrupt is still pending")
		}
		return nil
	})
	if err != nil {
		if status.Code(err) != codes.DeadlineExceeded {
			logThreadControlFailure(s.Logger, request.GetScope(), request.GetControlOperationId(), "delivery", err)
		}
		return nil, err
	}
	return &bridgev1.AwaitChildInterruptResponse{Outcome: &bridgev1.AwaitChildInterruptResponse_Completed{Completed: &bridgev1.AwaitChildInterruptCompleted{Targets: outcomes}}}, nil
}

func logThreadControlEvent(logger *slog.Logger, event string, scope *bridgev1.RuntimeScope, threadID, operationID string, targetCount, projectionCount int, durationMS ...int64) {
	if logger == nil || scope == nil {
		return
	}
	defer func() { _ = recover() }()
	attrs := []any{
		slog.String("event.kind", event),
		slog.String("operation", "thread_control"),
		slog.String("operation.id", safeActorDiagnosticIdentity(operationID)),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("thread.id", safeActorDiagnosticIdentity(threadID)),
		slog.Int("target.count", targetCount),
	}
	if event == "thread_interrupt_completed" {
		duration := int64(0)
		if len(durationMS) == 1 && durationMS[0] >= 0 {
			duration = durationMS[0]
		}
		attrs = append(attrs, slog.Int("projection.count", projectionCount), slog.String("outcome", "completed"), slog.Int64("duration.ms", duration))
	}
	if event == "thread_interrupt_stale" {
		attrs = append(attrs, slog.String("stale.reason", "binding_custody_changed"))
	}
	logger.Info("thread control lifecycle advanced", attrs...)
}

func logThreadControlFailure(logger *slog.Logger, scope *bridgev1.RuntimeScope, operationID, stage string, err error) {
	if logger == nil || scope == nil {
		return
	}
	defer func() { _ = recover() }()
	logger.Error("thread control failed",
		slog.String("event.kind", "thread_control_failed"),
		slog.String("operation", "thread_control"),
		slog.String("operation.id", safeActorDiagnosticIdentity(operationID)),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("thread.id", scope.GetSessionThreadId()),
		slog.String("stage", stage),
		slog.String("error.class", "thread_control"),
		slog.String("error.code", status.Code(err).String()),
		slog.String("error.message_safe", "thread control failed"),
	)
}

func logActorBoundaryRejected(logger *slog.Logger, scope *bridgev1.RuntimeScope, operation, operationID, phase string, rejection error) {
	if logger == nil || scope == nil || rejection == nil {
		return
	}
	defer func() { _ = recover() }()
	logger.Warn("actor boundary rejected",
		slog.String("event.kind", "actor_boundary_rejected"),
		slog.String("operation", operation),
		slog.String("operation.id", safeActorDiagnosticIdentity(operationID)),
		slog.String("phase", phase),
		slog.String("reason", status.Code(rejection).String()),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("thread.id", scope.GetSessionThreadId()),
	)
}

func safeActorDiagnosticIdentity(value string) string {
	if !validActorIdentity(value) {
		return "invalid"
	}
	return value
}

// logCommittedThreadInterrupt derives the public-safe control identity from the
// durable interrupt event. Logging stays outside the declaration transaction
// and cannot change typed-result application or retry behavior.
func (s *PostgreSQLBridgeAPIStore) logCommittedThreadInterrupt(ctx context.Context, request *bridgev1.CommitInputsRequest, event string, projectionCount int, durationMS int64) {
	if s == nil || s.Logger == nil || request == nil || request.GetRuntimeInputId() == "" {
		return
	}
	var payloadJSON string
	var targetCount int
	if err := s.withScopeReadOnlyTx(ctx, request.GetScope(), "agentruntimebridge.interrupt_log_identity", func(tx *dbconnect.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT payload_json FROM session_events
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND type=$4
			  AND payload_json::jsonb->>'runtime_input_id'=$5`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), childInterruptRequestedEventType, request.GetRuntimeInputId(),
		).Scan(&payloadJSON); err != nil {
			return err
		}
		var payload childInterruptEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM session_events
			WHERE workspace_id=$1 AND session_id=$2 AND type=$3
			  AND payload_json::jsonb->>'source_tool_use_event_id'=$4`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), childInterruptRequestedEventType, payload.SourceToolUseEventID,
		).Scan(&targetCount)
	}); err != nil {
		return
	}
	var payload childInterruptEventPayload
	if json.Unmarshal([]byte(payloadJSON), &payload) != nil {
		return
	}
	logThreadControlEvent(s.Logger, event, request.GetScope(), payload.RootChildThreadID, payload.ControlOperationID, targetCount, projectionCount, durationMS)
}

func childInterruptTargetIDsTx(ctx context.Context, tx *dbconnect.Tx, command childControlCommand) ([]string, error) {
	if command.includeDescendants {
		return childLifecycleSubtreeIDsTx(ctx, tx, command.scope, command.rootChildThreadID)
	}
	var parentID string
	if err := tx.QueryRow(ctx, `SELECT parent_thread_id FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND role <> 'main' FOR SHARE`,
		command.scope.GetWorkspaceId(), command.scope.GetSessionId(), command.rootChildThreadID).Scan(&parentID); dbconnect.IsNoRows(err) {
		return nil, status.Error(codes.NotFound, "child thread not found")
	} else if err != nil {
		return nil, err
	}
	if parentID != command.scope.GetSessionThreadId() {
		return nil, status.Error(codes.FailedPrecondition, "child interrupt target is not owned by the parent thread")
	}
	return []string{command.rootChildThreadID}, nil
}

func deriveChildControlCommandTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sourceID string) (childControlCommand, error) {
	var payload string
	if err := tx.QueryRow(ctx, `SELECT payload_json FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4 AND type='agent.tool_use' AND visibility='public' FOR SHARE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), sourceID).Scan(&payload); dbconnect.IsNoRows(err) {
		return childControlCommand{}, status.Error(codes.FailedPrecondition, "child control source Tool Use is invalid")
	} else if err != nil {
		return childControlCommand{}, err
	}
	var tool runtimeToolUseEventPayload
	if err := json.Unmarshal([]byte(payload), &tool); err != nil {
		return childControlCommand{}, status.Error(codes.FailedPrecondition, "child control source Tool Use is malformed")
	}
	command := childControlCommand{scope: scope, sourceToolUseEventID: sourceID}
	switch tool.Name {
	case "interrupt_agent":
		command.action = bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT
	case "close_agent":
		command.action = bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE
		command.includeDescendants = true
	default:
		return childControlCommand{}, status.Error(codes.FailedPrecondition, "child control source Tool name is invalid")
	}
	var input struct {
		TaskName string `json:"task_name"`
	}
	if err := json.Unmarshal(tool.Input, &input); err != nil || input.TaskName == "" {
		return childControlCommand{}, status.Error(codes.FailedPrecondition, "child control source Tool input is malformed")
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND parent_thread_id=$3 AND task_name=$4 AND role='subagent' FOR SHARE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), input.TaskName,
	).Scan(&command.rootChildThreadID); dbconnect.IsNoRows(err) {
		return childControlCommand{}, status.Error(codes.NotFound, "child thread not found")
	} else if err != nil {
		return childControlCommand{}, err
	}
	return command, nil
}

func deriveChildControlCommandByOperationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, operationID string) (childControlCommand, error) {
	var sourceID string
	if err := tx.QueryRow(ctx, `SELECT payload_json::jsonb ->> 'source_tool_use_event_id'
		FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type=$3
		AND payload_json::jsonb ->> 'control_operation_id'=$4
		ORDER BY event_id LIMIT 1 FOR SHARE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), childInterruptRequestedEventType, operationID,
	).Scan(&sourceID); dbconnect.IsNoRows(err) {
		return childControlCommand{}, status.Error(codes.FailedPrecondition, "child interrupt control operation is unknown")
	} else if err != nil {
		return childControlCommand{}, err
	}
	command, err := deriveChildControlCommandTx(ctx, tx, scope, sourceID)
	if err != nil {
		return childControlCommand{}, err
	}
	command.controlOperationID = operationID
	return command, nil
}

func childControlSourceTerminalTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sourceID string) (bool, error) {
	var terminal bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND type IN ('agent.tool_result','agent.mcp_tool_result') AND (payload_json::jsonb ->> 'tool_use_event_id'=$4 OR payload_json::jsonb ->> 'tool_use_id'=$4 OR payload_json::jsonb ->> 'mcp_tool_use_id'=$4))`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), sourceID).Scan(&terminal)
	return terminal, err
}

func insertChildInterruptTargetTx(ctx context.Context, tx *dbconnect.Tx, command childControlCommand, targetID string, now time.Time) (*bridgev1.ChildInterruptTarget, error) {
	var threadStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3 FOR UPDATE`,
		command.scope.GetWorkspaceId(), command.scope.GetSessionId(), targetID).Scan(&threadStatus); err != nil {
		return nil, err
	}
	disposition := bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PENDING_CONTROL
	dispositionName := "pending_control"
	switch threadStatus {
	case "closed_for_runtime":
		disposition, dispositionName = bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_ALREADY_CLOSED, "already_closed"
	case "failed":
		disposition, dispositionName = bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PRESERVED_FAILED, "preserved_failed"
	case "terminated":
		disposition, dispositionName = bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PRESERVED_TERMINATED, "preserved_terminated"
	}
	eventID := stableRuntimeID("child_interrupt_event", command.scope.GetWorkspaceId(), command.scope.GetSessionId(), command.sourceToolUseEventID, targetID)
	runtimeInputID := stableRuntimeID("child_interrupt_input", command.scope.GetWorkspaceId(), command.scope.GetSessionId(), command.sourceToolUseEventID, targetID)
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scopeForThread(command.scope, targetID))
	if err != nil {
		return nil, err
	}
	payload := childInterruptEventPayload{
		Type: childInterruptRequestedEventType, SourceToolUseEventID: command.sourceToolUseEventID,
		ControlOperationID: command.controlOperationID,
		RootChildThreadID:  command.rootChildThreadID, Action: childControlActionName(command.action),
		IncludeDescendants: command.includeDescendants, TargetThreadID: targetID,
		Disposition: dispositionName, RequestedAt: now.UTC().Format(time.RFC3339Nano),
	}
	if disposition == bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PENDING_CONTROL {
		payload.RuntimeInputID = runtimeInputID
	}
	payloadJSON, err := marshalBridgeJSON(payload)
	if err != nil {
		return nil, err
	}
	processedAt := any(now)
	if disposition == bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PENDING_CONTROL {
		processedAt = nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_events (workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,visibility,session_visible,runtime_write_id,processed_at,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,'internal',false,$4,$8,$9,$9)`,
		command.scope.GetWorkspaceId(), command.scope.GetSessionId(), targetID, eventID, sequence, childInterruptRequestedEventType, payloadJSON, processedAt, now); err != nil {
		return nil, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scopeForThread(command.scope, targetID), eventID, "internal", false, now); err != nil {
		return nil, err
	}
	target := &bridgev1.ChildInterruptTarget{ChildThreadId: targetID, Disposition: disposition}
	if disposition != bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PENDING_CONTROL {
		return target, nil
	}
	target.RuntimeInputId = &runtimeInputID
	target.InterruptEventId = &eventID
	target.InterruptEventSequence = &sequence
	eventIDs, _ := json.Marshal([]string{eventID})
	if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,sequence_from,sequence_to,status,created_at,updated_at) VALUES ($1,$2,$3,$4,'interrupt_control',$5,$6,$6,'queued',$7,$7)`,
		command.scope.GetWorkspaceId(), command.scope.GetSessionId(), targetID, runtimeInputID, string(eventIDs), sequence, now); err != nil {
		return nil, err
	}
	payloadBytes, err := json.Marshal(runtimeInputQueuePayload{
		WorkspaceID: command.scope.GetWorkspaceId(), SessionID: command.scope.GetSessionId(), SessionThreadID: targetID,
		RuntimeInputID: runtimeInputID, EventIDs: []string{eventID}, SequenceFrom: sequence, SequenceTo: sequence, InputKind: "interrupt_control",
	})
	if err != nil {
		return nil, err
	}
	ws := workspace.ID(command.scope.GetWorkspaceId())
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{ID: queue.NewJobID(), WorkspaceID: ws, Kind: queue.KindRuntimeInput,
		PartitionKey: queue.FormatSessionPartitionKey(ws, command.scope.GetSessionId()), DedupeKey: queue.FormatRuntimeInputDedupeKey(ws, command.scope.GetSessionId(), runtimeInputID),
		PayloadVersion: 1, PayloadJSON: payloadBytes, Priority: 100, MaxAttempts: queue.DefaultMaxAttempts, Now: now})
	return target, err
}

func readChildInterruptCensusTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, operationID string) ([]*bridgev1.ChildInterruptTarget, bool, error) {
	rows, err := tx.Query(ctx, `SELECT session_thread_id,event_id,sequence,payload_json FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type=$3 AND payload_json::jsonb ->> 'control_operation_id'=$4 ORDER BY session_thread_id FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), childInterruptRequestedEventType, operationID)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	var targets []*bridgev1.ChildInterruptTarget
	for rows.Next() {
		var threadID, eventID, payloadJSON string
		var sequence int64
		if err := rows.Scan(&threadID, &eventID, &sequence, &payloadJSON); err != nil {
			return nil, false, err
		}
		var payload childInterruptEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.TargetThreadID != threadID {
			return nil, false, status.Error(codes.FailedPrecondition, "stored child interrupt census is invalid")
		}
		disposition := childInterruptDisposition(payload.Disposition)
		if disposition == bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_UNSPECIFIED {
			return nil, false, status.Error(codes.FailedPrecondition, "stored child interrupt disposition is invalid")
		}
		target := &bridgev1.ChildInterruptTarget{ChildThreadId: threadID, Disposition: disposition}
		if disposition == bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PENDING_CONTROL {
			if payload.RuntimeInputID == "" {
				return nil, false, status.Error(codes.FailedPrecondition, "stored child interrupt input identity is missing")
			}
			target.RuntimeInputId, target.InterruptEventId, target.InterruptEventSequence = &payload.RuntimeInputID, &eventID, &sequence
		}
		targets = append(targets, target)
	}
	return targets, len(targets) > 0, rows.Err()
}

func readChildInterruptCensusBySourceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sourceID string) ([]*bridgev1.ChildInterruptTarget, string, bool, error) {
	var operationID string
	err := tx.QueryRow(ctx, `SELECT payload_json::jsonb ->> 'control_operation_id' FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND type=$3
		AND payload_json::jsonb ->> 'source_tool_use_event_id'=$4
		ORDER BY event_id LIMIT 1 FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), childInterruptRequestedEventType, sourceID,
	).Scan(&operationID)
	if dbconnect.IsNoRows(err) {
		return nil, "", false, nil
	}
	if err != nil {
		return nil, "", false, err
	}
	if operationID == "" {
		return nil, "", false, status.Error(codes.FailedPrecondition, "stored child interrupt control operation is missing")
	}
	targets, ok, err := readChildInterruptCensusTx(ctx, tx, scope, operationID)
	return targets, operationID, ok, err
}

func validateStoredChildInterruptRequestTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sourceID, rootID string, action bridgev1.ChildControlAction, descendants bool) error {
	rows, err := tx.Query(ctx, `SELECT payload_json FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type=$3 AND payload_json::jsonb ->> 'source_tool_use_event_id'=$4 FOR SHARE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), childInterruptRequestedEventType, sourceID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var payload childInterruptEventPayload
		if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload.RootChildThreadID != rootID || payload.Action != childControlActionName(action) || payload.IncludeDescendants != descendants || payload.SourceToolUseEventID != sourceID {
			return status.Error(codes.AlreadyExists, "child interrupt idempotency conflict")
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count == 0 {
		return status.Error(codes.FailedPrecondition, "stored child interrupt census is missing")
	}
	return nil
}

func childInterruptOutcomeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, target *bridgev1.ChildInterruptTarget) (*bridgev1.ChildInterruptTargetOutcome, bool, error) {
	outcome := &bridgev1.ChildInterruptTargetOutcome{ChildThreadId: target.GetChildThreadId()}
	switch target.GetDisposition() {
	case bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_ALREADY_CLOSED:
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_DUPLICATE
		return outcome, false, nil
	case bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PRESERVED_FAILED:
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_PRESERVED_FAILED
		return outcome, false, nil
	case bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PRESERVED_TERMINATED:
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_PRESERVED_TERMINATED
		return outcome, false, nil
	}
	var inboxStatus, threadStatus string
	err := tx.QueryRow(ctx, `SELECT inbox.status,thread.status FROM session_runtime_inbox inbox JOIN session_threads thread ON thread.workspace_id=inbox.workspace_id AND thread.session_id=inbox.session_id AND thread.id=inbox.session_thread_id WHERE inbox.workspace_id=$1 AND inbox.session_id=$2 AND inbox.runtime_input_id=$3 FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), target.GetRuntimeInputId()).Scan(&inboxStatus, &threadStatus)
	if dbconnect.IsNoRows(err) {
		return nil, false, status.Error(codes.FailedPrecondition, "child interrupt inbox is missing")
	}
	if err != nil {
		return nil, false, err
	}
	switch threadStatus {
	case "failed":
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_PRESERVED_FAILED
		return outcome, false, nil
	case "terminated":
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_PRESERVED_TERMINATED
		return outcome, false, nil
	}
	switch inboxStatus {
	case "committed":
		active, err := targetHasActiveTaskNotificationJobsTx(ctx, tx, scope, target.GetChildThreadId())
		if err != nil {
			return nil, false, err
		}
		if active {
			return outcome, true, nil
		}
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_COMPLETED
		return outcome, false, nil
	case "cancelled", "dead_lettered":
		outcome.Outcome = bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_DELIVERY_FAILED
		code := "child_interrupt_delivery_failed"
		outcome.ErrorCode = &code
		return outcome, false, nil
	case "queued", "delivering", "accepted":
		return outcome, true, nil
	default:
		return nil, false, status.Error(codes.FailedPrecondition, "child interrupt inbox status is invalid")
	}
}

// deferTargetTaskNotificationsTx closes the pending-Queue side of notification
// custody before applying the shared parked transition. A concurrently leased
// job cannot pass the Session lock until it observes the close fence and ACKs
// its own token through deferLeasedTaskNotificationTx.
func deferTargetTaskNotificationsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, targetIDs []string, now time.Time) error {
	if len(targetIDs) == 0 {
		return nil
	}
	ws := workspace.ID(scope.GetWorkspaceId())
	for _, targetID := range targetIDs {
		rows, err := tx.Query(ctx, `SELECT q.id,q.kind,q.partition_key,q.dedupe_key
			FROM session_runtime_inbox inbox
			JOIN queue_jobs q ON q.workspace_id=inbox.workspace_id
			 AND q.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
			WHERE inbox.workspace_id=$1 AND inbox.session_id=$2 AND inbox.session_thread_id=$3
			 AND inbox.input_kind='task_notification' AND inbox.status IN ('queued','delivering','accepted','parked')
			 AND q.status='pending' FOR UPDATE OF inbox,q`, scope.GetWorkspaceId(), scope.GetSessionId(), targetID)
		if err != nil {
			return err
		}
		type pendingJob struct{ id, kind, partitionKey, dedupeKey string }
		var pending []pendingJob
		for rows.Next() {
			var job pendingJob
			if err := rows.Scan(&job.id, &job.kind, &job.partitionKey, &job.dedupeKey); err != nil {
				_ = rows.Close()
				return err
			}
			pending = append(pending, job)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, job := range pending {
			updated, err := queue.CancelTx(ctx, tx, queue.TargetedCancelRequest{WorkspaceID: ws, JobID: job.id, Kind: job.kind, PartitionKey: job.partitionKey, DedupeKey: job.dedupeKey, Now: now})
			if err != nil {
				return err
			}
			if !updated {
				return status.Error(codes.Aborted, "task notification Queue authority changed during close admission")
			}
		}
		inboxRows, err := tx.Query(ctx, `SELECT runtime_input_id FROM session_runtime_inbox
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			 AND input_kind='task_notification' AND status IN ('queued','delivering','accepted','parked')
			ORDER BY created_at,runtime_input_id FOR UPDATE`, scope.GetWorkspaceId(), scope.GetSessionId(), targetID)
		if err != nil {
			return err
		}
		var runtimeInputIDs []string
		for inboxRows.Next() {
			var runtimeInputID string
			if err := inboxRows.Scan(&runtimeInputID); err != nil {
				_ = inboxRows.Close()
				return err
			}
			runtimeInputIDs = append(runtimeInputIDs, runtimeInputID)
		}
		if err := inboxRows.Close(); err != nil {
			return err
		}
		binding := runtimeBindingForDelivery{
			BindingID: scope.GetBinding().GetBindingId(), BindingGeneration: scope.GetBinding().GetBindingGeneration(),
			PodUID: scope.GetBinding().GetTargetPodUid(),
		}
		for _, runtimeInputID := range runtimeInputIDs {
			parked, err := parkTaskNotificationInboxTx(
				ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), targetID, runtimeInputID, binding, now,
			)
			if err != nil {
				return err
			}
			if !parked {
				return status.Error(codes.Aborted, "task notification Inbox authority changed during close admission")
			}
		}
	}
	return nil
}

func targetHasActiveTaskNotificationJobsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, targetID string) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM session_runtime_inbox inbox JOIN queue_jobs q
		 ON q.workspace_id=inbox.workspace_id
		AND q.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE inbox.workspace_id=$1 AND inbox.session_id=$2 AND inbox.session_thread_id=$3
		 AND inbox.input_kind='task_notification' AND inbox.status IN ('queued','parked')
		 AND q.status IN ('pending','leased'))`, scope.GetWorkspaceId(), scope.GetSessionId(), targetID).Scan(&active)
	return active, err
}

func validateChildCloseCensusTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, childThreadID string, subtreeIDs []string, operationID string) error {
	stored, ok, err := readChildInterruptCensusTx(ctx, tx, scope, operationID)
	if err != nil {
		return err
	}
	if !ok {
		return status.Error(codes.FailedPrecondition, "child close requires its frozen interrupt census")
	}
	command, err := deriveChildControlCommandByOperationTx(ctx, tx, scope, operationID)
	if err != nil {
		return err
	}
	if command.rootChildThreadID != childThreadID || command.action != bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE || !command.includeDescendants {
		return status.Error(codes.FailedPrecondition, "child close control operation is invalid")
	}
	if err := validateStoredChildInterruptRequestTx(ctx, tx, scope, command.sourceToolUseEventID, childThreadID, bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, true); err != nil {
		return err
	}
	if terminal, err := childControlSourceTerminalTx(ctx, tx, scope, command.sourceToolUseEventID); err != nil {
		return err
	} else if terminal {
		return status.Error(codes.FailedPrecondition, "child_control_source_terminal")
	}
	frozenIDs := make([]string, 0, len(stored))
	for _, target := range stored {
		frozenIDs = append(frozenIDs, target.GetChildThreadId())
	}
	sort.Strings(frozenIDs)
	sortedSubtree := append([]string(nil), subtreeIDs...)
	sort.Strings(sortedSubtree)
	if len(frozenIDs) != len(sortedSubtree) {
		return status.Error(codes.FailedPrecondition, "child close subtree changed after admission")
	}
	for index := range frozenIDs {
		if frozenIDs[index] != sortedSubtree[index] {
			return status.Error(codes.FailedPrecondition, "child close subtree changed after admission")
		}
	}
	for _, target := range stored {
		outcome, pending, err := childInterruptOutcomeTx(ctx, tx, scope, target)
		if err != nil {
			return err
		}
		if pending {
			return status.Error(codes.FailedPrecondition, "child close interrupt is still pending")
		}
		if outcome.GetOutcome() == bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_DELIVERY_FAILED {
			return status.Error(codes.FailedPrecondition, "child_close_delivery_failed")
		}
	}
	return nil
}

func childControlActionName(action bridgev1.ChildControlAction) string {
	if action == bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE {
		return "close"
	}
	return "interrupt"
}

func childInterruptDisposition(value string) bridgev1.ChildInterruptDisposition {
	switch value {
	case "pending_control":
		return bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PENDING_CONTROL
	case "already_closed":
		return bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_ALREADY_CLOSED
	case "preserved_failed":
		return bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PRESERVED_FAILED
	case "preserved_terminated":
		return bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_PRESERVED_TERMINATED
	default:
		return bridgev1.ChildInterruptDisposition_CHILD_INTERRUPT_DISPOSITION_UNSPECIFIED
	}
}
