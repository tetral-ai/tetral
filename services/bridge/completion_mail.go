package agentruntimebridge

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const (
	MailFetchMaxEnvelopes = 4
)

// appendSubagentMailEnvelopeTx owns the sender-side half of later direct agent
// mail. Runtime supplies the already interpreted target and bounded text;
// Bridge validates only the deterministic identity and exact parent-child
// ownership before birthing Event and Inbox/Queue custody atomically.
func appendSubagentMailEnvelopeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	deliveryID string,
	targetThreadID string,
	sourceToolUseEventID string,
	content string,
	now time.Time,
) (runtimecontrol.StoredAgentMailEnvelope, error) {
	if deliveryID == "" || targetThreadID == "" || sourceToolUseEventID == "" || content == "" || len([]byte(content)) > runtimecontrol.AgentMailContentMaxBytes {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "agent mail identity and bounded content are required")
	}
	if deliveryID != runtimecontrol.AgentMailDeliveryID(sourceToolUseEventID, targetThreadID) {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "agent mail delivery identity is invalid")
	}
	if content != strings.TrimSpace(content) {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "agent mail content is not normalized")
	}
	if terminal, err := childControlSourceTerminalTx(ctx, tx, scope, sourceToolUseEventID); err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	} else if terminal {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail source Tool Use is terminal")
	}
	var targetTaskName string
	if err := tx.QueryRow(ctx, `SELECT task_name
		FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND parent_thread_id=$4
		AND role='subagent' AND visibility='public'
		AND status NOT IN ('closed_for_runtime','failed','terminated')
		FOR SHARE`, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, scope.GetSessionThreadId()).Scan(&targetTaskName); dbconnect.IsNoRows(err) {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail target is not a receivable public sub-agent owned by the parent")
	} else if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	messageJSON, err := runtimecontrol.PublicAgentMailMessageJSON(content)
	if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	eventPayloadJSON, err := runtimecontrol.MarshalJSON(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         scope.GetSessionThreadId(),
		"target_thread_id":         targetThreadID,
		"target_task_name":         targetTaskName,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	eventID := runtimecontrol.StableRuntimeID("agent_mail_sent_event", scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), deliveryID)
	sequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(), SessionThreadID: scope.GetSessionThreadId(),
		EventID: eventID, Sequence: sequence, Type: "agent.thread_message_sent",
		PayloadJSON: eventPayloadJSON, ProjectionJSON: eventPayloadJSON, Visibility: "public", SessionVisible: true,
		RuntimeWriteID: deliveryID, CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	if err := runtimecontrol.BirthCompletionMailCustodyTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, deliveryID, now); err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	return runtimecontrol.StoredAgentMailEnvelope{
		SentEventID: eventID, SentSequence: sequence, SentThreadID: scope.GetSessionThreadId(),
		DeliveryID: deliveryID, SourceThreadID: scope.GetSessionThreadId(), TargetThreadID: targetThreadID,
		SourceToolUseEventID: sourceToolUseEventID, Content: content, PublicMessageJSON: json.RawMessage(messageJSON),
	}, nil
}

// appendDeclaredSubagentInitialEnvelopeTx persists the Runtime-declared initial
// input without rereading Tool business arguments. The executable route has
// already been locked by CreateSubagentThread; this helper owns only the
// declared parent-child envelope and its durable Inbox/Queue birth.
func appendDeclaredSubagentInitialEnvelopeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	targetThreadID string,
	sourceToolUseEventID string,
	targetTaskName string,
	content string,
	now time.Time,
) (runtimecontrol.StoredAgentMailEnvelope, error) {
	if targetThreadID == "" || sourceToolUseEventID == "" || !validActorTaskName(targetTaskName) || !validActorInitialPrompt(content) {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "sub-agent initial input declaration is invalid")
	}
	var targetExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND parent_thread_id=$4
		 AND role='subagent' AND visibility='public' AND task_name=$5
	)`, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, scope.GetSessionThreadId(), targetTaskName).Scan(&targetExists); err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	if !targetExists {
		return runtimecontrol.StoredAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "sub-agent input target is invalid")
	}
	deliveryID := runtimecontrol.AgentMailDeliveryID(sourceToolUseEventID, targetThreadID)
	messageJSON, err := runtimecontrol.PublicAgentMailMessageJSON(content)
	if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	eventPayloadJSON, err := runtimecontrol.MarshalJSON(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         scope.GetSessionThreadId(),
		"target_thread_id":         targetThreadID,
		"target_task_name":         targetTaskName,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	eventID := runtimecontrol.StableRuntimeID("agent_mail_sent_event", scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), deliveryID)
	sequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(), SessionThreadID: scope.GetSessionThreadId(),
		EventID: eventID, Sequence: sequence, Type: "agent.thread_message_sent",
		PayloadJSON: eventPayloadJSON, ProjectionJSON: eventPayloadJSON, Visibility: "public", SessionVisible: true,
		RuntimeWriteID: deliveryID, CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	if err := runtimecontrol.BirthCompletionMailCustodyTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, deliveryID, now); err != nil {
		return runtimecontrol.StoredAgentMailEnvelope{}, err
	}
	return runtimecontrol.StoredAgentMailEnvelope{
		SentEventID: eventID, SentSequence: sequence, SentThreadID: scope.GetSessionThreadId(),
		DeliveryID: deliveryID, SourceThreadID: scope.GetSessionThreadId(), TargetThreadID: targetThreadID,
		SourceToolUseEventID: sourceToolUseEventID, Content: content, PublicMessageJSON: json.RawMessage(messageJSON),
	}, nil
}

// appendDeclaredSubagentReceivedEventTx births the target-side durable input
// fact. A target interrupt may delay Queue delivery, but it does not erase a
// healthy source's already authorized input birth.
func appendDeclaredSubagentReceivedEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	targetScope *bridgev1.RuntimeScope,
	envelope runtimecontrol.StoredAgentMailEnvelope,
	now time.Time,
) (string, error) {
	threadScope, err := runtimecontrol.LockThreadMutationRowTx(ctx, tx, targetScope)
	if err != nil {
		return "", err
	}
	if threadScope.Role != "subagent" || envelope.TargetThreadID != targetScope.GetSessionThreadId() {
		return "", status.Error(codes.FailedPrecondition, "sub-agent input target is invalid")
	}
	sourceTaskName, err := runtimecontrol.SessionThreadCallableTaskNameTx(ctx, tx, targetScope, envelope.SourceThreadID)
	if err != nil {
		return "", err
	}
	eventPayloadJSON, err := runtimecontrol.MarshalJSON(map[string]any{
		"type":                     "agent.thread_message_received",
		"delivery_id":              envelope.DeliveryID,
		"source_thread_id":         envelope.SourceThreadID,
		"source_task_name":         runtimecontrol.NullableJSONString(sourceTaskName),
		"source_tool_use_event_id": envelope.SourceToolUseEventID,
		"message":                  envelope.PublicMessageJSON,
	})
	if err != nil {
		return "", err
	}
	eventID := runtimecontrol.StableRuntimeID("agent_mail_received_event", targetScope.GetWorkspaceId(), targetScope.GetSessionId(), targetScope.GetSessionThreadId(), envelope.DeliveryID)
	sequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, targetScope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.PublicProjection("agent.thread_message_received")
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: targetScope.GetWorkspaceId(), SessionID: targetScope.GetSessionId(), SessionThreadID: targetScope.GetSessionThreadId(),
		EventID: eventID, Sequence: sequence, Type: "agent.thread_message_received",
		PayloadJSON: eventPayloadJSON, ProjectionJSON: eventPayloadJSON, Visibility: visibility, SessionVisible: sessionVisible,
		CreatedAt: now,
	}); err != nil {
		return "", err
	}
	return eventID, nil
}

func appendDeclaredCompletionMailTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope runtimecontrol.ThreadMutationScope,
	durableTurnID string,
	text string,
	now time.Time,
) (string, error) {
	return runtimecontrol.AppendDeclaredCompletionMailForSourceTx(
		ctx,
		tx,
		scope,
		threadScope,
		durableTurnID,
		text,
		now,
	)
}
