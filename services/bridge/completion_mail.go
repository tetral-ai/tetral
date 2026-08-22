package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strings"
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

const (
	MailFetchMaxEnvelopes    = 4
	AgentMailContentMaxBytes = 2 * 1024 * 1024
)

type storedAgentMailEnvelope struct {
	SentEventID          string
	SentSequence         int64
	SentThreadID         string
	DeliveryID           string
	SourceThreadID       string
	TargetThreadID       string
	SourceToolUseEventID string
	Content              string
	PublicMessageJSON    json.RawMessage
}

type admittedAgentMailDelivery struct {
	Envelope         storedAgentMailEnvelope
	ReceivedEventID  string
	ReceivedSequence int64
}

func agentMailDeliveryID(sourceToolUseEventID string, targetThreadID string) string {
	digest := sha256.Sum256([]byte(sourceToolUseEventID + ":" + targetThreadID + ":0"))
	return "delivery_" + hex.EncodeToString(digest[:])[:32]
}

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
) (storedAgentMailEnvelope, error) {
	if deliveryID == "" || targetThreadID == "" || sourceToolUseEventID == "" || content == "" || len([]byte(content)) > AgentMailContentMaxBytes {
		return storedAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "agent mail identity and bounded content are required")
	}
	if deliveryID != agentMailDeliveryID(sourceToolUseEventID, targetThreadID) {
		return storedAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "agent mail delivery identity is invalid")
	}
	if content != strings.TrimSpace(content) {
		return storedAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "agent mail content is not normalized")
	}
	if terminal, err := childControlSourceTerminalTx(ctx, tx, scope, sourceToolUseEventID); err != nil {
		return storedAgentMailEnvelope{}, err
	} else if terminal {
		return storedAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail source Tool Use is terminal")
	}
	var targetTaskName string
	if err := tx.QueryRow(ctx, `SELECT task_name
		FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND parent_thread_id=$4
		AND role='subagent' AND visibility='public'
		AND status NOT IN ('closed_for_runtime','failed','terminated')
		FOR SHARE`, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, scope.GetSessionThreadId()).Scan(&targetTaskName); dbconnect.IsNoRows(err) {
		return storedAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail target is not a receivable public sub-agent owned by the parent")
	} else if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	messageJSON, err := publicAgentMailMessageJSON(content)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	eventPayloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         scope.GetSessionThreadId(),
		"target_thread_id":         targetThreadID,
		"target_task_name":         targetTaskName,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	eventID := stableRuntimeID("agent_mail_sent_event", scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), deliveryID)
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,
		visibility,session_visible,runtime_write_id,projection_json,created_at,updated_at,processed_at
	) VALUES ($1,$2,$3,$4,$5,'agent.thread_message_sent',$6,'public',true,$7,$6,$8,$8,$8)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
		eventPayloadJSON, deliveryID, now); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, "public", true, now); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if err := birthCompletionMailCustodyTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, deliveryID, now); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	return storedAgentMailEnvelope{
		SentEventID: eventID, SentSequence: sequence, SentThreadID: scope.GetSessionThreadId(),
		DeliveryID: deliveryID, SourceThreadID: scope.GetSessionThreadId(), TargetThreadID: targetThreadID,
		SourceToolUseEventID: sourceToolUseEventID, Content: content, PublicMessageJSON: json.RawMessage(messageJSON),
	}, nil
}

// appendDeclaredSubagentInitialEnvelopeTx persists the Runtime-declared opening
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
) (storedAgentMailEnvelope, error) {
	if targetThreadID == "" || sourceToolUseEventID == "" || !validActorTaskName(targetTaskName) || !validActorInitialPrompt(content) {
		return storedAgentMailEnvelope{}, status.Error(codes.InvalidArgument, "sub-agent initial input declaration is invalid")
	}
	var targetExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND parent_thread_id=$4
		 AND role='subagent' AND visibility='public' AND task_name=$5
	)`, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, scope.GetSessionThreadId(), targetTaskName).Scan(&targetExists); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if !targetExists {
		return storedAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "sub-agent initial input target is invalid")
	}
	deliveryID := agentMailDeliveryID(sourceToolUseEventID, targetThreadID)
	messageJSON, err := publicAgentMailMessageJSON(content)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	eventPayloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         scope.GetSessionThreadId(),
		"target_thread_id":         targetThreadID,
		"target_task_name":         targetTaskName,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	eventID := stableRuntimeID("agent_mail_sent_event", scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), deliveryID)
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,
		visibility,session_visible,runtime_write_id,projection_json,created_at,updated_at,processed_at
	) VALUES ($1,$2,$3,$4,$5,'agent.thread_message_sent',$6,'public',true,$7,$6,$8,$8,$8)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
		eventPayloadJSON, deliveryID, now); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, "public", true, now); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if err := birthCompletionMailCustodyTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), targetThreadID, deliveryID, now); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	return storedAgentMailEnvelope{
		SentEventID: eventID, SentSequence: sequence, SentThreadID: scope.GetSessionThreadId(),
		DeliveryID: deliveryID, SourceThreadID: scope.GetSessionThreadId(), TargetThreadID: targetThreadID,
		SourceToolUseEventID: sourceToolUseEventID, Content: content, PublicMessageJSON: json.RawMessage(messageJSON),
	}, nil
}

func appendDeclaredSubagentInitialReceivedEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	targetScope *bridgev1.RuntimeScope,
	envelope storedAgentMailEnvelope,
	now time.Time,
) (string, error) {
	threadScope, err := lockThreadMutationTx(ctx, tx, targetScope)
	if err != nil {
		return "", err
	}
	if threadScope.role != "subagent" || envelope.TargetThreadID != targetScope.GetSessionThreadId() {
		return "", status.Error(codes.FailedPrecondition, "sub-agent initial input target is invalid")
	}
	sourceTaskName, err := sessionThreadCallableTaskNameTx(ctx, tx, targetScope, envelope.SourceThreadID)
	if err != nil {
		return "", err
	}
	eventPayloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "agent.thread_message_received",
		"delivery_id":              envelope.DeliveryID,
		"source_thread_id":         envelope.SourceThreadID,
		"source_task_name":         nullableJSONString(sourceTaskName),
		"source_tool_use_event_id": envelope.SourceToolUseEventID,
		"message":                  envelope.PublicMessageJSON,
	})
	if err != nil {
		return "", err
	}
	eventID := stableRuntimeID("agent_mail_received_event", targetScope.GetWorkspaceId(), targetScope.GetSessionId(), targetScope.GetSessionThreadId(), envelope.DeliveryID)
	sequence, err := nextSessionEventSequenceTx(ctx, tx, targetScope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.publicProjection("agent.thread_message_received")
	if _, err := tx.Exec(ctx, `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,
		visibility,session_visible,projection_json,created_at,updated_at,processed_at
	) VALUES ($1,$2,$3,$4,$5,'agent.thread_message_received',$6,$7,$8,$6,$9,$9,NULL)`,
		targetScope.GetWorkspaceId(), targetScope.GetSessionId(), targetScope.GetSessionThreadId(), eventID, sequence,
		eventPayloadJSON, visibility, sessionVisible, now); err != nil {
		return "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, targetScope, eventID, visibility, sessionVisible, now); err != nil {
		return "", err
	}
	return eventID, nil
}

func publicAgentMailMessageJSON(content string) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"content": []map[string]string{{"type": "text", "text": content}},
	})
}

// agentMailContentFromPublicMessage isolates the target-owned mail body from
// the frozen public Event projection. Runtime delivery and cold load carry
// this text only; the broad public Message representation never crosses the
// private target boundary.
func agentMailContentFromPublicMessage(raw json.RawMessage) (string, error) {
	var message struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &message); err != nil || len(message.Content) == 0 {
		return "", status.Error(codes.FailedPrecondition, "agent mail content is malformed")
	}
	texts := make([]string, 0, len(message.Content))
	for _, block := range message.Content {
		if block.Type != "text" {
			return "", status.Error(codes.FailedPrecondition, "agent mail content is not text")
		}
		texts = append(texts, block.Text)
	}
	content := strings.Join(texts, "\n")
	if content == "" || len([]byte(content)) > AgentMailContentMaxBytes {
		return "", status.Error(codes.FailedPrecondition, "agent mail content exceeds its bound")
	}
	return content, nil
}

func completionDeliveryID(childThreadID string, runtimeWriteID string) string {
	digest := sha256.Sum256([]byte(childThreadID + ":" + runtimeWriteID))
	return "delivery_" + hex.EncodeToString(digest[:])[:32]
}

func completionRuntimeInputID(deliveryID string) string {
	return "agent_mail:" + deliveryID
}

func loadStoredAgentMailEnvelopeByDeliveryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	deliveryID string,
) (storedAgentMailEnvelope, error) {
	rows, err := tx.Query(ctx,
		`SELECT event_id, sequence, session_thread_id, payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND type = 'agent.thread_message_sent'
		    AND payload_json::jsonb ->> 'delivery_id' = $3
		  ORDER BY sequence ASC, event_id ASC
		  LIMIT 2
		  FOR UPDATE`,
		workspaceID,
		sessionID,
		deliveryID,
	)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	defer func() { _ = rows.Close() }()
	var envelope storedAgentMailEnvelope
	var payloadJSON string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return storedAgentMailEnvelope{}, err
		}
		return storedAgentMailEnvelope{}, status.Error(codes.NotFound, "agent mail envelope not found")
	}
	if err := rows.Scan(&envelope.SentEventID, &envelope.SentSequence, &envelope.SentThreadID, &payloadJSON); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	if rows.Next() {
		return storedAgentMailEnvelope{}, status.Error(codes.AlreadyExists, "agent mail delivery id is not unique")
	}
	if err := rows.Err(); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	var payload struct {
		DeliveryID           string          `json:"delivery_id"`
		SourceThreadID       string          `json:"source_thread_id"`
		TargetThreadID       string          `json:"target_thread_id"`
		SourceToolUseEventID string          `json:"source_tool_use_event_id"`
		Message              json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil ||
		payload.DeliveryID == "" ||
		payload.SourceThreadID == "" ||
		payload.TargetThreadID == "" ||
		payload.SourceToolUseEventID == "" ||
		len(payload.Message) == 0 {
		return storedAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail envelope is malformed")
	}
	publicMessage, err := validatedPublicInterAgentMessageJSON(payload.Message)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	content, err := agentMailContentFromPublicMessage(publicMessage)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	envelope.DeliveryID = payload.DeliveryID
	envelope.SourceThreadID = payload.SourceThreadID
	envelope.TargetThreadID = payload.TargetThreadID
	envelope.SourceToolUseEventID = payload.SourceToolUseEventID
	envelope.Content = content
	envelope.PublicMessageJSON = publicMessage
	if envelope.SentThreadID != envelope.SourceThreadID {
		return storedAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail sent event does not belong to its declared source")
	}
	if err := validateAgentMailEnvelopeRelationshipTx(ctx, tx, workspaceID, sessionID, envelope); err != nil {
		return storedAgentMailEnvelope{}, err
	}
	return envelope, nil
}

func validateAgentMailEnvelopeRelationshipTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	envelope storedAgentMailEnvelope,
) error {
	if envelope.SourceThreadID == envelope.TargetThreadID {
		return status.Error(codes.FailedPrecondition, "agent mail source and target must differ")
	}
	var legal bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_threads child
			 WHERE child.workspace_id = $1
			   AND child.session_id = $2
			   AND child.role = 'subagent'
			   AND (
			     (child.id = $3 AND child.parent_thread_id = $4)
			     OR
			     (child.id = $4 AND child.parent_thread_id = $3)
			   )
		)`,
		workspaceID,
		sessionID,
		envelope.SourceThreadID,
		envelope.TargetThreadID,
	).Scan(&legal); err != nil {
		return err
	}
	if !legal {
		return status.Error(codes.FailedPrecondition, "agent mail envelope does not describe a parent-child delivery")
	}
	return nil
}

func admitAgentMailDeliveryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	targetScope *bridgev1.RuntimeScope,
	envelope storedAgentMailEnvelope,
	binding runtimeBindingForDelivery,
	now time.Time,
) (admittedAgentMailDelivery, error) {
	threadScope, err := lockThreadMutationTx(ctx, tx, targetScope)
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	if threadScope.role != "main" && threadScope.role != "subagent" {
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail must target a main or sub-agent thread")
	}
	if envelope.TargetThreadID != targetScope.GetSessionThreadId() {
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail target does not match the durable envelope")
	}
	var inboxStatus string
	var inboxBindingID, inboxPodUID sql.NullString
	var inboxBindingGeneration sql.NullInt64
	var inboxEventIDsJSON string
	var inboxSequenceFrom, inboxSequenceTo sql.NullInt64
	err = tx.QueryRow(ctx, `SELECT status, binding_id, binding_generation, target_pod_uid,
		event_ids_json, sequence_from, sequence_to
		FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		AND runtime_input_id=$4 AND input_kind='agent_mail'
		FOR UPDATE`, targetScope.GetWorkspaceId(), targetScope.GetSessionId(),
		targetScope.GetSessionThreadId(), completionRuntimeInputID(envelope.DeliveryID),
	).Scan(&inboxStatus, &inboxBindingID, &inboxBindingGeneration, &inboxPodUID,
		&inboxEventIDsJSON, &inboxSequenceFrom, &inboxSequenceTo)
	if dbconnect.IsNoRows(err) {
		return admittedAgentMailDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_inbox_custody_missing", message: "agent mail has no producer custody", retryable: false}
	}
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	switch inboxStatus {
	case "queued", "delivering", "accepted", "committed":
	case "dead_lettered", "cancelled":
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail delivery is terminal")
	default:
		return admittedAgentMailDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_inbox_status_invalid", message: "agent mail Inbox status is invalid", retryable: false}
	}
	sourceTaskName, err := sessionThreadCallableTaskNameTx(ctx, tx, targetScope, envelope.SourceThreadID)
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	eventPayloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "agent.thread_message_received",
		"delivery_id":              envelope.DeliveryID,
		"source_thread_id":         envelope.SourceThreadID,
		"source_task_name":         nullableJSONString(sourceTaskName),
		"source_tool_use_event_id": envelope.SourceToolUseEventID,
		"message":                  envelope.PublicMessageJSON,
	})
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	var (
		receivedEventID     string
		receivedSequence    int64
		receivedPayloadJSON string
	)
	err = tx.QueryRow(ctx,
		`SELECT event_id, sequence, payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $4
		  ORDER BY sequence ASC, event_id ASC
		  LIMIT 1
		  FOR UPDATE`,
		targetScope.GetWorkspaceId(),
		targetScope.GetSessionId(),
		targetScope.GetSessionThreadId(),
		envelope.DeliveryID,
	).Scan(&receivedEventID, &receivedSequence, &receivedPayloadJSON)
	if dbconnect.IsNoRows(err) {
		if closing, fenceErr := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, targetScope.GetWorkspaceId(), targetScope.GetSessionId(), targetScope.GetSessionThreadId()); fenceErr != nil {
			return admittedAgentMailDelivery{}, fenceErr
		} else if closing {
			return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail target is closing")
		}
		if !threadReceivableTx(threadScope) {
			return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail target is not receivable")
		}
		receivedEventID = stableRuntimeID(
			"agent_mail_received_event",
			targetScope.GetWorkspaceId(),
			targetScope.GetSessionId(),
			targetScope.GetSessionThreadId(),
			envelope.DeliveryID,
		)
		receivedSequence, err = nextSessionEventSequenceTx(ctx, tx, targetScope)
		if err != nil {
			return admittedAgentMailDelivery{}, err
		}
		visibility, sessionVisible := threadScope.publicProjection("agent.thread_message_received")
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, projection_json, created_at, updated_at, processed_at
			) VALUES ($1, $2, $3, $4, $5, 'agent.thread_message_received', $6, $7, $8, $6, $9, $9, NULL)`,
			targetScope.GetWorkspaceId(),
			targetScope.GetSessionId(),
			targetScope.GetSessionThreadId(),
			receivedEventID,
			receivedSequence,
			eventPayloadJSON,
			visibility,
			sessionVisible,
			now,
		); err != nil {
			return admittedAgentMailDelivery{}, err
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, targetScope, receivedEventID, visibility, sessionVisible, now); err != nil {
			return admittedAgentMailDelivery{}, err
		}
		receivedPayloadJSON = eventPayloadJSON
	} else if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	if normalizeJSONForCompare(json.RawMessage(receivedPayloadJSON)) != normalizeJSONForCompare(json.RawMessage(eventPayloadJSON)) {
		return admittedAgentMailDelivery{}, status.Error(codes.AlreadyExists, "agent mail delivery replay conflicts with the admitted source")
	}
	if !threadReceivableTx(threadScope) {
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail target is not receivable")
	}
	if inboxStatus == "accepted" {
		var inboxEventIDs []string
		if err := json.Unmarshal([]byte(inboxEventIDsJSON), &inboxEventIDs); err != nil ||
			len(inboxEventIDs) != 1 || inboxEventIDs[0] != receivedEventID ||
			!inboxSequenceFrom.Valid || !inboxSequenceTo.Valid ||
			inboxSequenceFrom.Int64 != receivedSequence || inboxSequenceTo.Int64 != receivedSequence ||
			!inboxBindingID.Valid || inboxBindingID.String != binding.BindingID ||
			!inboxBindingGeneration.Valid || inboxBindingGeneration.Int64 != binding.BindingGeneration ||
			!inboxPodUID.Valid || inboxPodUID.String != binding.PodUID {
			return admittedAgentMailDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_inbox_custody_invalid", message: "accepted agent mail custody conflicts with the current Runtime", retryable: false}
		}
	}
	if inboxStatus != "accepted" {
		job := RuntimeJob{
			WorkspaceID:     targetScope.GetWorkspaceId(),
			SessionID:       targetScope.GetSessionId(),
			SessionThreadID: targetScope.GetSessionThreadId(),
			RuntimeInputID:  completionRuntimeInputID(envelope.DeliveryID),
			Kind:            queue.KindRuntimeInput,
			InputKind:       "agent_mail",
			EventIDs:        []string{receivedEventID},
			SequenceFrom:    receivedSequence,
			SequenceTo:      receivedSequence,
		}
		if err := claimAgentMailInboxDeliveryTx(ctx, tx, job, binding, now); err != nil {
			return admittedAgentMailDelivery{}, err
		}
	}
	return admittedAgentMailDelivery{
		Envelope:         envelope,
		ReceivedEventID:  receivedEventID,
		ReceivedSequence: receivedSequence,
	}, nil
}

// The sent-event transaction creates agent-mail custody before the received
// projection has a sequence. Admission fills that deterministic projection
// identity while binding the existing row; it never inserts replacement
// custody from the Queue payload.
func claimAgentMailInboxDeliveryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	binding runtimeBindingForDelivery,
	now time.Time,
) error {
	if len(job.EventIDs) != 1 || job.SequenceFrom <= 0 || job.SequenceTo != job.SequenceFrom {
		return runtimeDeliveryPrepareError{kind: "runtime_inbox_custody_invalid", message: "agent mail projection identity is invalid", retryable: false}
	}
	eventIDsJSON, err := json.Marshal(job.EventIDs)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `UPDATE session_runtime_inbox
		SET event_ids_json=$5,sequence_from=$6,sequence_to=$6,
		    status=CASE WHEN status='committed' THEN 'committed' ELSE 'delivering' END,
		    binding_id=$7,binding_generation=$8,target_pod_uid=$9,updated_at=$10
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND runtime_input_id=$4
		  AND input_kind='agent_mail'
		  AND (
		    (status='queued' AND (
		      (event_ids_json='[]' AND sequence_from IS NULL AND sequence_to IS NULL)
		      OR (event_ids_json=$5 AND sequence_from=$6 AND sequence_to=$6)
		    ))
		    OR (status='delivering' AND event_ids_json=$5 AND sequence_from=$6 AND sequence_to=$6
		        AND binding_id=$7 AND binding_generation=$8 AND target_pod_uid=$9)
		    OR (status='committed' AND event_ids_json=$5 AND sequence_from=$6 AND sequence_to=$6)
		  )`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RuntimeInputID,
		string(eventIDsJSON), job.SequenceFrom, binding.BindingID, binding.BindingGeneration,
		binding.PodUID, now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return runtimeDeliveryPrepareError{kind: "runtime_inbox_custody_invalid", message: "agent mail has no matching producer custody", retryable: false}
	}
	return nil
}

func appendDeclaredCompletionMailTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	durableTurnID string,
	text string,
	now time.Time,
) (string, error) {
	return appendDeclaredCompletionMailForSourceTx(
		ctx,
		tx,
		scope,
		threadScope,
		durableTurnID,
		text,
		now,
	)
}

func appendDeclaredCompletionMailForSourceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	sourceID string,
	text string,
	now time.Time,
) (string, error) {
	if strings.TrimSpace(text) == "" {
		return "", status.Error(codes.InvalidArgument, "completion mail text is required")
	}
	if threadScope.role != "subagent" || threadScope.status == "closed_for_runtime" {
		return "", status.Error(codes.InvalidArgument, "completion mail requires a live sub-agent thread")
	}

	parentThreadID, sourceToolUseEventID, targetTaskName, err := completionLineageTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	deliveryID := completionDeliveryID(scope.GetSessionThreadId(), sourceID)
	messageJSON, err := publicAgentMailMessageJSON(text)
	if err != nil {
		return "", err
	}
	eventPayloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         scope.GetSessionThreadId(),
		"target_thread_id":         parentThreadID,
		"target_task_name":         nullableJSONString(targetTaskName),
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		return "", err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.publicProjection("agent.thread_message_sent")
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.thread_message_sent', $6, $7, $8, $9, $6, $10, $10, $10)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		eventPayloadJSON,
		visibility,
		sessionVisible,
		sourceID,
		now,
	); err != nil {
		return "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return "", err
	}
	if err := birthCompletionMailCustodyTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
		deliveryID,
		now,
	); err != nil {
		return "", err
	}
	return eventID, nil
}

// Completion mail is born with its Runtime Inbox row and Queue job in the same
// transaction as the sent event. Delivery only binds this existing custody to
// a Runtime; no later lifecycle may reconstruct it from the event ledger.
func birthCompletionMailCustodyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) error {
	runtimeInputID := completionRuntimeInputID(deliveryID)
	if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,
		event_ids_json,status,created_at,updated_at
	) VALUES ($1,$2,$3,$4,'agent_mail','[]','queued',$5,$5)`,
		workspaceID, sessionID, targetThreadID, runtimeInputID, now,
	); err != nil {
		return err
	}
	_, err := enqueueAgentMailWakeTx(
		ctx,
		tx,
		workspaceID,
		sessionID,
		targetThreadID,
		deliveryID,
		now,
	)
	return err
}

func enqueueAgentMailWakeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) (bool, error) {
	request, jobID, err := agentMailWakeEnqueueRequest(
		workspaceID,
		sessionID,
		targetThreadID,
		deliveryID,
		now,
	)
	if err != nil {
		return false, err
	}
	active, err := queue.EnqueueTx(ctx, tx, request)
	if err != nil {
		return false, err
	}
	return validateAgentMailWakeJob(active, jobID, workspaceID, sessionID, targetThreadID, deliveryID)
}

func agentMailWakeEnqueueRequest(
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) (queue.EnqueueRequest, string, error) {
	runtimeInputID := completionRuntimeInputID(deliveryID)
	queuePayload, err := json.Marshal(map[string]any{
		"workspace_id":      workspaceID,
		"session_id":        sessionID,
		"session_thread_id": targetThreadID,
		"runtime_input_id":  runtimeInputID,
		"event_ids":         []string{},
		"sequence_from":     0,
		"sequence_to":       0,
		"input_kind":        "agent_mail",
	})
	if err != nil {
		return queue.EnqueueRequest{}, "", err
	}
	ws := workspace.ID(workspaceID)
	jobID := id.New(queue.JobIDPrefix)
	return queue.EnqueueRequest{
		ID:             jobID,
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, sessionID, runtimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    queuePayload,
		MaxAttempts:    queue.DefaultMaxAttempts,
		Now:            now,
	}, jobID, nil
}

func validateAgentMailWakeJob(
	active *queue.Job,
	jobID string,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
) (bool, error) {
	runtimeInputID := completionRuntimeInputID(deliveryID)
	var activePayload struct {
		WorkspaceID     string `json:"workspace_id"`
		SessionID       string `json:"session_id"`
		SessionThreadID string `json:"session_thread_id"`
		RuntimeInputID  string `json:"runtime_input_id"`
		InputKind       string `json:"input_kind"`
	}
	if json.Unmarshal(active.PayloadJSON, &activePayload) != nil ||
		activePayload.WorkspaceID != workspaceID ||
		activePayload.SessionID != sessionID ||
		activePayload.SessionThreadID != targetThreadID ||
		activePayload.RuntimeInputID != runtimeInputID ||
		activePayload.InputKind != "agent_mail" {
		return false, status.Error(codes.AlreadyExists, "agent mail wake conflicts with the durable delivery")
	}
	return active.ID == jobID, nil
}

func completionLineageTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (string, string, sql.NullString, error) {
	var parentThreadID string
	var sourceToolUseEventID string
	if err := tx.QueryRow(ctx,
		`SELECT t.parent_thread_id, e.payload_json::jsonb ->> 'source_tool_use_event_id'
		   FROM session_threads t
		   JOIN LATERAL (
			SELECT payload_json
			  FROM session_events
			 WHERE workspace_id = t.workspace_id
			   AND session_id = t.session_id
			   AND session_thread_id = t.id
			   AND type = 'session.thread_created'
			 ORDER BY sequence ASC
			 LIMIT 1
		   ) e ON TRUE
		  WHERE t.workspace_id = $1
		    AND t.session_id = $2
		    AND t.id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&parentThreadID, &sourceToolUseEventID); dbconnect.IsNoRows(err) {
		return "", "", sql.NullString{}, status.Error(codes.FailedPrecondition, "sub-agent completion lineage is missing")
	} else if err != nil {
		return "", "", sql.NullString{}, err
	}
	if parentThreadID == "" || sourceToolUseEventID == "" {
		return "", "", sql.NullString{}, status.Error(codes.FailedPrecondition, "sub-agent completion lineage is incomplete")
	}
	targetTaskName, err := sessionThreadCallableTaskNameTx(ctx, tx, scope, parentThreadID)
	return parentThreadID, sourceToolUseEventID, targetTaskName, err
}
