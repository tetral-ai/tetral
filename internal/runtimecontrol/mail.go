package runtimecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func RequireAgentMailInputTargetTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	threadScope, err := LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if threadScope.Role != "main" && threadScope.Role != "subagent" {
		return status.Error(codes.FailedPrecondition, "agent mail must target a main or sub-agent thread")
	}
	if !threadReceivableTx(threadScope) {
		return status.Error(codes.FailedPrecondition, "agent mail target is not receivable")
	}
	return nil
}

func ValidatedPublicInterAgentMessageJSON(raw json.RawMessage) (json.RawMessage, error) {
	var message map[string]json.RawMessage
	if json.Unmarshal(raw, &message) != nil || message == nil {
		return nil, status.Error(codes.InvalidArgument, "inter-agent message must be an object")
	}
	content, exists := message["content"]
	if !exists || len(message) != 1 {
		return nil, status.Error(codes.InvalidArgument, "inter-agent message requires only content")
	}
	if err := validatePublicInterAgentContent(content); err != nil {
		return nil, err
	}
	return raw, nil
}

func validatePublicInterAgentContent(raw json.RawMessage) error {
	var blocks []map[string]json.RawMessage
	if !jsonArray(raw) || json.Unmarshal(raw, &blocks) != nil {
		return status.Error(codes.InvalidArgument, "inter-agent public message content must be an array")
	}
	for _, block := range blocks {
		blockType, ok := requiredJSONString(block, "type")
		if !ok {
			return status.Error(codes.InvalidArgument, "inter-agent public content block requires type")
		}
		switch blockType {
		case "text":
			if !onlyJSONFields(block, "type", "text") {
				return status.Error(codes.InvalidArgument, "inter-agent public text block has unsupported fields")
			}
			if _, ok := requiredJSONString(block, "text"); !ok {
				return status.Error(codes.InvalidArgument, "inter-agent public text block requires text")
			}
		case "image":
			if !onlyJSONFields(block, "type", "source") || validatePublicInterAgentSource(block["source"], false) != nil {
				return status.Error(codes.InvalidArgument, "inter-agent public image block is invalid")
			}
		case "document":
			if !onlyJSONFields(block, "type", "source", "context", "title") ||
				!optionalNullableJSONString(block, "context") ||
				!optionalNullableJSONString(block, "title") ||
				validatePublicInterAgentSource(block["source"], true) != nil {
				return status.Error(codes.InvalidArgument, "inter-agent public document block is invalid")
			}
		default:
			return status.Error(codes.InvalidArgument, "inter-agent public content block type is unsupported")
		}
	}
	return nil
}

func validatePublicInterAgentSource(raw json.RawMessage, document bool) error {
	var source map[string]json.RawMessage
	if json.Unmarshal(raw, &source) != nil || source == nil {
		return errors.New("content source must be an object")
	}
	sourceType, ok := requiredJSONString(source, "type")
	if !ok {
		return errors.New("content source requires type")
	}
	switch sourceType {
	case "base64":
		if !onlyJSONFields(source, "type", "data", "media_type") {
			return errors.New("base64 content source has unsupported fields")
		}
		if _, ok := requiredJSONString(source, "data"); !ok {
			return errors.New("base64 content source requires data")
		}
		if _, ok := requiredJSONString(source, "media_type"); !ok {
			return errors.New("base64 content source requires media type")
		}
	case "url":
		if !onlyJSONFields(source, "type", "url") {
			return errors.New("URL content source has unsupported fields")
		}
		if _, ok := requiredJSONString(source, "url"); !ok {
			return errors.New("URL content source requires URL")
		}
	case "file":
		if !onlyJSONFields(source, "type", "file_id") {
			return errors.New("file content source has unsupported fields")
		}
		if _, ok := requiredJSONString(source, "file_id"); !ok {
			return errors.New("file content source requires file id")
		}
	case "text":
		if !document || !onlyJSONFields(source, "type", "data", "media_type") {
			return errors.New("plain-text source is only valid for documents")
		}
		if _, ok := requiredJSONString(source, "data"); !ok {
			return errors.New("plain-text document source requires data")
		}
		mediaType, ok := requiredJSONString(source, "media_type")
		if !ok || mediaType != "text/plain" {
			return errors.New("plain-text document source requires text/plain media type")
		}
	default:
		return errors.New("content source type is unsupported")
	}
	return nil
}

func jsonArray(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '['
}

func requiredJSONString(object map[string]json.RawMessage, field string) (string, bool) {
	var value string
	raw, exists := object[field]
	if !exists || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func optionalNullableJSONString(object map[string]json.RawMessage, field string) bool {
	raw, exists := object[field]
	if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return true
	}
	var value string
	return json.Unmarshal(raw, &value) == nil
}

func onlyJSONFields(object map[string]json.RawMessage, fields ...string) bool {
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
	}
	for field := range object {
		if _, ok := allowed[field]; !ok {
			return false
		}
	}
	return true
}

func threadReceivableTx(threadScope ThreadMutationScope) bool {
	switch threadScope.Status {
	case "closed_for_runtime", "terminated", "failed":
		return false
	default:
		return true
	}
}

func normalizeJSONForCompare(raw json.RawMessage) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return string(raw)
	}
	return string(normalized)
}

const AgentMailContentMaxBytes = 2 * 1024 * 1024

type StoredAgentMailEnvelope struct {
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
	Envelope         StoredAgentMailEnvelope
	ReceivedEventID  string
	ReceivedSequence int64
}

func PublicAgentMailMessageJSON(content string) (string, error) {
	return MarshalJSON(map[string]any{
		"content": []map[string]string{{"type": "text", "text": content}},
	})
}

// AgentMailContentFromPublicMessage isolates the target-owned mail body from
// the frozen public Event projection. Runtime delivery and cold load carry
// this text only; the broad public Message representation never crosses the
// private target boundary.
func AgentMailContentFromPublicMessage(raw json.RawMessage) (string, error) {
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

func CompletionRuntimeInputID(deliveryID string) string {
	return "agent_mail:" + deliveryID
}

func LoadStoredAgentMailEnvelopeByDeliveryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	deliveryID string,
) (StoredAgentMailEnvelope, error) {
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
		return StoredAgentMailEnvelope{}, err
	}
	defer func() { _ = rows.Close() }()
	var envelope StoredAgentMailEnvelope
	var payloadJSON string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return StoredAgentMailEnvelope{}, err
		}
		return StoredAgentMailEnvelope{}, status.Error(codes.NotFound, "agent mail envelope not found")
	}
	if err := rows.Scan(&envelope.SentEventID, &envelope.SentSequence, &envelope.SentThreadID, &payloadJSON); err != nil {
		return StoredAgentMailEnvelope{}, err
	}
	if rows.Next() {
		return StoredAgentMailEnvelope{}, status.Error(codes.AlreadyExists, "agent mail delivery id is not unique")
	}
	if err := rows.Err(); err != nil {
		return StoredAgentMailEnvelope{}, err
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
		return StoredAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail envelope is malformed")
	}
	publicMessage, err := ValidatedPublicInterAgentMessageJSON(payload.Message)
	if err != nil {
		return StoredAgentMailEnvelope{}, err
	}
	content, err := AgentMailContentFromPublicMessage(publicMessage)
	if err != nil {
		return StoredAgentMailEnvelope{}, err
	}
	envelope.DeliveryID = payload.DeliveryID
	envelope.SourceThreadID = payload.SourceThreadID
	envelope.TargetThreadID = payload.TargetThreadID
	envelope.SourceToolUseEventID = payload.SourceToolUseEventID
	envelope.Content = content
	envelope.PublicMessageJSON = publicMessage
	if envelope.SentThreadID != envelope.SourceThreadID {
		return StoredAgentMailEnvelope{}, status.Error(codes.FailedPrecondition, "agent mail sent event does not belong to its declared source")
	}
	if err := validateAgentMailEnvelopeRelationshipTx(ctx, tx, workspaceID, sessionID, envelope); err != nil {
		return StoredAgentMailEnvelope{}, err
	}
	return envelope, nil
}

func validateAgentMailEnvelopeRelationshipTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	envelope StoredAgentMailEnvelope,
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

func AdmitAgentMailDeliveryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	targetScope *bridgev1.RuntimeScope,
	envelope StoredAgentMailEnvelope,
	binding Binding,
	now time.Time,
) (admittedAgentMailDelivery, error) {
	threadScope, err := LockThreadMutationTx(ctx, tx, targetScope)
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	if threadScope.Role != "main" && threadScope.Role != "subagent" {
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
		targetScope.GetSessionThreadId(), CompletionRuntimeInputID(envelope.DeliveryID),
	).Scan(&inboxStatus, &inboxBindingID, &inboxBindingGeneration, &inboxPodUID,
		&inboxEventIDsJSON, &inboxSequenceFrom, &inboxSequenceTo)
	if dbconnect.IsNoRows(err) {
		return admittedAgentMailDelivery{}, PreparationError{Kind: "runtime_inbox_custody_missing", Message: "agent mail has no producer custody", Retryable: false}
	}
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	switch inboxStatus {
	case "queued", "delivering", "accepted", "committed":
	case "dead_lettered", "cancelled":
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail delivery is terminal")
	default:
		return admittedAgentMailDelivery{}, PreparationError{Kind: "runtime_inbox_status_invalid", Message: "agent mail Inbox status is invalid", Retryable: false}
	}
	sourceTaskName, err := SessionThreadCallableTaskNameTx(ctx, tx, targetScope, envelope.SourceThreadID)
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	eventPayloadJSON, err := MarshalJSON(map[string]any{
		"type":                     "agent.thread_message_received",
		"delivery_id":              envelope.DeliveryID,
		"source_thread_id":         envelope.SourceThreadID,
		"source_task_name":         NullableJSONString(sourceTaskName),
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
		if !threadReceivableTx(threadScope) {
			return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail target is not receivable")
		}
		receivedEventID = StableRuntimeID(
			"agent_mail_received_event",
			targetScope.GetWorkspaceId(),
			targetScope.GetSessionId(),
			targetScope.GetSessionThreadId(),
			envelope.DeliveryID,
		)
		receivedSequence, err = NextSessionEventSequenceTx(ctx, tx, targetScope)
		if err != nil {
			return admittedAgentMailDelivery{}, err
		}
		visibility, sessionVisible := threadScope.PublicProjection("agent.thread_message_received")
		if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
			WorkspaceID: targetScope.GetWorkspaceId(), SessionID: targetScope.GetSessionId(), SessionThreadID: targetScope.GetSessionThreadId(),
			EventID: receivedEventID, Sequence: receivedSequence, Type: "agent.thread_message_received",
			PayloadJSON: eventPayloadJSON, ProjectionJSON: eventPayloadJSON, Visibility: visibility, SessionVisible: sessionVisible,
			CreatedAt: now,
		}); err != nil {
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
			return admittedAgentMailDelivery{}, PreparationError{Kind: "runtime_inbox_custody_invalid", Message: "accepted agent mail custody conflicts with the current Runtime", Retryable: false}
		}
	}
	if inboxStatus != "accepted" {
		job := InputIdentity{
			WorkspaceID:     targetScope.GetWorkspaceId(),
			SessionID:       targetScope.GetSessionId(),
			SessionThreadID: targetScope.GetSessionThreadId(),
			RuntimeInputID:  CompletionRuntimeInputID(envelope.DeliveryID),
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
	job InputIdentity,
	binding Binding,
	now time.Time,
) error {
	if len(job.EventIDs) != 1 || job.SequenceFrom <= 0 || job.SequenceTo != job.SequenceFrom {
		return PreparationError{Kind: "runtime_inbox_custody_invalid", Message: "agent mail projection identity is invalid", Retryable: false}
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
	if !RowsAffected(result) {
		return PreparationError{Kind: "runtime_inbox_custody_invalid", Message: "agent mail has no matching producer custody", Retryable: false}
	}
	return nil
}

func AppendDeclaredCompletionMailForSourceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope ThreadMutationScope,
	sourceID string,
	text string,
	now time.Time,
) (string, error) {
	if strings.TrimSpace(text) == "" {
		return "", status.Error(codes.InvalidArgument, "completion mail text is required")
	}
	if threadScope.Role != "subagent" || threadScope.Status == "closed_for_runtime" {
		return "", status.Error(codes.InvalidArgument, "completion mail requires a live sub-agent thread")
	}

	parentThreadID, sourceToolUseEventID, targetTaskName, err := completionLineageTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	closing, err := childcontrol.ThreadOrAncestorClosingTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
	)
	if err != nil {
		return "", err
	}
	if closing {
		return "", nil
	}
	deliveryID := completionDeliveryID(scope.GetSessionThreadId(), sourceID)
	messageJSON, err := PublicAgentMailMessageJSON(text)
	if err != nil {
		return "", err
	}
	eventPayloadJSON, err := MarshalJSON(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         scope.GetSessionThreadId(),
		"target_thread_id":         parentThreadID,
		"target_task_name":         NullableJSONString(targetTaskName),
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		return "", err
	}
	eventID := id.New("evt_")
	sequence, err := NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.PublicProjection("agent.thread_message_sent")
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(), SessionThreadID: scope.GetSessionThreadId(),
		EventID: eventID, Sequence: sequence, Type: "agent.thread_message_sent",
		PayloadJSON: eventPayloadJSON, ProjectionJSON: eventPayloadJSON, Visibility: visibility, SessionVisible: sessionVisible,
		RuntimeWriteID: sourceID, CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return "", err
	}
	if err := BirthCompletionMailCustodyTx(
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
func BirthCompletionMailCustodyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) error {
	runtimeInputID := CompletionRuntimeInputID(deliveryID)
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
	request, jobID, err := AgentMailWakeEnqueueRequest(
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

func AgentMailWakeEnqueueRequest(
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) (queue.EnqueueRequest, string, error) {
	runtimeInputID := CompletionRuntimeInputID(deliveryID)
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
	runtimeInputID := CompletionRuntimeInputID(deliveryID)
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
	targetTaskName, err := SessionThreadCallableTaskNameTx(ctx, tx, scope, parentThreadID)
	return parentThreadID, sourceToolUseEventID, targetTaskName, err
}

// AgentMailDeliveryID constructs the deterministic declared envelope identity.
func AgentMailDeliveryID(sourceToolUseEventID string, targetThreadID string) string {
	digest := sha256.Sum256([]byte(sourceToolUseEventID + ":" + targetThreadID + ":0"))
	return "delivery_" + hex.EncodeToString(digest[:])[:32]
}
