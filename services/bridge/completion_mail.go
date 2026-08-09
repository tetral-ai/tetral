package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const (
	MailFetchMaxEnvelopes          = 4
	CompletionMailReconcilerMinAge = 300 * time.Second
)

type pendingCompletionDelivery struct {
	SessionID      string
	TargetThreadID string
	DeliveryID     string
	Sequence       int64
	CreatedAt      time.Time
}

type storedAgentMailEnvelope struct {
	SentEventID          string
	SentSequence         int64
	SentThreadID         string
	DeliveryID           string
	SourceThreadID       string
	TargetThreadID       string
	SourceToolUseEventID string
	MessageJSON          json.RawMessage
}

type admittedAgentMailDelivery struct {
	Envelope         storedAgentMailEnvelope
	ReceivedEventID  string
	ReceivedSequence int64
	Terminal         bool
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
	publicMessage, err := publicInterAgentMessageJSON(payload.Message)
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	envelope.DeliveryID = payload.DeliveryID
	envelope.SourceThreadID = payload.SourceThreadID
	envelope.TargetThreadID = payload.TargetThreadID
	envelope.SourceToolUseEventID = payload.SourceToolUseEventID
	envelope.MessageJSON = publicMessage
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

func loadOldestUncommittedCompletionEnvelopeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	parentThreadID string,
	childThreadID string,
) (storedAgentMailEnvelope, error) {
	var deliveryID string
	err := tx.QueryRow(ctx,
		`SELECT sent.payload_json::jsonb ->> 'delivery_id'
		   FROM session_events sent
		  WHERE sent.workspace_id = $1
		    AND sent.session_id = $2
		    AND sent.type = 'agent.thread_message_sent'
		    AND sent.payload_json::jsonb ->> 'source_thread_id' = $3
		    AND sent.payload_json::jsonb ->> 'target_thread_id' = $4
		    AND EXISTS (
		        SELECT 1
		          FROM session_threads child
		         WHERE child.workspace_id = sent.workspace_id
		           AND child.session_id = sent.session_id
		           AND child.id = $3
		           AND child.parent_thread_id = $4
		           AND child.role = 'subagent'
		    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events received
		         WHERE received.workspace_id = sent.workspace_id
		           AND received.session_id = sent.session_id
		           AND received.session_thread_id = $4
		           AND received.type = 'agent.thread_message_received'
		           AND received.payload_json::jsonb ->> 'delivery_id' =
		               sent.payload_json::jsonb ->> 'delivery_id'
		           AND received.processed_at IS NOT NULL
		    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_runtime_inbox inbox
		         WHERE inbox.workspace_id = sent.workspace_id
		           AND inbox.session_id = sent.session_id
		           AND inbox.session_thread_id = $4
		           AND inbox.runtime_input_id =
		               'agent_mail:' || (sent.payload_json::jsonb ->> 'delivery_id')
		           AND inbox.status = 'committed'
		    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events exhausted
		         WHERE exhausted.workspace_id = sent.workspace_id
		           AND exhausted.session_id = sent.session_id
		           AND exhausted.event_id =
		               'evt_runtime_exhausted_' || substr(encode(sha256(
		                   convert_to(sent.workspace_id, 'UTF8') ||
		                   decode('00', 'hex') ||
		                   convert_to(sent.session_id, 'UTF8') ||
		                   decode('00', 'hex') ||
		                   convert_to(
		                       'agent_mail:' || (sent.payload_json::jsonb ->> 'delivery_id'),
		                       'UTF8'
		                   ) ||
		                   decode('00', 'hex') ||
		                   convert_to('runtime_delivery_exhausted', 'UTF8')
		               ), 'hex'), 1, 24)
		           AND exhausted.type = 'session.error'
		           AND exhausted.payload_json::jsonb #>> '{error,retry_status,type}' = 'exhausted'
		    )
		  ORDER BY sent.sequence ASC, sent.event_id ASC
		  LIMIT 1
		  FOR UPDATE`,
		workspaceID,
		sessionID,
		childThreadID,
		parentThreadID,
	).Scan(&deliveryID)
	if dbconnect.IsNoRows(err) {
		return storedAgentMailEnvelope{}, status.Error(codes.NotFound, "uncommitted child completion mail not found")
	}
	if err != nil {
		return storedAgentMailEnvelope{}, err
	}
	return loadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, workspaceID, sessionID, deliveryID)
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
	exhausted, err := runtimeDeliveryExhaustionEventExistsTx(ctx, tx, RuntimeJob{
		WorkspaceID:     targetScope.GetWorkspaceId(),
		SessionID:       targetScope.GetSessionId(),
		SessionThreadID: targetScope.GetSessionThreadId(),
		RuntimeInputID:  completionRuntimeInputID(envelope.DeliveryID),
		Kind:            queue.KindRuntimeInput,
		InputKind:       "agent_mail",
	})
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	if exhausted {
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail delivery is exhausted")
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
		"message":                  envelope.MessageJSON,
	})
	if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	var (
		receivedEventID     string
		receivedSequence    int64
		receivedPayloadJSON string
		processedAt         sql.NullTime
	)
	err = tx.QueryRow(ctx,
		`SELECT event_id, sequence, payload_json, processed_at
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
	).Scan(&receivedEventID, &receivedSequence, &receivedPayloadJSON, &processedAt)
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
		processedAt = sql.NullTime{}
	} else if err != nil {
		return admittedAgentMailDelivery{}, err
	}
	if normalizeJSONForCompare(json.RawMessage(receivedPayloadJSON)) != normalizeJSONForCompare(json.RawMessage(eventPayloadJSON)) {
		return admittedAgentMailDelivery{}, status.Error(codes.AlreadyExists, "agent mail delivery replay conflicts with the admitted source")
	}
	var inboxStatus sql.NullString
	err = tx.QueryRow(ctx,
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND runtime_input_id = $4
		  FOR UPDATE`,
		targetScope.GetWorkspaceId(),
		targetScope.GetSessionId(),
		targetScope.GetSessionThreadId(),
		completionRuntimeInputID(envelope.DeliveryID),
	).Scan(&inboxStatus)
	if err != nil && !dbconnect.IsNoRows(err) {
		return admittedAgentMailDelivery{}, err
	}
	if dbconnect.IsNoRows(err) {
		inboxStatus = sql.NullString{}
	}
	terminal := processedAt.Valid || inboxStatus.String == "committed"
	if !terminal && !threadReceivableTx(threadScope) {
		return admittedAgentMailDelivery{}, status.Error(codes.FailedPrecondition, "agent mail target is not receivable")
	}
	if !terminal {
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
		if err := upsertRuntimeInboxDeliveryTx(ctx, tx, job, binding, now); err != nil {
			return admittedAgentMailDelivery{}, err
		}
	}
	return admittedAgentMailDelivery{
		Envelope:         envelope,
		ReceivedEventID:  receivedEventID,
		ReceivedSequence: receivedSequence,
		Terminal:         terminal,
	}, nil
}

func appendDeclaredCompletionMailTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	durableTurnID string,
	create *bridgev1.RuntimeMessageCreate,
	now time.Time,
) (*bridgev1.DurableEventStamp, *bridgev1.DurableMessageStamp, error) {
	return appendDeclaredCompletionMailForSourceTx(
		ctx,
		tx,
		scope,
		threadScope,
		"finish_idle",
		durableTurnID,
		create,
		now,
	)
}

func appendDeclaredCompletionMailForSourceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	sourceKind string,
	sourceID string,
	create *bridgev1.RuntimeMessageCreate,
	now time.Time,
) (*bridgev1.DurableEventStamp, *bridgev1.DurableMessageStamp, error) {
	if create == nil {
		return nil, nil, nil
	}
	if threadScope.role != "subagent" || threadScope.status == "closed_for_runtime" {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail requires a live sub-agent thread")
	}
	if create.GetMessageKind() != bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_COMPLETION_MAIL ||
		create.SourceEventId != nil || len(create.GetParts()) != 1 {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail create identity is invalid")
	}
	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(create.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail create info is invalid")
	}
	for _, field := range []string{"id", "sessionId", "sequence", "createdAt", "updatedAt", "providerId", "modelId", "parts"} {
		if _, present := messageInfo[field]; present {
			return nil, nil, status.Error(codes.InvalidArgument, "completion mail create contains a durable or routing field")
		}
	}
	if messageInfo["role"] != "user" || messageInfo["origin"] != "runtime" || messageInfo["status"] != "completed" {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail draft message is invalid")
	}
	part := create.GetParts()[0]
	if part == nil || part.GetPartKind() != "text" {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail part identity is invalid")
	}
	var partInfo struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(part.GetPartJson()), &partInfo); err != nil || partInfo.Type != "text" {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail part is invalid")
	}

	parentThreadID, sourceToolUseEventID, targetTaskName, err := completionLineageTx(ctx, tx, scope)
	if err != nil {
		return nil, nil, err
	}
	deliveryID := completionDeliveryID(scope.GetSessionThreadId(), sourceID)
	messageJSON, err := completionRuntimeMessageJSON(scope.GetSessionId(), deliveryID, partInfo.Text, now)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return nil, nil, err
	}
	if _, err := enqueueCompletionMailWakeTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
		deliveryID,
		now,
	); err != nil {
		return nil, nil, err
	}
	timestamp := now.UTC().Format(time.RFC3339Nano)
	messageID := "msg_" + deliveryID
	partID := messageID + "_text"
	return &bridgev1.DurableEventStamp{
			SessionThreadId: scope.GetSessionThreadId(),
			EventId:         eventID,
			EventSequence:   sequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}, &bridgev1.DurableMessageStamp{
			SessionThreadId: scope.GetSessionThreadId(),
			OwningEventId:   eventID,
			MessageId:       messageID,
			MessageSequence: 0,
			CreatedAt:       timestamp,
			UpdatedAt:       timestamp,
			Disposition:     bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			Parts: []*bridgev1.DurablePartStamp{{
				PartId:       partID,
				MessageId:    messageID,
				PartSequence: 0,
				CreatedAt:    timestamp,
				UpdatedAt:    timestamp,
				Disposition:  bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			}},
		}, nil
}

// Completion mail is durably identified by delivery id. Reconciliation may
// reuse the same wake only when its immutable envelope identity matches.
func enqueueCompletionMailWakeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) (bool, error) {
	inserted, err := enqueueAgentMailWakeTx(
		ctx,
		tx,
		workspaceID,
		sessionID,
		targetThreadID,
		deliveryID,
		now,
	)
	if err != nil {
		return false, err
	}
	return inserted, nil
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

func rearmPendingCompletionMailForThreadTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	now time.Time,
) (int, error) {
	deliveries, err := pendingCompletionDeliveriesForThreadTx(
		ctx,
		tx,
		workspaceID,
		sessionID,
		targetThreadID,
		MailFetchMaxEnvelopes,
	)
	if err != nil {
		return 0, err
	}
	rearmed := 0
	for _, delivery := range deliveries {
		ensured, err := enqueueCompletionMailWakeTx(
			ctx,
			tx,
			workspaceID,
			sessionID,
			targetThreadID,
			delivery.DeliveryID,
			now,
		)
		if err != nil {
			return 0, err
		}
		if ensured {
			rearmed++
		}
	}
	return rearmed, nil
}

func rearmPendingCompletionMailForSessionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	now time.Time,
) (int, error) {
	rows, err := tx.Query(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND role IN ('main', 'subagent')
		  ORDER BY id`,
		workspaceID,
		sessionID,
	)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	threadIDs := make([]string, 0)
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return 0, err
		}
		threadIDs = append(threadIDs, threadID)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rearmed := 0
	for _, threadID := range threadIDs {
		count, err := rearmPendingCompletionMailForThreadTx(ctx, tx, workspaceID, sessionID, threadID, now)
		if err != nil {
			return 0, err
		}
		rearmed += count
	}
	return rearmed, nil
}

func pendingCompletionDeliveriesForThreadTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	limit int,
) ([]pendingCompletionDelivery, error) {
	if limit <= 0 || limit > MailFetchMaxEnvelopes {
		limit = MailFetchMaxEnvelopes
	}
	rows, err := tx.Query(ctx,
		`SELECT sent.payload_json::jsonb ->> 'delivery_id',
		        sent.sequence,
		        sent.created_at
		   FROM session_events sent
		   JOIN sessions session_row
		     ON session_row.workspace_id = sent.workspace_id
		    AND session_row.id = sent.session_id
		    AND session_row.status <> 'terminated'
		    AND session_row.lifecycle_state <> 'deleted'
			   JOIN session_threads source
			     ON source.workspace_id = sent.workspace_id
			    AND source.session_id = sent.session_id
			    AND source.id = sent.payload_json::jsonb ->> 'source_thread_id'
			   JOIN session_threads target
			     ON target.workspace_id = sent.workspace_id
			    AND target.session_id = sent.session_id
			    AND target.id = $3
			  WHERE sent.workspace_id = $1
			    AND sent.session_id = $2
			    AND sent.type = 'agent.thread_message_sent'
			    AND sent.payload_json::jsonb ->> 'target_thread_id' = $3
			    AND (
			        (source.role = 'subagent' AND source.parent_thread_id = target.id)
			        OR (target.role = 'subagent' AND target.parent_thread_id = source.id)
			    )
			    AND (
			        NOT EXISTS (
			            SELECT 1
			              FROM session_events received
			             WHERE received.workspace_id = sent.workspace_id
			               AND received.session_id = sent.session_id
			               AND received.session_thread_id = $3
			               AND received.type = 'agent.thread_message_received'
			               AND received.payload_json::jsonb ->> 'delivery_id' =
			                   sent.payload_json::jsonb ->> 'delivery_id'
			        )
			        OR EXISTS (
			            SELECT 1
			              FROM session_events received
			              JOIN session_runtime_inbox inbox
			                ON inbox.workspace_id = received.workspace_id
			               AND inbox.session_id = received.session_id
			               AND inbox.session_thread_id = received.session_thread_id
			               AND inbox.runtime_input_id =
			                   'agent_mail:' || (received.payload_json::jsonb ->> 'delivery_id')
			               AND inbox.status = 'cancelled'
			             WHERE received.workspace_id = sent.workspace_id
			               AND received.session_id = sent.session_id
			               AND received.session_thread_id = $3
			               AND received.type = 'agent.thread_message_received'
			               AND received.payload_json::jsonb ->> 'delivery_id' =
			                   sent.payload_json::jsonb ->> 'delivery_id'
			               AND received.processed_at IS NULL
			        )
			    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events exhausted
		         WHERE exhausted.workspace_id = sent.workspace_id
		           AND exhausted.session_id = sent.session_id
		           AND exhausted.event_id =
		               'evt_runtime_exhausted_' || substr(encode(sha256(
		                   convert_to(sent.workspace_id, 'UTF8') ||
		                   decode('00', 'hex') ||
		                   convert_to(sent.session_id, 'UTF8') ||
		                   decode('00', 'hex') ||
		                   convert_to(
		                       'agent_mail:' || (sent.payload_json::jsonb ->> 'delivery_id'),
		                       'UTF8'
		                   ) ||
		                   decode('00', 'hex') ||
		                   convert_to('runtime_delivery_exhausted', 'UTF8')
		               ), 'hex'), 1, 24)
		           AND exhausted.type = 'session.error'
		           AND exhausted.payload_json::jsonb #>> '{error,retry_status,type}' = 'exhausted'
		    )
		  ORDER BY sent.sequence ASC, sent.event_id ASC
		  LIMIT $4`,
		workspaceID,
		sessionID,
		targetThreadID,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	deliveries := make([]pendingCompletionDelivery, 0, limit)
	for rows.Next() {
		var delivery pendingCompletionDelivery
		delivery.SessionID = sessionID
		delivery.TargetThreadID = targetThreadID
		if err := rows.Scan(&delivery.DeliveryID, &delivery.Sequence, &delivery.CreatedAt); err != nil {
			return nil, err
		}
		if delivery.DeliveryID == "" {
			return nil, status.Error(codes.FailedPrecondition, "pending completion delivery id is missing")
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, rows.Err()
}

func (s *PostgreSQLRuntimeDeliveryStore) RepairCompletionMail(
	ctx context.Context,
	workspaceID string,
	limit int,
) (int, error) {
	if s == nil || s.Client == nil {
		return 0, runtimeDeliveryPrepareError{
			kind:      "runtime_reconcile_unavailable",
			message:   "completion mail reconciler is unavailable",
			retryable: true,
		}
	}
	if workspaceID == "" {
		return 0, runtimeDeliveryPrepareError{
			kind:      "invalid_runtime_job_payload",
			message:   "completion mail reconcile workspace is required",
			retryable: false,
		}
	}
	if limit <= 0 || limit > defaultRuntimeInboxRepairBatch {
		limit = defaultRuntimeInboxRepairBatch
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	cutoff := now.Add(-CompletionMailReconcilerMinAge)
	repaired := 0
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.repair_completion_mail", func(tx *dbconnect.Tx) error {
		candidates, err := completionMailReconcileCandidatesTx(ctx, tx, workspaceID, cutoff, limit)
		if err != nil {
			return err
		}
		for _, candidate := range candidates {
			ensured, err := enqueueCompletionMailWakeTx(
				ctx,
				tx,
				workspaceID,
				candidate.SessionID,
				candidate.TargetThreadID,
				candidate.DeliveryID,
				now,
			)
			if err != nil {
				return err
			}
			if ensured {
				repaired++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return repaired, nil
}

func completionMailReconcileCandidatesTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	cutoff time.Time,
	limit int,
) ([]pendingCompletionDelivery, error) {
	rows, err := tx.Query(ctx,
		`WITH eligible AS (
			SELECT sent.session_id,
			       sent.payload_json::jsonb ->> 'target_thread_id' AS target_thread_id,
			       sent.payload_json::jsonb ->> 'delivery_id' AS delivery_id,
			       sent.sequence,
			       sent.event_id,
			       sent.created_at AS created_at,
			       ROW_NUMBER() OVER (
			           PARTITION BY sent.session_id, sent.payload_json::jsonb ->> 'target_thread_id'
			           ORDER BY sent.sequence ASC, sent.event_id ASC
			       ) AS recipient_rank
			  FROM session_events sent
			  JOIN sessions session_row
			    ON session_row.workspace_id = sent.workspace_id
			   AND session_row.id = sent.session_id
			   AND session_row.status <> 'terminated'
			   AND session_row.lifecycle_state <> 'deleted'
				  JOIN session_threads source
				    ON source.workspace_id = sent.workspace_id
				   AND source.session_id = sent.session_id
				   AND source.id = sent.payload_json::jsonb ->> 'source_thread_id'
				  JOIN session_threads target
				    ON target.workspace_id = sent.workspace_id
				   AND target.session_id = sent.session_id
				   AND target.id = sent.payload_json::jsonb ->> 'target_thread_id'
				   AND target.status NOT IN ('closed_for_runtime', 'terminated')
				 WHERE sent.workspace_id = $1
				   AND sent.type = 'agent.thread_message_sent'
				   AND sent.created_at <= $2
				   AND (
				       (source.role = 'subagent' AND source.parent_thread_id = target.id)
				       OR (target.role = 'subagent' AND target.parent_thread_id = source.id)
				   )
				   AND (
				       NOT EXISTS (
				           SELECT 1
				             FROM session_events received
				            WHERE received.workspace_id = sent.workspace_id
				              AND received.session_id = sent.session_id
				              AND received.session_thread_id =
				                  sent.payload_json::jsonb ->> 'target_thread_id'
				              AND received.type = 'agent.thread_message_received'
				              AND received.payload_json::jsonb ->> 'delivery_id' =
				                  sent.payload_json::jsonb ->> 'delivery_id'
				       )
				       OR EXISTS (
				           SELECT 1
				             FROM session_events received
				             JOIN session_runtime_inbox inbox
				               ON inbox.workspace_id = received.workspace_id
				              AND inbox.session_id = received.session_id
				              AND inbox.session_thread_id = received.session_thread_id
				              AND inbox.runtime_input_id =
				                  'agent_mail:' || (received.payload_json::jsonb ->> 'delivery_id')
				              AND inbox.status = 'cancelled'
				            WHERE received.workspace_id = sent.workspace_id
				              AND received.session_id = sent.session_id
				              AND received.session_thread_id =
				                  sent.payload_json::jsonb ->> 'target_thread_id'
				              AND received.type = 'agent.thread_message_received'
				              AND received.payload_json::jsonb ->> 'delivery_id' =
				                  sent.payload_json::jsonb ->> 'delivery_id'
				              AND received.processed_at IS NULL
				       )
				   )
			   AND NOT EXISTS (
			       SELECT 1
			         FROM queue_jobs active_wake
			        WHERE active_wake.workspace_id = sent.workspace_id
			          AND active_wake.dedupe_key =
			              'runtime_input:' || sent.workspace_id || ':' || sent.session_id ||
			              ':agent_mail:' || (sent.payload_json::jsonb ->> 'delivery_id')
			          AND active_wake.status IN ('pending', 'leased')
			   )
			   AND NOT EXISTS (
			       SELECT 1
			         FROM session_events exhausted
			        WHERE exhausted.workspace_id = sent.workspace_id
			          AND exhausted.session_id = sent.session_id
			          AND exhausted.event_id =
			              'evt_runtime_exhausted_' || substr(encode(sha256(
			                  convert_to(sent.workspace_id, 'UTF8') ||
			                  decode('00', 'hex') ||
			                  convert_to(sent.session_id, 'UTF8') ||
			                  decode('00', 'hex') ||
			                  convert_to(
			                      'agent_mail:' || (sent.payload_json::jsonb ->> 'delivery_id'),
			                      'UTF8'
			                  ) ||
			                  decode('00', 'hex') ||
			                  convert_to('runtime_delivery_exhausted', 'UTF8')
			              ), 'hex'), 1, 24)
			          AND exhausted.type = 'session.error'
			          AND exhausted.payload_json::jsonb #>> '{error,retry_status,type}' = 'exhausted'
			   )
		)
		SELECT session_id, target_thread_id, delivery_id, sequence, created_at
		  FROM eligible
		 WHERE recipient_rank <= $4
		 ORDER BY created_at ASC, session_id ASC, sequence ASC, event_id ASC
		 LIMIT $3`,
		workspaceID,
		cutoff,
		limit,
		MailFetchMaxEnvelopes,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	candidates := make([]pendingCompletionDelivery, 0, limit)
	for rows.Next() {
		var candidate pendingCompletionDelivery
		if err := rows.Scan(
			&candidate.SessionID,
			&candidate.TargetThreadID,
			&candidate.DeliveryID,
			&candidate.Sequence,
			&candidate.CreatedAt,
		); err != nil {
			return nil, err
		}
		if candidate.SessionID == "" || candidate.TargetThreadID == "" || candidate.DeliveryID == "" {
			return nil, status.Error(codes.FailedPrecondition, "completion mail reconcile candidate is malformed")
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
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

func completionRuntimeMessageJSON(sessionID string, deliveryID string, text string, now time.Time) (string, error) {
	timestamp := now
	messageID := "msg_" + deliveryID
	return marshalBridgeJSON(map[string]any{
		"id":        messageID,
		"sessionId": sessionID,
		"role":      "user",
		"origin":    "runtime",
		"sequence":  0,
		"status":    "completed",
		"createdAt": timestamp,
		"updatedAt": timestamp,
		"parts": []map[string]any{{
			"id":          messageID + "_text",
			"sessionId":   sessionID,
			"messageId":   messageID,
			"sequence":    0,
			"createdAt":   timestamp,
			"updatedAt":   timestamp,
			"type":        "text",
			"text":        text,
			"truncated":   false,
			"status":      "completed",
			"completedAt": timestamp,
		}},
		"content": []map[string]string{{"type": "text", "text": text}},
	})
}
