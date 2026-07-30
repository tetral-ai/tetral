package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

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
	MailFetchMaxBodyBytes          = 4 * 1024 * 1024
	CompletionMailReconcilerMinAge = 300 * time.Second
)

type pendingCompletionDelivery struct {
	SessionID      string
	TargetThreadID string
	DeliveryID     string
	Sequence       int64
	CreatedAt      time.Time
}

func completionDeliveryID(childThreadID string, runtimeWriteID string) string {
	digest := sha256.Sum256([]byte(childThreadID + ":" + runtimeWriteID))
	return "delivery_" + hex.EncodeToString(digest[:])[:32]
}

func completionRuntimeInputID(deliveryID string) string {
	return "agent_mail:" + deliveryID
}

func appendDeclaredCompletionMailTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	durableTurnID string,
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DurableEventStamp, *bridgev1.DurableMessageStamp, error) {
	return appendDeclaredCompletionMailForSourceTx(
		ctx,
		tx,
		scope,
		threadScope,
		"finish_idle",
		durableTurnID,
		draft,
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
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DurableEventStamp, *bridgev1.DurableMessageStamp, error) {
	if draft == nil {
		return nil, nil, nil
	}
	if threadScope.role != "subagent" || threadScope.status == "closed_for_runtime" {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail requires a live sub-agent thread")
	}
	if draft.GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPLETION_MAIL ||
		draft.GetSourceKind() != sourceKind ||
		draft.GetSourceId() != sourceID ||
		draft.GetSourceEventId() != "" ||
		len(draft.GetParts()) != 1 {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail draft identity is invalid")
	}
	expectedMessageLocalID := stableRuntimeID(
		"runtime_message_draft",
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		sourceKind,
		sourceID,
		runtimeDraftKindToken(draft.GetDraftKind()),
		strconv.FormatInt(int64(draft.GetOrdinal()), 10),
	)
	if draft.GetRuntimeLocalId() != expectedMessageLocalID {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail draft id is invalid")
	}
	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail draft info is invalid")
	}
	for _, field := range []string{"id", "sessionId", "sequence", "createdAt", "updatedAt", "providerId", "modelId", "parts"} {
		if _, present := messageInfo[field]; present {
			return nil, nil, status.Error(codes.InvalidArgument, "completion mail draft contains a durable or routing field")
		}
	}
	if messageInfo["role"] != "user" || messageInfo["origin"] != "runtime" || messageInfo["status"] != "completed" {
		return nil, nil, status.Error(codes.InvalidArgument, "completion mail draft message is invalid")
	}
	part := draft.GetParts()[0]
	if part == nil ||
		part.GetPartKind() != "text" ||
		part.GetOrdinal() != 0 ||
		part.GetRuntimeLocalPartId() != stableRuntimeID("runtime_message_part_draft", expectedMessageLocalID, "text", "0") {
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
	if err := enqueueCompletionMailWakeTx(
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
			SourceEventId:   deliveryID,
			EventId:         eventID,
			EventSequence:   sequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		}, &bridgev1.DurableMessageStamp{
			RuntimeLocalId:  draft.GetRuntimeLocalId(),
			SessionThreadId: scope.GetSessionThreadId(),
			OwningEventId:   eventID,
			MessageId:       messageID,
			MessageSequence: 0,
			CreatedAt:       timestamp,
			UpdatedAt:       timestamp,
			Disposition:     bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			Parts: []*bridgev1.DurablePartStamp{{
				RuntimeLocalPartId: part.GetRuntimeLocalPartId(),
				PartId:             partID,
				MessageId:          messageID,
				PartSequence:       0,
				CreatedAt:          timestamp,
				UpdatedAt:          timestamp,
				Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			}},
		}, nil
}

// enqueueCompletionMailWakeTx is single-flight per delivery: the runtime-input dedupe key
// coalesces re-armed wakes, so a delivery with a live poke is never enqueued twice.
func enqueueCompletionMailWakeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	targetThreadID string,
	deliveryID string,
	now time.Time,
) error {
	readiness, ok, err := loadLatestSessionPreparationReadinessForUpdateTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return err
	}
	if !ok || readiness.PreparationAttemptID == "" {
		return status.Error(codes.FailedPrecondition, "sub-agent completion has no active preparation")
	}
	runtimeInputID := completionRuntimeInputID(deliveryID)
	queuePayload, err := json.Marshal(map[string]any{
		"workspace_id":           workspaceID,
		"session_id":             sessionID,
		"session_thread_id":      targetThreadID,
		"runtime_input_id":       runtimeInputID,
		"preparation_attempt_id": readiness.PreparationAttemptID,
		"event_ids":              []string{},
		"sequence_from":          0,
		"sequence_to":            0,
		"input_kind":             "agent_mail",
	})
	if err != nil {
		return err
	}
	ws := workspace.ID(workspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID:             id.New(queue.JobIDPrefix),
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, sessionID, runtimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    queuePayload,
		MaxAttempts:    queue.DefaultMaxAttempts,
		Now:            now,
	})
	return err
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
	for _, delivery := range deliveries {
		if err := enqueueCompletionMailWakeTx(
			ctx,
			tx,
			workspaceID,
			sessionID,
			targetThreadID,
			delivery.DeliveryID,
			now,
		); err != nil {
			return 0, err
		}
	}
	return len(deliveries), nil
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
		    AND source.parent_thread_id = $3
		    AND source.role = 'subagent'
		  WHERE sent.workspace_id = $1
		    AND sent.session_id = $2
		    AND sent.type = 'agent.thread_message_sent'
		    AND sent.payload_json::jsonb ->> 'target_thread_id' = $3
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events received
		         WHERE received.workspace_id = sent.workspace_id
		           AND received.session_id = sent.session_id
		           AND received.session_thread_id = $3
		           AND received.type = 'agent.thread_message_received'
		           AND received.payload_json::jsonb ->> 'delivery_id' =
		               sent.payload_json::jsonb ->> 'delivery_id'
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
			if err := enqueueCompletionMailWakeTx(
				ctx,
				tx,
				workspaceID,
				candidate.SessionID,
				candidate.TargetThreadID,
				candidate.DeliveryID,
				now,
			); err != nil {
				return err
			}
			repaired++
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
			   AND source.parent_thread_id = sent.payload_json::jsonb ->> 'target_thread_id'
			   AND source.role = 'subagent'
			 WHERE sent.workspace_id = $1
			   AND sent.type = 'agent.thread_message_sent'
			   AND sent.created_at <= $2
			   AND NOT EXISTS (
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
			   AND NOT EXISTS (
			       SELECT 1
			         FROM queue_jobs live_poke
			        WHERE live_poke.workspace_id = sent.workspace_id
			          AND live_poke.dedupe_key =
			              'runtime_input:' || sent.workspace_id || ':' || sent.session_id ||
			              ':agent_mail:' || (sent.payload_json::jsonb ->> 'delivery_id')
			          AND live_poke.status IN ('pending', 'leased')
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
