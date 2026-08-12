package sessionevent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type FileAttachmentValidator interface {
	ValidateEventAttachments(context.Context, files.SessionTransaction, workspace.ID, []files.EventAttachmentReference) error
	PrimeEventAttachmentPDFCounts(context.Context, workspace.ID, []files.EventAttachmentReference) error
}

type PostgreSQLSessionEventStore struct {
	client                  *dbconnect.Client
	fileAttachmentValidator FileAttachmentValidator
	beforeIdempotencyInsert func() error
	beforeQueueJobInsert    func() error
}

type PostgreSQLStoreOption func(*PostgreSQLSessionEventStore)

func WithFileAttachmentValidator(validator FileAttachmentValidator) PostgreSQLStoreOption {
	return func(store *PostgreSQLSessionEventStore) {
		store.fileAttachmentValidator = validator
	}
}

func NewPostgreSQLStore(client *dbconnect.Client, options ...PostgreSQLStoreOption) *PostgreSQLSessionEventStore {
	store := &PostgreSQLSessionEventStore{client: client}
	for _, option := range options {
		option(store)
	}
	return store
}

// AppendClientEvents serializes public event admission on the session row and
// the runtime-status row. It numbers new events from MAX(sequence), stores
// them as unprocessed public input, and creates the matching Runtime Inbox and
// Queue custody in the same PostgreSQL transaction. Delivery may bind that
// custody to a Runtime Pod, but it never reconstructs an input from the event.
//
// The append path never scans, locks, or JSON-decodes stored session_events
// rows; the only session_events read is the bounded MAX(sequence) aggregate.
func (s *PostgreSQLSessionEventStore) AppendClientEvents(ctx context.Context, workspaceID workspace.ID, sessionID string, events []preparedEvent, idempotency appendIdempotency, settings appendSettings) (*appendOutcome, error) {
	if s == nil || s.client == nil {
		return nil, &ValidationError{Message: "session event store is required"}
	}
	fileReferences := fileAttachmentReferences(events)
	var outcome *appendOutcome
	for attempt := 0; attempt < 2; attempt++ {
		outcome = nil
		err := s.client.WithWorkspaceTx(ctx, string(workspaceID), "sessionevent.append_client_events", func(tx *dbconnect.Tx) error {
			admission, err := lockOpenSession(ctx, tx, workspaceID, sessionID)
			if err != nil {
				return err
			}
			runtimeStatus, err := lockSessionRuntimeStatus(ctx, tx, workspaceID, sessionID)
			if err != nil {
				return err
			}
			canonicalRequestHash, err := canonicalRequestHashForPreparedEvents(events)
			if err != nil {
				return err
			}
			existing, found, err := readSessionEventIdempotency(ctx, tx, workspaceID, sessionID, idempotency.keyDigest)
			if err != nil {
				return err
			}
			if found {
				if !bytes.Equal(existing.canonicalRequestHash, canonicalRequestHash) {
					return &ConflictError{Message: "idempotency key was used with a different request"}
				}
				replayedEvents, err := decodeSessionEventIdempotencyResponse(workspaceID, sessionID, existing.responseEventsJSON)
				if err != nil {
					return err
				}
				outcome = &appendOutcome{events: replayedEvents, replayed: true}
				return nil
			}
			if admission.mainThreadArchived && preparedEventsContainType(events, EventTypeUserMessage) {
				return &ConflictError{Message: "session primary thread is archived"}
			}
			if len(fileReferences) > 0 {
				if s.fileAttachmentValidator == nil {
					return errors.New("sessionevent: file attachment validator is required")
				}
				if err := s.fileAttachmentValidator.ValidateEventAttachments(
					ctx,
					sessionEventFilesTransaction{tx: tx},
					workspaceID,
					fileReferences,
				); err != nil {
					return err
				}
			}
			targetThreadIDsByEvent, err := resolveEventTargetThreadIDsForIdempotency(ctx, tx, workspaceID, sessionID, admission, events, settings.now)
			if err != nil {
				return err
			}
			for index, event := range events {
				if event.eventType == EventTypeUserInterrupt {
					continue
				}
				for _, targetThreadID := range targetThreadIDsByEvent[index] {
					closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, string(workspaceID), sessionID, targetThreadID)
					if err != nil {
						return err
					}
					if closing {
						return &ConflictError{Message: "session thread is closing"}
					}
				}
			}
			if err := rejectMessageWhileApprovalPending(ctx, tx, workspaceID, sessionID, runtimeStatus, events); err != nil {
				return err
			}
			appended := make([]*Event, 0, len(events))
			nextSequences := make(map[string]int64)
			for eventIndex, event := range events {
				sessionThreadIDs := targetThreadIDsByEvent[eventIndex]
				if event.eventType == EventTypeUserToolConfirmation {
					sessionThreadID, err := resolvePendingToolConfirmation(ctx, tx, workspaceID, sessionID, event.toolUseEventID, event.toolResult, event.denyMessage, settings.now)
					if err != nil {
						return err
					}
					if len(sessionThreadIDs) != 1 || sessionThreadIDs[0] != sessionThreadID {
						return errors.New("sessionevent: resolved tool confirmation target changed during admission")
					}
					sessionThreadIDs = []string{sessionThreadID}
				}
				for targetIndex, sessionThreadID := range sessionThreadIDs {
					eventID := event.id
					if targetIndex > 0 {
						eventID = NewEventID()
					}
					nextSequence, err := nextEventSequence(ctx, tx, workspaceID, sessionID, sessionThreadID, nextSequences)
					if err != nil {
						return err
					}
					sessionVisible := publicEventSessionVisible(event.eventType, sessionThreadID, admission.mainThreadID)
					if _, err := tx.Exec(ctx,
						`INSERT INTO session_events (
								workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
								visibility, session_visible, created_at, updated_at, processed_at
							) VALUES ($1, $2, $3, $4, $5, $6, $7, 'public', $8, $9, $9, NULL)`,
						string(workspaceID),
						sessionID,
						nullableString(sessionThreadID),
						eventID,
						nextSequence,
						event.eventType,
						string(event.payload),
						sessionVisible,
						settings.now,
					); err != nil {
						return err
					}
					if _, err := appendSessionEventStreamChange(ctx, tx, workspaceID, sessionID, sessionThreadID, eventID, sessionVisible, settings.now); err != nil {
						return err
					}
					appended = append(appended, &Event{
						ID:          eventID,
						WorkspaceID: workspaceID,
						SessionID:   sessionID,
						ThreadID:    sessionThreadID,
						Sequence:    nextSequence,
						Type:        event.eventType,
						Payload:     cloneRawMessage(event.payload),
						CreatedAt:   settings.now,
					})
				}
			}
			responseEventsJSON, err := encodeSessionEventIdempotencyResponse(appended)
			if err != nil {
				return err
			}
			if s.beforeIdempotencyInsert != nil {
				if err := s.beforeIdempotencyInsert(); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO session_event_idempotency_keys (
				workspace_id, session_id, idempotency_key_digest, canonical_request_hash, response_events_json, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $6)`,
				string(workspaceID),
				sessionID,
				idempotency.keyDigest,
				canonicalRequestHash,
				responseEventsJSON,
				settings.now,
			); err != nil {
				return err
			}
			if err := s.enqueueRuntimeInputJobs(ctx, tx, workspaceID, sessionID, appended, settings.now); err != nil {
				return err
			}
			if !runtimeStatus.cleanupClaimed {
				if err := clearStaleCleanupMarkers(ctx, tx, workspaceID, sessionID, settings.now); err != nil {
					return err
				}
			}
			outcome = &appendOutcome{events: appended}
			return nil
		})
		if err == nil {
			return outcome, nil
		}
		var pageCountRequired *files.EventAttachmentPageCountRequiredError
		if attempt == 0 && errors.As(err, &pageCountRequired) {
			if primeErr := s.fileAttachmentValidator.PrimeEventAttachmentPDFCounts(
				ctx,
				workspaceID,
				fileReferences,
			); primeErr != nil {
				return nil, primeErr
			}
			continue
		}
		return nil, err
	}
	return nil, errors.New("sessionevent: legacy PDF page count initialization did not converge")
}

type sessionEventFilesTransaction struct {
	tx *dbconnect.Tx
}

func (t sessionEventFilesTransaction) ExecContext(ctx context.Context, query string, args ...any) (interface {
	RowsAffected() (int64, error)
}, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t sessionEventFilesTransaction) Query(ctx context.Context, query string, args ...any) (files.Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t sessionEventFilesTransaction) QueryRow(ctx context.Context, query string, args ...any) files.Row {
	return t.tx.QueryRow(ctx, query, args...)
}

func fileAttachmentReferences(events []preparedEvent) []files.EventAttachmentReference {
	var references []files.EventAttachmentReference
	for _, event := range events {
		for _, reference := range event.fileReferences {
			references = append(references, files.EventAttachmentReference{
				BlockType: reference.blockType,
				FileID:    reference.fileID,
			})
		}
	}
	return references
}

func (s *PostgreSQLSessionEventStore) enqueueRuntimeInputJobs(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, events []*Event, now time.Time) error {
	jobIndex := 0
	for _, segment := range runtimeInputSegments(events) {
		if len(segment.events) == 0 {
			continue
		}
		for offset := 0; offset < len(segment.events); offset += queue.MaxRuntimeInputEventRefsPerJob {
			end := min(offset+queue.MaxRuntimeInputEventRefsPerJob, len(segment.events))
			chunk := segment.events[offset:end]
			runtimeInputID := NewRuntimeInputID()
			eventIDs := make([]string, 0, len(chunk))
			for _, event := range chunk {
				eventIDs = append(eventIDs, event.ID)
			}
			payload := runtimeInputQueuePayload{
				WorkspaceID:     string(workspaceID),
				SessionID:       sessionID,
				SessionThreadID: chunk[0].ThreadID,
				RuntimeInputID:  runtimeInputID,
				EventIDs:        eventIDs,
				SequenceFrom:    chunk[0].Sequence,
				SequenceTo:      chunk[len(chunk)-1].Sequence,
				InputKind:       segment.inputKind,
			}
			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return err
			}
			eventIDsJSON, err := json.Marshal(eventIDs)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (
				workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,
				event_ids_json,sequence_from,sequence_to,status,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'queued',$9,$9)`,
				string(workspaceID), sessionID, chunk[0].ThreadID, runtimeInputID,
				segment.inputKind, string(eventIDsJSON), chunk[0].Sequence,
				chunk[len(chunk)-1].Sequence, now,
			); err != nil {
				return err
			}
			if s.beforeQueueJobInsert != nil {
				if err := s.beforeQueueJobInsert(); err != nil {
					return err
				}
			}
			jobIndex++
			// Durable timestamps are microsecond-precision, so fan-out jobs are spaced
			// one microsecond apart to keep created_at ordering equal to insert order.
			chunkTime := now.Add(time.Duration(jobIndex) * time.Microsecond)
			if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
				WorkspaceID:    workspaceID,
				Kind:           queue.KindRuntimeInput,
				PartitionKey:   runtimeInputPartitionKey(workspaceID, sessionID),
				DedupeKey:      runtimeInputDedupeKey(workspaceID, sessionID, runtimeInputID),
				PayloadVersion: 1,
				PayloadJSON:    payloadJSON,
				Priority:       runtimeInputPriority(segment.inputKind),
				AvailableAt:    chunkTime,
				Now:            chunkTime,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func resolveEventTargetThreadIDs(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, admission sessionAdmissionState, event preparedEvent, now time.Time) ([]string, error) {
	switch event.eventType {
	case EventTypeUserInterrupt:
		if event.targetThreadID != "" {
			threadID, err := resolveInterruptTargetThreadID(ctx, tx, workspaceID, sessionID, event.targetThreadID)
			if err != nil {
				return nil, err
			}
			return []string{threadID}, nil
		}
		return listInterruptTargetThreadIDs(ctx, tx, workspaceID, sessionID)
	case EventTypeUserToolConfirmation:
		threadID, err := resolvePendingToolConfirmation(ctx, tx, workspaceID, sessionID, event.toolUseEventID, event.toolResult, event.denyMessage, now)
		if err != nil {
			return nil, err
		}
		return []string{threadID}, nil
	default:
		return []string{admission.mainThreadID}, nil
	}
}

func resolveEventTargetThreadIDsForIdempotency(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, admission sessionAdmissionState, events []preparedEvent, now time.Time) ([][]string, error) {
	targets := make([][]string, 0, len(events))
	for _, event := range events {
		var threadIDs []string
		var err error
		switch event.eventType {
		case EventTypeUserToolConfirmation:
			var threadID string
			threadID, err = resolvePendingToolConfirmationTarget(ctx, tx, workspaceID, sessionID, event.toolUseEventID)
			threadIDs = []string{threadID}
		default:
			threadIDs, err = resolveEventTargetThreadIDs(ctx, tx, workspaceID, sessionID, admission, event, now)
		}
		if err != nil {
			return nil, err
		}
		targets = append(targets, append([]string(nil), threadIDs...))
	}
	return targets, nil
}

type canonicalPreparedEventForIdempotency struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	TargetSelector string          `json:"target_selector"`
}

func canonicalRequestHashForPreparedEvents(events []preparedEvent) ([]byte, error) {
	canonical := make([]canonicalPreparedEventForIdempotency, 0, len(events))
	for _, event := range events {
		canonical = append(canonical, canonicalPreparedEventForIdempotency{
			Type:           event.eventType,
			Payload:        cloneRawMessage(event.payload),
			TargetSelector: preparedEventTargetSelector(event),
		})
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	return digest[:], nil
}

func preparedEventTargetSelector(event preparedEvent) string {
	switch event.eventType {
	case EventTypeUserInterrupt:
		if event.targetThreadID == "" {
			return "session"
		}
		return "thread:" + event.targetThreadID
	case EventTypeUserToolConfirmation:
		return "tool_use:" + event.toolUseEventID
	default:
		return "primary"
	}
}

func preparedEventsContainType(events []preparedEvent, eventType string) bool {
	for _, event := range events {
		if event.eventType == eventType {
			return true
		}
	}
	return false
}

func resolveInterruptTargetThreadID(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, targetThreadID string) (string, error) {
	var threadID string
	if err := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND visibility = 'public'
		    AND archived_at IS NULL`,
		string(workspaceID),
		sessionID,
		targetThreadID,
	).Scan(&threadID); dbconnect.IsNoRows(err) {
		return "", &NotFoundError{Message: "session thread not found"}
	} else if err != nil {
		return "", err
	}
	return threadID, nil
}

func listInterruptTargetThreadIDs(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND visibility = 'public'
		    AND archived_at IS NULL
		  ORDER BY created_at ASC, id ASC`,
		string(workspaceID),
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var threadIDs []string
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return nil, err
		}
		threadIDs = append(threadIDs, threadID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(threadIDs) == 0 {
		return nil, errors.New("sessionevent: no public interrupt targets")
	}
	return threadIDs, nil
}

func resolvePendingToolConfirmationTarget(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, toolUseEventID string) (string, error) {
	row := tx.QueryRow(ctx,
		`SELECT p.session_thread_id
		   FROM session_pending_tool_uses p
		   JOIN session_threads t
		     ON t.workspace_id = p.workspace_id
		    AND t.session_id = p.session_id
		    AND t.id = p.session_thread_id
		  WHERE p.workspace_id = $1
		    AND p.session_id = $2
		    AND p.tool_use_event_id = $3
		    AND t.visibility = 'public'
		    AND t.archived_at IS NULL
		  FOR UPDATE OF p`,
		string(workspaceID),
		sessionID,
		toolUseEventID,
	)
	var sessionThreadID string
	if err := row.Scan(&sessionThreadID); dbconnect.IsNoRows(err) {
		return "", &ConflictError{Message: "pending approval not found"}
	} else if err != nil {
		return "", err
	}
	return sessionThreadID, nil
}

type runtimeInputQueuePayload struct {
	WorkspaceID     string   `json:"workspace_id"`
	SessionID       string   `json:"session_id"`
	SessionThreadID string   `json:"session_thread_id"`
	RuntimeInputID  string   `json:"runtime_input_id"`
	EventIDs        []string `json:"event_ids"`
	SequenceFrom    int64    `json:"sequence_from"`
	SequenceTo      int64    `json:"sequence_to"`
	InputKind       string   `json:"input_kind"`
}

type runtimeInputSegment struct {
	inputKind string
	events    []*Event
}

func runtimeInputSegments(events []*Event) []runtimeInputSegment {
	var segments []runtimeInputSegment
	for _, event := range events {
		if event == nil {
			continue
		}
		inputKind := runtimeInputKindForEventType(event.Type)
		if inputKind == "" {
			continue
		}
		last := len(segments) - 1
		if last < 0 ||
			segments[last].inputKind != inputKind ||
			segments[last].events[0].ThreadID != event.ThreadID ||
			segments[last].events[len(segments[last].events)-1].Sequence+1 != event.Sequence {
			segments = append(segments, runtimeInputSegment{inputKind: inputKind})
			last = len(segments) - 1
		}
		segments[last].events = append(segments[last].events, event)
	}
	return segments
}

func runtimeInputKindForEventType(eventType string) string {
	switch eventType {
	case EventTypeUserMessage:
		return RuntimeInputKindMessages
	case EventTypeUserInterrupt:
		return RuntimeInputKindInterruptControl
	case EventTypeUserToolConfirmation:
		return RuntimeInputKindToolConfirmation
	default:
		return ""
	}
}

func runtimeInputPriority(inputKind string) int {
	if inputKind == RuntimeInputKindInterruptControl {
		return 100
	}
	return 0
}

func runtimeInputPartitionKey(workspaceID workspace.ID, sessionID string) string {
	return RuntimeInputPartitionKey(workspaceID, sessionID)
}

func runtimeInputDedupeKey(workspaceID workspace.ID, sessionID string, runtimeInputID string) string {
	return RuntimeInputDedupeKey(workspaceID, sessionID, runtimeInputID)
}

func appendSessionEventStreamChange(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, sessionThreadID string, eventID string, sessionVisible bool, now time.Time) (int64, error) {
	var streamPosition int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, 1, 'public', $5, $6)
		RETURNING stream_position`,
		string(workspaceID),
		sessionID,
		eventID,
		nullableString(sessionThreadID),
		sessionVisible,
		now,
	).Scan(&streamPosition); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_events
		    SET latest_stream_position = $4,
		        insert_stream_position = CASE
		            WHEN insert_stream_position = 0 THEN $4
		            ELSE insert_stream_position
		        END
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		string(workspaceID),
		sessionID,
		eventID,
		streamPosition,
	); err != nil {
		return 0, err
	}
	return streamPosition, nil
}

func publicEventSessionVisible(eventType string, sessionThreadID string, mainThreadID string) bool {
	if sessionThreadID == "" || sessionThreadID == mainThreadID {
		return true
	}
	switch eventType {
	case "agent.thread_message_sent",
		"agent.thread_message_received",
		"session.thread_created",
		"session.thread_status_running",
		"session.thread_status_idle",
		"session.thread_status_rescheduled",
		"session.thread_status_terminated":
		return true
	default:
		return false
	}
}

func rejectMessageWhileApprovalPending(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, runtimeStatus sessionRuntimeStatusAdmissionState, events []preparedEvent) error {
	hasMessage := false
	for _, event := range events {
		if event.eventType == EventTypeUserMessage {
			hasMessage = true
			break
		}
	}
	if !hasMessage || runtimeStatus.status != "idle" {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_pending_tool_uses p
			 WHERE p.workspace_id = $1
			   AND p.session_id = $2
			   AND p.status = 'pending'
		)`,
		string(workspaceID),
		sessionID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return &ConflictError{Message: "session has a pending approval"}
	}
	return nil
}

func resolvePendingToolConfirmation(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, toolUseEventID string, decision string, denyMessage string, now time.Time) (string, error) {
	row := tx.QueryRow(ctx,
		`SELECT p.session_thread_id, p.status
		   FROM session_pending_tool_uses p
		   JOIN session_threads t
		     ON t.workspace_id = p.workspace_id
		    AND t.session_id = p.session_id
		    AND t.id = p.session_thread_id
		  WHERE p.workspace_id = $1
		    AND p.session_id = $2
		    AND p.tool_use_event_id = $3
		    AND t.visibility = 'public'
		    AND t.archived_at IS NULL
		  FOR UPDATE OF p`,
		string(workspaceID),
		sessionID,
		toolUseEventID,
	)
	var sessionThreadID string
	var status string
	if err := row.Scan(&sessionThreadID, &status); dbconnect.IsNoRows(err) {
		return "", &ConflictError{Message: "pending approval not found"}
	} else if err != nil {
		return "", err
	}
	if status != "pending" {
		return "", &ConflictError{Message: "pending approval is not pending"}
	}
	var storedDenyMessage any
	if decision == string(ToolConfirmationResultDeny) && denyMessage != "" {
		storedDenyMessage = denyMessage
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving',
		        decision = $4,
		        deny_message = $5,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND tool_use_event_id = $3
		    AND status = 'pending'`,
		string(workspaceID),
		sessionID,
		toolUseEventID,
		decision,
		storedDenyMessage,
		now,
	)
	if err != nil {
		return "", err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if affected != 1 {
		return "", &ConflictError{Message: "pending approval is not pending"}
	}
	return sessionThreadID, nil
}

type storedSessionEventIdempotency struct {
	canonicalRequestHash []byte
	responseEventsJSON   string
}

func readSessionEventIdempotency(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, keyDigest []byte) (storedSessionEventIdempotency, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT canonical_request_hash, response_events_json
		   FROM session_event_idempotency_keys
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND idempotency_key_digest = $3
		  FOR UPDATE`,
		string(workspaceID),
		sessionID,
		keyDigest,
	)
	var existing storedSessionEventIdempotency
	if err := row.Scan(&existing.canonicalRequestHash, &existing.responseEventsJSON); dbconnect.IsNoRows(err) {
		return storedSessionEventIdempotency{}, false, nil
	} else if err != nil {
		return storedSessionEventIdempotency{}, false, err
	}
	return existing, true, nil
}

type sessionEventIdempotencyResponseEvent struct {
	ID              string          `json:"id"`
	SessionThreadID string          `json:"session_thread_id,omitempty"`
	Sequence        int64           `json:"sequence"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload_json"`
	CreatedAt       string          `json:"created_at"`
}

func encodeSessionEventIdempotencyResponse(events []*Event) (string, error) {
	response := make([]sessionEventIdempotencyResponseEvent, 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		response = append(response, sessionEventIdempotencyResponseEvent{
			ID:              event.ID,
			SessionThreadID: event.ThreadID,
			Sequence:        event.Sequence,
			Type:            event.Type,
			Payload:         cloneRawMessage(event.Payload),
			CreatedAt:       event.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	body, err := json.Marshal(response)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func decodeSessionEventIdempotencyResponse(workspaceID workspace.ID, sessionID string, raw string) ([]*Event, error) {
	var stored []sessionEventIdempotencyResponseEvent
	if err := json.Unmarshal([]byte(raw), &stored); err != nil {
		return nil, err
	}
	events := make([]*Event, 0, len(stored))
	for _, event := range stored {
		createdAt, err := time.Parse(time.RFC3339Nano, event.CreatedAt)
		if err != nil {
			return nil, err
		}
		events = append(events, &Event{
			ID:          event.ID,
			WorkspaceID: workspaceID,
			SessionID:   sessionID,
			ThreadID:    event.SessionThreadID,
			Sequence:    event.Sequence,
			Type:        event.Type,
			Payload:     cloneRawMessage(event.Payload),
			CreatedAt:   createdAt,
		})
	}
	return events, nil
}

type sessionAdmissionState struct {
	mainThreadID       string
	mainThreadArchived bool
}

func lockOpenSession(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string) (sessionAdmissionState, error) {
	row := tx.QueryRow(ctx,
		`SELECT status, lifecycle_state, archived_at, main_thread_id,
		        EXISTS (
		          SELECT 1
		            FROM session_threads t
		           WHERE t.workspace_id = sessions.workspace_id
		             AND t.session_id = sessions.id
		             AND t.id = sessions.main_thread_id
		             AND t.archived_at IS NOT NULL
		        )
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2
		  FOR UPDATE`,
		string(workspaceID),
		sessionID,
	)
	var status string
	var lifecycleState string
	var archivedAt sql.NullTime
	var mainThreadID sql.NullString
	var mainThreadArchived bool
	if err := row.Scan(&status, &lifecycleState, &archivedAt, &mainThreadID, &mainThreadArchived); dbconnect.IsNoRows(err) {
		return sessionAdmissionState{}, &NotFoundError{Message: "session not found"}
	} else if err != nil {
		return sessionAdmissionState{}, err
	}
	if archivedAt.Valid || lifecycleState == "archived" {
		return sessionAdmissionState{}, &ConflictError{Message: "session is archived"}
	}
	if lifecycleState == "deleted" {
		return sessionAdmissionState{}, &NotFoundError{Message: "session not found"}
	}
	if lifecycleState != "active" {
		return sessionAdmissionState{}, &ConflictError{Message: "session lifecycle transition is in progress"}
	}
	if status == "terminated" {
		return sessionAdmissionState{}, &ConflictError{Message: "session is terminated"}
	}
	if !mainThreadID.Valid || mainThreadID.String == "" {
		return sessionAdmissionState{}, errors.New("sessionevent: session main_thread_id invariant missing")
	}
	return sessionAdmissionState{mainThreadID: mainThreadID.String, mainThreadArchived: mainThreadArchived}, nil
}

type sessionRuntimeStatusAdmissionState struct {
	cleanupClaimed bool
	status         string
}

func lockSessionRuntimeStatus(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string) (sessionRuntimeStatusAdmissionState, error) {
	row := tx.QueryRow(ctx,
		`SELECT status, cleanup_claimed_at
		   FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2
		  FOR UPDATE`,
		string(workspaceID),
		sessionID,
	)
	var state sessionRuntimeStatusAdmissionState
	var cleanupClaimedAt sql.NullTime
	if err := row.Scan(&state.status, &cleanupClaimedAt); dbconnect.IsNoRows(err) {
		return sessionRuntimeStatusAdmissionState{}, errors.New("sessionevent: session_runtime_status invariant missing")
	} else if err != nil {
		return sessionRuntimeStatusAdmissionState{}, err
	}
	state.cleanupClaimed = cleanupClaimedAt.Valid
	return state, nil
}

func clearStaleCleanupMarkers(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_after = NULL,
		        cleanup_enqueued_at = NULL,
		        cleanup_job_id = NULL,
		        updated_at = $3
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND cleanup_claimed_at IS NULL`,
		string(workspaceID),
		sessionID,
		now,
	)
	return err
}

func nextEventSequence(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, sessionThreadID string, nextSequences map[string]int64) (int64, error) {
	if nextSequence, ok := nextSequences[sessionThreadID]; ok {
		nextSequence++
		nextSequences[sessionThreadID] = nextSequence
		return nextSequence, nil
	}
	latestSequence, err := maxEventSequence(ctx, tx, workspaceID, sessionID, sessionThreadID)
	if err != nil {
		return 0, err
	}
	nextSequence := latestSequence + 1
	nextSequences[sessionThreadID] = nextSequence
	return nextSequence, nil
}

// maxEventSequence returns the current highest sequence for one session-thread
// event scope, or 0 when that ledger scope has no rows. It is a bounded
// aggregate read: it does not select payloads, lock event rows, or scan by
// session-global event order.
func maxEventSequence(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, sessionThreadID string) (int64, error) {
	var latestSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0)
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id IS NOT DISTINCT FROM $3`,
		string(workspaceID),
		sessionID,
		nullableString(sessionThreadID),
	).Scan(&latestSequence); err != nil {
		return 0, err
	}
	return latestSequence, nil
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
