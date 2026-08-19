package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestWriteEventRejectionWithoutOptionalDeltaEmitsBoundedPhaseReason(t *testing.T) {
	var output bytes.Buffer
	store := &PostgreSQLBridgeAPIStore{Logger: slog.New(slog.NewJSONHandler(&output, nil))}
	operationID := strings.Repeat("x", 129)
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_write_event_diagnostic", "thr_write_event_diagnostic", "bind_write_event_diagnostic", 1, "pod_write_event_diagnostic"),
		RuntimeWriteId: operationID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid WriteEvent error = %v; want InvalidArgument", err)
	}
	logText := output.String()
	if strings.Count(logText, `"event.kind":"runtime_declaration_rejected"`) != 1 ||
		!strings.Contains(logText, `"phase":"declaration_validation"`) ||
		!strings.Contains(logText, `"reason":"identity"`) ||
		!strings.Contains(logText, `"operation.id":"invalid"`) ||
		strings.Contains(logText, operationID) {
		t.Fatalf("WriteEvent rejection diagnostic = %q", logText)
	}
}

func TestPostgreSQLReviewerOutcomeWritesAreStaleBehindInterruptBarrier(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_reviewer_outcome_barrier"
		parentID   = "thr_reviewer_outcome_barrier_parent"
		reviewerID = "thr_reviewer_outcome_barrier_reviewer"
		bindingID  = "bind_reviewer_outcome_barrier"
		podUID     = "pod_reviewer_outcome_barrier"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, created_at, updated_at
	) VALUES ('default', $1, $2, 'rin_reviewer_outcome_barrier_interrupt', 'interrupt_control',
		'[]', 1, 1, 'queued', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`, sessionID, parentID); err != nil {
		t.Fatalf("seed Reviewer outcome interrupt barrier: %v", err)
	}
	server := BridgeAPIServer{store: NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))}

	for _, eventType := range []string{"approval_review.decision", "approval_review.failure"} {
		writeID := "rwrite_reviewer_outcome_barrier_" + strings.TrimPrefix(eventType, "approval_review.")
		response, err := server.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: bridgeAPIScope(sessionID, reviewerID, bindingID, 1, podUID), RuntimeWriteId: writeID,
			ModelRequestId: "mreq_reviewer_outcome_barrier", EventType: eventType, PayloadJson: `{"type":"` + eventType + `"}`,
		})
		if err != nil || response.GetStale() == nil {
			t.Fatalf("late %s behind interrupt barrier = %#v/%v; want typed stale", eventType, response, err)
		}
		var events, operations int
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type=$3),
			(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND idempotency_key=$4)`,
			sessionID, reviewerID, eventType, writeID).Scan(&events, &operations); err != nil {
			t.Fatalf("read late %s residue: %v", eventType, err)
		}
		if events != 0 || operations != 0 {
			t.Fatalf("late %s events/operations = %d/%d; want zero parent-affecting writes", eventType, events, operations)
		}
	}
}

func TestWriteEventReturnsOperationSpecificDurableFacts(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_write_event_facts"
		threadID  = "sthr_write_event_facts"
		bindingID = "bind_write_event_facts"
		podUID    = "pod_write_event_facts"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, store.AttachmentBlobStore, "file_start_facts", "start.png", "image/png", "start")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_start_facts", 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_start_facts"}}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_start_facts", "messages",
		`["sevt_start_facts"]`, "committed", bindingID, podUID, 1, 1)

	startRequest := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_start", ModelRequestId: "mreq_facts",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: requestKindAgentProviderRequest,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: "sevt_start_facts", FileId: "file_start_facts"}},
	}
	first, err := store.WriteEvent(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	if first.GetCommitted() == nil || first.GetCommitted().GetEventId() == "" || first.GetCommitted().AssignedMessageSequence != nil {
		t.Fatalf("start result = %#v", first)
	}
	second, err := store.WriteEvent(context.Background(), startRequest)
	if err != nil {
		t.Fatalf("WriteEvent duplicate: %v", err)
	}
	if second.GetDuplicate() == nil || second.GetDuplicate().GetEventId() != first.GetCommitted().GetEventId() {
		t.Fatalf("duplicate result = %#v; first = %#v", second, first)
	}
	var startRows, consumptionRows int
	var consumedAtStart string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_file_attachment_consumptions WHERE workspace_id='default' AND session_id=$1),
		(SELECT request_start_event_id FROM session_file_attachment_consumptions WHERE workspace_id='default' AND session_id=$1)`, sessionID).
		Scan(&startRows, &consumptionRows, &consumedAtStart); err != nil {
		t.Fatalf("read Request Start consumption replay: %v", err)
	}
	if startRows != 1 || consumptionRows != 1 || consumedAtStart != first.GetCommitted().GetEventId() {
		t.Fatalf("Request Start replay facts = starts %d consumptions %d owner %q", startRows, consumptionRows, consumedAtStart)
	}

	message, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_message", ModelRequestId: "mreq_facts",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"hello"}]}`,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "hello"}}}}},
	})
	if err != nil {
		t.Fatalf("WriteEvent message: %v", err)
	}
	if message.GetCommitted() == nil || message.GetCommitted().AssignedMessageSequence == nil || message.GetCommitted().GetAssignedMessageSequence() != 1 || len(message.GetCommitted().GetCreatedToolUseEventIds()) != 0 {
		t.Fatalf("message result = %#v", message)
	}
	var stored string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id='mreq_facts'`, sessionID, threadID).Scan(&stored); err != nil {
		t.Fatalf("load stored context: %v", err)
	}
	var durable map[string]any
	if err := json.Unmarshal([]byte(stored), &durable); err != nil {
		t.Fatalf("decode stored context: %v", err)
	}
	if len(durable) != 1 || durable["parts"] == nil {
		t.Fatalf("stored context contains non-context fields: %#v", durable)
	}
}

func TestPostgreSQLWriteEventUsesOneOperationNamespaceWithoutChangingChildLowering(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_write_event_namespace"
		mainID    = "thr_write_event_namespace_main"
		childID   = "thr_write_event_namespace_child"
		bindingID = "bind_write_event_namespace"
		podUID    = "pod_write_event_namespace"
		writeID   = "rwrite_write_event_namespace"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, childID, bindingID, 1, podUID)
	request := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: writeID, EventType: "session.status_running",
		PayloadJson: `{"type":"session.status_running"}`,
	}
	committed, err := store.WriteEvent(context.Background(), request)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("child running WriteEvent = %#v/%v; want committed", committed, err)
	}
	replay, err := store.WriteEvent(context.Background(), request)
	if err != nil || replay.GetDuplicate().GetEventId() != committed.GetCommitted().GetEventId() {
		t.Fatalf("same WriteEvent replay = %#v/%v; want one stored receipt", replay, err)
	}
	crossType := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: writeID, EventType: "session.error",
		PayloadJson: `{"type":"session.error","error":{"type":"unknown_error","message":"conflict","retry_status":{"type":"terminal"}}}`,
	}
	if response, err := store.WriteEvent(context.Background(), crossType); response != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("cross-type WriteEvent reuse = %#v/%v; want idempotency conflict", response, err)
	}
	crossPayload := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: writeID, EventType: "session.status_running",
		PayloadJson: `{"type":"session.status_running","unexpected":"conflict"}`,
	}
	if response, err := store.WriteEvent(context.Background(), crossPayload); response != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("cross-payload WriteEvent reuse = %#v/%v; want idempotency conflict", response, err)
	}

	var operationCount, eventCount int
	var operationSource, eventType, payloadJSON, visibility, childStatus string
	var sessionVisible bool
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='write_event' AND idempotency_key=$3),
		(SELECT source_kind FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='write_event' AND idempotency_key=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND runtime_write_id=$3),
		(SELECT type FROM session_events WHERE workspace_id='default' AND session_id=$1 AND runtime_write_id=$3),
		(SELECT payload_json FROM session_events WHERE workspace_id='default' AND session_id=$1 AND runtime_write_id=$3),
		(SELECT visibility FROM session_events WHERE workspace_id='default' AND session_id=$1 AND runtime_write_id=$3),
		(SELECT session_visible FROM session_events WHERE workspace_id='default' AND session_id=$1 AND runtime_write_id=$3),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2)`,
		sessionID, childID, writeID,
	).Scan(&operationCount, &operationSource, &eventCount, &eventType, &payloadJSON, &visibility, &sessionVisible, &childStatus); err != nil {
		t.Fatalf("read WriteEvent namespace and child lowering facts: %v", err)
	}
	if operationCount != 1 || operationSource != "write_event" || eventCount != 1 || eventType != "session.thread_status_running" ||
		!strings.Contains(payloadJSON, `"type":"session.thread_status_running"`) || visibility != "public" || !sessionVisible || childStatus != "running" {
		t.Fatalf("WriteEvent namespace/lowering = operations %d/%s events %d/%s payload %s visibility %s/%t child %s",
			operationCount, operationSource, eventCount, eventType, payloadJSON, visibility, sessionVisible, childStatus)
	}
}

func TestWriteEventRequestStartRequiresUniqueCommittedMessageAuthorityAndSingleConsumption(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		seedInbox func(*testing.T, *sql.DB, string, string, string, string)
	}{
		{name: "missing"},
		{name: "uncommitted", seedInbox: func(t *testing.T, admin *sql.DB, sessionID, threadID, bindingID, podUID string) {
			seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_attachment_uncommitted", "messages",
				`["sevt_attachment_authority"]`, "accepted", bindingID, podUID, 1, 1)
		}},
		{name: "ambiguous", seedInbox: func(t *testing.T, admin *sql.DB, sessionID, threadID, bindingID, podUID string) {
			seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_attachment_committed", "messages",
				`["sevt_attachment_authority"]`, "committed", bindingID, podUID, 1, 1)
			seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_attachment_conflict", "rejection",
				`["sevt_attachment_authority"]`, "committed", bindingID, podUID, 1, 1)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			const sessionID = "sesn_attachment_authority"
			const threadID = "thr_attachment_authority"
			const bindingID = "bind_attachment_authority"
			const podUID = "pod_attachment_authority"
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
			store.AttachmentBlobStore = blob.NewFakeBlobStore()
			seedBridgeAPIFileAttachment(t, admin, store.AttachmentBlobStore, "file_attachment_authority", "authority.png", "image/png", "authority")
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_attachment_authority", 1, "user.message",
				`{"content":[{"type":"image","source":{"type":"file","file_id":"file_attachment_authority"}}]}`)
			if testCase.seedInbox != nil {
				testCase.seedInbox(t, admin, sessionID, threadID, bindingID, podUID)
			}
			response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeWriteId: "rwrite_attachment_authority",
				ModelRequestId: "mreq_attachment_authority", EventType: "span.model_request_start",
				PayloadJson: `{"type":"span.model_request_start"}`, ContextThroughMessageSequence: bridgeAPIInt64(0),
				RequestKind: requestKindAgentProviderRequest, ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{
					SourceEventId: "sevt_attachment_authority", FileId: "file_attachment_authority",
				}},
			})
			if status.Code(err) != codes.InvalidArgument || response != nil {
				t.Fatalf("unauthorized Request Start = %#v/%v; want InvalidArgument", response, err)
			}
		})
	}

	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_attachment_single_consumption"
	const threadID = "thr_attachment_single_consumption"
	const bindingID = "bind_attachment_single_consumption"
	const podUID = "pod_attachment_single_consumption"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIFileAttachment(t, admin, store.AttachmentBlobStore, "file_attachment_once", "once.png", "image/png", "once")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_attachment_once", 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_attachment_once"}}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_attachment_once", "messages",
		`["sevt_attachment_once"]`, "committed", bindingID, podUID, 1, 1)
	pair := &bridgev1.FileAttachmentPair{SourceEventId: "sevt_attachment_once", FileId: "file_attachment_once"}
	first := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_attachment_once_first", ModelRequestId: "mreq_attachment_once_first",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: requestKindAgentProviderRequest,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{pair},
	}
	committed, err := store.WriteEvent(context.Background(), first)
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("first attachment consumption = %#v/%v", committed, err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_attachment_once_end", ModelRequestId: first.ModelRequestId,
		FinishReason: "stop", UsageJson: `{}`,
	}); err != nil {
		t.Fatalf("close first attachment Request: %v", err)
	}
	replay, err := store.WriteEvent(context.Background(), first)
	if err != nil || replay.GetDuplicate() == nil {
		t.Fatalf("identical Request Start replay = %#v/%v; want duplicate", replay, err)
	}
	second, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_attachment_once_second", ModelRequestId: "mreq_attachment_once_second",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: requestKindAgentProviderRequest,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{pair},
	})
	if status.Code(err) != codes.AlreadyExists || second != nil {
		t.Fatalf("second Request Start consumption = %#v/%v; want AlreadyExists", second, err)
	}
	var starts, consumptions int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_file_attachment_consumptions WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&starts, &consumptions); err != nil {
		t.Fatalf("read single-consumption facts: %v", err)
	}
	if starts != 1 || consumptions != 1 {
		t.Fatalf("single-consumption facts = starts %d consumptions %d; want 1/1", starts, consumptions)
	}
}

func TestWriteEventRequestStartConsumptionRollsBackEventAndRelationTogether(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_start_consumption_rollback"
		threadID  = "thr_start_consumption_rollback"
		bindingID = "bind_start_consumption_rollback"
		podUID    = "pod_start_consumption_rollback"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, store.AttachmentBlobStore, "file_start_rollback", "rollback.png", "image/png", "rollback")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_start_rollback", 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_start_rollback"}}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_start_rollback", "messages",
		`["sevt_start_rollback"]`, "committed", bindingID, podUID, 1, 1)
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_start_consumption() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected consumption failure'; END $$;
		CREATE TRIGGER fail_start_consumption BEFORE INSERT ON session_file_attachment_consumptions
		FOR EACH ROW EXECUTE FUNCTION fail_start_consumption()`); err != nil {
		t.Fatalf("install atomic rollback fault: %v", err)
	}
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeWriteId: "rwrite_start_rollback",
		ModelRequestId: "mreq_start_rollback", EventType: "span.model_request_start",
		PayloadJson: `{"type":"span.model_request_start"}`, ContextThroughMessageSequence: bridgeAPIInt64(0),
		RequestKind:             requestKindAgentProviderRequest,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: "sevt_start_rollback", FileId: "file_start_rollback"}},
	})
	if err == nil || response != nil {
		t.Fatalf("faulted Request Start = %#v/%v; want transaction failure", response, err)
	}
	var starts, consumptions, operations int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_file_attachment_consumptions WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND idempotency_key='rwrite_start_rollback')`, sessionID).
		Scan(&starts, &consumptions, &operations); err != nil {
		t.Fatalf("read atomic rollback facts: %v", err)
	}
	if starts != 0 || consumptions != 0 || operations != 0 {
		t.Fatalf("faulted Request Start facts = starts %d consumptions %d operations %d; want zero", starts, consumptions, operations)
	}
}

func TestWriteEventRejectsContextOnNonMemberEvent(t *testing.T) {
	store := NewPostgreSQLBridgeAPIStore(nil)
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: bridgeAPIScope("sesn", "sthr", "bind", 1, "pod"), RuntimeWriteId: "rwrite", EventType: "session.status_running", PayloadJson: `{}`,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "invalid"}}}}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent error = %v; want InvalidArgument", err)
	}
}

func TestWriteEventRejectsSecondOpenRequest(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_second_open_request"
		threadID  = "thr_second_open_request"
		bindingID = "bind_second_open_request"
		podUID    = "pod_second_open_request"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_open_request_one", "mreq_open_request_one", requestKindAgentProviderRequest, 0)

	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_open_request_two", ModelRequestId: "mreq_open_request_two",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: requestKindAgentProviderRequest,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second open request error = %v; want FailedPrecondition", err)
	}
	var starts int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'`, sessionID, threadID).Scan(&starts); err != nil {
		t.Fatalf("count request starts: %v", err)
	}
	if starts != 1 {
		t.Fatalf("durable request starts = %d; want 1", starts)
	}
}

func TestWriteEventRejectsHistoricalModelToolCallIDReuse(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_historical_tool_call_id"
		threadID        = "thr_historical_tool_call_id"
		bindingID       = "bind_historical_tool_call_id"
		podUID          = "pod_historical_tool_call_id"
		modelToolCallID = "call_history_global"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_history_first_start", "mreq_history_first", requestKindAgentProviderRequest, 0)
	first, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_history_first_tool", ModelRequestId: "mreq_history_first",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"apply_patch","input":{},"evaluated_permission":"ask"}`,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest(modelToolCallID, "apply_patch", `{}`),
		CanonicalExecutionInputJson: `{}`,
	})
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("first Tool Call = %#v/%v", first, err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_history_first_end", ModelRequestId: "mreq_history_first",
		FinishReason: "tool_calls", UsageJson: `{}`,
	}); err != nil {
		t.Fatalf("seal first request: %v", err)
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_history_second_start", "mreq_history_second", requestKindAgentProviderRequest, 1)

	_, err = store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_history_second_tool", ModelRequestId: "mreq_history_second",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"apply_patch","input":{},"evaluated_permission":"ask"}`,
		AssistantContextDelta:       bridgeToolCallContextDeltaForTest(modelToolCallID, "apply_patch", `{}`),
		CanonicalExecutionInputJson: `{}`,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("historical Tool Call reuse error = %v; want AlreadyExists", err)
	}
	var declarations int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages AS message CROSS JOIN LATERAL jsonb_array_elements(message.data_json::jsonb -> 'parts') AS part WHERE message.workspace_id='default' AND message.session_id=$1 AND message.session_thread_id=$2 AND part ->> 'type'='tool_call' AND part ->> 'modelToolCallId'=$3`, sessionID, threadID, modelToolCallID).Scan(&declarations); err != nil {
		t.Fatalf("count Tool Call declarations: %v", err)
	}
	if declarations != 1 {
		t.Fatalf("Tool Call declarations = %d; want 1", declarations)
	}
}
