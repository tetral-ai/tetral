package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge inputs protocol-family boundary.

func TestValidateApprovalReviewTextBounds(t *testing.T) {
	for _, test := range []struct {
		name string
		text []string
		code codes.Code
	}{
		{name: "count at limit", text: make([]string, maxApprovalReviewTextParts), code: codes.OK},
		{name: "count over limit", text: make([]string, maxApprovalReviewTextParts+1), code: codes.InvalidArgument},
		{name: "aggregate bytes at limit", text: []string{strings.Repeat("x", maxApprovalReviewTextBytes)}, code: codes.OK},
		{name: "aggregate bytes over limit", text: []string{strings.Repeat("x", maxApprovalReviewTextBytes+1)}, code: codes.InvalidArgument},
		{name: "invalid UTF-8", text: []string{string([]byte{0xff})}, code: codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := status.Code(validateApprovalReviewText(test.text)); got != test.code {
				t.Fatalf("validateApprovalReviewText code = %s; want %s", got, test.code)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreInterruptReplayFollowsDurableTurnAuthority(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_turn_authority"
		threadID  = "thr_interrupt_turn_authority"
		bindingID = "bind_interrupt_turn_authority"
		podUID    = "pod_interrupt_turn_authority"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "evt_interrupt_turn_open")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_old", 2, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_interrupt_old", "interrupt_control",
		`["evt_interrupt_old"]`, "accepted", bindingID, podUID, 2, 2)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	oldRequest := &bridgev1.CommitInputsRequest{Scope: scope, RuntimeInputId: "rin_interrupt_old"}
	first, err := store.CommitInputs(context.Background(), oldRequest)
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("first interrupt commit = %#v/%v; want committed", first, err)
	}
	openReplay, err := store.CommitInputs(context.Background(), oldRequest)
	if err != nil || openReplay.GetCommitted() == nil {
		t.Fatalf("open-turn lost-response replay = %#v/%v; want identical committed receipt", openReplay, err)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_turn_idle", 3, "session.status_idle", `{"type":"session.status_idle"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='idle'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
		t.Fatalf("settle durable interrupt turn: %v", err)
	}
	settledReplay, err := store.CommitInputs(context.Background(), oldRequest)
	if err != nil || settledReplay.GetStale() == nil {
		t.Fatalf("settled interrupt replay = %#v/%v; want stale no-op", settledReplay, err)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_new", 4, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_interrupt_new", "interrupt_control",
		`["evt_interrupt_new"]`, "accepted", bindingID, podUID, 4, 4)
	newer, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_interrupt_new",
	})
	if err != nil || newer.GetCommitted() == nil {
		t.Fatalf("newer idle interrupt = %#v/%v; want committed", newer, err)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_pre_successor", 5, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_interrupt_pre_successor", "interrupt_control",
		`["evt_interrupt_pre_successor"]`, "accepted", bindingID, podUID, 5, 5)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_successor_open", 6, "session.status_running", `{"type":"session.status_running"}`)
	oldSuccessor, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_interrupt_pre_successor",
	})
	if err != nil || oldSuccessor.GetStale() == nil {
		t.Fatalf("pre-successor interrupt = %#v/%v; want stale", oldSuccessor, err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_post_successor", 7, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_interrupt_post_successor", "interrupt_control",
		`["evt_interrupt_post_successor"]`, "accepted", bindingID, podUID, 7, 7)
	postSuccessor, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_interrupt_post_successor",
	})
	if err != nil || postSuccessor.GetCommitted() == nil {
		t.Fatalf("post-successor interrupt = %#v/%v; want committed", postSuccessor, err)
	}
}

func TestPostgreSQLWriteRequestEndJoinsPrecommittedActiveRunInterrupt(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_precommitted_active_interrupt"
		threadID       = "thr_precommitted_active_interrupt"
		bindingID      = "bind_precommitted_active_interrupt"
		podUID         = "pod_precommitted_active_interrupt"
		modelRequestID = "mreq_precommitted_active_interrupt"
		interruptID    = "rin_precommitted_active_interrupt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "evt_precommitted_active_run")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_precommitted_active_start", modelRequestID, requestKindAgentProviderRequest, 0)
	var interruptSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).
		Scan(&interruptSequence); err != nil {
		t.Fatalf("allocate active-run interrupt sequence: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_precommitted_active_interrupt", interruptSequence, "user.interrupt", `{}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, interruptID, "interrupt_control",
		`["evt_precommitted_active_interrupt"]`, "accepted", bindingID, podUID, interruptSequence, interruptSequence)
	committed, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: interruptID,
	})
	if err != nil || committed.GetCommitted().GetInterrupt() == nil {
		t.Fatalf("precommit active-run interrupt = %#v/%v; want committed", committed, err)
	}
	ended, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_precommitted_active_end", ModelRequestId: modelRequestID,
		FinishReason: "cancelled", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{RuntimeInputId: interruptID},
	})
	if err != nil || ended.GetCommitted() == nil {
		t.Fatalf("close active request with precommitted interrupt = %#v/%v; want committed", ended, err)
	}
	var requestEnds, interruptOperations int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1
		  AND session_thread_id=$2 AND model_request_id=$3 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1
		  AND session_thread_id=$2 AND operation='commit_inputs' AND source_kind='interrupt_control' AND idempotency_key=$4)`,
		sessionID, threadID, modelRequestID, interruptID).Scan(&requestEnds, &interruptOperations); err != nil {
		t.Fatalf("read joined active interrupt facts: %v", err)
	}
	if requestEnds != 1 || interruptOperations != 1 {
		t.Fatalf("joined active interrupt facts = request ends %d, input operations %d; want 1,1", requestEnds, interruptOperations)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsAcceptedMessage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit", "thr_bridge_commit")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit", "bind_bridge_commit", 1, "pod_uid_commit")
	seedBridgeAPIRuntimeInput(t, admin, "default", "sesn_bridge_commit", "thr_bridge_commit", "rin_bridge_commit", "bind_bridge_commit", "pod_uid_commit", "evt_bridge_commit")
	initialStreamPosition := seedBridgeAPIStreamChange(t, admin, "default", "sesn_bridge_commit", "thr_bridge_commit", "evt_bridge_commit", 1, "public", true)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-commit-inputs-test-key-32")
	var declarationLogs bytes.Buffer
	store.Logger = slog.New(slog.NewJSONHandler(&declarationLogs, nil))
	request := &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit", "thr_bridge_commit", "bind_bridge_commit", 1, "pod_uid_commit"),
		RuntimeInputId: "rin_bridge_commit",
	}
	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs: %v", err)
	}
	committed := response.GetCommitted()
	if committed == nil || committed.GetInterrupt() != nil || len(committed.GetContext().GetAssignedContextSequences()) != 1 {
		t.Fatalf("CommitInputs outcome = %#v; want one assigned context sequence", response)
	}
	assignedSequence := committed.GetContext().GetAssignedContextSequences()[0]
	loadResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: request.GetScope(),
	})
	if err != nil {
		t.Fatalf("LoadContext committed input: %v", err)
	}
	var loaded bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
		t.Fatalf("decode committed input context: %v", err)
	}
	if len(loaded.ContextEntries) != 1 {
		t.Fatalf("loaded context entries = %d; want one committed input", len(loaded.ContextEntries))
	}
	var loadedPart struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if len(loaded.ContextEntries[0].Parts) != 1 {
		t.Fatalf("loaded context parts = %d; want one text part", len(loaded.ContextEntries[0].Parts))
	}
	if err := json.Unmarshal(loaded.ContextEntries[0].Parts[0], &loadedPart); err != nil {
		t.Fatalf("decode loaded committed input: %v", err)
	}
	if loaded.ContextEntries[0].MessageSequence != assignedSequence ||
		loaded.ContextEntries[0].ContextKind != "user" || loadedPart.Type != "text" || loadedPart.Text != "hello" {
		t.Fatalf("loaded committed input = %#v/%#v; want equivalent durable user content", loaded.ContextEntries[0], loadedPart)
	}
	replay, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs lost-ACK replay: %v", err)
	}
	if replay.GetCommitted() == nil || !proto.Equal(
		committed.GetContext(),
		replay.GetCommitted().GetContext(),
	) {
		t.Fatalf("CommitInputs lost-ACK replay = %#v; want identical durable delta", replay)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_bridge_commit_replacement',
		        binding_generation = 2,
		        agent_runtime_pod_uid = 'pod_uid_commit_replacement',
		        updated_at = '2026-01-01T00:00:01Z'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_commit'`,
	); err != nil {
		t.Fatalf("replace runtime binding before replay: %v", err)
	}
	response, err = store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs replay: %v", err)
	}
	if response.GetStale() == nil {
		t.Fatalf("replacement-custody replay = %#v; want stale", response)
	}
	logText := declarationLogs.String()
	for _, fragment := range []string{
		`"event.kind":"runtime_declaration_committed"`,
		`"event.kind":"runtime_declaration_replayed"`,
		`"declaration.source.kind":"messages"`,
		`"operation.id":"rin_bridge_commit"`,
		`"thread.id":"thr_bridge_commit"`,
		`"runtime.binding.current":true`,
		`"runtime.binding.current":false`,
		`"binding.id":"bind_bridge_commit_replacement"`,
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("declaration logs missing %s: %s", fragment, logText)
		}
	}
	if strings.Contains(logText, `"text":"hello"`) {
		t.Fatalf("declaration logs contain model-visible input: %s", logText)
	}
	if strings.Contains(logText, `"declaration.source.id"`) || strings.Contains(logText, `"session.thread.id"`) {
		t.Fatalf("declaration logs contain retired identity fields: %s", logText)
	}

	var inboxStatus string
	var processedAt sql.NullString
	var eventRevision int64
	var latestStreamPosition int64
	var messageCount int
	var messageDataJSON string
	var streamChangeCount int
	var maxStreamRevision int64
	var maxStreamPosition int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_commit'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read inbox status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at, revision, latest_stream_position FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_commit'`).Scan(&processedAt, &eventRevision, &latestStreamPosition); err != nil {
		t.Fatalf("read processed_at: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND source_event_id = 'evt_bridge_commit' AND kind = 'user'`).Scan(&messageCount); err != nil {
		t.Fatalf("read projected message count: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages WHERE workspace_id = 'default' AND source_event_id = 'evt_bridge_commit' AND kind = 'user'`).Scan(&messageDataJSON); err != nil {
		t.Fatalf("read projected user message: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(MAX(revision), 0), COALESCE(MAX(stream_position), 0)
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = 'evt_bridge_commit'`).Scan(&streamChangeCount, &maxStreamRevision, &maxStreamPosition); err != nil {
		t.Fatalf("read processed stream changes: %v", err)
	}
	if inboxStatus != "committed" || !processedAt.Valid || messageCount != 1 {
		t.Fatalf("commit side effects status=%q processed=%v messages=%d; want committed/processed/1", inboxStatus, processedAt.Valid, messageCount)
	}
	assertBridgeUserContextProjection(t, messageDataJSON, "hello")
	if eventRevision != 2 || streamChangeCount != 2 || maxStreamRevision != 2 || latestStreamPosition != maxStreamPosition || latestStreamPosition <= initialStreamPosition {
		t.Fatalf("processed stream revision = eventRev %d changeCount %d maxRev %d latest %d initial %d maxPos %d; want same-event revision update only once",
			eventRevision, streamChangeCount, maxStreamRevision, latestStreamPosition, initialStreamPosition, maxStreamPosition)
	}

}

func TestPostgreSQLBridgeAPIStoreCommitInputsReturnsFirstTurnFileAttachmentDelta(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_commit_media"
		threadID       = "thr_bridge_commit_media"
		runtimeInputID = "rin_bridge_commit_media"
		eventID        = "evt_bridge_commit_media"
		imageFileID    = "file_bridge_commit_media_image"
		documentFileID = "file_bridge_commit_media_document"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_commit_media", 1, "pod_uid_commit_media")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_bridge_commit_media_image"}},{"type":"document","source":{"type":"file","file_id":"file_bridge_commit_media_document"}}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages",
		`["evt_bridge_commit_media"]`, "accepted", "bind_bridge_commit_media", "pod_uid_commit_media", 1, 1)
	attachmentStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, imageFileID, "image.png", "image/png", "image")
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, documentFileID, "report.pdf", "application/pdf", "document")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, "bind_bridge_commit_media", 1, "pod_uid_commit_media"),
		RuntimeInputId: runtimeInputID,
	})
	if err != nil {
		t.Fatalf("CommitInputs media: %v", err)
	}
	contextApplication := response.GetCommitted().GetContext()
	if len(contextApplication.GetAssignedContextSequences()) != 0 {
		t.Fatalf("attachment-only assigned context = %v; want none", contextApplication.GetAssignedContextSequences())
	}
	delta := contextApplication.GetPendingAttachmentJson()
	if len(delta) != 2 {
		t.Fatalf("attachment delta = %#v; want image and document references", delta)
	}
	attachments := make(map[string]bridgeLoadContextPendingAttachment)
	for _, raw := range delta {
		var attachment bridgeLoadContextPendingAttachment
		if err := json.Unmarshal([]byte(raw), &attachment); err != nil {
			t.Fatalf("decode attachment delta: %v", err)
		}
		if attachment.Origin.FileBacked == nil || attachment.Origin.FileBacked.SourceEventID != eventID {
			t.Fatalf("attachment delta = %#v; want source event identity", attachment)
		}
		attachments[attachment.Origin.FileBacked.FileID] = attachment
	}
	if attachments[imageFileID].Mime != "image/png" || attachments[imageFileID].Filename != "image.png" ||
		attachments[documentFileID].Mime != "application/pdf" || attachments[documentFileID].Filename != "report.pdf" {
		t.Fatalf("attachment delta = %#v; want committed image and document metadata", attachments)
	}
	var userMessages int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND source_event_id=$3`,
		sessionID, threadID, eventID).Scan(&userMessages); err != nil {
		t.Fatalf("count attachment-only context messages: %v", err)
	}
	if userMessages != 0 {
		t.Fatalf("attachment-only context messages = %d; want none", userMessages)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsKeepsTextAndAttachmentFromOneInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_commit_mixed_media"
		threadID  = "thr_bridge_commit_mixed_media"
		eventID   = "evt_bridge_commit_mixed_media"
		fileID    = "file_bridge_commit_mixed_media"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_commit_mixed_media", 1, "pod_bridge_commit_mixed_media")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message",
		`{"content":[{"type":"text","text":"inspect this image"},{"type":"image","source":{"type":"file","file_id":"file_bridge_commit_mixed_media"}}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_bridge_commit_mixed_media", "messages",
		`["evt_bridge_commit_mixed_media"]`, "accepted", "bind_bridge_commit_mixed_media", "pod_bridge_commit_mixed_media", 1, 1)
	attachmentStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, fileID, "mixed.png", "image/png", "mixed")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-commit-mixed-media-key-32")
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_commit_mixed_media", 1, "pod_bridge_commit_mixed_media")
	response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_bridge_commit_mixed_media",
	})
	if err != nil {
		t.Fatalf("CommitInputs mixed media: %v", err)
	}
	application := response.GetCommitted().GetContext()
	if len(application.GetAssignedContextSequences()) != 1 || len(application.GetPendingAttachmentJson()) != 1 {
		t.Fatalf("mixed input receipt = %#v; want one text sequence and one attachment", application)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext mixed media: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode mixed media context: %v", err)
	}
	if len(payload.ContextEntries) != 1 || len(payload.PendingAttachments) != 1 ||
		payload.PendingAttachments[0].Origin.FileBacked == nil || payload.PendingAttachments[0].Origin.FileBacked.FileID != fileID {
		t.Fatalf("mixed input cold context = entries %#v attachments %#v", payload.ContextEntries, payload.PendingAttachments)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsFencesRuntimeInboxBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_fence", "thr_bridge_commit_fence")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_fence", "bind_bridge_commit_fence", 1, "pod_uid_commit_fence")
	seedBridgeAPIRuntimeInput(t, admin, "default", "sesn_bridge_commit_fence", "thr_bridge_commit_fence", "rin_bridge_commit_fence", "bind_bridge_commit_fence", "pod_uid_other", "evt_bridge_commit_fence")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit_fence", "thr_bridge_commit_fence", "bind_bridge_commit_fence", 1, "pod_uid_commit_fence"),
		RuntimeInputId: "rin_bridge_commit_fence",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CommitInputs fenced inbox err = %v; want FailedPrecondition", err)
	}
	var inboxStatus string
	var processedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_commit_fence'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read fenced inbox status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_commit_fence'`).Scan(&processedAt); err != nil {
		t.Fatalf("read fenced event processed_at: %v", err)
	}
	if inboxStatus != "delivering" || processedAt.Valid {
		t.Fatalf("fenced commit status=%q processed=%v; want untouched delivering/unprocessed", inboxStatus, processedAt.Valid)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsRejectsToolConfirmationAsMessage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_confirmation_message", "bind_bridge_commit_confirmation_message", 1, "pod_uid_commit_confirmation_message")
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "evt_bridge_message_tool", 1)
	setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "evt_bridge_message_tool", "resolving")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "evt_bridge_confirmation_as_message", 2, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"evt_bridge_message_tool","result":"allow"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "rin_bridge_confirmation_as_message", "messages", `["evt_bridge_confirmation_as_message"]`, "accepted", "bind_bridge_commit_confirmation_message", "pod_uid_commit_confirmation_message", 2, 2)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "bind_bridge_commit_confirmation_message", 1, "pod_uid_commit_confirmation_message"),
		RuntimeInputId: "rin_bridge_confirmation_as_message",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CommitInputs messages with tool_confirmation err = %v; want FailedPrecondition", err)
	}

	var inboxStatus string
	var processedAt sql.NullString
	var pendingStatus string
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_confirmation_as_message'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read inbox status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_confirmation_as_message'`).Scan(&processedAt); err != nil {
		t.Fatalf("read processed_at: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_pending_tool_uses WHERE workspace_id = 'default' AND tool_use_event_id = 'evt_bridge_message_tool'`).Scan(&pendingStatus); err != nil {
		t.Fatalf("read pending status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND source_event_id = 'evt_bridge_confirmation_as_message'`).Scan(&messageCount); err != nil {
		t.Fatalf("read message count: %v", err)
	}
	if inboxStatus != "accepted" || processedAt.Valid || pendingStatus != "resolving" || messageCount != 0 {
		t.Fatalf("side effects inbox=%q processed=%v pending=%q messages=%d; want accepted/unprocessed/resolving/0",
			inboxStatus, processedAt.Valid, pendingStatus, messageCount)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsRecordsGeneratedPendingApprovalDecision(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_generated_start", "mreq_bridge_generated_tool_use", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_generated_tool_use",
		ModelRequestId: "mreq_bridge_generated_tool_use",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"dangerous_tool","input":{"path":"README.md"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		AssistantContextDelta: bridgeToolCallContextDeltaForTest(
			"tool-call-generated", "dangerous_tool", `{"path":"README.md"}`,
		),
	})
	if err != nil {
		t.Fatalf("WriteEvent generated tool use: %v", err)
	}
	toolUseEventID := toolUse.GetCommitted().GetEventId()
	if toolUseEventID == "" {
		t.Fatalf("WriteEvent generated tool use outcome = %#v; want committed event", toolUse)
	}
	setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", toolUseEventID, "resolving")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "evt_bridge_generated_confirm", 3, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"`+toolUseEventID+`","result":"allow"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "rin_bridge_generated_confirm", "tool_confirmation", `["evt_bridge_generated_confirm"]`, "accepted", "bind_bridge_generated_confirm", "pod_uid_generated_confirm", 3, 3)

	request := &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm"),
		RuntimeInputId: "rin_bridge_generated_confirm",
	}
	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs generated confirmation: %v", err)
	}
	replay, err := store.CommitInputs(context.Background(), proto.Clone(request).(*bridgev1.CommitInputsRequest))
	if err != nil {
		t.Fatalf("CommitInputs generated confirmation lost-ACK replay: %v", err)
	}
	if response.GetCommitted() == nil || replay.GetCommitted() == nil ||
		!proto.Equal(response.GetCommitted().GetContext(), replay.GetCommitted().GetContext()) {
		t.Fatalf("generated confirmation replay = %#v / %#v; want identical committed context", response, replay)
	}
	var pendingStatus string
	var decision sql.NullString
	var resolvedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, decision, resolved_at
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_generated_confirm'
		    AND session_thread_id = 'thr_bridge_generated_confirm'
		    AND tool_use_event_id = $1`,
		toolUseEventID).Scan(&pendingStatus, &decision, &resolvedAt); err != nil {
		t.Fatalf("read generated pending approval after confirmation: %v", err)
	}
	if response.GetCommitted() == nil ||
		pendingStatus != "resolving" || !decision.Valid || decision.String != "allow" || resolvedAt.Valid {
		t.Fatalf("generated confirmation outcome=%#v pending=%q decision=%v resolved=%v; want committed/resolving/allow/unresolved",
			response, pendingStatus, decision, resolvedAt.Valid)
	}
	var confirmationMessages int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages
		WHERE workspace_id='default' AND session_id='sesn_bridge_generated_confirm'
		  AND session_thread_id='thr_bridge_generated_confirm' AND source_event_id='evt_bridge_generated_confirm'`).Scan(&confirmationMessages); err != nil {
		t.Fatalf("count generated confirmation context: %v", err)
	}
	if confirmationMessages != 1 {
		t.Fatalf("generated confirmation context facts = %d; want exactly one", confirmationMessages)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsInterAgentMessageExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inter_agent", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	const (
		sourceToolUseEventID = "evt_bridge_inter_agent_send"
		childThreadID        = "thr_bridge_inter_agent_child"
		content              = "hello child"
	)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", sourceToolUseEventID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_thr_bridge_inter_agent_child","message":"hello child"}}`)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("inter-agent-context-test-key-32b")
	parentScope := bridgeAPIScope("sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", childThreadID)
	deliveryID := agentMailDeliveryID(sourceToolUseEventID, childThreadID)
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
	delivered, err := store.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: parentScope, DeliveryId: deliveryID, TargetThreadId: childThreadID,
		SourceToolUseEventId: sourceToolUseEventID, Content: content,
	})
	if err != nil {
		t.Fatalf("DeliverInterAgentMail: %v", err)
	}
	if delivered.GetCommitted() == nil {
		t.Fatalf("delivery outcome = %+v; want committed", delivered)
	}
	childScope := bridgeAPIScope("sesn_bridge_inter_agent", childThreadID, "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	runtimeInputID := completionRuntimeInputID(deliveryID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.ID("default"),
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    "inter-agent-vertical",
		MaxJobs:       1,
		LeaseDuration: time.Minute,
		Now:           now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease resolved inter-agent wake = %#v/%v; want one", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode resolved inter-agent wake: %v", err)
	}
	if job.RuntimeInputID != runtimeInputID ||
		job.SessionThreadID != childThreadID ||
		job.InputKind != "agent_mail" {
		t.Fatalf("resolved inter-agent job = %#v; want exact child mail wake", job)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.AcceptAgentMail == nil || plan.StaleAccepted {
		t.Fatalf("PrepareRuntimeCommand inter-agent delivery = %#v/%v; want live Runtime command", plan, err)
	}
	if plan.AcceptAgentMail.GetContent() != content || plan.AcceptAgentMail.GetDeliveryId() != deliveryID {
		t.Fatalf("prepared inter-agent command = %#v; want exact stored mail content and delivery identity", plan.AcceptAgentMail)
	}
	if _, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), job, plan.AttemptedBinding); err != nil {
		t.Fatalf("MarkRuntimeInputAccepted inter-agent delivery: %v", err)
	}
	if acknowledged, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.ID("default"),
		JobID:       leased[0].ID,
		LeaseToken:  leased[0].LeaseToken,
		Now:         now.Add(3 * time.Second),
	}); err != nil || !acknowledged {
		t.Fatalf("ACK resolved inter-agent wake = %v/%v; want true/nil", acknowledged, err)
	}
	request := &bridgev1.CommitInputsRequest{
		Scope: childScope, RuntimeInputId: runtimeInputID,
	}

	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs inter_agent_message: %v", err)
	}
	if response.GetCommitted() == nil {
		t.Fatalf("inter-agent commit outcome = %#v; want committed", response)
	}
	replay, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs inter-agent replay: %v", err)
	}
	if replay.GetCommitted() == nil {
		t.Fatalf("inter-agent replay outcome = %#v; want committed", replay)
	}
	loadResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: childScope,
	})
	if err != nil {
		t.Fatalf("LoadContext inter-agent message: %v", err)
	}
	var loaded bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
		t.Fatalf("decode inter-agent context: %v", err)
	}
	if len(loaded.ContextEntries) != 1 || loaded.ContextEntries[0].ContextKind != "user" || len(loaded.ContextEntries[0].Parts) != 1 ||
		testJSONPathString(t, string(loaded.ContextEntries[0].Parts[0]), "type") != "text" {
		t.Fatalf("loaded inter-agent context = %s; want one user text entry", loadResponse.GetContextJson())
	}
	var receivedEventID string
	var receivedVisibility string
	var receivedSessionVisible bool
	var receivedPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, visibility, session_visible, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_child'
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $1`, deliveryID).Scan(&receivedEventID, &receivedVisibility, &receivedSessionVisible, &receivedPayloadJSON); err != nil {
		t.Fatalf("read received event: %v", err)
	}
	if receivedVisibility != "public" || !receivedSessionVisible ||
		testJSONPathString(t, receivedPayloadJSON, "source_thread_id") != "thr_bridge_inter_agent_parent" ||
		testJSONPathString(t, receivedPayloadJSON, "source_tool_use_event_id") != sourceToolUseEventID {
		t.Fatalf("received event = visibility %s sessionVisible %v payload %s; want public parent attribution", receivedVisibility, receivedSessionVisible, receivedPayloadJSON)
	}
	if !strings.Contains(receivedPayloadJSON, `"source_task_name":null`) {
		t.Fatalf("received event payload = %s; want null callable name for primary source", receivedPayloadJSON)
	}
	assertDurableInterAgentPublicContent(t, receivedPayloadJSON, content)
	var sentPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_parent'
		    AND type = 'agent.thread_message_sent'
		    AND payload_json::jsonb ->> 'delivery_id' = $1`, deliveryID).Scan(&sentPayloadJSON); err != nil {
		t.Fatalf("read sent event: %v", err)
	}
	if testJSONPathString(t, sentPayloadJSON, "target_thread_id") != childThreadID ||
		testJSONPathString(t, sentPayloadJSON, "target_task_name") != "task_"+childThreadID {
		t.Fatalf("sent event payload = %s; want target child ID and callable task_name", sentPayloadJSON)
	}
	assertDurableInterAgentPublicContent(t, sentPayloadJSON, content)
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_child'
		    AND kind = 'user'
		    AND source_event_id = $1`,
		receivedEventID,
	).Scan(&messageCount); err != nil {
		t.Fatalf("read received projection count: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("received message projections = %d; want exactly one", messageCount)
	}
	var streamChangeCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		receivedEventID,
	).Scan(&streamChangeCount); err != nil {
		t.Fatalf("read received stream change count: %v", err)
	}
	if streamChangeCount != 2 {
		t.Fatalf("received stream changes = %d; want admission and processing revisions", streamChangeCount)
	}

}

func TestPostgreSQLBridgeAPIStoreCompletionCommitReplayProjectsOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_completion_push_first"
		mainID    = "thr_bridge_completion_push_first_main"
		childID   = "thr_bridge_completion_push_first_child"
		delivery  = "delivery_bridge_completion_push_first"
		bindingID = "bind_bridge_completion_push_first"
		podUID    = "pod_bridge_completion_push_first"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	messageJSON := bridgePublicMessageJSONForTest(t, completionMailEnvelope("main", "task_"+childID, "push-first body"))
	request := bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		completionRuntimeInputID(delivery),
		delivery,
		childID,
		"sevt_bridge_completion_push_first_spawn",
		messageJSON,
	)
	committed, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs completion: %v", err)
	}
	if committed.GetCommitted() == nil {
		t.Fatalf("completion outcome = %#v; want committed", committed)
	}

	replayed, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs completion replay: %v", err)
	}
	if replayed.GetCommitted() == nil {
		t.Fatalf("completion replay outcome = %#v; want committed", replayed)
	}

	var receiptCount, projectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*) FROM session_events
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND type='agent.thread_message_received'
			    AND payload_json::jsonb ->> 'delivery_id'=$3),
			(SELECT count(*) FROM session_messages
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND kind='user' AND data_json LIKE '%Message Type: FINAL_ANSWER%')`,
		sessionID,
		mainID,
		delivery,
	).Scan(&receiptCount, &projectionCount); err != nil {
		t.Fatalf("read push-first completion evidence: %v", err)
	}
	if receiptCount != 1 || projectionCount != 1 {
		t.Fatalf("push-first receipt/projection = %d/%d; want 1/1", receiptCount, projectionCount)
	}
}

func TestPostgreSQLBridgeAPIStoreConcurrentCompletionCommitCreatesOneReceipt(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_completion_race"
		mainID    = "thr_bridge_completion_race_main"
		childID   = "thr_bridge_completion_race_child"
		delivery  = "delivery_bridge_completion_race"
		bindingID = "bind_bridge_completion_race"
		podUID    = "pod_bridge_completion_race"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	messageJSON := bridgePublicMessageJSONForTest(t, completionMailEnvelope("main", "task_"+childID, "racing completion"))
	request := bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		completionRuntimeInputID(delivery),
		delivery,
		childID,
		"sevt_bridge_completion_race_spawn",
		messageJSON,
	)

	type result struct {
		response *bridgev1.CommitInputsResponse
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			candidate := proto.Clone(request).(*bridgev1.CommitInputsRequest)
			response, err := store.CommitInputs(context.Background(), candidate)
			results <- result{response: response, err: err}
		}()
	}
	close(start)

	committedCount := 0
	for range 2 {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("concurrent completion receipt: %v", outcome.err)
		}
		if outcome.response.GetCommitted() != nil {
			committedCount++
		}
	}
	if committedCount != 2 {
		t.Fatalf("concurrent completion outcomes = committed:%d; want two committed views of one durable result", committedCount)
	}

	var receiptCount, projectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*) FROM session_events
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND type='agent.thread_message_received'
			    AND payload_json::jsonb ->> 'delivery_id'=$3),
			(SELECT count(*) FROM session_messages
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND kind='user' AND data_json LIKE '%Message Type: FINAL_ANSWER%')`,
		sessionID,
		mainID,
		delivery,
	).Scan(&receiptCount, &projectionCount); err != nil {
		t.Fatalf("read concurrent completion receipt: %v", err)
	}
	if receiptCount != 1 || projectionCount > 1 {
		t.Fatalf("concurrent completion receipt/projection = %d/%d; want 1 and at most 1", receiptCount, projectionCount)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsCompletionMailOnMainExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_bridge_completion_main"
		mainThreadID = "thr_bridge_completion_main"
		childID      = "thr_bridge_completion_child"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_completion_main", 1, "pod_uid_completion_main")

	messageJSON := bridgePublicMessageJSONForTest(t, completionMailEnvelope("main", "task_"+childID, "done"))
	request := bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope(sessionID, mainThreadID, "bind_bridge_completion_main", 1, "pod_uid_completion_main"),
		"agent_mail:delivery_bridge_completion_main",
		"delivery_bridge_completion_main",
		childID,
		"sevt_bridge_completion_spawn",
		messageJSON,
	)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	first, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs completion mail on main: %v", err)
	}
	replay, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs completion mail replay: %v", err)
	}
	if first.GetCommitted() == nil || replay.GetCommitted() == nil {
		t.Fatalf("completion mail outcomes = %#v/%#v; want committed replay", first, replay)
	}
	var receivedCount, messageCount int
	var projectedDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		    AND type='agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id'='delivery_bridge_completion_main'`,
		sessionID, mainThreadID).Scan(&receivedCount); err != nil {
		t.Fatalf("count main completion receipts: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='user'
		    AND data_json LIKE '%Message Type: FINAL_ANSWER%'`,
		sessionID, mainThreadID).Scan(&messageCount); err != nil {
		t.Fatalf("count main completion messages: %v", err)
	}
	if receivedCount != 1 || messageCount != 1 {
		t.Fatalf("main completion receipt/message count = %d/%d; want 1/1", receivedCount, messageCount)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='user'
		    AND data_json LIKE '%Message Type: FINAL_ANSWER%'`,
		sessionID, mainThreadID,
	).Scan(&projectedDataJSON); err != nil {
		t.Fatalf("read main completion context: %v", err)
	}
	var projected struct {
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(projectedDataJSON), &projected); err != nil || len(projected.Parts) != 1 ||
		projected.Parts[0].Type != "text" || !strings.Contains(projected.Parts[0].Text, "Message Type: FINAL_ANSWER") ||
		strings.Contains(projectedDataJSON, `"role"`) || strings.Contains(projectedDataJSON, `"origin"`) {
		t.Fatalf("main completion context = %s; want one narrow text part", projectedDataJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsReviewerAndRejectionDrafts(t *testing.T) {
	t.Run("reviewer input", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID  = "sesn_bridge_reviewer_input"
			mainID     = "thr_bridge_reviewer_input_main"
			reviewerID = "thr_bridge_reviewer_input"
			reviewID   = "arvw_bridge_reviewer_input"
			bindingID  = "bind_bridge_reviewer_input"
			podUID     = "pod_bridge_reviewer_input"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewerID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.RuntimeBindingTokenHMACKey = []byte("reviewer-context-test-key-32byte")
		var rejectionLogs bytes.Buffer
		store.Logger = slog.New(slog.NewJSONHandler(&rejectionLogs, nil))
		admitted, err := store.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
			Scope: bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID), ReviewerThreadId: reviewerID, ReviewId: reviewID,
		})
		if err != nil || admitted.GetCommitted().GetRuntimeInputId() == "" {
			t.Fatalf("AdmitApprovalReviewInput = %#v/%v; want committed custody", admitted, err)
		}
		inputID := admitted.GetCommitted().GetRuntimeInputId()
		admissionReplay, err := store.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
			Scope: bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID), ReviewerThreadId: reviewerID, ReviewId: reviewID,
		})
		if err != nil || admissionReplay.GetDuplicate().GetRuntimeInputId() != inputID {
			t.Fatalf("approval review admission replay = %#v/%v; want duplicate %q", admissionReplay, err, inputID)
		}
		request := &bridgev1.CommitInputsRequest{
			Scope: bridgeAPIScope(sessionID, reviewerID, bindingID, 1, podUID), RuntimeInputId: inputID,
			ApprovalReviewText: []string{"review this action"},
		}
		response, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs reviewer input: %v", err)
		}
		if response.GetCommitted() == nil || response.GetCommitted().GetInterrupt() != nil ||
			len(response.GetCommitted().GetContext().GetAssignedContextSequences()) != 1 {
			t.Fatalf("reviewer input outcome = %#v; want one assigned context sequence", response)
		}

		var eventID string
		var eventType string
		var payloadJSON string
		var visibility string
		var sessionVisible bool
		var processed bool
		if err := admin.QueryRowContext(context.Background(),
			`SELECT event_id, type, payload_json, visibility, session_visible, processed_at IS NOT NULL
			   FROM session_events
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND session_thread_id = $2
			    AND type = 'approval_review.input'`,
			sessionID,
			reviewerID,
		).Scan(&eventID, &eventType, &payloadJSON, &visibility, &sessionVisible, &processed); err != nil {
			t.Fatalf("read reviewer input event: %v", err)
		}
		if eventType != "approval_review.input" || visibility != "internal" || sessionVisible || !processed {
			t.Fatalf("reviewer input event = type %q visibility %q session_visible %v processed %v; want internal processed approval_review.input", eventType, visibility, sessionVisible, processed)
		}
		if got := testJSONPathString(t, payloadJSON, "runtime_input_id"); got != inputID {
			t.Fatalf("reviewer input runtime_input_id = %q; want %q", got, inputID)
		}
		var storedMessageJSON string
		if err := admin.QueryRowContext(context.Background(),
			`SELECT data_json
			   FROM session_messages
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND session_thread_id = $2
			    AND source_event_id = $3`,
			sessionID,
			reviewerID,
			eventID,
		).Scan(&storedMessageJSON); err != nil {
			t.Fatalf("read reviewer input message: %v", err)
		}
		assertReviewerInputDeclarationValues(t, storedMessageJSON)

		loadResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
			Scope: request.GetScope(),
		})
		if err != nil {
			t.Fatalf("LoadContext reviewer input: %v", err)
		}
		var loaded bridgeLoadContextPayload
		if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
			t.Fatalf("decode reviewer input context: %v", err)
		}
		if len(loaded.ContextEntries) != 1 || loaded.ContextEntries[0].ContextKind != "user" {
			t.Fatalf("loaded reviewer context entries = %#v; want one user entry", loaded.ContextEntries)
		}
		loadedEntry, err := json.Marshal(loaded.ContextEntries[0])
		if err != nil {
			t.Fatalf("encode loaded reviewer context: %v", err)
		}
		assertReviewerInputDeclarationValues(t, string(loadedEntry))

		replay, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("replay CommitInputs reviewer input: %v", err)
		}
		if replay.GetCommitted() == nil || !proto.Equal(
			response.GetCommitted().GetContext(),
			replay.GetCommitted().GetContext(),
		) {
			t.Fatalf("reviewer input lost-ACK replay = %#v/%#v; want identical durable delta", response, replay)
		}
		rejectionLogs.Reset()
		wrongCaller := internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
			ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
			KubernetesPodUID: "pod_bridge_reviewer_input_other",
		})
		if _, err := store.CommitInputs(wrongCaller, request); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("reviewer input wrong-caller err = %v; want PermissionDenied", err)
		}
		logText := rejectionLogs.String()
		if strings.Count(logText, `"event.kind":"runtime_declaration_rejected"`) != 1 ||
			!strings.Contains(logText, `"rejection.kind":"authorization"`) || strings.Contains(logText, "approve") {
			t.Fatalf("reviewer authorization rejection log = %q; want one safe authorization record", logText)
		}
		rejectionLogs.Reset()
		wrongTarget := proto.Clone(request).(*bridgev1.CommitInputsRequest)
		wrongTarget.Scope = bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
		wrongTarget.RuntimeInputId = "rin_bridge_reviewer_wrong_target"
		if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
			workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,
			binding_id,binding_generation,target_pod_uid,created_at,updated_at
		) VALUES ('default',$1,$2,$3,'approval_review','[]','accepted',$4,1,$5,now(),now())`,
			sessionID, mainID, wrongTarget.GetRuntimeInputId(), bindingID, podUID,
		); err != nil {
			t.Fatalf("seed invalid reviewer Inbox target: %v", err)
		}
		if _, err := store.CommitInputs(context.Background(), wrongTarget); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("reviewer input wrong-target err = %v; want FailedPrecondition", err)
		}
		logText = rejectionLogs.String()
		if strings.Count(logText, `"event.kind":"runtime_declaration_rejected"`) != 1 ||
			!strings.Contains(logText, `"rejection.kind":"lineage"`) ||
			!strings.Contains(logText, `"thread.role":"main"`) || strings.Contains(logText, "approve") {
			t.Fatalf("reviewer lineage rejection log = %q; want one safe lineage record", logText)
		}
		var eventCount int
		var messageCount int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM session_events WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND event_id = $3`,
			sessionID,
			reviewerID,
			eventID,
		).Scan(&eventCount); err != nil {
			t.Fatalf("count reviewer input events: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM session_messages WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND source_event_id = $3`,
			sessionID,
			reviewerID,
			eventID,
		).Scan(&messageCount); err != nil {
			t.Fatalf("count reviewer input messages: %v", err)
		}
		if eventCount != 1 || messageCount != 1 {
			t.Fatalf("reviewer input durable rows = %d events/%d messages; want 1/1", eventCount, messageCount)
		}
	})

	t.Run("rejection", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_bridge_rejection"
			threadID  = "thr_bridge_rejection"
			eventID1  = "evt_bridge_rejection_1"
			eventID2  = "evt_bridge_rejection_2"
			inputID   = "rin_bridge_rejection"
			bindingID = "bind_bridge_rejection"
			podUID    = "pod_bridge_rejection"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID1, 1, "user.message", `{"type":"user.message","content":[{"type":"text","text":"oversized one"}]}`)
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID2, 2, "user.message", `{"type":"user.message","content":[{"type":"text","text":"oversized two"}]}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, inputID, "rejection", `["`+eventID1+`","`+eventID2+`"]`, "accepted", bindingID, podUID, 1, 2)
		request := &bridgev1.CommitInputsRequest{
			Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
		}
		response, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs rejection: %v", err)
		}
		if response.GetCommitted() == nil || len(response.GetCommitted().GetContext().GetAssignedContextSequences()) != 1 {
			t.Fatalf("CommitInputs batched rejection outcome = %+v; want one canonical assistant projection", response)
		}
		var processedEvents, projectedMessages int
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND event_id IN ($2,$3) AND processed_at IS NOT NULL),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$4 AND kind='assistant')`,
			sessionID, eventID1, eventID2, threadID,
		).Scan(&processedEvents, &projectedMessages); err != nil {
			t.Fatalf("read batched rejection result: %v", err)
		}
		if processedEvents != 2 || projectedMessages != 1 {
			t.Fatalf("batched rejection durable result = %d processed events/%d context rows; want 2/1", processedEvents, projectedMessages)
		}
	})
}

func assertReviewerInputDeclarationValues(t *testing.T, raw string) {
	t.Helper()
	var entry struct {
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("decode reviewer input context: %v", err)
	}
	if len(entry.Parts) != 1 || entry.Parts[0].Type != "text" || entry.Parts[0].Text != "review this action" {
		t.Fatalf("reviewer input context = %+v; want byte-preserved text", entry)
	}
}

func TestAdmitAgentMailDeliveryRejectsApprovalReviewerTarget(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_bridge_completion_reviewer_receipt"
		mainID     = "thr_bridge_completion_reviewer_receipt_main"
		sourceID   = "thr_bridge_completion_reviewer_receipt_source"
		reviewerID = "thr_bridge_completion_reviewer_receipt_target"
		bindingID  = "bind_bridge_completion_reviewer_receipt"
		podUID     = "pod_bridge_completion_reviewer_receipt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, sourceID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewerID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	client := dbconnect.NewClientForTesting(runtime)
	err := client.WithWorkspaceTx(context.Background(), "default", "test.reject_reviewer_agent_mail", func(tx *dbconnect.Tx) error {
		binding, err := readRuntimeBindingForDeliveryTx(context.Background(), tx, "default", sessionID)
		if err != nil {
			return err
		}
		_, err = admitAgentMailDeliveryTx(
			context.Background(),
			tx,
			bridgeAPIScope(sessionID, reviewerID, bindingID, 1, podUID),
			storedAgentMailEnvelope{
				DeliveryID:           "delivery_reviewer_rejected",
				SourceThreadID:       sourceID,
				TargetThreadID:       reviewerID,
				SourceToolUseEventID: "sevt_reviewer_rejected_spawn",
				Content:              "completion",
				PublicMessageJSON:    json.RawMessage(bridgePublicMessageJSONForTest(t, "completion")),
			},
			binding,
			time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC),
		)
		return err
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("admit completion mail on reviewer err = %v; want FailedPrecondition", err)
	}
}
