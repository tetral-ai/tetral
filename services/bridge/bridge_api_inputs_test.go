package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/eventwire"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge inputs protocol-family boundary.

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
	runtimeLocalID := stableRuntimeID(
		"runtime_message_draft",
		"default",
		"sesn_bridge_commit",
		"thr_bridge_commit",
		"messages",
		"rin_bridge_commit",
		"user_input",
		"0",
	)
	request := &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit", "thr_bridge_commit", "bind_bridge_commit", 1, "pod_uid_commit"),
		RuntimeInputId: "rin_bridge_commit",
		EventIds:       []string{"evt_bridge_commit"},
		SequenceFrom:   1,
		SequenceTo:     1,
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  runtimeLocalID,
			SourceKind:      "messages",
			SourceId:        "rin_bridge_commit",
			SourceEventId:   "evt_bridge_commit",
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT,
			MessageInfoJson: `{"role":"user","origin":"user","status":"completed"}`,
			Parts: []*bridgev1.RuntimePartDraft{{
				RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", runtimeLocalID, "text", "0"),
				PartKind:           "text",
				PartJson:           `{"type":"text","text":"hello","truncated":false,"status":"completed"}`,
			}},
		}},
	}
	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	var storedSourceKind, storedDigest, storedReceipt string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT source_kind, declaration_digest, receipt_json
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_commit'
		    AND session_thread_id = 'thr_bridge_commit'
		    AND operation = 'commit_inputs'
		    AND idempotency_key = 'rin_bridge_commit'`,
	).Scan(&storedSourceKind, &storedDigest, &storedReceipt); err != nil {
		t.Fatalf("read stored declaration receipt: %v", err)
	}
	if storedSourceKind != "messages" || storedDigest == "" || storedReceipt == "" {
		t.Fatalf("stored declaration source=%q digest=%q receipt=%q; want complete identity and receipt", storedSourceKind, storedDigest, storedReceipt)
	}
	if got := response.GetDeclaration().GetReceipts(); len(got) != 1 ||
		len(got[0].GetEvents()) != 1 ||
		len(got[0].GetMessages()) != 1 ||
		len(got[0].GetMessages()[0].GetParts()) != 1 {
		t.Fatalf("declaration receipt = %#v; want one event/message/part stamp", response.GetDeclaration())
	}
	if response.GetDeclaration().GetApplicationDisposition() != bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY ||
		response.GetDeclaration().GetObservedBindingId() != "bind_bridge_commit" ||
		response.GetDeclaration().GetObservedBindingGeneration() != 1 {
		t.Fatalf("initial declaration custody = %#v; want current binding 1", response.GetDeclaration())
	}
	loadResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          request.GetScope(),
		RuntimeInputId: "rin_bridge_commit_cold_load",
	})
	if err != nil {
		t.Fatalf("LoadContext committed input: %v", err)
	}
	var loaded bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
		t.Fatalf("decode committed input context: %v", err)
	}
	if len(loaded.Messages) != 1 {
		t.Fatalf("loaded messages = %d; want one committed input", len(loaded.Messages))
	}
	var loadedMessage struct {
		ID            string `json:"id"`
		Sequence      int64  `json:"sequence"`
		OwningEventID string `json:"owningEventId"`
		EventSequence int64  `json:"eventSequence"`
		ProviderID    string `json:"providerId"`
		ModelID       string `json:"modelId"`
		Parts         []struct {
			ID        string `json:"id"`
			MessageID string `json:"messageId"`
			Sequence  int64  `json:"sequence"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(loaded.Messages[0], &loadedMessage); err != nil {
		t.Fatalf("decode loaded committed input: %v", err)
	}
	messageStamp := response.GetDeclaration().GetReceipts()[0].GetMessages()[0]
	if loadedMessage.ID != messageStamp.GetMessageId() ||
		loadedMessage.Sequence != messageStamp.GetMessageSequence() ||
		loadedMessage.OwningEventID != messageStamp.GetOwningEventId() ||
		loadedMessage.EventSequence != 1 ||
		loadedMessage.ProviderID != "" ||
		loadedMessage.ModelID != "" ||
		len(loadedMessage.Parts) != 1 ||
		loadedMessage.Parts[0].ID != messageStamp.GetParts()[0].GetPartId() ||
		loadedMessage.Parts[0].MessageID != messageStamp.GetMessageId() ||
		loadedMessage.Parts[0].Sequence != messageStamp.GetParts()[0].GetPartSequence() {
		t.Fatalf("loaded committed input = %#v; want receipt-stamped durable message", loadedMessage)
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
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", response.GetAck().GetStatus())
	}
	if response.GetDeclaration().GetApplicationDisposition() != bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY ||
		response.GetDeclaration().GetObservedBindingId() != "bind_bridge_commit_replacement" ||
		response.GetDeclaration().GetObservedBindingGeneration() != 2 {
		t.Fatalf("replay declaration custody = %#v; want freshly observed replacement binding", response.GetDeclaration())
	}
	logText := declarationLogs.String()
	for _, fragment := range []string{
		`"event.kind":"runtime_declaration_committed"`,
		`"event.kind":"runtime_declaration_replayed"`,
		`"declaration.source.kind":"messages"`,
		`"declaration.source.id":"rin_bridge_commit"`,
		`"receipt.application_disposition":"current_custody"`,
		`"receipt.application_disposition":"stale_custody"`,
		`"binding.id":"bind_bridge_commit_replacement"`,
	} {
		if !strings.Contains(logText, fragment) {
			t.Fatalf("declaration logs missing %s: %s", fragment, logText)
		}
	}
	if strings.Contains(logText, `"text":"hello"`) {
		t.Fatalf("declaration logs contain model-visible input: %s", logText)
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
	assertBridgeRuntimeUserProjection(t, messageDataJSON, "sesn_bridge_commit", "hello")
	if eventRevision != 2 || streamChangeCount != 2 || maxStreamRevision != 2 || latestStreamPosition != maxStreamPosition || latestStreamPosition <= initialStreamPosition {
		t.Fatalf("processed stream revision = eventRev %d changeCount %d maxRev %d latest %d initial %d maxPos %d; want same-event revision update only once",
			eventRevision, streamChangeCount, maxStreamRevision, latestStreamPosition, initialStreamPosition, maxStreamPosition)
	}

	conflict := proto.Clone(request).(*bridgev1.CommitInputsRequest)
	conflict.Drafts[0].Parts[0].PartJson = `{"type":"text","text":"different","truncated":false,"status":"completed"}`
	if _, err := store.CommitInputs(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting CommitInputs err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsReturnsFirstTurnFileAttachmentDelta(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_commit_media"
		threadID       = "thr_bridge_commit_media"
		runtimeInputID = "rin_bridge_commit_media"
		eventID        = "evt_bridge_commit_media"
		fileID         = "file_bridge_commit_media"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_commit_media", 1, "pod_uid_commit_media")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message",
		`{"content":[{"type":"text","text":"inspect"},{"type":"image","source":{"type":"file","file_id":"file_bridge_commit_media"}}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "messages",
		`["evt_bridge_commit_media"]`, "accepted", "bind_bridge_commit_media", "pod_uid_commit_media", 1, 1)
	attachmentStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, attachmentStore, fileID, "image.png", "image/png", "image")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, "bind_bridge_commit_media", 1, "pod_uid_commit_media"),
		RuntimeInputId: runtimeInputID,
		InputKind:      "messages",
		EventIds:       []string{eventID},
		SequenceFrom:   1,
		SequenceTo:     1,
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeUserInputDraftForTest("default", sessionID, threadID, runtimeInputID, eventID, "inspect"),
		},
	})
	if err != nil {
		t.Fatalf("CommitInputs media: %v", err)
	}
	delta := response.GetDeclaration().GetReceipts()[0].GetPendingAttachmentDeltaJson()
	if len(delta) != 1 {
		t.Fatalf("attachment delta = %#v; want one first-turn file reference", delta)
	}
	var attachment bridgeLoadContextPendingAttachment
	if err := json.Unmarshal([]byte(delta[0]), &attachment); err != nil {
		t.Fatalf("decode attachment delta: %v", err)
	}
	if attachment.Origin.FileBacked == nil ||
		attachment.Origin.FileBacked.SourceEventID != eventID ||
		attachment.Origin.FileBacked.FileID != fileID ||
		attachment.Mime != "image/png" ||
		attachment.Filename != "image.png" {
		t.Fatalf("attachment delta = %#v; want committed media reference and metadata", attachment)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsRejectsMissingAcceptedMessageSnapshot(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_snapshot", "thr_bridge_commit_snapshot")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_snapshot", "bind_bridge_commit_snapshot", 1, "pod_uid_commit_snapshot")
	seedBridgeAPIRuntimeInput(t, admin, "default", "sesn_bridge_commit_snapshot", "thr_bridge_commit_snapshot", "rin_bridge_commit_snapshot", "bind_bridge_commit_snapshot", "pod_uid_commit_snapshot", "evt_bridge_commit_snapshot")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	_, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit_snapshot", "thr_bridge_commit_snapshot", "bind_bridge_commit_snapshot", 1, "pod_uid_commit_snapshot"),
		RuntimeInputId: "rin_bridge_commit_snapshot",
		EventIds:       []string{"evt_bridge_commit_snapshot"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CommitInputs missing snapshot err = %v; want InvalidArgument", err)
	}
	var operationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_commit_snapshot' AND operation = 'commit_inputs'`).Scan(&operationCount); err != nil {
		t.Fatalf("count commit operations: %v", err)
	}
	if operationCount != 0 {
		t.Fatalf("commit operations = %d; want no side effect", operationCount)
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
		EventIds:       []string{"evt_bridge_commit_fence"},
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeUserInputDraftForTest("default", "sesn_bridge_commit_fence", "thr_bridge_commit_fence", "rin_bridge_commit_fence", "evt_bridge_commit_fence", "hello"),
		},
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

func TestPostgreSQLBridgeAPIStoreCommitInputsRejectsInboxIdentityConflictWithoutDurableAdvance(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_payload_conflict", "bind_bridge_commit_payload_conflict", 1, "pod_uid_commit_payload_conflict")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict", "evt_bridge_commit_expected", 1, "user.message", `{"content":[{"type":"text","text":"expected"}]}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict", "evt_bridge_commit_other", 2, "user.message", `{"content":[{"type":"text","text":"other"}]}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict", "rin_bridge_commit_payload_conflict", "messages", `["evt_bridge_commit_expected"]`, "accepted", "bind_bridge_commit_payload_conflict", "pod_uid_commit_payload_conflict", 1, 1)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict", "bind_bridge_commit_payload_conflict", 1, "pod_uid_commit_payload_conflict"),
		RuntimeInputId: "rin_bridge_commit_payload_conflict",
		InputKind:      "messages",
		EventIds:       []string{"evt_bridge_commit_other"},
		SequenceFrom:   2,
		SequenceTo:     2,
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeUserInputDraftForTest("default", "sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict", "rin_bridge_commit_payload_conflict", "evt_bridge_commit_other", "other"),
		},
	}
	if _, err := store.CommitInputs(context.Background(), request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CommitInputs conflicting inbox identity err = %v; want AlreadyExists", err)
	}
	assertCommitInputsConflictDidNotAdvance(t, admin, "sesn_bridge_commit_payload_conflict", "rin_bridge_commit_payload_conflict", []string{"evt_bridge_commit_expected", "evt_bridge_commit_other"})

	request.EventIds = []string{"evt_bridge_commit_expected"}
	request.SequenceFrom = 1
	request.SequenceTo = 1
	request.InputKind = "interrupt_control"
	request.Drafts = nil
	if _, err := store.CommitInputs(context.Background(), request); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CommitInputs conflicting inbox kind err = %v; want AlreadyExists", err)
	}
	assertCommitInputsConflictDidNotAdvance(t, admin, "sesn_bridge_commit_payload_conflict", "rin_bridge_commit_payload_conflict", []string{"evt_bridge_commit_expected", "evt_bridge_commit_other"})
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
		InputKind:      "messages",
		EventIds:       []string{"evt_bridge_confirmation_as_message"},
		SequenceFrom:   2,
		SequenceTo:     2,
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeUserInputDraftForTest("default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "rin_bridge_confirmation_as_message", "evt_bridge_confirmation_as_message", "allow"),
		},
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

func TestPostgreSQLBridgeAPIStoreCommitInputsSettlesControlInputs(t *testing.T) {
	t.Run("interrupt_control", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_interrupt", "thr_bridge_commit_interrupt")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_interrupt", "bind_bridge_commit_interrupt", 1, "pod_uid_commit_interrupt")
		seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_commit_interrupt", "thr_bridge_commit_interrupt", "evt_bridge_interrupted_tool", 1)
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_interrupt", "thr_bridge_commit_interrupt", "evt_bridge_commit_interrupt", 3, "user.interrupt", `{"type":"user.interrupt"}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_interrupt", "thr_bridge_commit_interrupt", "rin_bridge_commit_interrupt", "interrupt_control", `["evt_bridge_commit_interrupt"]`, "accepted", "bind_bridge_commit_interrupt", "pod_uid_commit_interrupt", 3, 3)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		cancellationLocalID := stableRuntimeID(
			"runtime_message_draft",
			"default",
			"sesn_bridge_commit_interrupt",
			"thr_bridge_commit_interrupt",
			"interrupt_control",
			"rin_bridge_commit_interrupt",
			"cancellation",
			"0",
		)
		request := &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope("sesn_bridge_commit_interrupt", "thr_bridge_commit_interrupt", "bind_bridge_commit_interrupt", 1, "pod_uid_commit_interrupt"),
			RuntimeInputId: "rin_bridge_commit_interrupt",
			InputKind:      "interrupt_control",
			EventIds:       []string{"evt_bridge_commit_interrupt"},
			SequenceFrom:   3,
			SequenceTo:     3,
			Drafts: []*bridgev1.RuntimeMessageDraft{{
				RuntimeLocalId: cancellationLocalID,
				SourceKind:     "interrupt_control",
				SourceId:       "rin_bridge_commit_interrupt",
				SourceEventId:  "evt_bridge_commit_interrupt",
				DraftKind:      bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_CANCELLATION,
				MessageInfoJson: `{"role":"assistant","origin":"agent","status":"completed",
					"toolUseEventId":"evt_bridge_interrupted_tool"}`,
				Parts: []*bridgev1.RuntimePartDraft{{
					RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", cancellationLocalID, "text", "0"),
					PartKind:           "text",
					PartJson:           `{"type":"text","text":"The pending tool call was cancelled.","status":"completed"}`,
				}},
			}},
			PendingToolCancellations: []*bridgev1.PendingToolCancellationDraft{{
				ToolUseEventId: "evt_bridge_interrupted_tool",
				RuntimeLocalId: cancellationLocalID,
			}},
		}
		response, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs interrupt_control: %v", err)
		}
		replay, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs interrupt_control replay: %v", err)
		}

		var inboxStatus string
		var processedAt sql.NullString
		var messageCount int
		var pendingStatus string
		var pendingResultEventID sql.NullString
		var terminalResultCount int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_commit_interrupt'`).Scan(&inboxStatus); err != nil {
			t.Fatalf("read interrupt inbox: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_commit_interrupt'`).Scan(&processedAt); err != nil {
			t.Fatalf("read interrupt processed_at: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND source_event_id = 'evt_bridge_commit_interrupt'`).Scan(&messageCount); err != nil {
			t.Fatalf("read interrupt message count: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, result_event_id FROM session_pending_tool_uses WHERE workspace_id = 'default' AND tool_use_event_id = 'evt_bridge_interrupted_tool'`).Scan(&pendingStatus, &pendingResultEventID); err != nil {
			t.Fatalf("read interrupted pending tool: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_commit_interrupt' AND type = 'agent.tool_result' AND payload_json::jsonb ->> 'tool_use_id' = 'evt_bridge_interrupted_tool'`).Scan(&terminalResultCount); err != nil {
			t.Fatalf("read interrupted terminal result count: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
			replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
			inboxStatus != "committed" || !processedAt.Valid || messageCount != 1 || pendingStatus != "cancelled" ||
			!pendingResultEventID.Valid || pendingResultEventID.String != "evt_bridge_commit_interrupt" || terminalResultCount != 0 ||
			len(response.GetDeclaration().GetReceipts()) != 1 ||
			len(response.GetDeclaration().GetReceipts()[0].GetPendingToolDeltaJson()) != 1 {
			t.Fatalf("interrupt commit ack=%s replay=%s inbox=%q processed=%v messages=%d pending=%q result=%v terminal=%d receipt=%#v; want one declared cancellation and no extra result event",
				response.GetAck().GetStatus(), replay.GetAck().GetStatus(), inboxStatus, processedAt.Valid, messageCount, pendingStatus, pendingResultEventID, terminalResultCount, response.GetDeclaration())
		}
	})

	t.Run("tool_confirmation", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_confirm", "thr_bridge_commit_confirm")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_confirm", "bind_bridge_commit_confirm", 1, "pod_uid_commit_confirm")
		seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_commit_confirm", "thr_bridge_commit_confirm", "evt_bridge_pending_tool", 1)
		setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_commit_confirm", "thr_bridge_commit_confirm", "evt_bridge_pending_tool", "resolving")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_confirm", "thr_bridge_commit_confirm", "evt_bridge_commit_confirm", 2, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"evt_bridge_pending_tool","result":"deny","deny_message":"not now"}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_confirm", "thr_bridge_commit_confirm", "rin_bridge_commit_confirm", "tool_confirmation", `["evt_bridge_commit_confirm"]`, "accepted", "bind_bridge_commit_confirm", "pod_uid_commit_confirm", 2, 2)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		request := &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope("sesn_bridge_commit_confirm", "thr_bridge_commit_confirm", "bind_bridge_commit_confirm", 1, "pod_uid_commit_confirm"),
			RuntimeInputId: "rin_bridge_commit_confirm",
			InputKind:      "tool_confirmation",
			EventIds:       []string{"evt_bridge_commit_confirm"},
			SequenceFrom:   2,
			SequenceTo:     2,
			Drafts: []*bridgev1.RuntimeMessageDraft{
				bridgeInputDraftForTest(
					"default",
					"sesn_bridge_commit_confirm",
					"thr_bridge_commit_confirm",
					"tool_confirmation",
					"rin_bridge_commit_confirm",
					"evt_bridge_commit_confirm",
					bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_APPROVAL_INPUT,
					"user",
					"Approval denied: not now",
				),
			},
		}
		response, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs tool_confirmation: %v", err)
		}
		replay, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs tool_confirmation replay: %v", err)
		}

		var inboxStatus string
		var processedAt sql.NullString
		var pendingStatus string
		var decision sql.NullString
		var denyMessage sql.NullString
		var resolvedAt sql.NullString
		var messageCount int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_commit_confirm'`).Scan(&inboxStatus); err != nil {
			t.Fatalf("read confirmation inbox: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_commit_confirm'`).Scan(&processedAt); err != nil {
			t.Fatalf("read confirmation processed_at: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, decision, deny_message, resolved_at
			   FROM session_pending_tool_uses
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_commit_confirm'
			    AND session_thread_id = 'thr_bridge_commit_confirm'
			    AND tool_use_event_id = 'evt_bridge_pending_tool'`).Scan(&pendingStatus, &decision, &denyMessage, &resolvedAt); err != nil {
			t.Fatalf("read pending approval: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND source_event_id = 'evt_bridge_commit_confirm'`).Scan(&messageCount); err != nil {
			t.Fatalf("read confirmation message count: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
			replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
			inboxStatus != "committed" || !processedAt.Valid || pendingStatus != "resolving" ||
			!decision.Valid || decision.String != "deny" || !denyMessage.Valid || denyMessage.String != "not now" ||
			resolvedAt.Valid || messageCount != 1 {
			t.Fatalf("confirmation commit ack=%s replay=%s inbox=%q processed=%v pending=%q decision=%v deny=%v resolved=%v messages=%d; want committed/duplicate/committed/processed/resolving/deny/unresolved/1",
				response.GetAck().GetStatus(), replay.GetAck().GetStatus(), inboxStatus, processedAt.Valid, pendingStatus, decision, denyMessage, resolvedAt.Valid, messageCount)
		}
	})

	t.Run("tool_confirmation cannot flip recorded decision", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_confirm_flip", "bind_bridge_commit_confirm_flip", 1, "pod_uid_commit_confirm_flip")
		seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "evt_bridge_pending_tool_flip", 1)
		setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "evt_bridge_pending_tool_flip", "resolving")
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "evt_bridge_commit_confirm_flip_deny", 2, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"evt_bridge_pending_tool_flip","result":"deny","deny_message":"first decision"}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "rin_bridge_commit_confirm_flip_deny", "tool_confirmation", `["evt_bridge_commit_confirm_flip_deny"]`, "accepted", "bind_bridge_commit_confirm_flip", "pod_uid_commit_confirm_flip", 2, 2)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		_, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope("sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "bind_bridge_commit_confirm_flip", 1, "pod_uid_commit_confirm_flip"),
			RuntimeInputId: "rin_bridge_commit_confirm_flip_deny",
			InputKind:      "tool_confirmation",
			EventIds:       []string{"evt_bridge_commit_confirm_flip_deny"},
			SequenceFrom:   2,
			SequenceTo:     2,
			Drafts: []*bridgev1.RuntimeMessageDraft{
				bridgeApprovalInputDraftForTest("default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "rin_bridge_commit_confirm_flip_deny", "evt_bridge_commit_confirm_flip_deny", "Approval denied: first decision"),
			},
		})
		if err != nil {
			t.Fatalf("CommitInputs initial confirmation: %v", err)
		}
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "evt_bridge_commit_confirm_flip_allow", 3, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"evt_bridge_pending_tool_flip","result":"allow"}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "rin_bridge_commit_confirm_flip_allow", "tool_confirmation", `["evt_bridge_commit_confirm_flip_allow"]`, "accepted", "bind_bridge_commit_confirm_flip", "pod_uid_commit_confirm_flip", 3, 3)

		_, err = store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope("sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "bind_bridge_commit_confirm_flip", 1, "pod_uid_commit_confirm_flip"),
			RuntimeInputId: "rin_bridge_commit_confirm_flip_allow",
			InputKind:      "tool_confirmation",
			EventIds:       []string{"evt_bridge_commit_confirm_flip_allow"},
			SequenceFrom:   3,
			SequenceTo:     3,
			Drafts: []*bridgev1.RuntimeMessageDraft{
				bridgeApprovalInputDraftForTest("default", "sesn_bridge_commit_confirm_flip", "thr_bridge_commit_confirm_flip", "rin_bridge_commit_confirm_flip_allow", "evt_bridge_commit_confirm_flip_allow", "Approval allowed"),
			},
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("flipped CommitInputs err = %v; want FailedPrecondition", err)
		}

		var pendingStatus string
		var decision sql.NullString
		var denyMessage sql.NullString
		var resolvedAt sql.NullString
		var allowInboxStatus string
		var allowProcessedAt sql.NullString
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, decision, deny_message, resolved_at
			   FROM session_pending_tool_uses
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_commit_confirm_flip'
			    AND session_thread_id = 'thr_bridge_commit_confirm_flip'
			    AND tool_use_event_id = 'evt_bridge_pending_tool_flip'`).Scan(&pendingStatus, &decision, &denyMessage, &resolvedAt); err != nil {
			t.Fatalf("read pending approval after flip attempt: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_commit_confirm_flip_allow'`).Scan(&allowInboxStatus); err != nil {
			t.Fatalf("read flipped confirmation inbox: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_commit_confirm_flip_allow'`).Scan(&allowProcessedAt); err != nil {
			t.Fatalf("read flipped confirmation processed_at: %v", err)
		}
		if pendingStatus != "resolving" || !decision.Valid || decision.String != "deny" ||
			!denyMessage.Valid || denyMessage.String != "first decision" || resolvedAt.Valid ||
			allowInboxStatus != "accepted" || allowProcessedAt.Valid {
			t.Fatalf("flip side effects pending=%q decision=%v deny=%v resolved=%v inbox=%q processed=%v; want original resolving deny and rollback",
				pendingStatus, decision, denyMessage, resolvedAt.Valid, allowInboxStatus, allowProcessedAt.Valid)
		}
	})

	t.Run("stale tool_confirmation fails closed", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_commit_stale_confirm", "thr_bridge_commit_stale_confirm")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_commit_stale_confirm", "bind_bridge_commit_stale_confirm", 1, "pod_uid_commit_stale_confirm")
		seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_commit_stale_confirm", "thr_bridge_commit_stale_confirm", "evt_bridge_stale_tool", 1)
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_pending_tool_uses
			    SET status = 'resolved',
			        resolved_at = '2026-01-01T00:00:01Z'
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_commit_stale_confirm'
			    AND session_thread_id = 'thr_bridge_commit_stale_confirm'
			    AND tool_use_event_id = 'evt_bridge_stale_tool'`); err != nil {
			t.Fatalf("mark pending row resolved: %v", err)
		}
		seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_commit_stale_confirm", "thr_bridge_commit_stale_confirm", "evt_bridge_commit_stale_confirm", 2, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"evt_bridge_stale_tool","result":"allow"}`)
		seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_commit_stale_confirm", "thr_bridge_commit_stale_confirm", "rin_bridge_commit_stale_confirm", "tool_confirmation", `["evt_bridge_commit_stale_confirm"]`, "accepted", "bind_bridge_commit_stale_confirm", "pod_uid_commit_stale_confirm", 2, 2)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		_, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope("sesn_bridge_commit_stale_confirm", "thr_bridge_commit_stale_confirm", "bind_bridge_commit_stale_confirm", 1, "pod_uid_commit_stale_confirm"),
			RuntimeInputId: "rin_bridge_commit_stale_confirm",
			InputKind:      "tool_confirmation",
			EventIds:       []string{"evt_bridge_commit_stale_confirm"},
			SequenceFrom:   2,
			SequenceTo:     2,
			Drafts: []*bridgev1.RuntimeMessageDraft{
				bridgeApprovalInputDraftForTest("default", "sesn_bridge_commit_stale_confirm", "thr_bridge_commit_stale_confirm", "rin_bridge_commit_stale_confirm", "evt_bridge_commit_stale_confirm", "Approval allowed"),
			},
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("stale CommitInputs err = %v; want FailedPrecondition", err)
		}

		var inboxStatus string
		var processedAt sql.NullString
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'rin_bridge_commit_stale_confirm'`).Scan(&inboxStatus); err != nil {
			t.Fatalf("read stale confirmation inbox: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(),
			`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = 'evt_bridge_commit_stale_confirm'`).Scan(&processedAt); err != nil {
			t.Fatalf("read stale confirmation processed_at: %v", err)
		}
		if inboxStatus != "accepted" || processedAt.Valid {
			t.Fatalf("stale confirmation side effects inbox=%q processed=%v; want accepted/unprocessed rollback", inboxStatus, processedAt.Valid)
		}
	})
}

func TestPostgreSQLBridgeAPIStoreCommitInputsRecordsGeneratedPendingApprovalDecision(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm")
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_generated_tool_use",
		ModelRequestId: "mreq_bridge_generated_tool_use",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"dangerous_tool","input":{"path":"README.md"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_generated_tool_use",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"tool-call-generated","toolName":"dangerous_tool","state":{"status":"running","input":{"value":{"path":"README.md"},"preview":"{\"path\":\"README.md\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent generated tool use: %v", err)
	}
	setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", toolUse.GetEventId(), "resolving")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "evt_bridge_generated_confirm", 2, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"`+toolUse.GetEventId()+`","result":"allow"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "rin_bridge_generated_confirm", "tool_confirmation", `["evt_bridge_generated_confirm"]`, "accepted", "bind_bridge_generated_confirm", "pod_uid_generated_confirm", 2, 2)

	response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm"),
		RuntimeInputId: "rin_bridge_generated_confirm",
		InputKind:      "tool_confirmation",
		EventIds:       []string{"evt_bridge_generated_confirm"},
		SequenceFrom:   2,
		SequenceTo:     2,
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeApprovalInputDraftForTest("default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "rin_bridge_generated_confirm", "evt_bridge_generated_confirm", "Approval allowed"),
		},
	})
	if err != nil {
		t.Fatalf("CommitInputs generated confirmation: %v", err)
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
		toolUse.GetEventId()).Scan(&pendingStatus, &decision, &resolvedAt); err != nil {
		t.Fatalf("read generated pending approval after confirmation: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		pendingStatus != "resolving" || !decision.Valid || decision.String != "allow" || resolvedAt.Valid {
		t.Fatalf("generated confirmation ack=%s pending=%q decision=%v resolved=%v; want committed/resolving/allow/unresolved",
			response.GetAck().GetStatus(), pendingStatus, decision, resolvedAt.Valid)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsInterAgentMessageExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_inter_agent", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", "evt_bridge_inter_agent_spawn", 1, "agent.tool_use", `{}`)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	parentScope := bridgeAPIScope("sesn_bridge_inter_agent", "thr_bridge_inter_agent_parent", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                   parentScope,
		ParentThreadId:          "thr_bridge_inter_agent_parent",
		ChildThreadId:           "thr_bridge_inter_agent_child",
		Role:                    "subagent",
		TaskName:                "child",
		AgentType:               "general",
		SourceToolUseEventId:    "evt_bridge_inter_agent_spawn",
		ForkTurns:               "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(t, "sesn_bridge_inter_agent", "msg_bridge_inter_agent_seed", "seed", "thr_bridge_inter_agent_parent", "evt_bridge_inter_agent_spawn", "none"),
	}); err != nil {
		t.Fatalf("CreateChildThread: %v", err)
	}
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, "sesn_bridge_inter_agent", "msg_bridge_inter_agent_delivery", "hello child")
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          parentScope,
		RuntimeWriteId: "rwrite_bridge_inter_agent_sent",
		EventType:      "agent.thread_message_sent",
		PayloadJson:    bridgeInterAgentSentEventJSON(t, "delivery_bridge_inter_agent_0", "thr_bridge_inter_agent_parent", "thr_bridge_inter_agent_child", "child", "evt_bridge_inter_agent_send", messageJSON),
		SessionVisible: true,
	}); err != nil {
		t.Fatalf("WriteEvent inter-agent sent: %v", err)
	}
	beforeReceived, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_inter_agent_child",
		DeliveryId:    "delivery_bridge_inter_agent_0",
	})
	if err != nil {
		t.Fatalf("ResolveInterAgentDelivery before receive: %v", err)
	}
	if !beforeReceived.GetSentExists() || beforeReceived.GetReceivedExists() || !beforeReceived.GetChildReceivable() ||
		strings.Contains(beforeReceived.GetChildThreadJson(), "hello child") {
		t.Fatalf("delivery before receive = %+v; want sent only and no message payload leakage", beforeReceived)
	}
	request := bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_inter_agent", "thr_bridge_inter_agent_child", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent"),
		"rin_bridge_inter_agent",
		"delivery_bridge_inter_agent_0",
		"thr_bridge_inter_agent_parent",
		"evt_bridge_inter_agent_send",
		messageJSON,
	)

	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs inter_agent_message: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("inter-agent commit ack = %s; want committed", response.GetAck().GetStatus())
	}
	replay, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs inter-agent replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("inter-agent replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	afterReceived, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_inter_agent_child",
		DeliveryId:    "delivery_bridge_inter_agent_0",
	})
	if err != nil {
		t.Fatalf("ResolveInterAgentDelivery after receive: %v", err)
	}
	if !afterReceived.GetSentExists() || !afterReceived.GetReceivedExists() || !afterReceived.GetChildReceivable() {
		t.Fatalf("delivery after receive = %+v; want sent/received/receivable", afterReceived)
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
		    AND payload_json::jsonb ->> 'delivery_id' = 'delivery_bridge_inter_agent_0'`).Scan(&receivedEventID, &receivedVisibility, &receivedSessionVisible, &receivedPayloadJSON); err != nil {
		t.Fatalf("read received event: %v", err)
	}
	if receivedVisibility != "public" || !receivedSessionVisible ||
		testJSONPathString(t, receivedPayloadJSON, "source_thread_id") != "thr_bridge_inter_agent_parent" ||
		testJSONPathString(t, receivedPayloadJSON, "source_tool_use_event_id") != "evt_bridge_inter_agent_send" {
		t.Fatalf("received event = visibility %s sessionVisible %v payload %s; want public parent attribution", receivedVisibility, receivedSessionVisible, receivedPayloadJSON)
	}
	if !strings.Contains(receivedPayloadJSON, `"source_task_name":null`) {
		t.Fatalf("received event payload = %s; want null callable name for primary source", receivedPayloadJSON)
	}
	assertDurableInterAgentPublicContentPreservesRuntimeMessage(t, receivedPayloadJSON, "hello child")
	var sentPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_inter_agent'
		    AND session_thread_id = 'thr_bridge_inter_agent_parent'
		    AND type = 'agent.thread_message_sent'
		    AND payload_json::jsonb ->> 'delivery_id' = 'delivery_bridge_inter_agent_0'`).Scan(&sentPayloadJSON); err != nil {
		t.Fatalf("read sent event: %v", err)
	}
	if testJSONPathString(t, sentPayloadJSON, "target_thread_id") != "thr_bridge_inter_agent_child" ||
		testJSONPathString(t, sentPayloadJSON, "target_task_name") != "child" {
		t.Fatalf("sent event payload = %s; want target child ID and callable task_name", sentPayloadJSON)
	}
	assertDurableInterAgentPublicContentPreservesRuntimeMessage(t, sentPayloadJSON, "hello child")
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

func TestPostgreSQLBridgeAPIStorePulledCompletionMailCommitsTypedProjection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_completion_pull"
		mainID    = "thr_bridge_completion_pull_main"
		childID   = "thr_bridge_completion_pull_child"
		delivery  = "delivery_bridge_completion_pull"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_completion_pull", 1, "pod_bridge_completion_pull")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, mainID, "bind_bridge_completion_pull", 1, "pod_bridge_completion_pull")
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_bridge_completion_pull", completionMailEnvelope("main", "task_"+childID, "pulled body"))
	request := bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		scope,
		completionRuntimeInputID(delivery),
		delivery,
		childID,
		"sevt_bridge_completion_pull_spawn",
		messageJSON,
	)
	response, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs pull receipt: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("pull receipt status = %s; want committed", response.GetAck().GetStatus())
	}

	replay, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs push replay after pull: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("push replay status = %s; want duplicate", replay.GetAck().GetStatus())
	}
	var receivedCount, projectionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*) FROM session_events
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND type='agent.thread_message_received'
			    AND payload_json::jsonb ->> 'delivery_id'=$3),
			(SELECT count(*) FROM session_messages
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND kind='user')`,
		sessionID,
		mainID,
		delivery,
	).Scan(&receivedCount, &projectionCount); err != nil {
		t.Fatalf("read pull receipt projection evidence: %v", err)
	}
	if receivedCount != 1 || projectionCount != 1 {
		t.Fatalf("pull receipt events/projections = %d/%d; want 1/1", receivedCount, projectionCount)
	}
}

func TestPostgreSQLBridgeAPIStorePushedCompletionMailProjectsOnceBeforePullDedupes(t *testing.T) {
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
	messageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_push_first",
		completionMailEnvelope("main", "task_"+childID, "push-first body"),
	)
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
	pushed, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs push-first completion: %v", err)
	}
	if pushed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("push-first status = %s; want committed", pushed.GetAck().GetStatus())
	}

	pulled, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs pull after push: %v", err)
	}
	if pulled.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("pull-after-push status = %s; want duplicate", pulled.GetAck().GetStatus())
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

func TestPostgreSQLBridgeAPIStoreConcurrentCompletionPushAndPullCommitOneReceipt(t *testing.T) {
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
	messageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_race",
		completionMailEnvelope("main", "task_"+childID, "racing completion"),
	)
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

	statuses := map[bridgev1.BridgeWriteStatus]int{}
	for range 2 {
		outcome := <-results
		if outcome.err != nil {
			t.Fatalf("concurrent completion receipt: %v", outcome.err)
		}
		statuses[outcome.response.GetAck().GetStatus()]++
	}
	if statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED] != 1 ||
		statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE] != 1 {
		t.Fatalf("concurrent completion receipt statuses = %#v; want one committed and one duplicate", statuses)
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

	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_bridge_completion_main", completionMailEnvelope("main", "task_"+childID, "done"))
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
	if first.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("completion mail ack/replay = %s/%s; want committed/duplicate", first.GetAck().GetStatus(), replay.GetAck().GetStatus())
	}
	var receivedCount, messageCount int
	var projectedRole, projectedOrigin string
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
		`SELECT data_json::jsonb ->> 'role', data_json::jsonb ->> 'origin'
		   FROM session_messages
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='user'
		    AND data_json LIKE '%Message Type: FINAL_ANSWER%'`,
		sessionID, mainThreadID,
	).Scan(&projectedRole, &projectedOrigin); err != nil {
		t.Fatalf("read main completion message role: %v", err)
	}
	if projectedRole != "user" || projectedOrigin != "runtime" {
		t.Fatalf("main completion message role/origin = %q/%q; want user/runtime", projectedRole, projectedOrigin)
	}
}

func TestPostgreSQLBridgeAPIStoreSentInterAgentMessageUsesDurableTargetCallableTaskName(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_inter_agent_target_name"
		mainThreadID   = "thr_bridge_inter_agent_target_main"
		targetThreadID = "thr_bridge_inter_agent_target_child"
		bindingID      = "bind_bridge_inter_agent_target_name"
		podUID         = "pod_uid_inter_agent_target_name"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, targetThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET task_name = 'durable callable target',
		        agent_type = 'research'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND id = $2`,
		sessionID,
		targetThreadID,
	); err != nil {
		t.Fatalf("set durable target identity: %v", err)
	}

	messageJSON := bridgeRuntimeMessageWithPublicContentJSON(t, sessionID, "msg_bridge_inter_agent_target_name")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, mainThreadID, bindingID, 1, podUID)
	childResponse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_inter_agent_target_child",
		EventType:      "agent.thread_message_sent",
		PayloadJson: bridgeInterAgentSentEventJSON(
			t,
			"delivery_bridge_inter_agent_target_child",
			mainThreadID,
			targetThreadID,
			"runtime conflicting target",
			"sevt_bridge_inter_agent_target_tool",
			messageJSON,
		),
	})
	if err != nil {
		t.Fatalf("WriteEvent child target: %v", err)
	}
	childPayload := readBridgeEventPayloadByID(t, admin, sessionID, childResponse.GetEventId())
	if got := testJSONPathString(t, childPayload, "target_task_name"); got != "durable callable target" {
		t.Fatalf("durable target_task_name = %q; want session_threads task_name, payload=%s", got, childPayload)
	}
	if strings.Contains(childPayload, "runtime conflicting target") || strings.Contains(childPayload, `"target_task_name":"research"`) {
		t.Fatalf("durable sent payload retained Runtime name or agent_type identity: %s", childPayload)
	}
	assertDurableInterAgentOrderedPublicContentPreservesRuntimeMessage(t, childPayload)
	processedAt := "2026-07-14T12:34:56Z"
	childPublic, err := eventwire.MarshalPublicEvent(childResponse.GetEventId(), "agent.thread_message_sent", json.RawMessage(childPayload), &processedAt)
	if err != nil {
		t.Fatalf("project durable child target event: %v", err)
	}
	assertProjectedSentInterAgentEvent(t, childPublic, targetThreadID, "durable callable target", true)

	primaryResponse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_inter_agent_target_primary",
		EventType:      "agent.thread_message_sent",
		PayloadJson: bridgeInterAgentSentEventJSON(
			t,
			"delivery_bridge_inter_agent_target_primary",
			targetThreadID,
			mainThreadID,
			"runtime primary alias",
			"sevt_bridge_inter_agent_target_primary_tool",
			messageJSON,
		),
	})
	if err != nil {
		t.Fatalf("WriteEvent primary target: %v", err)
	}
	primaryPayload := readBridgeEventPayloadByID(t, admin, sessionID, primaryResponse.GetEventId())
	if !strings.Contains(primaryPayload, `"target_task_name":null`) || strings.Contains(primaryPayload, "runtime primary alias") {
		t.Fatalf("primary target payload must store a null callable name from the durable main row: %s", primaryPayload)
	}
	primaryPublic, err := eventwire.MarshalPublicEvent(primaryResponse.GetEventId(), "agent.thread_message_sent", json.RawMessage(primaryPayload), &processedAt)
	if err != nil {
		t.Fatalf("project durable primary target event: %v", err)
	}
	assertProjectedSentInterAgentEvent(t, primaryPublic, mainThreadID, "", false)
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsMalformedPublicInterAgentMessage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_inter_agent_malformed"
		mainThreadID   = "thr_bridge_inter_agent_malformed_main"
		targetThreadID = "thr_bridge_inter_agent_malformed_child"
		bindingID      = "bind_bridge_inter_agent_malformed"
		podUID         = "pod_uid_inter_agent_malformed"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, targetThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	for index, test := range []struct {
		name    string
		message string
	}{
		{name: "message is not an object", message: `"invalid"`},
		{name: "content is not an array", message: `{"content":{"type":"text","text":"invalid"}}`},
		{name: "text content lacks text", message: `{"content":[{"type":"text"}]}`},
		{name: "parts are absent", message: `{}`},
		{name: "runtime part is not text", message: `{"parts":[{"type":"image","text":"invalid"}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeWriteID := fmt.Sprintf("rwrite_bridge_inter_agent_malformed_%d", index)
			payload := fmt.Sprintf(`{"type":"agent.thread_message_sent","delivery_id":"delivery_malformed_%d","source_thread_id":%q,"target_thread_id":%q,"target_task_name":"runtime alias","source_tool_use_event_id":"sevt_malformed_%d","message":%s}`,
				index, mainThreadID, targetThreadID, index, test.message)
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope:          bridgeAPIScope(sessionID, mainThreadID, bindingID, 1, podUID),
				RuntimeWriteId: runtimeWriteID,
				EventType:      "agent.thread_message_sent",
				PayloadJson:    payload,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteEvent malformed message err = %v; want InvalidArgument", err)
			}
			assertRejectedSentInterAgentWriteHasNoDurableSideEffects(t, admin, sessionID, runtimeWriteID)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReceivedInterAgentMessageUsesSourceCallableTaskName(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_inter_agent_source_name"
		mainThreadID   = "thr_bridge_inter_agent_source_main"
		sourceThreadID = "thr_bridge_inter_agent_source_child"
		targetThreadID = "thr_bridge_inter_agent_target_child"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, sourceThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, targetThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_inter_agent_source_name", 1, "pod_uid_inter_agent_source_name")
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_bridge_inter_agent_source_name", "hello sibling")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	if _, err := store.CommitInputs(context.Background(), bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope(sessionID, targetThreadID, "bind_bridge_inter_agent_source_name", 1, "pod_uid_inter_agent_source_name"),
		"rin_bridge_inter_agent_source_name",
		"delivery_bridge_inter_agent_source_name",
		sourceThreadID,
		"sevt_bridge_inter_agent_source_tool",
		messageJSON,
	)); err != nil {
		t.Fatalf("CommitInputs sibling inter-agent message: %v", err)
	}
	var payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND session_thread_id = $2
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = 'delivery_bridge_inter_agent_source_name'`,
		sessionID,
		targetThreadID,
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("read sibling received event: %v", err)
	}
	if got, want := testJSONPathString(t, payloadJSON, "source_task_name"), "task_"+sourceThreadID; got != want {
		t.Fatalf("source_task_name = %q; want source thread callable name %q", got, want)
	}
	assertDurableInterAgentPublicContentPreservesRuntimeMessage(t, payloadJSON, "hello sibling")
}

func TestPostgreSQLBridgeAPIStoreCommitInputsProjectsReviewerAndRejectionDrafts(t *testing.T) {
	t.Run("reviewer input", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID  = "sesn_bridge_reviewer_input"
			mainID     = "thr_bridge_reviewer_input_main"
			reviewerID = "thr_bridge_reviewer_input"
			eventID    = "evt_bridge_reviewer_input"
			inputID    = "rin_bridge_reviewer_input"
			bindingID  = "bind_bridge_reviewer_input"
			podUID     = "pod_bridge_reviewer_input"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, mainID)
		seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainID, reviewerID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		seedBridgeAPIEvent(t, admin, "default", sessionID, reviewerID, eventID, 1, "approval_review.decision", `{"decision":"approve"}`)
		request := &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope(sessionID, reviewerID, bindingID, 1, podUID),
			RuntimeInputId: inputID,
			InputKind:      "approval_review",
			EventIds:       []string{eventID},
			SequenceFrom:   1,
			SequenceTo:     1,
			Drafts: []*bridgev1.RuntimeMessageDraft{
				bridgeInputDraftForTest(
					"default",
					sessionID,
					reviewerID,
					"approval_review",
					inputID,
					eventID,
					bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_REVIEWER_INPUT,
					"user",
					"approve",
				),
			},
		}
		response, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs reviewer input: %v", err)
		}
		assertSingleCommitInputReceipt(t, response, "approval_review", inputID, eventID)
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
			Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
			RuntimeInputId: inputID,
			InputKind:      "rejection",
			EventIds:       []string{eventID1, eventID2},
			SequenceFrom:   1,
			SequenceTo:     2,
			Drafts: []*bridgev1.RuntimeMessageDraft{
				bridgeInputDraftForTest(
					"default",
					sessionID,
					threadID,
					"rejection",
					inputID,
					eventID1,
					bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_REJECTION,
					"assistant",
					"Input was not accepted.",
				),
				bridgeInputDraftForTestOrdinal(
					"default",
					sessionID,
					threadID,
					"rejection",
					inputID,
					eventID2,
					bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_REJECTION,
					"assistant",
					"Input was not accepted.",
					1,
				),
			},
		}
		response, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs rejection: %v", err)
		}
		receipts := response.GetDeclaration().GetReceipts()
		if len(receipts) != 1 || len(receipts[0].GetEvents()) != 2 || len(receipts[0].GetMessages()) != 2 {
			t.Fatalf("CommitInputs batched rejection receipt = %+v; want two settled sources and projections", response.GetDeclaration())
		}
	})
}

func assertSingleCommitInputReceipt(t *testing.T, response *bridgev1.CommitInputsResponse, sourceKind string, sourceID string, eventID string) {
	t.Helper()
	receipts := response.GetDeclaration().GetReceipts()
	if len(receipts) != 1 || receipts[0].GetSourceKind() != sourceKind || receipts[0].GetSourceId() != sourceID ||
		len(receipts[0].GetEvents()) != 1 || receipts[0].GetEvents()[0].GetEventId() != eventID ||
		len(receipts[0].GetMessages()) != 1 || receipts[0].GetMessages()[0].GetOwningEventId() != eventID {
		t.Fatalf("CommitInputs receipt = %+v; want one %s receipt for %s", response.GetDeclaration(), sourceKind, sourceID)
	}
}

func TestPostgreSQLBridgeAPIStoreRejectsCompletionReceiptOnApprovalReviewer(t *testing.T) {
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
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.CommitInputs(context.Background(), bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope(sessionID, reviewerID, bindingID, 1, podUID),
		"agent_mail:delivery_reviewer_rejected",
		"delivery_reviewer_rejected",
		sourceID,
		"sevt_reviewer_rejected_spawn",
		bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_reviewer_rejected", "completion"),
	))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CommitInputs completion receipt on reviewer err = %v; want FailedPrecondition", err)
	}
	var receivedCount, messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		    AND type='agent.thread_message_received'`,
		sessionID, reviewerID,
	).Scan(&receivedCount); err != nil {
		t.Fatalf("count reviewer received events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`,
		sessionID, reviewerID,
	).Scan(&messageCount); err != nil {
		t.Fatalf("count reviewer completion messages: %v", err)
	}
	if receivedCount != 1 || messageCount != 0 {
		t.Fatalf("reviewer completion receipt side effects = admitted event %d message %d; want 1/0", receivedCount, messageCount)
	}
}

func TestResolveInterAgentDeliveryUsesReadOnlyRepairPath(t *testing.T) {
	sourceBytes, err := os.ReadFile("bridge_api_children.go")
	if err != nil {
		t.Fatalf("read bridge_api_children.go: %v", err)
	}
	source := string(sourceBytes)
	resolveStart := strings.Index(source, "func (s *PostgreSQLBridgeAPIStore) ResolveInterAgentDelivery")
	resolveEnd := strings.Index(source, "func (s *PostgreSQLBridgeAPIStore) MarkChildThreadClosed")
	if resolveStart < 0 || resolveEnd < resolveStart {
		t.Fatal("ResolveInterAgentDelivery function boundaries not found")
	}
	resolveBody := source[resolveStart:resolveEnd]
	if !strings.Contains(resolveBody, "withScopeReadOnlyTx") || strings.Contains(resolveBody, "withScopeTx(") {
		t.Fatalf("ResolveInterAgentDelivery must use read-only scope tx, body:\n%s", resolveBody)
	}
	helperStart := strings.Index(source, "func readChildThreadForDeliveryTx")
	helperEnd := strings.Index(source, "func interAgentDeliveryEventExistsTx")
	if helperStart < 0 || helperEnd < helperStart {
		t.Fatal("readChildThreadForDeliveryTx function boundaries not found")
	}
	if strings.Contains(source[helperStart:helperEnd], "FOR UPDATE") {
		t.Fatalf("readChildThreadForDeliveryTx must not take a row lock:\n%s", source[helperStart:helperEnd])
	}
}
