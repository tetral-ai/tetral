package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/eventwire"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
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
	request := &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_commit", "thr_bridge_commit", "bind_bridge_commit", 1, "pod_uid_commit"),
		RuntimeInputId: "rin_bridge_commit",
		EventIds:       []string{"evt_bridge_commit"},
		SequenceFrom:   1,
		SequenceTo:     1,
		MessageCreates: []*bridgev1.RuntimeMessageCreate{bridgeUserInputCreateForTest(
			"default", "sesn_bridge_commit", "thr_bridge_commit", "rin_bridge_commit", "evt_bridge_commit", "hello",
		)},
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
		ID         string `json:"id"`
		Sequence   int64  `json:"sequence"`
		ProviderID string `json:"providerId"`
		ModelID    string `json:"modelId"`
		Parts      []struct {
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
		`"operation.id":"rin_bridge_commit"`,
		`"thread.id":"thr_bridge_commit"`,
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
	assertBridgeRuntimeUserProjection(t, messageDataJSON, "sesn_bridge_commit", "hello")
	if eventRevision != 2 || streamChangeCount != 2 || maxStreamRevision != 2 || latestStreamPosition != maxStreamPosition || latestStreamPosition <= initialStreamPosition {
		t.Fatalf("processed stream revision = eventRev %d changeCount %d maxRev %d latest %d initial %d maxPos %d; want same-event revision update only once",
			eventRevision, streamChangeCount, maxStreamRevision, latestStreamPosition, initialStreamPosition, maxStreamPosition)
	}

	conflict := proto.Clone(request).(*bridgev1.CommitInputsRequest)
	conflict.MessageCreates[0].Parts[0].PartJson = `{"type":"text","text":"different","truncated":false,"status":"completed"}`
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
		MessageCreates: []*bridgev1.RuntimeMessageCreate{
			bridgeUserInputCreateForTest("default", sessionID, threadID, runtimeInputID, eventID, "inspect"),
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
		MessageCreates: []*bridgev1.RuntimeMessageCreate{
			bridgeUserInputCreateForTest("default", "sesn_bridge_commit_fence", "thr_bridge_commit_fence", "rin_bridge_commit_fence", "evt_bridge_commit_fence", "hello"),
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
		MessageCreates: []*bridgev1.RuntimeMessageCreate{
			bridgeUserInputCreateForTest("default", "sesn_bridge_commit_payload_conflict", "thr_bridge_commit_payload_conflict", "rin_bridge_commit_payload_conflict", "evt_bridge_commit_other", "other"),
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
	request.MessageCreates = nil
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
		MessageCreates: []*bridgev1.RuntimeMessageCreate{
			bridgeUserInputCreateForTest("default", "sesn_bridge_commit_confirmation_message", "thr_bridge_commit_confirmation_message", "rin_bridge_confirmation_as_message", "evt_bridge_confirmation_as_message", "allow"),
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
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_bridge_generated_tool_use",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartCreateForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"tool-call-generated","toolName":"dangerous_tool","state":{"status":"running","input":{"value":{"path":"README.md"},"preview":"{\"path\":\"README.md\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent generated tool use: %v", err)
	}
	setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", toolUse.GetEventId(), "resolving")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "evt_bridge_generated_confirm", 3, "user.tool_confirmation", `{"type":"user.tool_confirmation","tool_use_id":"`+toolUse.GetEventId()+`","result":"allow"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "rin_bridge_generated_confirm", "tool_confirmation", `["evt_bridge_generated_confirm"]`, "accepted", "bind_bridge_generated_confirm", "pod_uid_generated_confirm", 3, 3)

	response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope:          bridgeAPIScope("sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "bind_bridge_generated_confirm", 1, "pod_uid_generated_confirm"),
		RuntimeInputId: "rin_bridge_generated_confirm",
		InputKind:      "tool_confirmation",
		EventIds:       []string{"evt_bridge_generated_confirm"},
		SequenceFrom:   3,
		SequenceTo:     3,
		MessageCreates: []*bridgev1.RuntimeMessageCreate{
			bridgeApprovalInputCreateForTest("default", "sesn_bridge_generated_confirm", "thr_bridge_generated_confirm", "rin_bridge_generated_confirm", "evt_bridge_generated_confirm", "Approval allowed"),
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
	store.RuntimeBindingTokenHMACKey = []byte("inter-agent-context-test-key-32b")
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
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
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
	if beforeReceived.GetDeliveryId() != "delivery_bridge_inter_agent_0" ||
		beforeReceived.GetSourceThreadId() != "thr_bridge_inter_agent_parent" ||
		beforeReceived.GetTargetThreadId() != "thr_bridge_inter_agent_child" ||
		beforeReceived.GetSourceToolUseEventId() != "evt_bridge_inter_agent_send" ||
		beforeReceived.GetReceivedEventId() == "" ||
		beforeReceived.GetReceivedSequence() <= 0 {
		t.Fatalf("resolved delivery = %+v; want stored envelope identity and admitted source stamps", beforeReceived)
	}
	assertDurableInterAgentPublicContentPreservesRuntimeMessage(t, `{"message":`+beforeReceived.GetMessageJson()+`}`, "hello child")
	childScope := bridgeAPIScope("sesn_bridge_inter_agent", "thr_bridge_inter_agent_child", "bind_bridge_inter_agent", 1, "pod_uid_inter_agent")
	runtimeInputID := completionRuntimeInputID("delivery_bridge_inter_agent_0")
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
		job.SessionThreadID != "thr_bridge_inter_agent_child" ||
		job.InputKind != "agent_mail" {
		t.Fatalf("resolved inter-agent job = %#v; want exact child mail wake", job)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.Clock = func() time.Time { return now.Add(2 * time.Second) }
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), job)
	if err != nil || plan.Request == nil || plan.StaleAccepted {
		t.Fatalf("PrepareRuntimeCommand inter-agent delivery = %#v/%v; want live Runtime command", plan, err)
	}
	if plan.Request.GetPayloadJson() == "{}" ||
		len(plan.Request.GetEventIds()) != 1 ||
		plan.Request.GetEventIds()[0] != beforeReceived.GetReceivedEventId() ||
		plan.Request.GetSequenceFrom() != beforeReceived.GetReceivedSequence() ||
		plan.Request.GetSequenceTo() != beforeReceived.GetReceivedSequence() {
		t.Fatalf("prepared inter-agent command = %#v; want exact stored envelope and admitted source stamps", plan.Request)
	}
	assertDurableInterAgentPublicContentPreservesRuntimeMessage(t, plan.Request.GetPayloadJson(), "hello child")
	if _, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), job, plan.Request); err != nil {
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
	create := bridgeMessageCreateForTest(
		bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_AGENT_MAIL_INPUT,
		"user", "runtime",
		bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"hello child","truncated":false,"status":"completed"}`},
	)
	request := &bridgev1.CommitInputsRequest{
		Scope:          childScope,
		RuntimeInputId: runtimeInputID,
		InputKind:      "agent_mail",
		EventIds:       []string{beforeReceived.GetReceivedEventId()},
		SequenceFrom:   beforeReceived.GetReceivedSequence(),
		SequenceTo:     beforeReceived.GetReceivedSequence(),
		MessageCreates: []*bridgev1.RuntimeMessageCreate{create},
	}

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
	loadResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          childScope,
		RuntimeInputId: "rin_bridge_inter_agent_cold_load",
	})
	if err != nil {
		t.Fatalf("LoadContext inter-agent message: %v", err)
	}
	var loaded bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
		t.Fatalf("decode inter-agent context: %v", err)
	}
	if len(loaded.Messages) != 1 || testJSONPathString(t, string(loaded.Messages[0]), "origin") != "runtime" {
		t.Fatalf("loaded inter-agent messages = %s; want one runtime-origin message", loadResponse.GetContextJson())
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

func TestPostgreSQLBridgeAPIStorePullSelectsOldestCompletionFromProducerCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID     = "sesn_bridge_completion_pull"
		mainID        = "thr_bridge_completion_pull_main"
		childID       = "thr_bridge_completion_pull_child"
		firstDelivery = "delivery_bridge_completion_pull_first"
		nextDelivery  = "delivery_bridge_completion_pull_next"
		bindingID     = "bind_bridge_completion_pull"
		podUID        = "pod_bridge_completion_pull"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	firstMessageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_pull_first",
		completionMailEnvelope("main", "task_"+childID, "first pulled body"),
	)
	firstPublicMessageJSON, err := publicInterAgentMessageJSON(json.RawMessage(firstMessageJSON))
	if err != nil {
		t.Fatalf("project first completion envelope: %v", err)
	}
	nextMessageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_pull_next",
		completionMailEnvelope("main", "task_"+childID, "next pulled body"),
	)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_pull_first", 1,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, firstDelivery, childID, mainID, "", "sevt_bridge_completion_pull_spawn_first", firstMessageJSON))
	seedAgentMailCustody(t, admin, sessionID, mainID, firstDelivery, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_pull_next", 2,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, nextDelivery, childID, mainID, "", "sevt_bridge_completion_pull_spawn_next", nextMessageJSON))
	seedAgentMailCustody(t, admin, sessionID, mainID, nextDelivery, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	first, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         scope,
		ChildThreadId: childID,
	})
	if err != nil {
		t.Fatalf("pull oldest completion: %v", err)
	}
	if first.GetDeliveryId() != firstDelivery ||
		first.GetSourceThreadId() != childID ||
		first.GetTargetThreadId() != mainID ||
		first.GetSourceToolUseEventId() != "sevt_bridge_completion_pull_spawn_first" ||
		first.GetReceivedEventId() == "" ||
		first.GetReceivedSequence() <= 0 ||
		first.GetMessageJson() != string(firstPublicMessageJSON) {
		t.Fatalf("oldest pulled completion = %+v; want exact first stored envelope", first)
	}
	replay, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         scope,
		ChildThreadId: childID,
	})
	if err != nil {
		t.Fatalf("repeat oldest completion pull: %v", err)
	}
	if replay.GetDeliveryId() != first.GetDeliveryId() ||
		replay.GetReceivedEventId() != first.GetReceivedEventId() ||
		replay.GetReceivedSequence() != first.GetReceivedSequence() ||
		replay.GetMessageJson() != first.GetMessageJson() {
		t.Fatalf("repeated completion pull = %+v; want stable first result %+v", replay, first)
	}
	assertActiveCompletionWake(t, admin, sessionID, firstDelivery, true)
	assertActiveCompletionWake(t, admin, sessionID, nextDelivery, true)
	var receivedCount, inboxCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*) FROM session_events
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND type='agent.thread_message_received'
			    AND payload_json::jsonb ->> 'delivery_id'=$3),
			(SELECT count(*) FROM session_runtime_inbox
			  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			    AND runtime_input_id=$4 AND status='delivering')`,
		sessionID,
		mainID,
		firstDelivery,
		completionRuntimeInputID(firstDelivery),
	).Scan(&receivedCount, &inboxCount); err != nil {
		t.Fatalf("read pull admission evidence: %v", err)
	}
	if receivedCount != 1 || inboxCount != 1 {
		t.Fatalf("pull received/inbox rows = %d/%d; want 1/1", receivedCount, inboxCount)
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
	committed, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs completion: %v", err)
	}
	if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("completion status = %s; want committed", committed.GetAck().GetStatus())
	}

	replayed, err := store.CommitInputs(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInputs completion replay: %v", err)
	}
	if replayed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("completion replay status = %s; want duplicate", replayed.GetAck().GetStatus())
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

func TestPostgreSQLBridgeAPIStoreSentInterAgentBirthRollsBackEventInboxAndQueueTogether(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_inter_agent_birth_rollback"
		parentID  = "thr_inter_agent_birth_parent"
		childID   = "thr_inter_agent_birth_child"
		bindingID = "bind_inter_agent_birth"
		podUID    = "pod_inter_agent_birth"
		delivery  = "delivery_inter_agent_birth"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_inter_agent_queue_birth() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'injected inter-agent Queue failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_inter_agent_queue_birth BEFORE INSERT ON queue_jobs
		FOR EACH ROW WHEN (NEW.kind = 'runtime_input') EXECUTE FUNCTION fail_inter_agent_queue_birth()`); err != nil {
		t.Fatalf("install inter-agent Queue failure: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_inter_agent_birth", "rollback")
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID),
		RuntimeWriteId: "rwrite_inter_agent_birth",
		EventType:      "agent.thread_message_sent",
		PayloadJson: bridgeInterAgentSentEventJSON(
			t, delivery, parentID, childID, "child", "evt_inter_agent_birth_tool", messageJSON,
		),
		SessionVisible: true,
	}); err == nil {
		t.Fatal("WriteEvent succeeded despite injected inter-agent Queue failure")
	}
	var sent, inbox, jobs, operations int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb ->> 'runtime_input_id'=$2),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND idempotency_key='rwrite_inter_agent_birth')`,
		sessionID, completionRuntimeInputID(delivery),
	).Scan(&sent, &inbox, &jobs, &operations); err != nil {
		t.Fatalf("read rolled-back inter-agent custody: %v", err)
	}
	if sent != 0 || inbox != 0 || jobs != 0 || operations != 0 {
		t.Fatalf("rolled-back inter-agent custody = event %d Inbox %d Queue %d receipt %d; want 0/0/0/0", sent, inbox, jobs, operations)
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
		sessionID     = "sesn_bridge_inter_agent_source_name"
		mainThreadID  = "thr_bridge_inter_agent_source_main"
		childThreadID = "thr_bridge_inter_agent_source_child"
		bindingID     = "bind_bridge_inter_agent_source_name"
		podUID        = "pod_uid_inter_agent_source_name"
		deliveryID    = "delivery_bridge_inter_agent_source_name"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_bridge_inter_agent_source_name", "child complete")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	childScope := bridgeAPIScope(sessionID, childThreadID, bindingID, 1, podUID)
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          childScope,
		RuntimeWriteId: "rwrite_bridge_inter_agent_source_name",
		EventType:      "agent.thread_message_sent",
		PayloadJson: bridgeInterAgentSentEventJSON(
			t,
			deliveryID,
			childThreadID,
			mainThreadID,
			"",
			"sevt_bridge_inter_agent_source_tool",
			messageJSON,
		),
		SessionVisible: true,
	}); err != nil {
		t.Fatalf("WriteEvent child completion: %v", err)
	}
	resolved, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope:         bridgeAPIScope(sessionID, mainThreadID, bindingID, 1, podUID),
		ChildThreadId: childThreadID,
		DeliveryId:    deliveryID,
	})
	if err != nil {
		t.Fatalf("ResolveInterAgentDelivery child completion: %v", err)
	}
	if resolved.GetSourceThreadId() != childThreadID || resolved.GetTargetThreadId() != mainThreadID {
		t.Fatalf("resolved child completion = %+v; want child-to-parent envelope", resolved)
	}
	var payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND session_thread_id = $2
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $3`,
		sessionID,
		mainThreadID,
		deliveryID,
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("read child completion received event: %v", err)
	}
	if got, want := testJSONPathString(t, payloadJSON, "source_task_name"), "task_"+childThreadID; got != want {
		t.Fatalf("source_task_name = %q; want source thread callable name %q", got, want)
	}
	assertDurableInterAgentPublicContentPreservesRuntimeMessage(t, payloadJSON, "child complete")
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
		var sharedFixture struct {
			MessageCreate struct {
				MessageInfo map[string]any `json:"messageInfo"`
				Part        map[string]any `json:"part"`
			} `json:"messageCreate"`
		}
		fixtureBytes, err := os.ReadFile(filepath.Join("..", "agent-runtime", "testdata", "reviewer-input-declaration.json"))
		if err != nil {
			t.Fatalf("read shared reviewer declaration: %v", err)
		}
		if err := json.Unmarshal(fixtureBytes, &sharedFixture); err != nil {
			t.Fatalf("decode shared reviewer declaration: %v", err)
		}
		messageInfoJSON, err := json.Marshal(sharedFixture.MessageCreate.MessageInfo)
		if err != nil {
			t.Fatalf("encode shared reviewer message info: %v", err)
		}
		partJSON, err := json.Marshal(sharedFixture.MessageCreate.Part)
		if err != nil {
			t.Fatalf("encode shared reviewer part: %v", err)
		}
		request := &bridgev1.CommitInputsRequest{
			Scope:          bridgeAPIScope(sessionID, reviewerID, bindingID, 1, podUID),
			RuntimeInputId: inputID,
			InputKind:      "approval_review",
			EventIds:       []string{eventID},
			MessageCreates: []*bridgev1.RuntimeMessageCreate{{
				MessageKind:     bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REVIEWER_INPUT,
				MessageInfoJson: string(messageInfoJSON),
				Parts: []*bridgev1.RuntimePartCreate{{
					PartKind: "text",
					PartJson: string(partJSON),
				}},
			}},
		}
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.RuntimeBindingTokenHMACKey = []byte("reviewer-context-test-key-32byte")
		var rejectionLogs bytes.Buffer
		store.Logger = slog.New(slog.NewJSONHandler(&rejectionLogs, nil))
		response, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("CommitInputs reviewer input: %v", err)
		}
		assertSingleCommitInputReceipt(t, response, "approval_review", inputID, eventID)
		eventStamp := response.GetDeclaration().GetReceipts()[0].GetEvents()[0]
		if eventStamp.GetDisposition() != bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED || eventStamp.GetEventSequence() <= 0 {
			t.Fatalf("reviewer input event stamp = %+v; want newly created durable event", eventStamp)
		}

		var eventType string
		var payloadJSON string
		var visibility string
		var sessionVisible bool
		var processed bool
		if err := admin.QueryRowContext(context.Background(),
			`SELECT type, payload_json, visibility, session_visible, processed_at IS NOT NULL
			   FROM session_events
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND session_thread_id = $2
			    AND event_id = $3`,
			sessionID,
			reviewerID,
			eventID,
		).Scan(&eventType, &payloadJSON, &visibility, &sessionVisible, &processed); err != nil {
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
			Scope:          request.GetScope(),
			RuntimeInputId: "rin_bridge_reviewer_input_cold_load",
		})
		if err != nil {
			t.Fatalf("LoadContext reviewer input: %v", err)
		}
		var loaded bridgeLoadContextPayload
		if err := json.Unmarshal([]byte(loadResponse.GetContextJson()), &loaded); err != nil {
			t.Fatalf("decode reviewer input context: %v", err)
		}
		if len(loaded.Messages) != 1 {
			t.Fatalf("loaded reviewer messages = %d; want one", len(loaded.Messages))
		}
		assertReviewerInputDeclarationValues(t, string(loaded.Messages[0]))

		replay, err := store.CommitInputs(context.Background(), request)
		if err != nil {
			t.Fatalf("replay CommitInputs reviewer input: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
			replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
			t.Fatalf("reviewer input ACKs = %s/%s; want committed/duplicate", response.GetAck().GetStatus(), replay.GetAck().GetStatus())
		}
		assertSingleCommitInputReceipt(t, replay, "approval_review", inputID, eventID)
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
		wrongTarget.EventIds = []string{"evt_bridge_reviewer_wrong_target"}
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
			Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
			RuntimeInputId: inputID,
			InputKind:      "rejection",
			EventIds:       []string{eventID1, eventID2},
			SequenceFrom:   1,
			SequenceTo:     2,
			MessageCreates: []*bridgev1.RuntimeMessageCreate{
				bridgeMessageCreateForTest(
					bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REJECTION,
					"assistant", "agent",
					bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"Input was not accepted.","truncated":false,"status":"completed"}`},
				),
				bridgeMessageCreateForTest(
					bridgev1.RuntimeMessageCreateKind_RUNTIME_MESSAGE_CREATE_KIND_REJECTION,
					"assistant", "agent",
					bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"Input was not accepted.","truncated":false,"status":"completed"}`},
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

func assertReviewerInputDeclarationValues(t *testing.T, raw string) {
	t.Helper()
	var message struct {
		Role   string `json:"role"`
		Origin string `json:"origin"`
		Status string `json:"status"`
		Parts  []struct {
			Type      string `json:"type"`
			Text      string `json:"text"`
			Truncated bool   `json:"truncated"`
			Status    string `json:"status"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		t.Fatalf("decode reviewer input message: %v", err)
	}
	if message.Role != "user" || message.Origin != "runtime" || message.Status != "completed" || len(message.Parts) != 1 ||
		message.Parts[0].Type != "text" || message.Parts[0].Text != "review this action" || message.Parts[0].Truncated || message.Parts[0].Status != "completed" {
		t.Fatalf("reviewer input declaration = %+v; want byte-preserved Runtime semantics", message)
	}
}

func assertSingleCommitInputReceipt(t *testing.T, response *bridgev1.CommitInputsResponse, sourceKind string, sourceID string, eventID string) {
	t.Helper()
	receipts := response.GetDeclaration().GetReceipts()
	if len(receipts) != 1 || receipts[0].GetSourceKind() != sourceKind || receipts[0].GetOperationId() != sourceID ||
		len(receipts[0].GetEvents()) != 1 || receipts[0].GetEvents()[0].GetEventId() != eventID ||
		len(receipts[0].GetMessages()) != 1 {
		t.Fatalf("CommitInputs receipt = %+v; want one %s receipt for %s", response.GetDeclaration(), sourceKind, sourceID)
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
				MessageJSON:          json.RawMessage(bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_reviewer_rejected", "completion")),
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

func TestPostgreSQLResolveInterAgentDeliveryReplaysAcceptedColdCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_agent_mail_accepted_cold"
		mainID     = "thr_agent_mail_accepted_cold_main"
		childID    = "thr_agent_mail_accepted_cold_child"
		bindingID  = "bind_agent_mail_accepted_cold"
		podUID     = "pod_agent_mail_accepted_cold"
		deliveryID = "delivery_agent_mail_accepted_cold"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_agent_mail_accepted_cold", completionMailEnvelope("main", "task_"+childID, "done"))
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_agent_mail_accepted_cold_sent", 1, "agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, deliveryID, childID, mainID, "", "evt_agent_mail_accepted_cold_spawn", messageJSON))
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	commitRequest := bridgeAgentMailCommitRequestForTest(t, admin, scope, "agent_mail:"+deliveryID, deliveryID, childID, "evt_agent_mail_accepted_cold_spawn", messageJSON)
	receivedID := commitRequest.GetEventIds()[0]
	enqueue, _, err := agentMailWakeEnqueueRequest("default", sessionID, mainID, deliveryID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("build accepted cold Queue custody: %v", err)
	}
	if _, err := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)).Enqueue(context.Background(), enqueue); err != nil {
		t.Fatalf("enqueue accepted cold Queue custody: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope: scope, ChildThreadId: childID, DeliveryId: deliveryID,
	})
	if err != nil {
		t.Fatalf("ResolveInterAgentDelivery accepted cold custody: %v", err)
	}
	if response.GetReceivedEventId() != receivedID || response.GetReceivedSequence() != 1 ||
		!strings.Contains(response.GetMessageJson(), "done") {
		t.Fatalf("accepted cold mail response = %#v; want existing durable projection", response)
	}
	var inboxStatus, queueStatus string
	var receivedEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id='agent_mail:' || $2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1
		 AND type='agent.thread_message_received' AND payload_json::jsonb ->> 'delivery_id'=$2),
		(SELECT status FROM queue_jobs WHERE workspace_id='default'
		 AND dedupe_key='runtime_input:default:' || $1 || ':agent_mail:' || $2)`,
		sessionID, deliveryID).Scan(&inboxStatus, &receivedEvents, &queueStatus); err != nil {
		t.Fatalf("read accepted cold mail replay: %v", err)
	}
	if inboxStatus != "accepted" || receivedEvents != 1 || queueStatus != queue.StatusPending {
		t.Fatalf("accepted cold mail replay = Inbox %q / received events %d / Queue %q; want accepted / one / pending", inboxStatus, receivedEvents, queueStatus)
	}
}
