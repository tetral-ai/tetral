package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLBridgeAPIStoreResolvesFileAttachmentMetadataWithoutReadingBlobs(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_file_metadata"
		threadID  = "thr_bridge_file_metadata"
		eventID   = "sevt_bridge_file_metadata"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_file_metadata", 1, "pod_bridge_file_metadata")
	blobStore := &countingGetBlobStore{inner: blob.NewFakeBlobStore()}
	seedBridgeAPIFileAttachment(t, admin, blobStore.inner, "file_bridge_metadata_image", "image.png", "image/png", "image")
	seedBridgeAPIFileAttachment(t, admin, blobStore.inner, "file_bridge_metadata_doc", "notes.txt", "text/plain", "document")
	seedBridgeAPIFileAttachment(t, admin, blobStore.inner, "file_bridge_metadata_other", "other.png", "image/png", "other")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message",
		`{"content":[{"type":"document","source":{"type":"file","file_id":"file_bridge_metadata_doc"}},{"type":"image","source":{"type":"file","file_id":"file_bridge_metadata_image"}},{"type":"document","source":{"type":"file","file_id":"file_bridge_metadata_absent"}}]}`)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.FileBlobStore = blobStore
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_file_metadata", 1, "pod_bridge_file_metadata")
	response, err := store.ResolveFileAttachmentMetadata(context.Background(), &bridgev1.ResolveFileAttachmentMetadataRequest{
		Scope: scope,
		Attachments: []*bridgev1.FileAttachmentPair{
			{SourceEventId: eventID, FileId: "file_bridge_metadata_doc"},
			{SourceEventId: eventID, FileId: "file_bridge_metadata_image"},
		},
	})
	if err != nil {
		t.Fatalf("ResolveFileAttachmentMetadata: %v", err)
	}
	if blobStore.getCalls != 0 {
		t.Fatalf("metadata preflight blob reads = %d; want zero", blobStore.getCalls)
	}
	if len(response.GetAttachments()) != 2 {
		t.Fatalf("metadata response count = %d; want two request-order results", len(response.GetAttachments()))
	}
	documentMetadata := response.GetAttachments()[0].GetMetadata()
	imageMetadata := response.GetAttachments()[1].GetMetadata()
	if documentMetadata.GetAttachment().GetFileId() != "file_bridge_metadata_doc" ||
		documentMetadata.GetMime() != "text/plain" ||
		documentMetadata.GetFilename() != "notes.txt" ||
		documentMetadata.GetSizeBytes() != int64(len("document")) ||
		response.GetAttachments()[0].GetRejected() != nil ||
		imageMetadata.GetAttachment().GetFileId() != "file_bridge_metadata_image" ||
		imageMetadata.GetMime() != "image/png" {
		t.Fatalf("metadata response = %+v; want request-order metadata", response.GetAttachments())
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE files SET deleted_at = '2026-01-01T00:00:01Z'
		  WHERE workspace_id = 'default' AND file_id = 'file_bridge_metadata_image'`); err != nil {
		t.Fatalf("delete file after admission: %v", err)
	}
	deleted, err := store.ResolveFileAttachmentMetadata(context.Background(), &bridgev1.ResolveFileAttachmentMetadataRequest{
		Scope:       scope,
		Attachments: []*bridgev1.FileAttachmentPair{{SourceEventId: eventID, FileId: "file_bridge_metadata_image"}},
	})
	if err != nil {
		t.Fatalf("ResolveFileAttachmentMetadata deleted file: %v", err)
	}
	if len(deleted.GetAttachments()) != 1 ||
		deleted.GetAttachments()[0].GetMetadata() != nil ||
		deleted.GetAttachments()[0].GetRejected().GetAttachment().GetFileId() != "file_bridge_metadata_image" ||
		deleted.GetAttachments()[0].GetRejected().GetReason() != bridgev1.FileAttachmentRejectionReason_FILE_ATTACHMENT_REJECTION_REASON_DELETED {
		t.Fatalf("deleted metadata = %+v; want per-ref deleted state", deleted.GetAttachments())
	}
	if blobStore.getCalls != 0 {
		t.Fatalf("deleted metadata preflight blob reads = %d; want zero", blobStore.getCalls)
	}

	absent, err := store.ResolveFileAttachmentMetadata(context.Background(), &bridgev1.ResolveFileAttachmentMetadataRequest{
		Scope:       scope,
		Attachments: []*bridgev1.FileAttachmentPair{{SourceEventId: eventID, FileId: "file_bridge_metadata_absent"}},
	})
	if err != nil || len(absent.GetAttachments()) != 1 ||
		absent.GetAttachments()[0].GetRejected().GetReason() != bridgev1.FileAttachmentRejectionReason_FILE_ATTACHMENT_REJECTION_REASON_DELETED {
		t.Fatalf("absent metadata = %+v, err = %v; want per-ref deleted state", absent.GetAttachments(), err)
	}

	for name, pair := range map[string]*bridgev1.FileAttachmentPair{
		"file not carried by event":       {SourceEventId: eventID, FileId: "file_bridge_metadata_other"},
		"event outside thread":            {SourceEventId: "sevt_bridge_file_metadata_other_thread", FileId: "file_bridge_metadata_doc"},
		"missing event with absent file":  {SourceEventId: "sevt_bridge_file_metadata_missing", FileId: "file_bridge_metadata_absent"},
		"missing event with deleted file": {SourceEventId: "sevt_bridge_file_metadata_missing", FileId: "file_bridge_metadata_image"},
	} {
		t.Run(name, func(t *testing.T) {
			if name == "event outside thread" {
				seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, "thr_bridge_file_metadata_other")
				seedBridgeAPIEvent(t, admin, "default", sessionID, "thr_bridge_file_metadata_other", pair.GetSourceEventId(), 1, "user.message",
					`{"content":[{"type":"document","source":{"type":"file","file_id":"file_bridge_metadata_doc"}}]}`)
			}
			_, err := store.ResolveFileAttachmentMetadata(context.Background(), &bridgev1.ResolveFileAttachmentMetadataRequest{
				Scope: scope, Attachments: []*bridgev1.FileAttachmentPair{pair},
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("ResolveFileAttachmentMetadata invalid pair err = %v; want InvalidArgument", err)
			}
			if blobStore.getCalls != 0 {
				t.Fatalf("invalid metadata preflight blob reads = %d; want zero", blobStore.getCalls)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReadsBoundedOffsetFileAttachmentChunks(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_file_chunk"
		threadID  = "thr_bridge_file_chunk"
		eventID   = "sevt_bridge_file_chunk"
		fileID    = "file_bridge_chunk"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_file_chunk", 1, "pod_bridge_file_chunk")
	inner := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, inner, fileID, "chunk.txt", "text/plain", "0123456789")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message",
		`{"content":[{"type":"document","source":{"type":"file","file_id":"file_bridge_chunk"}}]}`)
	rangeStore := &rangeRecordingBlobStore{inner: inner}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.FileBlobStore = rangeStore
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_file_chunk", 1, "pod_bridge_file_chunk")
	request := &bridgev1.ReadFileAttachmentChunkRequest{
		Scope: scope, Attachment: &bridgev1.FileAttachmentPair{SourceEventId: eventID, FileId: fileID},
		Offset: 2, Length: 4,
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := store.ReadFileAttachmentChunk(context.Background(), request)
		if err != nil {
			t.Fatalf("ReadFileAttachmentChunk attempt %d: %v", attempt, err)
		}
		if string(response.GetData()) != "2345" {
			t.Fatalf("chunk attempt %d = %q; want 2345", attempt, string(response.GetData()))
		}
	}
	if rangeStore.getCalls != 0 || len(rangeStore.ranges) != 2 ||
		rangeStore.ranges[0].offset != 2 || rangeStore.ranges[0].length != 4 {
		t.Fatalf("chunk reads full/ranges = %d/%+v; want two identical range reads", rangeStore.getCalls, rangeStore.ranges)
	}
	rangeStore.truncateRangesBy = 1
	if _, err := store.ReadFileAttachmentChunk(context.Background(), request); status.Code(err) != codes.Internal {
		t.Fatalf("short stored blob range err = %v; want Internal", err)
	}
	rangeStore.truncateRangesBy = 0
	if _, err := store.ReadFileAttachmentChunk(context.Background(), &bridgev1.ReadFileAttachmentChunkRequest{
		Scope: scope, Attachment: request.GetAttachment(), Offset: 0, Length: 8*1024*1024 + 1,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized chunk err = %v; want InvalidArgument", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE files SET deleted_at = '2026-01-01T00:00:01Z'
		  WHERE workspace_id = 'default' AND file_id = $1`, fileID); err != nil {
		t.Fatalf("delete chunk file: %v", err)
	}
	rangesBeforeDeletedRead := len(rangeStore.ranges)
	deleted, err := store.ReadFileAttachmentChunk(context.Background(), request)
	if err != nil || deleted.GetRejected().GetReason() != bridgev1.FileAttachmentRejectionReason_FILE_ATTACHMENT_REJECTION_REASON_DELETED ||
		deleted.GetData() != nil {
		t.Fatalf("deleted chunk = response %+v err %v; want in-band deleted rejection", deleted, err)
	}
	if len(rangeStore.ranges) != rangesBeforeDeletedRead {
		t.Fatalf("deleted chunk performed blob read; ranges=%+v", rangeStore.ranges)
	}

	store.FileBlobStore = nil
	if _, err := store.ReadFileAttachmentChunk(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("file blob adapter outage err = %v; want Unavailable", err)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextDerivesCommittedUnconsumedFileAttachments(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_file_pending"
		threadID  = "thr_bridge_file_pending"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_file_pending", 1, "pod_bridge_file_pending")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-file-pending-token-key-32")
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_file_pending", 1, "pod_bridge_file_pending")
	transient := createBridgeTransientAttachmentForTest(t, store, scope, "file_pending_transient", "sevt_file_pending_tool", []byte("transient"))

	for _, file := range []struct {
		id, name, mime, body string
	}{
		{"file_pending_consumed", "consumed.png", "image/png", "consumed"},
		{"file_pending_document", "document.txt", "text/plain", "document"},
		{"file_pending_unprojected", "unprojected.png", "image/png", "unprojected"},
		{"file_pending_later", "later.png", "image/png", "later"},
	} {
		seedBridgeAPIFileAttachment(t, admin, store.AttachmentBlobStore, file.id, file.name, file.mime, file.body)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_file_pending_first", 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_pending_consumed"}},{"type":"document","source":{"type":"file","file_id":"file_pending_document"}},{"type":"document","source":{"type":"file","file_id":"file_pending_document"}}]}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_file_pending_unprojected", 2, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_pending_unprojected"}}]}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "sevt_file_pending_later", 3, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_pending_later"}}]}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET processed_at=now()
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND event_id IN ('sevt_file_pending_first', 'sevt_file_pending_later')`, sessionID, threadID); err != nil {
		t.Fatalf("mark attachment inputs processed: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_file_pending_first", "messages",
		`["sevt_file_pending_first"]`, "committed", "bind_bridge_file_pending", "pod_bridge_file_pending", 1, 1)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_file_pending_later", "messages",
		`["sevt_file_pending_later"]`, "committed", "bind_bridge_file_pending", "pod_bridge_file_pending", 3, 3)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_file_pending_start", "mreq_file_pending", "agent_provider_request", 0)
	_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_file_pending_end", ModelRequestId: "mreq_file_pending",
		FinishReason: "stop", UsageJson: `{}`,
	})
	if err != nil {
		t.Fatalf("seed request end: %v", err)
	}
	var requestEndEventID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND model_request_id = 'mreq_file_pending' AND type = 'span.model_request_end'`,
		sessionID, threadID,
	).Scan(&requestEndEventID); err != nil {
		t.Fatalf("read request end event: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_file_attachment_consumptions (
			workspace_id, session_id, session_thread_id, request_end_event_id, source_event_id, file_id
		) VALUES ('default', $1, $2, $3, 'sevt_file_pending_first', 'file_pending_consumed')`,
		sessionID, threadID, requestEndEventID); err != nil {
		t.Fatalf("seed consumed file attachment: %v", err)
	}

	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext pending attachments: %v", err)
	}
	var payload struct {
		PendingAttachments []struct {
			Origin struct {
				Transient *struct {
					AttachmentRef string `json:"attachmentRef"`
				} `json:"transient"`
				FileBacked *struct {
					SourceEventID string `json:"sourceEventId"`
					FileID        string `json:"fileId"`
				} `json:"fileBacked"`
			} `json:"origin"`
			Mime     string `json:"mime"`
			Filename string `json:"filename"`
		} `json:"pendingAttachments"`
	}
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse pending attachments: %v", err)
	}
	if len(payload.PendingAttachments) != 3 {
		t.Fatalf("pending attachments = %+v; want transient plus two unconsumed projected refs", payload.PendingAttachments)
	}
	if payload.PendingAttachments[0].Origin.Transient == nil ||
		payload.PendingAttachments[0].Origin.Transient.AttachmentRef != transient.GetAttachmentRef() {
		t.Fatalf("first pending attachment = %+v; want active transient row", payload.PendingAttachments[0])
	}
	wantFiles := []string{"file_pending_document", "file_pending_later"}
	for index, wantFile := range wantFiles {
		item := payload.PendingAttachments[index+1]
		if item.Origin.FileBacked == nil || item.Origin.FileBacked.FileID != wantFile {
			t.Fatalf("file pending attachment %d = %+v; want %s in message/block order", index, item, wantFile)
		}
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextRequiresUnambiguousCommittedMessageCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_file_authority"
		threadID  = "thr_bridge_file_authority"
		bindingID = "bind_bridge_file_authority"
		podUID    = "pod_bridge_file_authority"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-file-authority-token-key")

	type custodyCase struct {
		name      string
		sequence  int64
		status    string
		inputKind string
	}
	cases := []custodyCase{
		{name: "committed", sequence: 1, status: "committed", inputKind: "messages"},
		{name: "dead_lettered", sequence: 2, status: "dead_lettered", inputKind: "messages"},
		{name: "cancelled", sequence: 3, status: "cancelled", inputKind: "messages"},
		{name: "rejected", sequence: 4, status: "committed", inputKind: "rejection"},
		{name: "conflicting", sequence: 5, status: "committed", inputKind: "messages"},
		{name: "missing", sequence: 6},
	}
	attachmentStore := blob.NewFakeBlobStore()
	for _, testCase := range cases {
		eventID := "sevt_file_authority_" + testCase.name
		fileID := "file_authority_" + testCase.name
		seedBridgeAPIFileAttachment(t, admin, attachmentStore, fileID, testCase.name+".png", "image/png", testCase.name)
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, testCase.sequence, "user.message",
			`{"content":[{"type":"image","source":{"type":"file","file_id":"`+fileID+`"}}]}`)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET processed_at=now()
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, eventID); err != nil {
			t.Fatalf("mark %s event processed: %v", testCase.name, err)
		}
		if testCase.status != "" {
			seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_file_authority_"+testCase.name,
				testCase.inputKind, `[`+strconv.Quote(eventID)+`]`, testCase.status, bindingID, podUID,
				testCase.sequence, testCase.sequence)
		}
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_file_authority_conflict",
		"rejection", `["sevt_file_authority_conflicting"]`, "committed", bindingID, podUID, 5, 5)

	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
	})
	if err != nil {
		t.Fatalf("LoadContext attachment authority: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode attachment authority context: %v", err)
	}
	if len(payload.PendingAttachments) != 1 || payload.PendingAttachments[0].Origin.FileBacked == nil ||
		payload.PendingAttachments[0].Origin.FileBacked.FileID != "file_authority_committed" {
		t.Fatalf("pending attachments = %#v; want only unambiguous committed message custody", payload.PendingAttachments)
	}
}

func TestPendingFileAttachmentQueryUsesMediaBoundedCommittedCustodyLookups(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID     = "sesn_bridge_file_plan"
		threadID      = "thr_bridge_file_plan"
		otherThreadID = "thr_bridge_file_plan_other"
		eventID       = "sevt_bridge_file_plan_media"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, otherThreadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_bridge_file_plan"}}]}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET processed_at=now()
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND event_id=$3`,
		sessionID, threadID, eventID); err != nil {
		t.Fatalf("mark media input processed: %v", err)
	}
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, "rin_bridge_file_plan_media", "messages",
		`["sevt_bridge_file_plan_media"]`, "committed", "bind_bridge_file_plan", "pod_bridge_file_plan", 1, 1)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, binding_id,
		binding_generation, target_pod_uid, created_at, updated_at, committed_at
	)
	SELECT 'default', $1, $2, 'rin_bridge_file_plan_history_' || value::text,
	       'messages', jsonb_build_array('sevt_unrelated_' || value::text)::text,
	       value + 100, value + 100, 'committed', 'bind_bridge_file_plan', 1,
	       'pod_bridge_file_plan', '2026-01-01T00:00:00Z',
	       '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	  FROM generate_series(1, 1000) AS value`, sessionID, threadID); err != nil {
		t.Fatalf("seed unrelated Inbox history: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, binding_id,
		binding_generation, target_pod_uid, created_at, updated_at, committed_at
	)
	SELECT 'default', $1, $2, 'rin_bridge_file_plan_other_' || value::text,
	       'messages', jsonb_build_array('sevt_other_' || value::text)::text,
	       value, value, 'committed', 'bind_bridge_file_plan', 1,
	       'pod_bridge_file_plan', '2026-01-01T00:00:00Z',
	       '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	  FROM generate_series(1, 10000) AS value`, sessionID, otherThreadID); err != nil {
		t.Fatalf("seed other-Thread Inbox history: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type,
		payload_json, visibility, session_visible, processed_at, created_at, updated_at
	)
	SELECT 'default', $1, $2, 'sevt_bridge_file_plan_media_' || value::text,
	       value + 1, 'user.message',
	       jsonb_build_object('content', jsonb_build_array(jsonb_build_object(
	           'type', 'image', 'source', jsonb_build_object(
	               'type', 'file', 'file_id', 'file_bridge_file_plan_' || value::text))))::text,
	       'public', TRUE, '2026-01-01T00:00:00Z',
	       '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	  FROM generate_series(1, 31) AS value`, sessionID, threadID); err != nil {
		t.Fatalf("seed additional media events: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, sequence_from, sequence_to, status, binding_id,
		binding_generation, target_pod_uid, created_at, updated_at, committed_at
	)
	SELECT 'default', $1, $2, 'rin_bridge_file_plan_media_' || value::text,
	       'messages', jsonb_build_array('sevt_bridge_file_plan_media_' || value::text)::text,
	       value + 1, value + 1, 'committed', 'bind_bridge_file_plan', 1,
	       'pod_bridge_file_plan', '2026-01-01T00:00:00Z',
	       '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	  FROM generate_series(1, 31) AS value`, sessionID, threadID); err != nil {
		t.Fatalf("seed additional media Inbox authority: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, visibility, session_visible, processed_at, created_at, updated_at
		)
		SELECT 'default', $1, $2,
		       'sevt_bridge_file_plan_' || value::text,
		       value + 1000,
		       'user.message',
		       '{"content":[{"type":"text","text":"non-media"}]}',
		       'public',
		       TRUE,
		       '2026-01-01T00:00:00Z',
		       '2026-01-01T00:00:00Z',
		       '2026-01-01T00:00:00Z'
		  FROM generate_series(1, 10000) AS value`,
		sessionID, threadID,
	); err != nil {
		t.Fatalf("seed non-media events: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `ANALYZE session_events; ANALYZE session_runtime_inbox`); err != nil {
		t.Fatalf("analyze media query tables: %v", err)
	}
	rows, err := admin.QueryContext(context.Background(),
		"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) "+loadThreadPendingFileAttachmentsSQL,
		"default", sessionID, threadID,
	)
	if err != nil {
		t.Fatalf("explain pending file query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var planLines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan pending file query plan: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("pending file query plan rows: %v", err)
	}
	plan := strings.Join(planLines, "\n")
	if !strings.Contains(plan, "session_events_pending_media_lookup") {
		t.Fatalf("pending file query does not use the media lookup index:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan on session_events") {
		t.Fatalf("pending file query scans non-media session events:\n%s", plan)
	}
	if !strings.Contains(plan, "session_runtime_inbox_attachment_authority_lookup") {
		t.Fatalf("pending file query does not narrow Inbox authority by thread:\n%s", plan)
	}
	if strings.Contains(plan, "Seq Scan on session_runtime_inbox") {
		t.Fatalf("pending file query scans unrelated Inbox history:\n%s", plan)
	}
	if strings.Contains(plan, "session_runtime_inbox inbox (actual rows=1.00 loops=32)") {
		t.Fatalf("pending file query repeats the Inbox authority scan per media event:\n%s", plan)
	}
}

func seedBridgeAPIFileAttachment(t *testing.T, db *sql.DB, blobStore blob.BlobStore, fileID, filename, mime, body string) {
	t.Helper()
	objectID := "fobj_" + fileID
	blobKey := "files/default/" + objectID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ('default', $1, $2, $3, $4, '2026-01-01T00:00:00Z')`,
		objectID, blobKey, len(body), strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed file object %s: %v", fileID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, created_at)
		 VALUES ('default', $1, $2, $3, $4, TRUE, '2026-01-01T00:00:00Z')`,
		fileID, objectID, filename, mime); err != nil {
		t.Fatalf("seed file %s: %v", fileID, err)
	}
	if err := blobStore.Put(context.Background(), blobKey, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("seed file blob %s: %v", fileID, err)
	}
}

func seedBridgeAPIProjectedUserMessage(t *testing.T, db *sql.DB, sessionID, threadID, messageID, sourceEventID string, sequence int64) {
	t.Helper()
	dataJSON := `{"parts":[]}`
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, created_at, updated_at
		) VALUES ('default', $1, $2, $3, $4, 'user', $5, $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, threadID, messageID, sequence, dataJSON, sourceEventID); err != nil {
		t.Fatalf("seed projected user message %s: %v", sourceEventID, err)
	}
}

type rangeRecordingBlobStore struct {
	inner            *blob.FakeBlobStore
	getCalls         int
	truncateRangesBy int64
	ranges           []struct {
		offset int64
		length int64
	}
}

func (s *rangeRecordingBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *rangeRecordingBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.getCalls++
	return s.inner.Get(ctx, key)
}

func (s *rangeRecordingBlobStore) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	s.ranges = append(s.ranges, struct {
		offset int64
		length int64
	}{offset: offset, length: length})
	body, ok := s.inner.Bytes(key)
	if !ok {
		return nil, &blob.NotFoundError{Key: key}
	}
	end := offset + length
	if end > int64(len(body)) {
		end = int64(len(body))
	}
	if s.truncateRangesBy > 0 {
		end = max(offset, end-s.truncateRangesBy)
	}
	return io.NopCloser(strings.NewReader(string(body[offset:end]))), nil
}

func (s *rangeRecordingBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *rangeRecordingBlobStore) CopyObject(ctx context.Context, sourceKey, destinationKey string) error {
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *rangeRecordingBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *rangeRecordingBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}
