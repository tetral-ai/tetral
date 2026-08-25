package sessionevent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginefiles "github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testSessionEventIdempotencyKey = "idem_sessionevent_test"

func TestAppendClientEventsPersistsOrderedLedgerEvents(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	sessionID := "sesn_event_append"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime, WithClock(func() time.Time { return now }))

	result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, AppendRequest{
		Events: []IncomingEvent{
			{Type: EventTypeUserMessage, Content: []TextContentBlock{{Type: ContentBlockTypeText, Text: "hello"}}},
			{Type: EventTypeUserInterrupt},
		},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("result events = %d; want 2", len(result.Data))
	}
	if result.Data[0].Sequence != 1 || result.Data[1].Sequence != 2 {
		t.Fatalf("sequences = %d,%d; want 1,2", result.Data[0].Sequence, result.Data[1].Sequence)
	}
	if !strings.HasPrefix(result.Data[0].ID, IDPrefix) || !strings.HasPrefix(result.Data[1].ID, IDPrefix) {
		t.Fatalf("event ids = %q,%q; want %s prefix", result.Data[0].ID, result.Data[1].ID, IDPrefix)
	}
	assertSessionEventLedger(t, admin, sessionID, []storedSessionEvent{
		{sequence: 1, eventType: EventTypeUserMessage, payload: `{"content":[{"type":"text","text":"hello"}]}`},
		{sequence: 2, eventType: EventTypeUserInterrupt, payload: `{}`},
	})
	assertSessionEventStreamChanges(t, admin, sessionID, []sessionEventStreamChange{
		{eventID: result.Data[0].ID, sessionThreadID: sessionEventMainThreadID(sessionID), revision: 1, visibility: "public", sessionVisible: true},
		{eventID: result.Data[1].ID, sessionThreadID: sessionEventMainThreadID(sessionID), revision: 1, visibility: "public", sessionVisible: true},
	})
}

func TestDecodeAppendRequestAcceptsFileBackedImageAndDocumentBlocks(t *testing.T) {
	request, err := DecodeAppendRequest([]byte(`{
		"events": [{
			"type": "user.message",
			"content": [
				{"type":"text","text":"inspect these"},
				{"type":"image","source":{"type":"file","file_id":"file_image"}},
				{"type":"document","source":{"type":"file","file_id":"file_document"}}
			]
		}]
	}`))
	if err != nil {
		t.Fatalf("DecodeAppendRequest: %v", err)
	}
	got, err := json.Marshal(request.Events[0].Content)
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}
	assertJSONEqual(t, got, `[{"type":"text","text":"inspect these"},{"type":"image","source":{"type":"file","file_id":"file_image"}},{"type":"document","source":{"type":"file","file_id":"file_document"}}]`)
}

func TestAppendClientEventsPreservesEmptyTextBlockField(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	sessionID := "sesn_event_empty_text"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	request, err := DecodeAppendRequest([]byte(
		`{"events":[{"type":"user.message","content":[{"type":"text","text":""}]}]}`,
	))
	if err != nil {
		t.Fatalf("DecodeAppendRequest: %v", err)
	}
	if _, err := service.AppendClientEvents(
		context.Background(),
		workspace.DefaultID,
		sessionID,
		"idem_empty_text",
		request,
	); err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	assertSessionEventLedger(t, admin, sessionID, []storedSessionEvent{{
		sequence:  1,
		eventType: EventTypeUserMessage,
		payload:   `{"content":[{"type":"text","text":""}]}`,
	}})
}

func TestDecodeAppendRequestRejectsNonFileMediaSourcesWithUploadFirstMessage(t *testing.T) {
	for _, sourceType := range []string{"base64", "url"} {
		t.Run(sourceType, func(t *testing.T) {
			_, err := DecodeAppendRequest([]byte(fmt.Sprintf(
				`{"events":[{"type":"user.message","content":[{"type":"image","source":{"type":%q,"data":"ignored"}}]}]}`,
				sourceType,
			)))
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error = %T %v; want ValidationError", err, err)
			}
			want := sourceType + " source is not supported; upload the bytes via /v1/files and reference the file_id"
			if validation.Message != want {
				t.Fatalf("message = %q; want %q", validation.Message, want)
			}
		})
	}
}

func TestPrepareIncomingEventsEnforcesFileAttachmentLimitAcrossBatch(t *testing.T) {
	events := make([]IncomingEvent, 0, 2)
	for eventIndex, count := range []int{16, 17} {
		content := make([]ContentBlock, 0, count)
		for blockIndex := 0; blockIndex < count; blockIndex++ {
			content = append(content, ContentBlock{
				Type: ContentBlockTypeImage,
				Source: &ContentSource{
					Type:   ContentSourceTypeFile,
					FileID: fmt.Sprintf("file_%d_%d", eventIndex, blockIndex),
				},
			})
		}
		events = append(events, IncomingEvent{Type: EventTypeUserMessage, Content: content})
	}
	_, err := prepareIncomingEvents(AppendRequest{Events: events}, DefaultLimits())
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "events request exceeds the file attachment limit" {
		t.Fatalf("message = %q; want file attachment limit", validation.Message)
	}
}

func TestPrepareIncomingEventsRejectsMissingFileSourceWithoutPanicking(t *testing.T) {
	_, err := prepareIncomingEvents(AppendRequest{Events: []IncomingEvent{{
		Type:    EventTypeUserMessage,
		Content: []ContentBlock{{Type: ContentBlockTypeImage}},
	}}}, DefaultLimits())
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("error = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "content block source is required" {
		t.Fatalf("message = %q; want source required", validation.Message)
	}
}

func TestAppendClientEventsValidatesAndPersistsFileBackedBlocksAtomically(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	sessionID := "sesn_event_multimodal"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	blobStore := blob.NewFakeBlobStore()
	seedSessionEventFile(t, admin, blobStore, workspace.DefaultID, "file_image", "fobj_image", "image.png", "image/png", []byte("png"), 3, nil)
	seedSessionEventFile(t, admin, blobStore, workspace.DefaultID, "file_document", "fobj_document", "document.pdf", "application/pdf", nil, 1024, int64(2))
	service := newSessionEventServiceWithFilesForTest(runtime, blobStore)

	result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_multimodal", AppendRequest{
		Events: []IncomingEvent{{
			Type: EventTypeUserMessage,
			Content: []ContentBlock{
				{Type: ContentBlockTypeText, Text: "inspect"},
				{Type: ContentBlockTypeImage, Source: &ContentSource{Type: ContentSourceTypeFile, FileID: "file_image"}},
				{Type: ContentBlockTypeDocument, Source: &ContentSource{Type: ContentSourceTypeFile, FileID: "file_document"}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("events = %d; want 1", len(result.Data))
	}
	assertSessionEventLedger(t, admin, sessionID, []storedSessionEvent{{
		sequence:  1,
		eventType: EventTypeUserMessage,
		payload:   `{"content":[{"type":"text","text":"inspect"},{"type":"image","source":{"type":"file","file_id":"file_image"}},{"type":"document","source":{"type":"file","file_id":"file_document"}}]}`,
	}})
}

func TestAppendClientEventsRejectsInvalidFileBackedBlocksWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name          string
		blockType     string
		fileID        string
		mimeType      string
		sizeBytes     int64
		pdfPageCount  any
		seedWorkspace workspace.ID
		wantMessage   string
	}{
		{
			name:        "missing",
			blockType:   ContentBlockTypeImage,
			fileID:      "file_missing",
			wantMessage: "file_id is invalid",
		},
		{
			name:          "foreign workspace",
			blockType:     ContentBlockTypeImage,
			fileID:        "file_foreign",
			mimeType:      "image/png",
			sizeBytes:     1,
			seedWorkspace: workspace.ID("wksp_foreign_media"),
			wantMessage:   "file_id is invalid",
		},
		{
			name:        "mime mismatch",
			blockType:   ContentBlockTypeImage,
			fileID:      "file_mismatch",
			mimeType:    "text/plain",
			sizeBytes:   1,
			wantMessage: "file type does not match the content block type",
		},
		{
			name:        "image too large",
			blockType:   ContentBlockTypeImage,
			fileID:      "file_large_image",
			mimeType:    "image/png",
			sizeBytes:   enginefiles.MaxEventImageBytes + 1,
			wantMessage: "image file exceeds the 10 MB limit",
		},
		{
			name:         "PDF bytes too large",
			blockType:    ContentBlockTypeDocument,
			fileID:       "file_large_pdf",
			mimeType:     "application/pdf",
			sizeBytes:    enginefiles.MaxEventPDFBytesPerRequest + 1,
			pdfPageCount: int64(1),
			wantMessage:  "PDF files exceed the 32 MB per-request limit",
		},
		{
			name:         "PDF pages too large",
			blockType:    ContentBlockTypeDocument,
			fileID:       "file_long_pdf",
			mimeType:     "application/pdf",
			sizeBytes:    1,
			pdfPageCount: enginefiles.MaxEventPDFPagesPerRequest + 1,
			wantMessage:  "PDF files exceed the 600-page per-request limit",
		},
		{
			name:         "unreadable PDF",
			blockType:    ContentBlockTypeDocument,
			fileID:       "file_bad_pdf",
			mimeType:     "application/pdf",
			sizeBytes:    1,
			pdfPageCount: int64(-1),
			wantMessage:  "referenced file is not a readable PDF",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := newSessionEventStoreTestDB(t)
			sessionID := "sesn_invalid_media_" + strings.ReplaceAll(testCase.name, " ", "_")
			seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
			blobStore := blob.NewFakeBlobStore()
			if testCase.mimeType != "" {
				seedWorkspace := testCase.seedWorkspace
				if seedWorkspace == "" {
					seedWorkspace = workspace.DefaultID
				} else {
					seedSessionEventWorkspace(t, admin, seedWorkspace)
				}
				seedSessionEventFile(
					t,
					admin,
					blobStore,
					seedWorkspace,
					testCase.fileID,
					"fobj_"+strings.ReplaceAll(testCase.name, " ", "_"),
					"media.bin",
					testCase.mimeType,
					nil,
					testCase.sizeBytes,
					testCase.pdfPageCount,
				)
			}
			service := newSessionEventServiceWithFilesForTest(runtime, blobStore)
			_, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_invalid_media", AppendRequest{
				Events: []IncomingEvent{{
					Type: EventTypeUserMessage,
					Content: []ContentBlock{{
						Type: testCase.blockType,
						Source: &ContentSource{
							Type:   ContentSourceTypeFile,
							FileID: testCase.fileID,
						},
					}},
				}},
			})
			var validation *enginefiles.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error = %T %v; want files.ValidationError", err, err)
			}
			if validation.Message != testCase.wantMessage {
				t.Fatalf("message = %q; want %q", validation.Message, testCase.wantMessage)
			}
			assertSessionEventLedger(t, admin, sessionID, nil)
			assertSessionEventIdempotencyRowCount(t, admin, sessionID, 0)
			if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
				t.Fatalf("queue jobs = %d; want 0", got)
			}
		})
	}
}

func TestAppendClientEventsRejectsFileOwnedByAnotherSessionBeforeBirth(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	const (
		sessionA = "sesn_event_attachment_scope_a"
		sessionB = "sesn_event_attachment_scope_b"
	)
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionA)
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionB)
	blobStore := blob.NewFakeBlobStore()
	for _, file := range []struct {
		id      string
		object  string
		session string
	}{
		{id: "file_attachment_scope_global", object: "fobj_attachment_scope_global"},
		{id: "file_attachment_scope_a", object: "fobj_attachment_scope_a", session: sessionA},
		{id: "file_attachment_scope_b", object: "fobj_attachment_scope_b", session: sessionB},
	} {
		seedSessionEventFile(t, admin, blobStore, workspace.DefaultID, file.id, file.object, file.id+".png", "image/png", []byte("image"), 5, nil)
		if file.session != "" {
			if _, err := admin.ExecContext(context.Background(), `UPDATE files
				SET scope_type = 'session', scope_id = $2
				WHERE workspace_id = $1 AND file_id = $3`, string(workspace.DefaultID), file.session, file.id); err != nil {
				t.Fatalf("scope file %s: %v", file.id, err)
			}
		}
	}
	service := newSessionEventServiceWithFilesForTest(runtime, blobStore)
	appendFile := func(idempotencyKey, fileID string) error {
		_, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionA, idempotencyKey, AppendRequest{
			Events: []IncomingEvent{{
				Type: EventTypeUserMessage,
				Content: []ContentBlock{{
					Type:   ContentBlockTypeImage,
					Source: &ContentSource{Type: ContentSourceTypeFile, FileID: fileID},
				}},
			}},
		})
		return err
	}

	err := appendFile("idem_attachment_scope_cross_session", "file_attachment_scope_b")
	var validation *enginefiles.ValidationError
	if !errors.As(err, &validation) || validation.Message != "file_id is invalid" {
		t.Fatalf("cross-Session attachment error = %T %v; want invalid file", err, err)
	}
	assertSessionEventLedger(t, admin, sessionA, nil)
	assertSessionEventIdempotencyRowCount(t, admin, sessionA, 0)
	if jobs := readSessionEventQueueJobs(t, admin, sessionA); len(jobs) != 0 {
		t.Fatalf("cross-Session attachment Queue births = %d; want zero", len(jobs))
	}
	var inboxRows, consumptions int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id = $1 AND session_id = $2),
		(SELECT count(*) FROM session_file_attachment_consumptions WHERE workspace_id = $1 AND session_id = $2)`,
		string(workspace.DefaultID), sessionA).Scan(&inboxRows, &consumptions); err != nil {
		t.Fatalf("read rejected attachment custody: %v", err)
	}
	if inboxRows != 0 || consumptions != 0 {
		t.Fatalf("rejected attachment custody = Inbox:%d consumptions:%d; want zero", inboxRows, consumptions)
	}

	if err := appendFile("idem_attachment_scope_global", "file_attachment_scope_global"); err != nil {
		t.Fatalf("append Workspace-global attachment: %v", err)
	}
	if err := appendFile("idem_attachment_scope_owner", "file_attachment_scope_a"); err != nil {
		t.Fatalf("append owning-Session attachment: %v", err)
	}
	assertSessionEventIdempotencyRowCount(t, admin, sessionA, 2)
}

func TestAppendClientEventsLazilyCountsLegacyPDFOnce(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	sessionID := "sesn_event_legacy_pdf"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	blobStore := blob.NewFakeBlobStore()
	pdf := minimalSessionEventPDF(2)
	seedSessionEventFile(t, admin, blobStore, workspace.DefaultID, "file_legacy_pdf", "fobj_legacy_pdf", "legacy.pdf", "application/pdf", pdf, int64(len(pdf)), nil)
	service := newSessionEventServiceWithFilesForTest(runtime, blobStore)

	_, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_legacy_pdf", AppendRequest{
		Events: []IncomingEvent{{
			Type: EventTypeUserMessage,
			Content: []ContentBlock{{
				Type:   ContentBlockTypeDocument,
				Source: &ContentSource{Type: ContentSourceTypeFile, FileID: "file_legacy_pdf"},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	var pageCount sql.NullInt64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT pdf_page_count FROM file_objects
		  WHERE workspace_id=$1 AND object_id='fobj_legacy_pdf'`,
		string(workspace.DefaultID),
	).Scan(&pageCount); err != nil {
		t.Fatalf("read lazy PDF page count: %v", err)
	}
	if !pageCount.Valid || pageCount.Int64 != 2 {
		t.Fatalf("lazy PDF page count = %#v; want 2", pageCount)
	}
}

func TestAppendClientEventsCountsSharedLegacyPDFObjectOncePerRequest(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	sessionID := "sesn_event_shared_legacy_pdf"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	baseBlobStore := blob.NewFakeBlobStore()
	countingStore := &countingGetBlobStore{BlobStore: baseBlobStore}
	pdf := minimalSessionEventPDF(2)
	seedSessionEventFile(
		t,
		admin,
		countingStore,
		workspace.DefaultID,
		"file_shared_pdf_a",
		"fobj_shared_pdf",
		"shared-a.pdf",
		"application/pdf",
		pdf,
		int64(len(pdf)),
		nil,
	)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO files (
			file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at
		) VALUES (
			'file_shared_pdf_b', $1, 'fobj_shared_pdf', 'shared-b.pdf',
			'application/pdf', false, '2026-01-01T00:00:00Z'
		)`,
		string(workspace.DefaultID),
	); err != nil {
		t.Fatalf("seed shared PDF alias: %v", err)
	}
	service := newSessionEventServiceWithFilesForTest(runtime, countingStore)

	if _, err := service.AppendClientEvents(
		context.Background(),
		workspace.DefaultID,
		sessionID,
		"idem_shared_legacy_pdf",
		AppendRequest{Events: []IncomingEvent{{
			Type: EventTypeUserMessage,
			Content: []ContentBlock{
				{
					Type:   ContentBlockTypeDocument,
					Source: &ContentSource{Type: ContentSourceTypeFile, FileID: "file_shared_pdf_b"},
				},
				{
					Type:   ContentBlockTypeDocument,
					Source: &ContentSource{Type: ContentSourceTypeFile, FileID: "file_shared_pdf_a"},
				},
			},
		}}},
	); err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if countingStore.getCalls != 1 {
		t.Fatalf("shared PDF BlobStore.Get calls = %d; want 1", countingStore.getCalls)
	}
}

func TestRejectedLegacyPDFReferencePersistsUnreadableSentinelOnce(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	sessionID := "sesn_event_unreadable_legacy_pdf"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	baseBlobStore := blob.NewFakeBlobStore()
	countingStore := &countingGetBlobStore{BlobStore: baseBlobStore}
	body := []byte("not a PDF")
	seedSessionEventFile(
		t,
		admin,
		countingStore,
		workspace.DefaultID,
		"file_unreadable_legacy_pdf",
		"fobj_unreadable_legacy_pdf",
		"unreadable.pdf",
		"application/pdf",
		body,
		int64(len(body)),
		nil,
	)
	service := newSessionEventServiceWithFilesForTest(runtime, countingStore)
	request := AppendRequest{Events: []IncomingEvent{{
		Type: EventTypeUserMessage,
		Content: []ContentBlock{{
			Type: ContentBlockTypeDocument,
			Source: &ContentSource{
				Type:   ContentSourceTypeFile,
				FileID: "file_unreadable_legacy_pdf",
			},
		}},
	}}}

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := service.AppendClientEvents(
			context.Background(),
			workspace.DefaultID,
			sessionID,
			fmt.Sprintf("idem_unreadable_legacy_pdf_%d", attempt),
			request,
		)
		var validation *enginefiles.ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("attempt %d error = %T %v; want files.ValidationError", attempt, err, err)
		}
		if validation.Message != "referenced file is not a readable PDF" {
			t.Fatalf("attempt %d message = %q; want unreadable PDF", attempt, validation.Message)
		}
	}
	if countingStore.getCalls != 1 {
		t.Fatalf("unreadable PDF BlobStore.Get calls = %d; want 1", countingStore.getCalls)
	}
	var pageCount sql.NullInt64
	if err := admin.QueryRowContext(context.Background(), `SELECT pdf_page_count
		FROM file_objects
		WHERE workspace_id=$1 AND object_id='fobj_unreadable_legacy_pdf'`,
		string(workspace.DefaultID),
	).Scan(&pageCount); err != nil {
		t.Fatalf("read unreadable PDF sentinel: %v", err)
	}
	if !pageCount.Valid || pageCount.Int64 != -1 {
		t.Fatalf("unreadable PDF page count = %#v; want -1", pageCount)
	}
	assertSessionEventLedger(t, admin, sessionID, nil)
}

type countingGetBlobStore struct {
	blob.BlobStore
	getCalls int
}

func (s *countingGetBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.getCalls++
	return s.BlobStore.Get(ctx, key)
}

func TestAppendClientEventsAppendsUnboundedPendingEventsWithoutQuota(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	sessionID := "sesn_event_unbounded_pending"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime, WithClock(func() time.Time { return now }))

	const totalMessages = 40
	const interruptAt = 20
	for index := 1; index <= totalMessages; index++ {
		request := messageAppendRequest("message")
		if index == interruptAt {
			request = AppendRequest{Events: []IncomingEvent{{Type: EventTypeUserInterrupt}}}
		}
		result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, sessionEventTestIdempotencyKey("unbounded", index), request)
		if err != nil {
			t.Fatalf("AppendClientEvents #%d: %v", index, err)
		}
		if len(result.Data) != 1 {
			t.Fatalf("AppendClientEvents #%d returned %d events; want 1", index, len(result.Data))
		}
		if got := result.Data[0].Sequence; got != int64(index) {
			t.Fatalf("AppendClientEvents #%d sequence = %d; want %d", index, got, index)
		}
		if index == interruptAt && result.Data[0].Type != EventTypeUserInterrupt {
			t.Fatalf("interrupt append #%d type = %q; want %s", index, result.Data[0].Type, EventTypeUserInterrupt)
		}
	}

	rows := readSessionEventLedgerRows(t, admin, sessionID)
	if len(rows) != totalMessages {
		t.Fatalf("ledger rows = %d; want %d", len(rows), totalMessages)
	}
	for index, row := range rows {
		wantSequence := int64(index + 1)
		if row.sequence != wantSequence {
			t.Fatalf("ledger row %d sequence = %d; want %d", index, row.sequence, wantSequence)
		}
		if row.processedAt.Valid {
			t.Fatalf("ledger row %d processed_at = %q; want NULL", index, row.processedAt.String)
		}
		if wantSequence == interruptAt {
			if row.eventType != EventTypeUserInterrupt {
				t.Fatalf("interrupt row type = %q; want %s", row.eventType, EventTypeUserInterrupt)
			}
			assertJSONEqual(t, []byte(row.payload), `{}`)
		}
	}
}

func TestAppendClientEventsConcurrentAppendsAssignUniqueSequences(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_concurrent"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	const appendsPerGoroutine = 15
	var wait sync.WaitGroup
	errs := make(chan error, 2*appendsPerGoroutine)
	for goroutine := 0; goroutine < 2; goroutine++ {
		wait.Add(1)
		go func(goroutineIndex int) {
			defer wait.Done()
			for index := 0; index < appendsPerGoroutine; index++ {
				if _, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, sessionEventTestIdempotencyKey("concurrent", goroutineIndex, index), messageAppendRequest("concurrent")); err != nil {
					errs <- err
				}
			}
		}(goroutine)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent AppendClientEvents: %v", err)
	}

	rows := readSessionEventLedgerRows(t, admin, sessionID)
	if len(rows) != 2*appendsPerGoroutine {
		t.Fatalf("ledger rows = %d; want %d", len(rows), 2*appendsPerGoroutine)
	}
	for index, row := range rows {
		wantSequence := int64(index + 1)
		if row.sequence != wantSequence {
			t.Fatalf("ledger row %d sequence = %d; want strictly increasing unique %d", index, row.sequence, wantSequence)
		}
	}
}

func TestAppendClientEventsWritesWorkerNeutralLedgerOnly(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_worker_neutral"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	if _, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("ledger only")); err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	assertSessionEventLedgerHasNoSourceColumn(t, admin)
}

func TestSessionEventProductionCodeDoesNotCallDispatcher(t *testing.T) {
	for _, forbidden := range []string{
		"Dispatcher",
		"DispatchSession",
		"session-" + "dispatcher",
		"internalgrpc",
	} {
		for _, entry := range readSessionEventProductionFiles(t) {
			if strings.Contains(entry.source, forbidden) {
				t.Fatalf("%s contains forbidden dispatcher dependency %q", entry.name, forbidden)
			}
		}
	}
	for _, entry := range readSessionEventProductionFiles(t) {
		if strings.Contains(entry.source, "log.") || strings.Contains(entry.source, "slog.") {
			t.Fatalf("%s logs from sessionevent production path; admission must not log raw keys or event content", entry.name)
		}
	}
}

func TestAppendClientEventStorePathIsOPurityConstant(t *testing.T) {
	source := readSessionEventStoreSource(t)
	normalized := strings.ToLower(strings.Join(strings.Fields(source), " "))

	// The only allowed reads against session_events on the append path are the
	// MAX(sequence) aggregate and the INSERT; the sessions row is the single
	// serialization lock. A FOR UPDATE against session_events or any selection
	// of payload_json from existing rows would reintroduce O(history) work.
	if strings.Contains(normalized, "from session_events") && strings.Contains(normalized, "for update") {
		// Confirm no FOR UPDATE statement is scoped to session_events.
		if strings.Contains(normalized, "from session_events") {
			// Inspect each statement-like fragment for the forbidden pairing.
			for _, fragment := range strings.Split(normalized, ";") {
				if strings.Contains(fragment, "from session_events") && strings.Contains(fragment, "for update") {
					t.Fatalf("append path locks session_events with FOR UPDATE; serialization must use the sessions row only")
				}
			}
		}
	}
	if strings.Contains(normalized, "payload_json from session_events") ||
		strings.Contains(normalized, "select payload_json") {
		t.Fatalf("append path selects payload_json from stored rows; append must not decode ledger history")
	}
}

func readSessionEventStoreSource(t *testing.T) string {
	t.Helper()
	return readSessionEventProductionFile(t, "postgresql_store.go")
}

func readSessionEventProductionFile(t *testing.T, name string) string {
	t.Helper()
	// #nosec G304 -- fixed sibling production source file under test.
	body, err := os.ReadFile(filepath.Clean(name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(body)
}

func TestAppendClientEventsRejectsClosedSessionStates(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         string
		lifecycleState string
		archivedAt     *time.Time
	}{
		{name: "terminated", status: "terminated", lifecycleState: "active"},
		{name: "archived lifecycle", status: "idle", lifecycleState: "archived"},
		{name: "archived timestamp", status: "idle", lifecycleState: "active", archivedAt: ptrTime(time.Date(2026, 6, 9, 11, 0, 0, 0, time.UTC))},
		{name: "transition", status: "idle", lifecycleState: "archiving"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := newSessionEventStoreTestDB(t)
			sessionID := "sesn_event_closed_" + strings.ReplaceAll(tc.name, " ", "_")
			seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
			setSessionEventSessionState(t, admin, workspace.DefaultID, sessionID, tc.status, tc.lifecycleState, tc.archivedAt)
			service := newSessionEventServiceForTest(runtime)

			if _, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("closed")); err == nil {
				t.Fatal("AppendClientEvents accepted a closed session")
			} else {
				var conflict *ConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf("error = %T %v; want ConflictError", err, err)
				}
			}
		})
	}
}

func TestAppendClientEventsFailsClosedWhenSessionMainThreadMissing(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_missing_main_thread"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	if _, err := admin.ExecContext(ctx,
		`UPDATE sessions
		    SET main_thread_id = NULL
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("clear main_thread_id: %v", err)
	}
	service := newSessionEventServiceForTest(runtime)

	_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_missing_main_thread", messageAppendRequest("blocked"))
	if err == nil {
		t.Fatal("AppendClientEvents accepted a session without main_thread_id")
	}
	var validation *ValidationError
	var conflict *ConflictError
	var notFound *NotFoundError
	if errors.As(err, &validation) || errors.As(err, &conflict) || errors.As(err, &notFound) {
		t.Fatalf("error = %T %v; want internal invariant error", err, err)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
		t.Fatalf("ledger rows = %d; want none", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("queue jobs = %d; want none", got)
	}
}

func TestAppendClientEventsWritesIdempotencyAndRuntimeInputQueueJobsAtomically(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_idempotency_new"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, AppendRequest{
		Events: []IncomingEvent{
			{Type: EventTypeUserMessage, Content: []TextContentBlock{{Type: ContentBlockTypeText, Text: "hello"}}},
			{Type: EventTypeUserInterrupt},
		},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if len(result.Data) != 2 {
		t.Fatalf("result events = %d; want 2", len(result.Data))
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 2 {
		t.Fatalf("ledger rows = %d; want 2", got)
	}
	assertSessionEventIdempotencyRowCount(t, admin, sessionID, 1)
	assertSessionEventIdempotencyMetadata(t, admin, sessionID, testSessionEventIdempotencyKey)
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 2 {
		t.Fatalf("queue jobs = %#v; want messages and interrupt runtime_input jobs", jobs)
	}
	assertRuntimeInputQueueJob(t, findRuntimeInputQueueJob(t, jobs, RuntimeInputKindMessages), RuntimeInputKindMessages, 0, []string{result.Data[0].ID}, 1, 1)
	assertRuntimeInputQueueJob(t, findRuntimeInputQueueJob(t, jobs, RuntimeInputKindInterruptControl), RuntimeInputKindInterruptControl, 100, []string{result.Data[1].ID}, 2, 2)
	assertSessionEventInboxMatchesQueue(t, admin, sessionID, jobs)
}

func TestAppendClientEventsBirthRemainsAtomicWhileLeaseRacesSessionOwner(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const sessionID = "sesn_event_birth_lease_race"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	service := NewService(store)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	seed, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_birth_lease_race_seed", messageAppendRequest("existing candidate"))
	if err != nil || len(seed.Data) != 1 {
		t.Fatalf("seed existing Queue candidate = %#v/%v; want one event", seed, err)
	}
	paused := make(chan struct{})
	release := make(chan struct{})
	store.beforeQueueJobInsert = func() error {
		close(paused)
		<-release
		return nil
	}

	appendDone := make(chan error, 1)
	go func() {
		_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_birth_lease_race", messageAppendRequest("atomic birth"))
		appendDone <- err
	}()
	<-paused
	type leaseResult struct {
		jobs []*queue.Job
		err  error
	}
	leaseDone := make(chan leaseResult, 1)
	go func() {
		jobs, err := queueStore.Lease(ctx, queue.LeaseRequest{
			WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-before-birth",
			MaxJobs: 1, LeaseDuration: time.Minute,
		})
		leaseDone <- leaseResult{jobs: jobs, err: err}
	}()
	select {
	case result := <-leaseDone:
		t.Fatalf("Lease passed the active Session birth owner: jobs=%#v err=%v", result.jobs, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if events := readSessionEventLedgerRows(t, admin, sessionID); len(events) != 1 {
		t.Fatalf("visible Events during uncommitted birth = %#v; want only the committed seed", events)
	}
	if jobs := readSessionEventQueueJobs(t, admin, sessionID); len(jobs) != 1 {
		t.Fatalf("visible Queue jobs during uncommitted birth = %#v; want only the committed seed", jobs)
	}
	close(release)
	if err := <-appendDone; err != nil {
		t.Fatalf("AppendClientEvents after lease race: %v", err)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(readSessionEventLedgerRows(t, admin, sessionID)) != 2 || len(jobs) != 2 {
		t.Fatalf("committed birth facts = events:%d jobs:%d; want 2/2", len(readSessionEventLedgerRows(t, admin, sessionID)), len(jobs))
	}
	result := <-leaseDone
	if result.err != nil || len(result.jobs) != 1 || result.jobs[0].ID != jobs[0].id {
		t.Fatalf("Lease after birth = %#v/%v; want committed predecessor %s", result.jobs, result.err, jobs[0].id)
	}
	if acknowledged, err := queueStore.Ack(ctx, queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: result.jobs[0].ID, LeaseToken: result.jobs[0].LeaseToken,
	}); err != nil || !acknowledged {
		t.Fatalf("ack predecessor = %t/%v; want true/nil", acknowledged, err)
	}
	leased := mustLeaseSessionEventJob(t, queueStore, sessionID, "bridge-after-birth")
	if leased.ID != jobs[1].id {
		t.Fatalf("post-birth lease = %s; want %s", leased.ID, jobs[1].id)
	}
}

func TestAppendClientEventsRevalidatesAfterQueueLeaseOwnsSession(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const sessionID = "sesn_event_lease_birth_race"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	service := NewService(store)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	seed, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_lease_birth_seed", messageAppendRequest("lease predecessor"))
	if err != nil || len(seed.Data) != 1 {
		t.Fatalf("seed lease predecessor = %#v/%v; want one event", seed, err)
	}
	seedJobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(seedJobs) != 1 {
		t.Fatalf("seed Queue jobs = %#v; want one", seedJobs)
	}

	blocker, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin Queue candidate blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var blockedJob string
	if err := blocker.QueryRowContext(ctx, `SELECT id FROM queue_jobs WHERE workspace_id='default' AND id=$1 FOR UPDATE`, seedJobs[0].id).Scan(&blockedJob); err != nil {
		t.Fatalf("lock Queue candidate: %v", err)
	}
	var blockerPID int
	if err := blocker.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read Queue candidate blocker pid: %v", err)
	}
	type leaseResult struct {
		jobs []*queue.Job
		err  error
	}
	leaseDone := make(chan leaseResult, 1)
	go func() {
		jobs, err := queueStore.Lease(ctx, queue.LeaseRequest{
			WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-lease-before-birth",
			MaxJobs: 1, LeaseDuration: time.Minute,
		})
		leaseDone <- leaseResult{jobs: jobs, err: err}
	}()
	waitForSessionEventLockWaiters(t, admin, blockerPID, 1)

	appendEntered := make(chan struct{})
	appendDone := make(chan error, 1)
	go func() {
		close(appendEntered)
		_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_lease_birth_race", messageAppendRequest("born after lease"))
		appendDone <- err
	}()
	<-appendEntered
	select {
	case err := <-appendDone:
		t.Fatalf("Event birth passed active Queue lease owner: %v", err)
	default:
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release Queue candidate: %v", err)
	}
	leased := <-leaseDone
	if leased.err != nil || len(leased.jobs) != 1 || leased.jobs[0].ID != seedJobs[0].id {
		t.Fatalf("lease predecessor = %#v/%v; want %s", leased.jobs, leased.err, seedJobs[0].id)
	}
	if err := <-appendDone; err != nil {
		t.Fatalf("AppendClientEvents after Queue lease commit: %v", err)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(readSessionEventLedgerRows(t, admin, sessionID)) != 2 || len(jobs) != 2 {
		t.Fatalf("post-lease atomic birth facts = events:%d jobs:%d; want 2/2", len(readSessionEventLedgerRows(t, admin, sessionID)), len(jobs))
	}
	if jobs[1].id == seedJobs[0].id || jobs[1].status != queue.StatusPending {
		t.Fatalf("post-lease Queue custody = %#v; want distinct pending successor", jobs[1])
	}
}

func waitForSessionEventLockWaiters(t testing.TB, admin *sql.DB, blockerPID int, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := admin.QueryRow(`SELECT count(*) FROM pg_stat_activity activity
			WHERE $1 = ANY(pg_blocking_pids(activity.pid))`, blockerPID).Scan(&count); err != nil {
			t.Fatalf("read SessionEvent lock waiters: %v", err)
		}
		if count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Queue Lease did not block behind exact candidate owner %d", blockerPID)
}

func TestAppendClientEventsRemainsDurableBehindLeasedInterruptBarrier(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	const sessionID = "sesn_event_interrupt_barrier"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	interrupt, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_barrier", AppendRequest{
		Events: []IncomingEvent{{Type: EventTypeUserInterrupt}},
	})
	if err != nil || len(interrupt.Data) != 1 {
		t.Fatalf("append interrupt = %#v/%v; want one durable event", interrupt, err)
	}
	leased := mustLeaseSessionEventJob(t, queueStore, sessionID, "bridge-interrupt")
	if runtimeInputKindFromQueueJob(t, leased) != RuntimeInputKindInterruptControl {
		t.Fatalf("first leased input = %s; want interrupt", runtimeInputKindFromQueueJob(t, leased))
	}

	message, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_message_behind_interrupt", messageAppendRequest("after interrupt"))
	if err != nil || len(message.Data) != 1 {
		t.Fatalf("append message behind interrupt = %#v/%v; want one durable event", message, err)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 2 {
		t.Fatalf("durable jobs behind interrupt = %#v; want interrupt and message", jobs)
	}
	assertSessionEventInboxMatchesQueue(t, admin, sessionID, jobs)
	if got, err := queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-blocked",
		MaxJobs: 1, LeaseDuration: time.Minute,
	}); err != nil || len(got) != 0 {
		t.Fatalf("lease behind active interrupt = %#v/%v; want none", got, err)
	}
	if acknowledged, err := queueStore.Ack(ctx, queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: leased.ID, LeaseToken: leased.LeaseToken,
	}); err != nil || !acknowledged {
		t.Fatalf("ack interrupt barrier = %t/%v; want true/nil", acknowledged, err)
	}
	follower := mustLeaseSessionEventJob(t, queueStore, sessionID, "bridge-follower")
	if runtimeInputKindFromQueueJob(t, follower) != RuntimeInputKindMessages {
		t.Fatalf("follower leased input = %s; want messages", runtimeInputKindFromQueueJob(t, follower))
	}
	if acknowledged, err := queueStore.Ack(ctx, queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: follower.ID, LeaseToken: follower.LeaseToken,
	}); err != nil || !acknowledged {
		t.Fatalf("ack message follower = %t/%v; want true/nil", acknowledged, err)
	}
	if got, err := queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-no-duplicate",
		MaxJobs: 1, LeaseDuration: time.Minute,
	}); err != nil || len(got) != 0 {
		t.Fatalf("lease after follower ACK = %#v/%v; want no duplicate", got, err)
	}
}

func mustLeaseSessionEventJob(t testing.TB, store *queue.PostgreSQLQueueStore, sessionID string, owner string) *queue.Job {
	t.Helper()
	jobs, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID:   workspace.DefaultID,
		Kinds:         []string{queue.KindRuntimeInput},
		LeaseOwner:    owner,
		MaxJobs:       1,
		LeaseDuration: time.Minute,
	})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease Session input for %s = %#v/%v; want one", sessionID, jobs, err)
	}
	return jobs[0]
}

func runtimeInputKindFromQueueJob(t testing.TB, job *queue.Job) string {
	t.Helper()
	var payload runtimeInputQueuePayload
	if err := json.Unmarshal(job.PayloadJSON, &payload); err != nil {
		t.Fatalf("decode Runtime input Queue payload: %v", err)
	}
	return payload.InputKind
}

func TestAppendClientEventsProducesContiguousRuntimeInputRanges(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	const sessionID = "sesn_event_contiguous_runtime_input"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_contiguous_runtime_input", AppendRequest{
		Events: []IncomingEvent{
			{Type: EventTypeUserMessage, Content: []TextContentBlock{{Type: ContentBlockTypeText, Text: "first"}}},
			{Type: EventTypeUserMessage, Content: []TextContentBlock{{Type: ContentBlockTypeText, Text: "second"}}},
		},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents contiguous messages: %v", err)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(result.Data) != 2 || len(jobs) != 1 {
		t.Fatalf("contiguous producer events/jobs = %d/%d; want 2/1", len(result.Data), len(jobs))
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindMessages, 0,
		[]string{result.Data[0].ID, result.Data[1].ID}, result.Data[0].Sequence, result.Data[1].Sequence)
	if result.Data[1].Sequence != result.Data[0].Sequence+1 {
		t.Fatalf("producer sequence = %d,%d; want contiguous", result.Data[0].Sequence, result.Data[1].Sequence)
	}
	assertSessionEventInboxMatchesQueue(t, admin, sessionID, jobs)
}

func TestAppendClientEventsBirthsOneMaximumReferenceRuntimeInput(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	const sessionID = "sesn_event_maximum_runtime_input"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	limits := DefaultLimits()
	limits.MaxEventsPerRequest = queue.MaxRuntimeInputEventRefsPerJob
	service := newSessionEventServiceForTest(runtime, WithLimits(limits))
	events := make([]IncomingEvent, queue.MaxRuntimeInputEventRefsPerJob)
	for index := range events {
		events[index] = IncomingEvent{Type: EventTypeUserMessage, Content: []TextContentBlock{{
			Type: ContentBlockTypeText, Text: fmt.Sprintf("message-%03d", index),
		}}}
	}
	result, err := service.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_maximum_runtime_input", AppendRequest{Events: events})
	if err != nil {
		t.Fatalf("AppendClientEvents maximum Runtime input: %v", err)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(result.Data) != queue.MaxRuntimeInputEventRefsPerJob || len(jobs) != 1 {
		t.Fatalf("maximum Runtime input birth = events %d jobs %d; want %d/1", len(result.Data), len(jobs), queue.MaxRuntimeInputEventRefsPerJob)
	}
	wantIDs := make([]string, len(result.Data))
	for index, event := range result.Data {
		if !strings.HasPrefix(event.ID, IDPrefix) {
			t.Fatalf("event %d id = %q; want production %q prefix", index, event.ID, IDPrefix)
		}
		wantIDs[index] = event.ID
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindMessages, 0, wantIDs, result.Data[0].Sequence, result.Data[len(result.Data)-1].Sequence)
	assertSessionEventInboxMatchesQueue(t, admin, sessionID, jobs)
}

func TestRuntimeInputSegmentsBreakAcrossNonRuntimeSequenceGaps(t *testing.T) {
	segments := runtimeInputSegments([]*Event{
		{ID: "evt_first", ThreadID: "thr_contiguous", Sequence: 1, Type: EventTypeUserMessage},
		{ID: "evt_non_runtime", ThreadID: "thr_contiguous", Sequence: 2, Type: "session.observation"},
		{ID: "evt_second", ThreadID: "thr_contiguous", Sequence: 3, Type: EventTypeUserMessage},
	})
	if len(segments) != 2 || len(segments[0].events) != 1 || len(segments[1].events) != 1 ||
		segments[0].events[0].ID != "evt_first" || segments[1].events[0].ID != "evt_second" {
		t.Fatalf("runtime input segments = %#v; want separate contiguous ranges", segments)
	}
}

func TestAppendClientEventsSessionInterruptTargetsOnlyMainThread(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_interrupt_main"
	mainThreadID := sessionEventMainThreadID(sessionID)
	publicChildID := "thread_interrupt_public_child"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, publicChildID, "subagent", "public", false)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, "thread_interrupt_archived", "subagent", "public", true)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, "thread_interrupt_reviewer", "approval_reviewer", "internal", false)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_main", AppendRequest{
		Events: []IncomingEvent{{Type: EventTypeUserInterrupt}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if len(result.Data) != 1 {
		t.Fatalf("result events = %d; want one main-thread interrupt", len(result.Data))
	}
	if result.Data[0].ThreadID != mainThreadID {
		t.Fatalf("interrupt target = %q; want main thread %q", result.Data[0].ThreadID, mainThreadID)
	}
	rows := readSessionEventLedgerRows(t, admin, sessionID)
	if len(rows) != 1 {
		t.Fatalf("ledger rows = %d; want 1", len(rows))
	}
	for index, row := range rows {
		if row.sequence != 1 || row.eventType != EventTypeUserInterrupt || row.payload != `{}` {
			t.Fatalf("ledger row %d = %+v; want first interrupt in its session-thread scope", index, row)
		}
		if row.processedAt.Valid {
			t.Fatalf("ledger row %d processed_at = %q; want NULL runtime input", index, row.processedAt.String)
		}
	}
	if rows[0].sessionThreadID != mainThreadID {
		t.Fatalf("ledger interrupt thread id = %q; want %q", rows[0].sessionThreadID, mainThreadID)
	}
	assertSessionEventStreamChanges(t, admin, sessionID, []sessionEventStreamChange{
		{eventID: result.Data[0].ID, sessionThreadID: mainThreadID, revision: 1, visibility: "public", sessionVisible: true},
	})
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want one main-thread interrupt runtime_input", jobs)
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindInterruptControl, 100, []string{result.Data[0].ID}, 1, 1)
	assertRuntimeInputQueueJobThread(t, jobs[0], mainThreadID)

	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, "thread_interrupt_added_after_first_admission", "subagent", "public", false)
	replay, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_main", AppendRequest{
		Events: []IncomingEvent{{Type: EventTypeUserInterrupt}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents replay after thread change: %v", err)
	}
	if len(replay.Data) != 1 || replay.Data[0].ID != result.Data[0].ID {
		t.Fatalf("interrupt replay = %+v; want original immutable main-thread response", replay.Data)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows after replay = %d; want 1", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 1 {
		t.Fatalf("queue jobs after replay = %d; want 1", got)
	}
}

func TestAppendClientEventsRejectsMessageToArchivedPrimaryThread(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_archived_primary"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_threads
		    SET archived_at = '2026-07-11T00:00:00Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3`,
		string(workspace.DefaultID), sessionID, sessionEventMainThreadID(sessionID)); err != nil {
		t.Fatalf("archive primary thread: %v", err)
	}
	service := newSessionEventServiceForTest(runtime)

	_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_archived_primary", AppendRequest{
		Events: []IncomingEvent{{Type: EventTypeUserMessage, Content: []TextContentBlock{{Type: ContentBlockTypeText, Text: "must not enter archived lane"}}}},
	})
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("AppendClientEvents archived primary err = %T %v; want ConflictError", err, err)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
		t.Fatalf("archived primary event rows = %d; want 0", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("archived primary queue jobs = %d; want 0", got)
	}
}

func TestAppendClientEventsReplaysAcceptedMessageAfterPrimaryArchive(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_replay_archived_primary"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)
	request := messageAppendRequest("accepted before archive")

	first, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_replay_archived_primary", request)
	if err != nil {
		t.Fatalf("AppendClientEvents first: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_threads SET archived_at = '2026-07-11T00:00:00Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND id = $3`,
		string(workspace.DefaultID), sessionID, sessionEventMainThreadID(sessionID)); err != nil {
		t.Fatalf("archive primary thread: %v", err)
	}
	replay, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_replay_archived_primary", request)
	if err != nil {
		t.Fatalf("AppendClientEvents replay: %v", err)
	}
	if len(first.Data) != 1 || len(replay.Data) != 1 || replay.Data[0].ID != first.Data[0].ID {
		t.Fatalf("replay = %+v; want original event %+v", replay.Data, first.Data)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows = %d; want one", got)
	}
	if _, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_fresh_archived_primary", request); err == nil {
		t.Fatal("fresh message entered archived primary lane")
	}
}

func TestAppendClientEventsExplicitUserInterruptTargetsPublicThread(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_interrupt_targeted"
	publicChildID := "thread_interrupt_targeted_child"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, publicChildID, "subagent", "public", false)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, "thread_interrupt_targeted_other", "subagent", "public", false)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_targeted", AppendRequest{
		Events: []IncomingEvent{{Type: EventTypeUserInterrupt, SessionThreadID: publicChildID}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents targeted interrupt: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].ThreadID != publicChildID {
		t.Fatalf("targeted interrupt events = %+v; want one child-thread event", result.Data)
	}
	rows := readSessionEventLedgerRows(t, admin, sessionID)
	if len(rows) != 1 || rows[0].sessionThreadID != publicChildID || rows[0].eventType != EventTypeUserInterrupt {
		t.Fatalf("ledger rows = %+v; want one targeted child interrupt", rows)
	}
	assertSessionEventStreamChanges(t, admin, sessionID, []sessionEventStreamChange{
		{eventID: result.Data[0].ID, sessionThreadID: publicChildID, revision: 1, visibility: "public", sessionVisible: false},
	})
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want one targeted interrupt runtime_input", jobs)
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindInterruptControl, 100, []string{result.Data[0].ID}, 1, 1)
	assertRuntimeInputQueueJobThread(t, jobs[0], publicChildID)
}

func TestAppendClientEventsRejectsInvalidExplicitInterruptTargetWithoutRows(t *testing.T) {
	for _, tc := range []struct {
		name       string
		targetID   string
		seedTarget func(t *testing.T, db *sql.DB, sessionID string, targetID string)
	}{
		{name: "missing", targetID: "thread_interrupt_missing"},
		{
			name:     "archived",
			targetID: "thread_interrupt_archived_explicit",
			seedTarget: func(t *testing.T, db *sql.DB, sessionID string, targetID string) {
				seedSessionEventThread(t, db, workspace.DefaultID, sessionID, targetID, "subagent", "public", true)
			},
		},
		{
			name:     "internal_reviewer",
			targetID: "thread_interrupt_internal_explicit",
			seedTarget: func(t *testing.T, db *sql.DB, sessionID string, targetID string) {
				seedSessionEventThread(t, db, workspace.DefaultID, sessionID, targetID, "approval_reviewer", "internal", false)
			},
		},
		{
			name:     "other_session",
			targetID: "thread_interrupt_other_session",
			seedTarget: func(t *testing.T, db *sql.DB, _ string, targetID string) {
				otherSessionID := "sesn_event_interrupt_other_session"
				seedSessionEventSession(t, db, workspace.DefaultID, otherSessionID)
				seedSessionEventThread(t, db, workspace.DefaultID, otherSessionID, targetID, "subagent", "public", false)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := newSessionEventStoreTestDB(t)
			ctx := context.Background()
			sessionID := "sesn_event_interrupt_invalid_" + tc.name
			seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
			seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
			if tc.seedTarget != nil {
				tc.seedTarget(t, admin, sessionID, tc.targetID)
			}
			service := newSessionEventServiceForTest(runtime)

			_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_invalid_"+tc.name, AppendRequest{
				Events: []IncomingEvent{{Type: EventTypeUserInterrupt, SessionThreadID: tc.targetID}},
			})
			if err == nil {
				t.Fatal("AppendClientEvents accepted invalid explicit interrupt target")
			}
			var notFound *NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("error = %T %v; want NotFoundError", err, err)
			}
			if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
				t.Fatalf("ledger rows = %d; want none", got)
			}
			if jobs := readSessionEventQueueJobs(t, admin, sessionID); len(jobs) != 0 {
				t.Fatalf("queue jobs = %#v; want none", jobs)
			}
		})
	}
}

func TestAppendClientEventsToolConfirmationResolvesPendingApproval(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_tool_confirmation"
	toolUseEventID := "sevt_tool_use_confirmation"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventPendingApproval(t, admin, workspace.DefaultID, sessionID, toolUseEventID)
	service := newSessionEventServiceForTest(runtime, WithClock(func() time.Time {
		return time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	}))

	result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_tool_confirmation", AppendRequest{
		Events: []IncomingEvent{{
			Type:        EventTypeUserToolConfirmation,
			ToolUseID:   toolUseEventID,
			Result:      ToolConfirmationResultDeny,
			DenyMessage: "not now",
		}},
	})
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	if len(result.Data) != 1 || result.Data[0].Type != EventTypeUserToolConfirmation {
		t.Fatalf("result = %#v; want one tool confirmation event", result.Data)
	}
	assertSessionEventLedger(t, admin, sessionID, []storedSessionEvent{
		{sequence: 1, eventType: EventTypeUserToolConfirmation, payload: `{"tool_use_id":"sevt_tool_use_confirmation","result":"deny","deny_message":"not now"}`},
	})
	assertSessionEventPendingApprovalDecision(t, admin, sessionID, toolUseEventID, "resolving", "deny", "not now")
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want one tool confirmation runtime_input", jobs)
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindToolConfirmation, 0, []string{result.Data[0].ID}, 1, 1)
	assertSessionEventStreamChanges(t, admin, sessionID, []sessionEventStreamChange{
		{eventID: result.Data[0].ID, sessionThreadID: sessionEventMainThreadID(sessionID), revision: 1, visibility: "public", sessionVisible: true},
	})
}

func TestAppendClientEventsRejectsNonPendingToolConfirmationWithoutRows(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		sessionID  string
		toolUseID  string
		status     string
		wantErrMsg string
	}{
		{
			name:       "resolving",
			sessionID:  "sesn_event_tool_confirmation_resolving",
			toolUseID:  "sevt_tool_use_resolving",
			status:     "resolving",
			wantErrMsg: "pending approval is not pending",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime, admin := newSessionEventStoreTestDB(t)
			ctx := context.Background()
			seedSessionEventSession(t, admin, workspace.DefaultID, testCase.sessionID)
			seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, testCase.sessionID)
			seedSessionEventPendingApproval(t, admin, workspace.DefaultID, testCase.sessionID, testCase.toolUseID)
			if testCase.status != "pending" {
				if _, err := admin.ExecContext(ctx,
					`UPDATE session_pending_tool_uses
					    SET status = $4
					  WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`,
					string(workspace.DefaultID),
					testCase.sessionID,
					testCase.toolUseID,
					testCase.status,
				); err != nil {
					t.Fatalf("mark pending approval %s: %v", testCase.status, err)
				}
			}
			service := newSessionEventServiceForTest(runtime, WithClock(func() time.Time {
				return time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
			}))

			_, err := service.AppendClientEvents(ctx, workspace.DefaultID, testCase.sessionID, "idem_"+testCase.name, AppendRequest{
				Events: []IncomingEvent{{
					Type:      EventTypeUserToolConfirmation,
					ToolUseID: testCase.toolUseID,
					Result:    ToolConfirmationResultAllow,
				}},
			})
			if err == nil {
				t.Fatal("AppendClientEvents accepted terminally invalid tool confirmation")
			}
			var conflict *ConflictError
			if !errors.As(err, &conflict) || conflict.Message != testCase.wantErrMsg {
				t.Fatalf("error = %T %v; want ConflictError %q", err, err, testCase.wantErrMsg)
			}
			if got := len(readSessionEventLedgerRows(t, admin, testCase.sessionID)); got != 0 {
				t.Fatalf("ledger rows = %d; want none", got)
			}
			if got := len(readSessionEventQueueJobs(t, admin, testCase.sessionID)); got != 0 {
				t.Fatalf("queue jobs = %d; want none", got)
			}
			assertSessionEventPendingApprovalStatus(t, admin, testCase.sessionID, testCase.toolUseID, testCase.status)
		})
	}
}

func TestAppendClientEventsRejectsMessageWhileApprovalPendingWithoutRows(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_pending_approval_blocks_message"
	toolUseEventID := "sevt_tool_use_blocks_message"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventPendingApproval(t, admin, workspace.DefaultID, sessionID, toolUseEventID)
	service := newSessionEventServiceForTest(runtime, WithClock(func() time.Time {
		return time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	}))

	_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_message_blocked_by_approval", messageAppendRequest("blocked"))
	if err == nil {
		t.Fatal("AppendClientEvents accepted user.message while approval is pending")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error = %T %v; want ConflictError", err, err)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
		t.Fatalf("ledger rows = %d; want none", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("queue jobs = %d; want none", got)
	}
	assertSessionEventPendingApprovalStatus(t, admin, sessionID, toolUseEventID, "pending")
}

func TestAppendClientEventsAdmitsMessageWhileRunningWithApprovalPending(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_running_with_pending_approval"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventPendingApproval(t, admin, workspace.DefaultID, sessionID, "sevt_running_pending_approval")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_runtime_status SET status = 'running', idle_since = NULL
		  WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), sessionID); err != nil {
		t.Fatalf("mark runtime running: %v", err)
	}
	service := newSessionEventServiceForTest(runtime, WithClock(func() time.Time {
		return time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	}))

	result, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_running_with_pending_approval", messageAppendRequest("admitted"))
	if err != nil {
		t.Fatalf("AppendClientEvents: %v", err)
	}
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(result.Data) != 1 || len(jobs) != 1 {
		t.Fatalf("admission = %+v jobs=%d; want one event and job", result.Data, len(jobs))
	}
}

func TestAppendClientEventsReplaysSameCanonicalRequestWithoutDuplicatingQueueJob(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_idempotency_replay"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)
	firstBody := []byte(`{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`)
	replayBody := []byte(`{
		"events": [
			{
				"content": [{"text": "hello", "type": "text"}],
				"type": "user.message"
			}
		]
	}`)
	firstRequest, err := DecodeAppendRequest(firstBody)
	if err != nil {
		t.Fatalf("decode first request: %v", err)
	}
	replayRequest, err := DecodeAppendRequest(replayBody)
	if err != nil {
		t.Fatalf("decode replay request: %v", err)
	}

	firstResult, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, firstRequest)
	if err != nil {
		t.Fatalf("first AppendClientEvents: %v", err)
	}
	replayResult, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, replayRequest)
	if err != nil {
		t.Fatalf("replay AppendClientEvents: %v", err)
	}

	if len(firstResult.Data) != 1 || len(replayResult.Data) != 1 || firstResult.Data[0].ID != replayResult.Data[0].ID {
		t.Fatalf("replay result = %#v; want original response event %#v", replayResult.Data, firstResult.Data)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows = %d; want original append only", got)
	}
	assertSessionEventIdempotencyRowCount(t, admin, sessionID, 1)
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want replay to reuse original admitted job only", jobs)
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindMessages, 0, []string{firstResult.Data[0].ID}, 1, 1)
}

func TestAppendClientEventsReplaysUserInterruptWithoutDuplicatingQueueJob(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_interrupt_replay"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)
	firstRequest, err := DecodeAppendRequest([]byte(`{"events":[{"type":"user.interrupt"}]}`))
	if err != nil {
		t.Fatalf("decode first request: %v", err)
	}
	replayRequest, err := DecodeAppendRequest([]byte(`{
		"events": [
			{"type": "user.interrupt"}
		]
	}`))
	if err != nil {
		t.Fatalf("decode replay request: %v", err)
	}

	firstResult, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "  idem_interrupt_replay  ", firstRequest)
	if err != nil {
		t.Fatalf("first AppendClientEvents: %v", err)
	}
	replayResult, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_replay", replayRequest)
	if err != nil {
		t.Fatalf("replay AppendClientEvents: %v", err)
	}

	if len(firstResult.Data) != 1 || len(replayResult.Data) != 1 {
		t.Fatalf("result lengths = %d/%d; want one event each", len(firstResult.Data), len(replayResult.Data))
	}
	if firstResult.Data[0].ID != replayResult.Data[0].ID || replayResult.Data[0].Type != EventTypeUserInterrupt {
		t.Fatalf("replay event = %#v; want original interrupt echo %#v", replayResult.Data[0], firstResult.Data[0])
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows = %d; want replay to append zero rows", got)
	}
	assertSessionEventIdempotencyMetadata(t, admin, sessionID, "idem_interrupt_replay")
	jobs := readSessionEventQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want replay to reuse original admitted interrupt job only", jobs)
	}
	assertRuntimeInputQueueJob(t, jobs[0], RuntimeInputKindInterruptControl, 100, []string{firstResult.Data[0].ID}, 1, 1)
}

func TestAppendClientEventsIdempotencyHashUsesSessionInterruptSelectorBeforeMainThreadResolution(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_interrupt_target_hash"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, "thread_interrupt_hash_child_1", "subagent", "public", false)
	service := newSessionEventServiceForTest(runtime)
	request, err := DecodeAppendRequest([]byte(`{"events":[{"type":"user.interrupt"}]}`))
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}

	firstResult, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_target_hash", request)
	if err != nil {
		t.Fatalf("first AppendClientEvents: %v", err)
	}
	if len(firstResult.Data) != 1 {
		t.Fatalf("first interrupt events = %d; want one main-thread interrupt", len(firstResult.Data))
	}
	seedSessionEventThread(t, admin, workspace.DefaultID, sessionID, "thread_interrupt_hash_child_2", "subagent", "public", false)

	replay, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_interrupt_target_hash", request)
	if err != nil {
		t.Fatalf("AppendClientEvents replay after target set changed: %v", err)
	}
	if len(replay.Data) != 1 || replay.Data[0].ID != firstResult.Data[0].ID {
		t.Fatalf("replay events = %+v; want original main-thread response", replay.Data)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows = %d; want original interrupt only", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 1 {
		t.Fatalf("queue jobs = %d; want original interrupt job only", got)
	}
}

func TestAppendClientEventsRejectsSameKeyDifferentRequestWithoutAppendOrQueueJob(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_idempotency_conflict"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	service := newSessionEventServiceForTest(runtime)

	if _, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("first")); err != nil {
		t.Fatalf("first AppendClientEvents: %v", err)
	}
	_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("different"))
	if err == nil {
		t.Fatal("AppendClientEvents accepted same idempotency key for different request")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("error = %T %v; want ConflictError", err, err)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 1 {
		t.Fatalf("ledger rows = %d; want only first append", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 1 {
		t.Fatalf("queue jobs = %d; want only first append job", got)
	}
}

func TestAppendClientEventsRollsBackWhenIdempotencyWriteFails(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_idempotency_rollback"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	store.beforeIdempotencyInsert = func() error { return errors.New("injected idempotency write failure") }
	service := NewService(store)

	if _, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("rollback")); err == nil {
		t.Fatal("AppendClientEvents succeeded despite injected idempotency failure")
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
		t.Fatalf("ledger rows = %d; want rollback", got)
	}
	assertSessionEventIdempotencyRowCount(t, admin, sessionID, 0)
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("queue jobs = %d; want rollback", got)
	}
}

func TestAppendClientEventsRollsBackWhenQueueJobWriteFails(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_queue_failure"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionEventRunnableRuntime(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	store.beforeQueueJobInsert = func() error { return errors.New("injected queue write failure") }
	service := NewService(store)

	if _, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("rollback")); err == nil {
		t.Fatal("AppendClientEvents succeeded despite injected queue write failure")
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
		t.Fatalf("ledger rows = %d; want rollback", got)
	}
	assertSessionEventIdempotencyRowCount(t, admin, sessionID, 0)
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("queue jobs = %d; want rollback", got)
	}
	assertSessionEventInboxRowCount(t, admin, sessionID, 0)
}

func TestAppendClientEventsRequiresRuntimeStatusFence(t *testing.T) {
	runtime, admin := newSessionEventStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_event_missing_runtime_status"
	seedSessionEventSession(t, admin, workspace.DefaultID, sessionID)
	if _, err := admin.ExecContext(ctx,
		`DELETE FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("delete runtime status: %v", err)
	}
	service := NewService(NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))

	_, err := service.AppendClientEvents(ctx, workspace.DefaultID, sessionID, testSessionEventIdempotencyKey, messageAppendRequest("blocked"))
	if err == nil {
		t.Fatal("AppendClientEvents accepted a session without runtime status fence")
	}
	if !strings.Contains(err.Error(), "session_runtime_status invariant missing") {
		t.Fatalf("error = %T %v; want missing runtime status invariant", err, err)
	}
	if got := len(readSessionEventLedgerRows(t, admin, sessionID)); got != 0 {
		t.Fatalf("ledger rows = %d; want no event without runtime status fence", got)
	}
	if got := len(readSessionEventQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("queue jobs = %d; want none without runtime status fence", got)
	}
}

type storedSessionEvent struct {
	sequence  int64
	eventType string
	payload   string
}

type sessionEventStreamChange struct {
	eventID         string
	sessionThreadID string
	revision        int64
	visibility      string
	sessionVisible  bool
}

// newSessionEventStoreTestDB follows the internal/session skip-gate convention:
// DB-backed session-event tests skip cleanly when TETRAL_TEST_DATABASE_URL is
// unset rather than hard-failing like the internal/storage policy.
func newSessionEventStoreTestDB(t *testing.T) (runtime, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func newSessionEventServiceForTest(db *sql.DB, options ...Option) *Service {
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	return NewService(store, options...)
}

func newSessionEventServiceWithFilesForTest(db *sql.DB, blobStore blob.BlobStore, options ...Option) *Service {
	client := dbconnect.NewClientForTesting(db)
	fileStore := enginefiles.NewPostgreSQLStore(client, blobStore)
	store := NewPostgreSQLStore(client, WithFileAttachmentValidator(fileStore))
	return NewService(store, options...)
}

func messageAppendRequest(text string) AppendRequest {
	return AppendRequest{Events: []IncomingEvent{{Type: EventTypeUserMessage, Content: []TextContentBlock{{Type: ContentBlockTypeText, Text: text}}}}}
}

func sessionEventTestIdempotencyKey(parts ...any) string {
	return fmt.Sprintf("idem_sessionevent_%v", parts)
}

func assertSessionEventIdempotencyRowCount(t *testing.T, db *sql.DB, sessionID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*)
		   FROM session_event_idempotency_keys
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&got); err != nil {
		t.Fatalf("query idempotency rows: %v", err)
	}
	if got != want {
		t.Fatalf("idempotency rows = %d; want %d", got, want)
	}
}

func assertSessionEventIdempotencyMetadata(t *testing.T, db *sql.DB, sessionID string, idempotencyKey string) {
	t.Helper()
	var keyDigest []byte
	var requestHash []byte
	if err := db.QueryRowContext(context.Background(),
		`SELECT idempotency_key_digest, canonical_request_hash
		   FROM session_event_idempotency_keys
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&keyDigest, &requestHash); err != nil {
		t.Fatalf("query idempotency metadata: %v", err)
	}
	wantKeyDigest := sha256.Sum256([]byte(strings.TrimSpace(idempotencyKey)))
	if !bytes.Equal(keyDigest, wantKeyDigest[:]) {
		t.Fatalf("idempotency key digest = %x; want sha256(trimmed key) %x", keyDigest, wantKeyDigest)
	}
	if bytes.Equal(keyDigest, []byte(idempotencyKey)) {
		t.Fatalf("idempotency key digest persisted raw key bytes %q", idempotencyKey)
	}
	if len(requestHash) != sha256.Size {
		t.Fatalf("canonical request hash length = %d; want %d", len(requestHash), sha256.Size)
	}
	if bytes.Equal(requestHash, keyDigest) || bytes.Equal(requestHash, []byte(idempotencyKey)) {
		t.Fatalf("canonical request hash appears to contain key material: hash=%x keyDigest=%x", requestHash, keyDigest)
	}
}

func assertSessionEventStreamChanges(t *testing.T, db *sql.DB, sessionID string, want []sessionEventStreamChange) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT event_id, session_thread_id, revision, visibility, session_visible
		   FROM session_event_stream_changes
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY stream_position`,
		string(workspace.DefaultID),
		sessionID,
	)
	if err != nil {
		t.Fatalf("query stream changes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []sessionEventStreamChange
	for rows.Next() {
		var change sessionEventStreamChange
		var sessionThreadID sql.NullString
		if err := rows.Scan(&change.eventID, &sessionThreadID, &change.revision, &change.visibility, &change.sessionVisible); err != nil {
			t.Fatalf("scan stream change: %v", err)
		}
		change.sessionThreadID = sessionThreadID.String
		got = append(got, change)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("stream changes: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("stream changes = %#v; want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("stream change %d = %#v; want %#v", index, got[index], want[index])
		}
	}
	for _, change := range got {
		var latest int64
		if err := db.QueryRowContext(context.Background(),
			`SELECT latest_stream_position
			   FROM session_events
			  WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3`,
			string(workspace.DefaultID),
			sessionID,
			change.eventID,
		).Scan(&latest); err != nil {
			t.Fatalf("query latest stream position: %v", err)
		}
		if latest == 0 {
			t.Fatalf("event %s latest_stream_position was not backfilled", change.eventID)
		}
	}
}

type sessionEventQueueJobRow struct {
	id             string
	kind           string
	partitionKey   string
	dedupeKey      string
	status         string
	payloadVersion int
	payloadJSON    string
	priority       int
	attemptCount   int
	maxAttempts    int
}

func readSessionEventQueueJobs(t *testing.T, db *sql.DB, sessionID string) []sessionEventQueueJobRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, kind, partition_key, dedupe_key, status, payload_version,
		        payload_json, priority, attempt_count, max_attempts
		   FROM queue_jobs
		  WHERE workspace_id = $1 AND partition_key = $2
		  ORDER BY created_at, id`,
		string(workspace.DefaultID),
		runtimeInputPartitionKey(workspace.DefaultID, sessionID),
	)
	if err != nil {
		t.Fatalf("query queue jobs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []sessionEventQueueJobRow
	for rows.Next() {
		var row sessionEventQueueJobRow
		if err := rows.Scan(
			&row.id,
			&row.kind,
			&row.partitionKey,
			&row.dedupeKey,
			&row.status,
			&row.payloadVersion,
			&row.payloadJSON,
			&row.priority,
			&row.attemptCount,
			&row.maxAttempts,
		); err != nil {
			t.Fatalf("scan queue job: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("queue jobs: %v", err)
	}
	return got
}

func assertSessionEventInboxMatchesQueue(t *testing.T, db *sql.DB, sessionID string, jobs []sessionEventQueueJobRow) {
	t.Helper()
	for _, job := range jobs {
		var payload runtimeInputQueuePayload
		if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
			t.Fatalf("decode runtime input queue payload: %v", err)
		}
		var threadID, inputKind, eventIDsJSON, status string
		var sequenceFrom, sequenceTo sql.NullInt64
		if err := db.QueryRowContext(context.Background(), `SELECT session_thread_id,input_kind,event_ids_json,sequence_from,sequence_to,status
			FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`,
			string(workspace.DefaultID), sessionID, payload.RuntimeInputID,
		).Scan(&threadID, &inputKind, &eventIDsJSON, &sequenceFrom, &sequenceTo, &status); err != nil {
			t.Fatalf("read runtime inbox birth for %s: %v", payload.RuntimeInputID, err)
		}
		var eventIDs []string
		if err := json.Unmarshal([]byte(eventIDsJSON), &eventIDs); err != nil {
			t.Fatalf("decode runtime inbox event ids: %v", err)
		}
		if threadID != payload.SessionThreadID || inputKind != payload.InputKind || status != "queued" ||
			!equalSessionEventStringSlices(eventIDs, payload.EventIDs) ||
			!sequenceFrom.Valid || sequenceFrom.Int64 != payload.SequenceFrom ||
			!sequenceTo.Valid || sequenceTo.Int64 != payload.SequenceTo {
			t.Fatalf("runtime inbox birth = thread %q kind %q events %v sequence %v..%v status %q; want queue payload %#v and queued",
				threadID, inputKind, eventIDs, sequenceFrom, sequenceTo, status, payload)
		}
	}
	assertSessionEventInboxRowCount(t, db, sessionID, len(jobs))
}

func assertSessionEventInboxRowCount(t *testing.T, db *sql.DB, sessionID string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2`, string(workspace.DefaultID), sessionID).Scan(&got); err != nil {
		t.Fatalf("count runtime inbox rows: %v", err)
	}
	if got != want {
		t.Fatalf("runtime inbox rows = %d; want %d", got, want)
	}
}

func assertRuntimeInputQueueJob(t *testing.T, job sessionEventQueueJobRow, inputKind string, priority int, eventIDs []string, sequenceFrom int64, sequenceTo int64) {
	t.Helper()
	if !strings.HasPrefix(job.id, queue.JobIDPrefix) {
		t.Fatalf("queue job id = %q; want %s prefix", job.id, queue.JobIDPrefix)
	}
	if job.kind != queue.KindRuntimeInput || job.status != "pending" || job.payloadVersion != 1 {
		t.Fatalf("queue job control fields = %#v; want pending runtime_input payload v1", job)
	}
	if !strings.HasPrefix(job.dedupeKey, "runtime_input:"+string(workspace.DefaultID)+":") {
		t.Fatalf("dedupe key = %q; want runtime_input workspace/session scoped key", job.dedupeKey)
	}
	if job.priority != priority {
		t.Fatalf("priority = %d; want %d", job.priority, priority)
	}
	if job.attemptCount != 0 || job.maxAttempts != 0 {
		t.Fatalf("attempt fields = %d/%d; want 0/0 with Queue-owned default resolution", job.attemptCount, job.maxAttempts)
	}
	if strings.Contains(job.payloadJSON, "hello") || strings.Contains(job.payloadJSON, "wait") || strings.Contains(job.payloadJSON, "rollback") {
		t.Fatalf("queue payload copies user content: %s", job.payloadJSON)
	}
	var payload runtimeInputQueuePayload
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime_input payload: %v", err)
	}
	if payload.WorkspaceID != string(workspace.DefaultID) || payload.InputKind != inputKind {
		t.Fatalf("payload identity = %#v; want workspace %s input_kind %s", payload, workspace.DefaultID, inputKind)
	}
	if payload.SessionThreadID == "" {
		t.Fatalf("payload missing session_thread_id: %#v", payload)
	}
	if !strings.HasPrefix(payload.RuntimeInputID, RuntimeInputIDPrefix) {
		t.Fatalf("runtime_input_id = %q; want %s prefix", payload.RuntimeInputID, RuntimeInputIDPrefix)
	}
	if payload.SequenceFrom != sequenceFrom || payload.SequenceTo != sequenceTo {
		t.Fatalf("sequence range = %d..%d; want %d..%d", payload.SequenceFrom, payload.SequenceTo, sequenceFrom, sequenceTo)
	}
	if !equalSessionEventStringSlices(payload.EventIDs, eventIDs) {
		t.Fatalf("event_ids = %v; want %v", payload.EventIDs, eventIDs)
	}
	wantDedupeKey := runtimeInputDedupeKey(workspace.DefaultID, payload.SessionID, payload.RuntimeInputID)
	if job.dedupeKey != wantDedupeKey {
		t.Fatalf("dedupe key = %q; want %q", job.dedupeKey, wantDedupeKey)
	}
}

func assertRuntimeInputQueueJobThread(t *testing.T, job sessionEventQueueJobRow, threadID string) {
	t.Helper()
	var payload runtimeInputQueuePayload
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime_input payload: %v", err)
	}
	if payload.SessionThreadID != threadID {
		t.Fatalf("runtime_input session_thread_id = %q; want %q", payload.SessionThreadID, threadID)
	}
}

func findRuntimeInputQueueJob(t *testing.T, jobs []sessionEventQueueJobRow, inputKind string) sessionEventQueueJobRow {
	t.Helper()
	for _, job := range jobs {
		if job.kind != queue.KindRuntimeInput {
			continue
		}
		var payload runtimeInputQueuePayload
		if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
			t.Fatalf("decode runtime_input payload: %v", err)
		}
		if payload.InputKind == inputKind {
			return job
		}
	}
	t.Fatalf("queue jobs = %#v; missing input_kind %s", jobs, inputKind)
	return sessionEventQueueJobRow{}
}

func equalSessionEventStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if a[index] != b[index] {
			return false
		}
	}
	return true
}

func assertSessionEventLedger(t *testing.T, db *sql.DB, sessionID string, want []storedSessionEvent) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT sequence, type, payload_json, processed_at
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY sequence`,
		string(workspace.DefaultID),
		sessionID,
	)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []storedSessionEvent
	for rows.Next() {
		var event storedSessionEvent
		var processedAt sql.NullString
		if err := rows.Scan(&event.sequence, &event.eventType, &event.payload, &processedAt); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		if processedAt.Valid {
			t.Fatalf("processed_at = %q; want NULL", processedAt.String)
		}
		got = append(got, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("event rows: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("events = %#v; want %#v", got, want)
	}
	for index := range got { //nolint:gosec // Length equality is checked immediately above.
		gotEvent := got[index]
		wantEvent := want[index] //nolint:gosec // Length equality is checked immediately above.
		if gotEvent.sequence != wantEvent.sequence || gotEvent.eventType != wantEvent.eventType {
			t.Fatalf("event[%d] = %#v; want %#v", index, gotEvent, wantEvent)
		}
		assertJSONEqual(t, []byte(gotEvent.payload), wantEvent.payload)
	}
}

func assertJSONEqual(t *testing.T, raw []byte, want string) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(raw, &gotValue); err != nil {
		t.Fatalf("json %s: %v", raw, err)
	}
	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("want json %s: %v", want, err)
	}
	gotBytes, err := json.Marshal(gotValue)
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wantBytes, err := json.Marshal(wantValue)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if string(gotBytes) != string(wantBytes) {
		t.Fatalf("json = %s; want %s", gotBytes, wantBytes)
	}
}

type sessionEventProductionFile struct {
	name   string
	source string
}

func readSessionEventProductionFiles(t *testing.T) []sessionEventProductionFile {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read sessionevent package: %v", err)
	}
	var files []sessionEventProductionFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Clean(entry.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		files = append(files, sessionEventProductionFile{name: path, source: string(body)})
	}
	return files
}

func seedSessionEventSession(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) {
	t.Helper()
	ctx := context.Background()
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	threadID := sessionEventMainThreadID(sessionID)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), "workspace-"+sessionID); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $3, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), agentID, "agent-"+sessionID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')`,
		string(workspaceID), agentVersionID, agentID, "hash-"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), environmentID, "environment-"+sessionID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at)
		 VALUES ($1, $2, $3, 'session', 'idle', 'active', $4, 1, $5, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, threadID, agentID, environmentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, 'main', 'public', 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), threadID, sessionID); err != nil {
		t.Fatalf("seed main thread: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, idle_since, created_at, updated_at
		) VALUES ($1, $2, 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
}

func seedSessionEventWorkspace(t *testing.T, db *sql.DB, workspaceID workspace.ID) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID),
	); err != nil {
		t.Fatalf("seed media workspace: %v", err)
	}
}

func seedSessionEventFile(
	t *testing.T,
	db *sql.DB,
	blobStore blob.BlobStore,
	workspaceID workspace.ID,
	fileID string,
	objectID string,
	filename string,
	mimeType string,
	body []byte,
	sizeBytes int64,
	pdfPageCount any,
) {
	t.Helper()
	blobKey := "files/" + string(workspaceID) + "/" + objectID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (
			object_id, workspace_id, blob_key, size_bytes, sha256, pdf_page_count, created_at
		) VALUES ($1, $2, $3, $4, 'sha', $5, '2026-01-01T00:00:00Z')`,
		objectID, string(workspaceID), blobKey, sizeBytes, pdfPageCount,
	); err != nil {
		t.Fatalf("seed media file object: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (
			file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at
		) VALUES ($1, $2, $3, $4, $5, false, '2026-01-01T00:00:00Z')`,
		fileID, string(workspaceID), objectID, filename, mimeType,
	); err != nil {
		t.Fatalf("seed media file identity: %v", err)
	}
	if body != nil {
		if err := blobStore.Put(context.Background(), blobKey, bytes.NewReader(body), int64(len(body))); err != nil {
			t.Fatalf("seed media blob: %v", err)
		}
	}
}

func minimalSessionEventPDF(pageCount int) []byte {
	var body strings.Builder
	body.WriteString("%PDF-1.4\n")
	offsets := make([]int, 3+pageCount)
	writeObject := func(number int, value string) {
		offsets[number] = body.Len()
		fmt.Fprintf(&body, "%d 0 obj\n%s\nendobj\n", number, value)
	}
	writeObject(1, "<< /Type /Catalog /Pages 2 0 R >>")
	kids := make([]string, 0, pageCount)
	for index := 0; index < pageCount; index++ {
		kids = append(kids, fmt.Sprintf("%d 0 R", 3+index))
	}
	writeObject(2, fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), pageCount))
	for index := 0; index < pageCount; index++ {
		writeObject(3+index, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>")
	}
	xrefOffset := body.Len()
	fmt.Fprintf(&body, "xref\n0 %d\n", len(offsets))
	body.WriteString("0000000000 65535 f \n")
	for number := 1; number < len(offsets); number++ {
		fmt.Fprintf(&body, "%010d 00000 n \n", offsets[number])
	}
	fmt.Fprintf(&body, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xrefOffset)
	return []byte(body.String())
}

func seedSessionEventThread(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, threadID string, role string, visibility string, archived bool) {
	t.Helper()
	var archivedAt any
	if archived {
		archivedAt = "2026-01-02T00:00:00Z"
	}
	var taskName any
	if role == "subagent" {
		taskName = "task_" + threadID
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name, created_at, last_active_at, archived_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'idle', $7, '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z', $8, '2026-01-01T00:01:00Z')`,
		string(workspaceID),
		threadID,
		sessionID,
		sessionEventMainThreadID(sessionID),
		role,
		visibility,
		taskName,
		archivedAt,
	); err != nil {
		t.Fatalf("seed session event thread: %v", err)
	}
}

func seedSessionEventRuntimeStatus(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, cleanup_after, cleanup_enqueued_at, cleanup_job_id, created_at, updated_at
		) VALUES ($1, $2, 'idle', '2026-01-02T00:00:00Z', '2026-01-02T00:01:00Z', 'qjob_stale_cleanup', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		ON CONFLICT (workspace_id, session_id) DO UPDATE
		    SET status = EXCLUDED.status,
		        cleanup_after = EXCLUDED.cleanup_after,
		        cleanup_enqueued_at = EXCLUDED.cleanup_enqueued_at,
		        cleanup_job_id = EXCLUDED.cleanup_job_id,
		        updated_at = EXCLUDED.updated_at`,
		string(workspaceID), sessionID); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
}

func seedSessionEventRunnableRuntime(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) {
	t.Helper()
	seedSessionEventRuntimeStatus(t, db, workspaceID, sessionID)
}

func seedSessionEventPendingApproval(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, toolUseEventID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'dangerous_tool', '{}', 'pending', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID),
		sessionID,
		sessionEventMainThreadID(sessionID),
		toolUseEventID,
		"call_"+toolUseEventID,
	); err != nil {
		t.Fatalf("seed pending approval: %v", err)
	}
}

func assertSessionEventPendingApprovalStatus(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_pending_tool_uses
		  WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`,
		string(workspace.DefaultID),
		sessionID,
		toolUseEventID,
	).Scan(&got); err != nil {
		t.Fatalf("query pending approval status: %v", err)
	}
	if got != want {
		t.Fatalf("pending approval status = %q; want %q", got, want)
	}
}

func assertSessionEventPendingApprovalDecision(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string, wantStatus string, wantDecision string, wantDenyMessage string) {
	t.Helper()
	var gotStatus string
	var gotDecision sql.NullString
	var gotDenyMessage sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, decision, deny_message
		   FROM session_pending_tool_uses
		  WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`,
		string(workspace.DefaultID),
		sessionID,
		toolUseEventID,
	).Scan(&gotStatus, &gotDecision, &gotDenyMessage); err != nil {
		t.Fatalf("query pending approval decision: %v", err)
	}
	if gotStatus != wantStatus || !gotDecision.Valid || gotDecision.String != wantDecision {
		t.Fatalf("pending approval status/decision = %q/%v; want %q/%q", gotStatus, gotDecision, wantStatus, wantDecision)
	}
	if wantDenyMessage == "" {
		if gotDenyMessage.Valid {
			t.Fatalf("pending approval deny_message = %v; want null", gotDenyMessage)
		}
		return
	}
	if !gotDenyMessage.Valid || gotDenyMessage.String != wantDenyMessage {
		t.Fatalf("pending approval deny_message = %v; want %q", gotDenyMessage, wantDenyMessage)
	}
}

func sessionEventMainThreadID(sessionID string) string {
	return "thread_" + sessionID
}

func assertSessionEventLedgerHasNoSourceColumn(t *testing.T, db *sql.DB) {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT EXISTS (
			SELECT 1
			  FROM information_schema.columns
			 WHERE table_schema = current_schema()
			   AND table_name = 'session_events'
			   AND column_name = 'source'
		)`).Scan(&exists); err != nil {
		t.Fatalf("query session_events source column: %v", err)
	}
	if exists {
		t.Fatal("session_events.source column exists; append must write worker-neutral ledger rows")
	}
}

type ledgerRow struct {
	sequence        int64
	sessionThreadID string
	eventType       string
	payload         string
	processedAt     sql.NullString
}

func readSessionEventLedgerRows(t *testing.T, db *sql.DB, sessionID string) []ledgerRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT sequence, COALESCE(session_thread_id, ''), type, payload_json, processed_at
			   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY latest_stream_position, event_id`,
		string(workspace.DefaultID),
		sessionID,
	)
	if err != nil {
		t.Fatalf("query ledger rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []ledgerRow
	for rows.Next() {
		var row ledgerRow
		if err := rows.Scan(&row.sequence, &row.sessionThreadID, &row.eventType, &row.payload, &row.processedAt); err != nil {
			t.Fatalf("scan ledger row: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("ledger rows: %v", err)
	}
	return got
}

func setSessionEventSessionState(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, status string, lifecycleState string, archivedAt *time.Time) {
	t.Helper()
	var archivedValue any
	if archivedAt != nil {
		archivedValue = archivedAt.UTC().Format(time.RFC3339Nano)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE sessions
		    SET status = $1, lifecycle_state = $2, archived_at = $3
		  WHERE workspace_id = $4 AND id = $5`,
		status, lifecycleState, archivedValue, string(workspaceID), sessionID); err != nil {
		t.Fatalf("set session state: %v", err)
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
