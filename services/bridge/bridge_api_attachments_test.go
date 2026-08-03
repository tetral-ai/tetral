package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge attachments protocol-family boundary.

func TestPostgreSQLBridgeAPIStoreResolveTransientAttachmentReadsActiveBlobWithScope(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_resolve", "thr_bridge_attachment_resolve")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_resolve", "bind_bridge_attachment_resolve", 1, "pod_uid_attachment_resolve")

	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_resolve", "thr_bridge_attachment_resolve", "bind_bridge_attachment_resolve", 1, "pod_uid_attachment_resolve")
	created := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_resolve_write_1", "sevt_attachment_resolve", []byte("image-bytes"))

	resolved, err := store.ResolveTransientAttachment(context.Background(), &bridgev1.ResolveTransientAttachmentRequest{
		Scope:                scope,
		AttachmentRef:        created.GetAttachmentRef(),
		SourceToolUseEventId: "sevt_attachment_resolve",
	})
	if err != nil {
		t.Fatalf("ResolveTransientAttachment: %v", err)
	}
	if string(resolved.GetResolved().GetData()) != "image-bytes" || resolved.GetResolved().GetExpiresAt() != "2026-01-01T12:15:00Z" {
		t.Fatalf("resolved data/expires = %q/%q; want bytes and TTL", string(resolved.GetResolved().GetData()), resolved.GetResolved().GetExpiresAt())
	}
	if resolved.GetResolved().GetAttachment().GetFilename() != "attachment_resolve_write_1.png" || resolved.GetResolved().GetAttachment().GetSourcePath() != "sandbox:attachment_resolve_write_1.png" {
		t.Fatalf("resolved attachment = %+v; want metadata from Bridge index", resolved.GetResolved().GetAttachment())
	}

	if _, err := store.ResolveTransientAttachment(context.Background(), &bridgev1.ResolveTransientAttachmentRequest{
		Scope:                scope,
		AttachmentRef:        created.GetAttachmentRef(),
		SourceToolUseEventId: "sevt_other",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ResolveTransientAttachment wrong source err = %v; want InvalidArgument", err)
	}

	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 16, 0, 0, time.UTC) }
	expired, err := store.ResolveTransientAttachment(context.Background(), &bridgev1.ResolveTransientAttachmentRequest{
		Scope:                scope,
		AttachmentRef:        created.GetAttachmentRef(),
		SourceToolUseEventId: "sevt_attachment_resolve",
	})
	if err != nil || expired.GetUnavailable() == nil || expired.GetResolved() != nil {
		t.Fatalf("ResolveTransientAttachment expired = response %+v err %v; want in-band unavailable", expired, err)
	}
}

func TestPostgreSQLBridgeAPIStoreResolveTransientAttachmentReturnsUnavailableForInactiveRowsWithoutBlobRead(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_gc_resolve", "thr_bridge_attachment_gc_resolve")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_gc_resolve", "bind_bridge_attachment_gc_resolve", 1, "pod_uid_attachment_gc_resolve")

	blobStore := &countingGetBlobStore{inner: blob.NewFakeBlobStore()}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_gc_resolve", "thr_bridge_attachment_gc_resolve", "bind_bridge_attachment_gc_resolve", 1, "pod_uid_attachment_gc_resolve")
	created := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_gc_resolve", "sevt_attachment_gc_resolve", []byte("image-bytes"))

	for _, attachmentStatus := range []string{"consumed", "deleting", "deleted"} {
		t.Run(attachmentStatus, func(t *testing.T) {
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_transient_attachments
				    SET status = $1, updated_at = '2026-01-01T12:00:01Z'
				  WHERE workspace_id = 'default'
				    AND session_id = 'sesn_bridge_attachment_gc_resolve'
				    AND session_thread_id = 'thr_bridge_attachment_gc_resolve'
				    AND attachment_ref = $2`,
				attachmentStatus, created.GetAttachmentRef(),
			); err != nil {
				t.Fatalf("mark attachment %s: %v", attachmentStatus, err)
			}

			readsBefore := blobStore.getCalls
			response, err := store.ResolveTransientAttachment(context.Background(), &bridgev1.ResolveTransientAttachmentRequest{
				Scope:                scope,
				AttachmentRef:        created.GetAttachmentRef(),
				SourceToolUseEventId: "sevt_attachment_gc_resolve",
			})
			if err != nil || response.GetUnavailable() == nil || response.GetResolved() != nil {
				t.Fatalf("ResolveTransientAttachment %s = response %+v err %v; want in-band unavailable", attachmentStatus, response, err)
			}
			if blobStore.getCalls != readsBefore {
				t.Fatalf("blob reads after %s resolve = %d; want unchanged %d", attachmentStatus, blobStore.getCalls, readsBefore)
			}

			var storedStatus string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status
				   FROM session_transient_attachments
				  WHERE workspace_id = 'default'
				    AND session_id = 'sesn_bridge_attachment_gc_resolve'
				    AND session_thread_id = 'thr_bridge_attachment_gc_resolve'
				    AND attachment_ref = $1`,
				created.GetAttachmentRef(),
			).Scan(&storedStatus); err != nil {
				t.Fatalf("read attachment status after %s resolve: %v", attachmentStatus, err)
			}
			if storedStatus != attachmentStatus {
				t.Fatalf("attachment status after rejected resolve = %q; want %q", storedStatus, attachmentStatus)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreResolveTransientAttachmentClassifiesAdapterOutageAndOversizedStoredBlob(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_taxonomy", "thr_bridge_attachment_taxonomy")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_taxonomy", "bind_bridge_attachment_taxonomy", 1, "pod_uid_attachment_taxonomy")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_taxonomy", "thr_bridge_attachment_taxonomy", "bind_bridge_attachment_taxonomy", 1, "pod_uid_attachment_taxonomy")
	request := &bridgev1.ResolveTransientAttachmentRequest{
		Scope: scope, AttachmentRef: "att_taxonomy", SourceToolUseEventId: "sevt_attachment_taxonomy",
	}

	if _, err := store.ResolveTransientAttachment(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("ResolveTransientAttachment adapter outage err = %v; want Unavailable", err)
	}

	blobStore := blob.NewFakeBlobStore()
	store.AttachmentBlobStore = blobStore
	created := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_taxonomy", request.GetSourceToolUseEventId(), []byte("small"))
	request.AttachmentRef = created.GetAttachmentRef()
	oversized := bytes.Repeat([]byte("x"), transientAttachmentMaxBytes+1)
	oversizedPointer := transientAttachmentBlobPointer(scope, created.GetAttachmentRef()) + "-oversized"
	if err := blobStore.Put(context.Background(), oversizedPointer, bytes.NewReader(oversized), int64(len(oversized))); err != nil {
		t.Fatalf("put oversized transient attachment blob: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_transient_attachments
		    SET blob_pointer = $1
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_attachment_taxonomy'
		    AND session_thread_id = 'thr_bridge_attachment_taxonomy'
		    AND attachment_ref = $2`,
		oversizedPointer,
		created.GetAttachmentRef(),
	); err != nil {
		t.Fatalf("point transient attachment at oversized stored blob: %v", err)
	}
	if _, err := store.ResolveTransientAttachment(context.Background(), request); status.Code(err) != codes.Internal {
		t.Fatalf("ResolveTransientAttachment oversized stored blob err = %v; want Internal", err)
	}
}

func TestPostgreSQLBridgeAPIStoreAttachmentResolversClassifyStaleScopeAsInvalidArgument(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_stale_scope", "thr_bridge_attachment_stale_scope")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_stale_scope", "bind_bridge_attachment_stale_scope", 1, "pod_uid_attachment_stale_scope")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	store.FileBlobStore = blob.NewFakeBlobStore()
	scope := bridgeAPIScope("sesn_bridge_attachment_stale_scope", "thr_bridge_attachment_stale_scope", "bind_bridge_attachment_stale_scope", 2, "pod_uid_attachment_stale_scope")
	pair := &bridgev1.FileAttachmentPair{
		SourceEventId: "sevt_attachment_stale_scope",
		FileId:        "file_attachment_stale_scope",
	}

	for name, call := range map[string]func() error{
		"transient": func() error {
			_, err := store.ResolveTransientAttachment(context.Background(), &bridgev1.ResolveTransientAttachmentRequest{
				Scope: scope, AttachmentRef: "att_stale_scope", SourceToolUseEventId: "sevt_tool_stale_scope",
			})
			return err
		},
		"file metadata": func() error {
			_, err := store.ResolveFileAttachmentMetadata(context.Background(), &bridgev1.ResolveFileAttachmentMetadataRequest{
				Scope: scope, Attachments: []*bridgev1.FileAttachmentPair{pair},
			})
			return err
		},
		"file chunk": func() error {
			_, err := store.ReadFileAttachmentChunk(context.Background(), &bridgev1.ReadFileAttachmentChunkRequest{
				Scope: scope, Attachment: pair, Offset: 0, Length: 1,
			})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("resolver stale scope err = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReconcileTransientAttachmentsDeletesExpiredAndConsumedAfterStateCommit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_gc", "thr_bridge_attachment_gc")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_gc", "bind_bridge_attachment_gc", 1, "pod_uid_attachment_gc")

	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_gc", "thr_bridge_attachment_gc", "bind_bridge_attachment_gc", 1, "pod_uid_attachment_gc")
	expired := createBridgeTransientAttachmentForTest(t, store, scope, "gc_expired", "sevt_gc_expired", []byte("expired"))
	consumed := createBridgeTransientAttachmentForTest(t, store, scope, "gc_consumed", "sevt_gc_consumed", []byte("consumed"))
	fresh := createBridgeTransientAttachmentForTest(t, store, scope, "gc_fresh", "sevt_gc_fresh", []byte("fresh"))
	protectedStaged := createBridgeTransientAttachmentForTest(t, store, scope, "gc_protected_staged", "sevt_gc_protected_staged", []byte("protected staged"))
	protectedUploading := createBridgeTransientAttachmentForTest(t, store, scope, "gc_protected_uploading", "sevt_gc_protected_uploading", []byte("protected uploading"))
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_transient_attachments
		    SET status = CASE
		          WHEN attachment_ref = $1 THEN 'consumed'
		          WHEN attachment_ref = $4 THEN 'staged'
		          WHEN attachment_ref = $5 THEN 'uploading'
		          ELSE status
		        END,
		        expires_at = CASE WHEN attachment_ref = $2 THEN '2026-01-01T13:00:00Z' ELSE expires_at END
		  WHERE workspace_id = 'default'
		    AND attachment_ref IN ($1, $2, $3, $4, $5)`,
		consumed.GetAttachmentRef(),
		fresh.GetAttachmentRef(),
		expired.GetAttachmentRef(),
		protectedStaged.GetAttachmentRef(),
		protectedUploading.GetAttachmentRef(),
	); err != nil {
		t.Fatalf("prepare attachment gc rows: %v", err)
	}
	protectedRows := []struct {
		ref           *bridgev1.TransientAttachmentRef
		sourceEventID string
	}{
		{ref: protectedStaged, sourceEventID: "sevt_gc_protected_staged"},
		{ref: protectedUploading, sourceEventID: "sevt_gc_protected_uploading"},
	}
	for index, protected := range protectedRows {
		protectedResult := `{"status":"success","attachment_ref":"` + protected.ref.GetAttachmentRef() + `"}`
		if _, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			model_tool_call_id, execution_state, execution_attempt_generation,
			result_digest, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_attachment_gc', 'thr_bridge_attachment_gc', $1, 'sandbox_tool',
			$2, 'view_image', '{}', 'committed', $3,
			$4, 'terminal_unconsumed', 1,
			$5, '2026-01-01T12:00:00Z', '2026-01-01T12:00:00Z'
		)`, protected.sourceEventID, "input_hash_gc_protected_"+strconv.Itoa(index),
			"call_gc_protected_"+strconv.Itoa(index), protectedResult, sha256Hex(protectedResult),
		); err != nil {
			t.Fatalf("seed unconsumed Sandbox execution for protected attachment: %v", err)
		}
	}
	checkedStateBeforeDelete := 0
	blobStore.SetDeleteHook(func(_ context.Context, key string) error {
		var statusValue string
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status
			   FROM session_transient_attachments
			  WHERE workspace_id = 'default'
			    AND blob_pointer = $1`,
			key,
		).Scan(&statusValue); err != nil {
			t.Fatalf("read status during blob delete: %v", err)
		}
		if statusValue != "deleting" {
			t.Fatalf("status during blob delete = %q; want deleting", statusValue)
		}
		checkedStateBeforeDelete++
		return nil
	})
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 16, 0, 0, time.UTC) }
	result, err := store.ReconcileTransientAttachments(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReconcileTransientAttachments: %v", err)
	}
	if result.Marked != 2 || result.Deleted != 2 || result.Failed != 0 || checkedStateBeforeDelete != 2 {
		t.Fatalf("gc result = %+v checked=%d; want two committed-before-delete removals", result, checkedStateBeforeDelete)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, expired.GetAttachmentRef()); got != "deleted" {
		t.Fatalf("expired status = %q; want deleted", got)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, consumed.GetAttachmentRef()); got != "deleted" {
		t.Fatalf("consumed status = %q; want deleted", got)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, fresh.GetAttachmentRef()); got != "active" {
		t.Fatalf("fresh status = %q; want active", got)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, protectedStaged.GetAttachmentRef()); got != "staged" {
		t.Fatalf("unconsumed execution attachment status = %q; want staged", got)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, protectedUploading.GetAttachmentRef()); got != "uploading" {
		t.Fatalf("unconsumed execution attachment status = %q; want uploading", got)
	}
	if _, ok := blobStore.Bytes(transientAttachmentBlobPointer(scope, fresh.GetAttachmentRef())); !ok {
		t.Fatalf("fresh attachment blob was deleted")
	}
	for _, protected := range []*bridgev1.TransientAttachmentRef{protectedStaged, protectedUploading} {
		if _, ok := blobStore.Bytes(transientAttachmentBlobPointer(scope, protected.GetAttachmentRef())); !ok {
			t.Fatalf("unconsumed execution attachment blob was deleted")
		}
	}
}

func TestPostgreSQLBridgeAPIStoreReconcileTransientAttachmentsRetriesDeleteFailures(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_gc_retry", "thr_bridge_attachment_gc_retry")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_gc_retry", "bind_bridge_attachment_gc_retry", 1, "pod_uid_attachment_gc_retry")

	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_gc_retry", "thr_bridge_attachment_gc_retry", "bind_bridge_attachment_gc_retry", 1, "pod_uid_attachment_gc_retry")
	attachment := createBridgeTransientAttachmentForTest(t, store, scope, "gc_retry", "sevt_gc_retry", []byte("retry"))
	blobStore.SetDeleteHook(func(context.Context, string) error {
		return errors.New("delete unavailable")
	})
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 16, 0, 0, time.UTC) }
	first, err := store.ReconcileTransientAttachments(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReconcileTransientAttachments first: %v", err)
	}
	if first.Marked != 1 || first.Deleted != 0 || first.Failed != 1 {
		t.Fatalf("first gc result = %+v; want retryable delete failure", first)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "deleting" {
		t.Fatalf("failed delete status = %q; want deleting", got)
	}
	if _, ok := blobStore.Bytes(transientAttachmentBlobPointer(scope, attachment.GetAttachmentRef())); !ok {
		t.Fatalf("failed delete removed blob")
	}
	blobStore.SetDeleteHook(nil)
	second, err := store.ReconcileTransientAttachments(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReconcileTransientAttachments second: %v", err)
	}
	if second.Marked != 1 || second.Deleted != 1 || second.Failed != 0 {
		t.Fatalf("second gc result = %+v; want retried delete success", second)
	}
	if got := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); got != "deleted" {
		t.Fatalf("retried delete status = %q; want deleted", got)
	}
}

func TestTransientAttachmentGCTelemetryIsSafeAndReportsFailures(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	logTransientAttachmentGC(logger, TransientAttachmentGCResult{Marked: 3, Deleted: 1, Failed: 2}, nil)
	logTransientAttachmentGC(logger, TransientAttachmentGCResult{}, errors.New("secret blob pointer / raw attachment bytes"))

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("GC log lines = %d; want 2: %q", len(lines), output.String())
	}
	var partial map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &partial); err != nil {
		t.Fatalf("decode partial GC log: %v", err)
	}
	if partial["event.kind"] != "transient_attachment.gc" || partial["error.code"] != "delete_failed" || partial["retryable"] != true || partial["terminal"] != false || partial["attachment.failed"] != float64(2) {
		t.Fatalf("partial GC log = %+v; want shared failure fields and counts", partial)
	}
	var failed map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &failed); err != nil {
		t.Fatalf("decode failed GC log: %v", err)
	}
	if failed["error.code"] != "scan_failed" || failed["error.message_safe"] != "transient attachment GC scan failed" {
		t.Fatalf("failed GC log = %+v; want safe scan failure", failed)
	}
	if strings.Contains(output.String(), "secret blob pointer") || strings.Contains(output.String(), "raw attachment bytes") {
		t.Fatalf("GC logs leaked raw error: %q", output.String())
	}
}
