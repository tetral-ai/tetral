package tetralsandbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestPostgreSQLSandboxMediaMaterializerStagesBlobBeforePublishingReference(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	now := time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`

	result, err := materializer.MaterializeResult(ctx, SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}, "view_image", `{"path":"/workspace/plot.png"}`, raw, now)
	if err != nil {
		t.Fatalf("MaterializeResult: %v", err)
	}
	var decoded struct {
		Result struct {
			AttachmentRef string `json:"attachment_ref"`
			DataBase64    string `json:"data_base64"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(result), &decoded); err != nil {
		t.Fatalf("decode materialized result: %v", err)
	}
	if decoded.Result.AttachmentRef == "" || decoded.Result.DataBase64 != "" {
		t.Fatalf("materialized result = %s; want attachment ref without inline bytes", result)
	}
	var status, pointer string
	if err := adminDB.QueryRow(`SELECT status, blob_pointer FROM session_transient_attachments
		WHERE workspace_id='ws_execution_store' AND attachment_ref=$1`, decoded.Result.AttachmentRef).Scan(&status, &pointer); err != nil {
		t.Fatalf("read staged attachment: %v", err)
	}
	body, ok := blobStore.Bytes(pointer)
	if status != "staged" || !ok || string(body) != "image-bytes" {
		t.Fatalf("staged attachment = status %q blob %q; want staged image bytes", status, string(body))
	}
	recovered, err := materializer.RecoverResult(ctx, SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	})
	if err != nil || !recovered.Found || !recovered.Ready || !json.Valid([]byte(recovered.ResultJSON)) {
		t.Fatalf("RecoverResult = %+v, %v; want staged result recovery", recovered, err)
	}
}

func TestPostgreSQLSandboxMediaMaterializerKeepsFailedUploadIndexedForCleanup(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	blobStore := blob.NewFakeBlobStore()
	putErr := errors.New("temporary Blob failure")
	blobStore.SetPutHook(func(context.Context, string) error { return putErr })
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	now := time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`

	if _, err := materializer.MaterializeResult(ctx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, now); !errors.Is(err, putErr) {
		t.Fatalf("MaterializeResult error = %T %v; want Blob failure", err, err)
	}
	var attachmentRef, pointer, status string
	if err := adminDB.QueryRow(`SELECT attachment_ref, blob_pointer, status FROM session_transient_attachments
		WHERE workspace_id='ws_execution_store' AND source_tool_use_event_id='evt_execution_a'`).Scan(&attachmentRef, &pointer, &status); err != nil {
		t.Fatalf("read retained attachment custody: %v", err)
	}
	if status != "uploading" {
		t.Fatalf("attachment status after transient failure = %q; want uploading", status)
	}

	if _, ok := blobStore.Bytes(pointer); ok {
		t.Fatalf("failed attachment %s unexpectedly reached Blob custody", attachmentRef)
	}
	recovered, err := materializer.RecoverResult(ctx, ref)
	if err != nil || !recovered.Found || recovered.Ready {
		t.Fatalf("RecoverResult = %+v, %v; want indexed upload failure", recovered, err)
	}
}

func TestPostgreSQLSandboxMediaMaterializerRetriesTransientStagedBlobInspection(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`
	if _, err := materializer.MaterializeResult(ctx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now()); err != nil {
		t.Fatalf("MaterializeResult: %v", err)
	}
	headErr := errors.New("temporary Blob HEAD failure")
	blobStore.SetHeadHook(func(context.Context, string) error { return headErr })

	if _, err := materializer.RecoverResult(ctx, ref); !errors.Is(err, headErr) {
		t.Fatalf("RecoverResult error = %T %v; want transient Blob failure", err, err)
	}
}

func TestPostgreSQLSandboxMediaRecoveryRejectsSameSizeBlobSubstitution(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`
	if _, err := materializer.MaterializeResult(ctx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now()); err != nil {
		t.Fatalf("MaterializeResult: %v", err)
	}
	materializer.blobs = substitutedGetBlobStore{BlobStore: blobStore, body: []byte("other-bytes")}

	if _, err := materializer.RecoverResult(ctx, ref); !errors.Is(err, errSandboxMediaStateConflict) {
		t.Fatalf("RecoverResult error = %T %v; want same-size Blob substitution rejection", err, err)
	}
}

func TestPostgreSQLSandboxMediaStagedReplayRejectsSameSizeBlobSubstitution(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`
	if _, err := materializer.MaterializeResult(ctx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now()); err != nil {
		t.Fatalf("MaterializeResult: %v", err)
	}
	materializer.blobs = substitutedGetBlobStore{BlobStore: blobStore, body: []byte("other-bytes")}

	if _, err := materializer.MaterializeResult(ctx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now()); !errors.Is(err, errSandboxMediaStateConflict) {
		t.Fatalf("staged replay error = %T %v; want same-size Blob substitution rejection", err, err)
	}
}

func TestPostgreSQLSandboxMediaRecoveryStopsBeforeBlobInspectionWithoutQueueAuthority(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	setupCtx := sandboxTestQueueContext(t, runtimeDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`
	if _, err := materializer.MaterializeResult(setupCtx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now()); err != nil {
		t.Fatalf("MaterializeResult: %v", err)
	}
	headCalls := 0
	blobStore.SetHeadHook(func(context.Context, string) error {
		headCalls++
		return nil
	})
	ctx := withSandboxQueueAuthority(context.Background(), &sandboxQueueAuthority{
		workspaceID: ref.WorkspaceID, jobID: "qjob_missing", leaseToken: "lease_missing",
		leasedUntil: time.Now().UTC().Add(time.Minute),
	})

	if _, err := materializer.RecoverResult(ctx, ref); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("RecoverResult error = %v; want Queue lease loss", err)
	}
	if headCalls != 0 {
		t.Fatalf("Blob HEAD calls = %d; want none without Queue authority", headCalls)
	}
}

func TestPostgreSQLSandboxMediaStagingRejectsSupersededQueueLease(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	payload, err := sandboxExecutionQueuePayload(ref)
	if err != nil {
		t.Fatalf("encode Sandbox execution Queue payload: %v", err)
	}
	request := queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: "ws_execution_store", Kind: queue.KindSandboxToolExecute,
		PartitionKey: queue.FormatSandboxExecutionPartitionKey(
			"ws_execution_store", ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID,
		),
		DedupeKey: queue.FormatSandboxToolExecuteDedupeKey(
			"ws_execution_store", ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID, 1,
		),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxToolExecuteMaxAttempts,
	}
	blobStore := blob.NewFakeBlobStore()
	putStarted := make(chan struct{})
	putRelease := make(chan struct{})
	blobStore.SetPutHook(func(ctx context.Context, _ string) error {
		close(putStarted)
		select {
		case <-putRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`
	firstResult := make(chan error, 1)
	_, secondCtx, _, _ := supersedeSandboxQueueLeaseAfter(t, runtimeDB, adminDB, request, func(firstCtx context.Context, _ *queuev1.QueueJob) {
		go func() {
			_, err := materializer.MaterializeResult(firstCtx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now())
			firstResult <- err
		}()
		select {
		case <-putStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("media upload did not reach Blob custody barrier")
		}
	})
	close(putRelease)
	if err := <-firstResult; !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("superseded media staging error = %v; want Queue authority loss", err)
	}
	var status string
	if err := adminDB.QueryRow(`SELECT status FROM session_transient_attachments
		WHERE workspace_id=$1 AND source_tool_use_event_id=$2`, ref.WorkspaceID, ref.ToolUseEventID).Scan(&status); err != nil {
		t.Fatalf("read attachment after stale staging: %v", err)
	}
	if status != "uploading" {
		t.Fatalf("attachment after stale staging = %q; want uploading custody", status)
	}
	result, err := materializer.MaterializeResult(secondCtx, ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now())
	if err != nil {
		t.Fatalf("successor MaterializeResult: %v", err)
	}
	if !json.Valid([]byte(result)) {
		t.Fatalf("successor materialized result is invalid JSON: %q", result)
	}
	if err := adminDB.QueryRow(`SELECT status FROM session_transient_attachments
		WHERE workspace_id=$1 AND source_tool_use_event_id=$2`, ref.WorkspaceID, ref.ToolUseEventID).Scan(&status); err != nil {
		t.Fatalf("read attachment after successor staging: %v", err)
	}
	if status != "staged" {
		t.Fatalf("attachment after successor staging = %q; want staged", status)
	}
}

type substitutedGetBlobStore struct {
	blob.BlobStore
	body []byte
}

func (s substitutedGetBlobStore) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.body)), nil
}
