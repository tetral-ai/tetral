package tetralsandbox

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
)

func TestPostgreSQLSandboxMediaMaterializerStagesBlobBeforePublishingReference(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	now := time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`

	result, err := materializer.MaterializeResult(context.Background(), SandboxExecutionRef{
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
	recovered, err := materializer.RecoverResult(context.Background(), SandboxExecutionRef{
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

	if _, err := materializer.MaterializeResult(context.Background(), ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, now); !errors.Is(err, putErr) {
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
	recovered, err := materializer.RecoverResult(context.Background(), ref)
	if err != nil || !recovered.Found || recovered.Ready {
		t.Fatalf("RecoverResult = %+v, %v; want indexed upload failure", recovered, err)
	}
}

func TestPostgreSQLSandboxMediaMaterializerRetriesTransientStagedBlobInspection(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	ref := SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("image-bytes")) + `"}}`
	if _, err := materializer.MaterializeResult(context.Background(), ref, "view_image", `{"path":"/workspace/plot.png"}`, raw, time.Now()); err != nil {
		t.Fatalf("MaterializeResult: %v", err)
	}
	headErr := errors.New("temporary Blob HEAD failure")
	blobStore.SetHeadHook(func(context.Context, string) error { return headErr })

	if _, err := materializer.RecoverResult(context.Background(), ref); !errors.Is(err, headErr) {
		t.Fatalf("RecoverResult error = %T %v; want transient Blob failure", err, err)
	}
}
