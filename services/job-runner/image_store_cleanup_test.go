package jobrunner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/blob/blobtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/imagestore"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Session deletion cleanup, as the job_runner role, deletes every attachment
// row of the deleted Session and the bytes no other row and no upload entry
// names, and keeps the bytes another Session's row or an upload entry still
// names.
func TestSessionDeleteCleanupReleasesOnlyUnsharedAttachmentBytes(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		deleted   = "sesn_delete_attachments"
		kept      = "sesn_keep_attachments"
		cleanupID = "delcln_delete_attachments"
	)
	for _, session := range []string{deleted, kept} {
		sessionfixture.SeedBridgeAPISession(t, admin, "default", session, "thr_"+session)
	}
	if _, err := admin.Exec(`UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2 WHERE workspace_id='default' AND id=$1`, deleted, cleanupID); err != nil {
		t.Fatal(err)
	}
	blobs := cleanupMinIO(t)
	roles := storagetest.OpenWorkloadDB(t, admin, "job_runner")
	writer := dbconnect.NewClientForTesting(roles.OpenWorkload(t, "api", nil))
	upload := func(data string) imagestore.Uploaded {
		t.Helper()
		upload, err := imagestore.Upload(context.Background(), writer, blobs, workspace.DefaultID, "image/png", []byte(data))
		if err != nil {
			t.Fatal(err)
		}
		return upload
	}
	for _, attachment := range []struct{ session, data string }{{deleted, "only deleted"}, {deleted, "shared"}, {kept, "shared"}, {deleted, "uploaded again"}} {
		uploaded := upload(attachment.data)
		if err := writer.WithWorkspaceTx(context.Background(), "default", "test.store_attachment", func(tx *dbconnect.Tx) error {
			_, err := imagestore.Store(context.Background(), tx, workspace.DefaultID, uploaded, attachment.session, "thr_"+attachment.session)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	upload("uploaded again")

	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(roles.DB), admin, 9090)
	store.AttachmentBlobStore = blobs
	result, err := store.finalizeSessionDeleteCleanup(context.Background(), RuntimeJob{
		Kind: queue.KindSessionDeleteCleanup, WorkspaceID: "default", SessionID: deleted,
		DeleteCleanupID: cleanupID, AttemptCount: 1, MaxAttempts: 5,
	})
	if err != nil || result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("cleanup = %+v, %v; want accepted", result, err)
	}
	var deletedRows, keptRows int
	if err := admin.QueryRow(`SELECT count(*) FILTER (WHERE session_id=$1), count(*) FILTER (WHERE session_id=$2) FROM session_attachments`, deleted, kept).Scan(&deletedRows, &keptRows); err != nil {
		t.Fatal(err)
	}
	if deletedRows != 0 || keptRows != 1 || cleanupBlobExists(t, blobs, "only deleted") || !cleanupBlobExists(t, blobs, "shared") || !cleanupBlobExists(t, blobs, "uploaded again") {
		t.Fatalf("after the cleanup: %d rows of the deleted Session, %d of the other; unshared bytes stored %v, shared %v, uploaded again %v; want 0, 1, false, true, true",
			deletedRows, keptRows, cleanupBlobExists(t, blobs, "only deleted"), cleanupBlobExists(t, blobs, "shared"), cleanupBlobExists(t, blobs, "uploaded again"))
	}
}

// cleanupBlobExists reports whether the bytes data are stored for the
// Workspace.
func cleanupBlobExists(t *testing.T, blobs blob.BlobStore, data string) bool {
	t.Helper()
	sum := sha256.Sum256([]byte(data))
	_, err := blobs.HeadObject(context.Background(), "attachments/default/"+hex.EncodeToString(sum[:]))
	var notFound *blob.NotFoundError
	if errors.As(err, &notFound) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

// cleanupMinIO is an S3 blob store on a fresh bucket of the test MinIO.
func cleanupMinIO(t *testing.T) *blob.S3BlobStore {
	t.Helper()
	endpoint := os.Getenv("TETRAL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Fatal("TETRAL_TEST_MINIO_ENDPOINT is required")
	}
	cfg := blob.Config{Endpoint: endpoint, Region: os.Getenv("TETRAL_TEST_MINIO_REGION"), Bucket: fmt.Sprintf("cleanup-images-%d", time.Now().UnixNano()),
		AccessKey: os.Getenv("TETRAL_TEST_MINIO_ACCESS_KEY"), SecretKey: os.Getenv("TETRAL_TEST_MINIO_SECRET_KEY"), AllowInsecure: true, LocalTestMode: true}
	if err := blobtest.CreateBucket(context.Background(), http.DefaultClient, cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := blobtest.DeleteBucketContents(context.Background(), http.DefaultClient, cfg); err != nil {
			t.Error(err)
		}
	})
	store, err := blob.NewS3BlobStore(context.Background(), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
