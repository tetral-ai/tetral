package imagestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/blob/blobtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Stores of one content racing a collection pass that deletes the content's
// last expired row all leave a row that resolves: the pass never deletes the
// bytes a storing transaction counted as stored. The first store's
// transaction stays open until the pass has finished or waits for the
// content's lock, and the bytes must still be stored then: a later store puts
// them again, so only that check sees a pass or a store that skips the lock.
func TestStoresRacingACollectionAllResolve(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_race", "thr_race")
	roles := storagetest.OpenWorkloadDB(t, admin, "computer_hub")
	writer := dbconnect.NewClientForTesting(roles.DB)
	collector := dbconnect.NewClientForTesting(roles.OpenWorkload(t, "bridge", nil))
	blobs := minioStore(t)
	ctx := context.Background()
	data := []byte("raced image bytes")
	store := func() Reference {
		var reference Reference
		if err := writer.WithWorkspaceTx(ctx, "default", "test.store", func(tx *dbconnect.Tx) error {
			var err error
			reference, err = Store(ctx, tx, blobs, workspace.DefaultID, "sesn_race", "thr_race", "image/png", data)
			return err
		}); err != nil {
			t.Error(err)
		}
		return reference
	}
	expired := store()
	if _, err := admin.ExecContext(ctx, `UPDATE session_attachments SET created_at=clock_timestamp()-interval '2 minutes' WHERE attachment_id=$1`, expired.AttachmentID); err != nil {
		t.Fatal(err)
	}

	references := make([]Reference, 50)
	stored, commit := make(chan error, 1), make(chan struct{})
	release := sync.OnceFunc(func() { close(commit) })
	t.Cleanup(release)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := writer.WithWorkspaceTx(ctx, "default", "test.store_open", func(tx *dbconnect.Tx) error {
			var err error
			references[0], err = Store(ctx, tx, blobs, workspace.DefaultID, "sesn_race", "thr_race", "image/png", data)
			stored <- err
			if err != nil {
				return err
			}
			<-commit
			return nil
		}); err != nil {
			t.Error(err)
		}
	}()
	if err := <-stored; err != nil {
		t.Fatal(err)
	}
	var result CollectResult
	var collectErr error
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		result, collectErr = CollectExpired(ctx, collector, blobs, time.Minute, 100)
	}()
	waitForLockWaitOrDone(t, admin, collected)
	_, err := blobs.HeadObject(ctx, blobKey("default", contentHash(data)))
	var notFound *blob.NotFoundError
	if errors.As(err, &notFound) {
		t.Fatal("the collection pass deleted the bytes an open storing transaction counted as stored")
	} else if err != nil {
		t.Fatal(err)
	}
	wg.Add(len(references) - 1)
	for i := 1; i < len(references); i++ {
		go func() {
			defer wg.Done()
			references[i] = store()
		}()
	}
	release()
	wg.Wait()
	<-collected
	if collectErr != nil || result.Deleted != 1 || result.Failed != 0 {
		t.Fatalf("collection = %+v, %v; want the one expired row deleted", result, collectErr)
	}
	for _, reference := range references {
		attachment, found := resolve(t, writer, blobs, "sesn_race", reference.AttachmentID)
		if !found || string(attachment.Data) != string(data) {
			t.Fatalf("attachment %s after the race: found %v, %q", reference.AttachmentID, found, attachment.Data)
		}
	}
}

// waitForLockWaitOrDone returns once done is closed or a session of the
// test database waits for an advisory lock.
func waitForLockWaitOrDone(t *testing.T, admin *sql.DB, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-done:
			return
		default:
		}
		var waiting int
		if err := admin.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the collection pass neither finished nor waited for the content's lock within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// resolve resolves attachmentID of sessionID with a one-minute lifetime.
func resolve(t *testing.T, client *dbconnect.Client, blobs blob.BlobStore, sessionID, attachmentID string) (Attachment, bool) {
	t.Helper()
	var attachment Attachment
	var found bool
	if err := client.WithWorkspaceReadOnlyTx(context.Background(), "default", "test.resolve", func(tx *dbconnect.Tx) error {
		var err error
		attachment, found, err = Resolve(context.Background(), tx, blobs, workspace.DefaultID, sessionID, attachmentID, time.Minute)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return attachment, found
}

// minioStore is an S3 blob store on a fresh bucket of the test MinIO.
func minioStore(t *testing.T) *blob.S3BlobStore {
	t.Helper()
	endpoint := os.Getenv("TETRAL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Fatal("TETRAL_TEST_MINIO_ENDPOINT is required")
	}
	cfg := blob.Config{Endpoint: endpoint, Region: os.Getenv("TETRAL_TEST_MINIO_REGION"), Bucket: fmt.Sprintf("imagestore-%d", time.Now().UnixNano()),
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
