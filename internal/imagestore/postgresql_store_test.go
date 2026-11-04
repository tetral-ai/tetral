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

// Upload and Store pairs racing a collection pass that deletes their
// content's last expired row all leave a row that resolves. A Store whose
// transaction stays open while a whole pass runs keeps the bytes through its
// upload entry. Uploads queued behind the pass on the content's lock record
// their entries after it has deleted the bytes, and only then put them.
func TestUploadsAndStoresRacingACollectionAllResolve(t *testing.T) {
	f := newUploadFixture(t)
	data := []byte("raced image bytes")
	expired := f.store(f.upload(data))
	f.exec(`UPDATE session_attachments SET created_at=clock_timestamp()-interval '2 minutes' WHERE attachment_id=$1`, expired.AttachmentID)

	references := make([]Reference, 51)
	first := f.upload(data)
	stored, commit := make(chan error, 1), make(chan struct{})
	release := sync.OnceFunc(func() { close(commit) })
	t.Cleanup(release)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := f.writer.WithWorkspaceTx(f.ctx, "default", "test.store_open", func(tx *dbconnect.Tx) error {
			var err error
			references[0], err = Store(f.ctx, tx, workspace.DefaultID, first, "sesn_race", "thr_race")
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
	if result := f.collect(); result.Deleted != 1 || result.Failed != 0 {
		t.Fatalf("collection = %+v; want the one expired row deleted", result)
	}
	if !f.blobExists(data) {
		t.Fatal("the collection pass deleted the bytes an open Store's upload entry names")
	}
	release()
	wg.Wait()

	other := []byte("raced image bytes again")
	otherExpired := f.store(f.upload(other))
	f.exec(`UPDATE session_attachments SET created_at=clock_timestamp()-interval '2 minutes' WHERE attachment_id=$1`, otherExpired.AttachmentID)
	lock, err := f.admin.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.ExecContext(f.ctx, "SELECT pg_advisory_xact_lock($1, $2)", contentLockCategory, contentLockResource("default", contentHash(other))); err != nil {
		t.Fatal(err)
	}
	collected := make(chan CollectResult, 1)
	go func() {
		result, err := CollectExpired(f.ctx, f.collector, f.blobs, time.Minute, 100)
		if err != nil {
			t.Error(err)
		}
		collected <- result
	}()
	waitForLockWaiters(t, f.admin, 1)
	wg.Add(len(references) - 1)
	for i := 1; i < len(references); i++ {
		go func() {
			defer wg.Done()
			references[i] = f.store(f.upload(other))
		}()
	}
	waitForLockWaiters(t, f.admin, len(references))
	if entries := f.entries(); entries != 0 {
		t.Fatalf("%d upload entries recorded while the content's lock was held; want 0", entries)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if result := <-collected; result.Deleted != 1 || result.Failed != 0 {
		t.Fatalf("collection = %+v; want the other content's expired row deleted", result)
	}
	wg.Wait()
	for i, reference := range references {
		want := other
		if i == 0 {
			want = data
		}
		attachment, found := resolve(t, f.writer, f.blobs, "sesn_race", reference.AttachmentID)
		if !found || string(attachment.Data) != string(want) {
			t.Fatalf("attachment %s after the race: found %v, %q", reference.AttachmentID, found, attachment.Data)
		}
	}
}

// An upload whose caller's transaction rolls back keeps its bytes until its
// check_after; the first collection pass after it deletes the bytes and the
// entry, and an entry whose content a row still names loses only the entry.
// An upload whose Store commits leaves no entry.
func TestUncommittedUploadsAreCollectedAfterTheirCheck(t *testing.T) {
	f := newUploadFixture(t)
	orphan, shared := []byte("orphan bytes"), []byte("shared bytes")
	f.rolledBack(f.upload(orphan))
	kept := f.store(f.upload(shared))
	if entries := f.entries(); entries != 1 {
		t.Fatalf("upload entries after a rolled-back and a committed Store = %d; want 1", entries)
	}
	f.rolledBack(f.upload(shared))
	if result := f.collect(); result != (CollectResult{}) || f.entries() != 2 || !f.blobExists(orphan) || !f.blobExists(shared) {
		t.Fatalf("pass before check_after = %+v with %d entries; want nothing released and both blobs stored", result, f.entries())
	}
	f.exec(`UPDATE session_attachment_uploads SET check_after=clock_timestamp()-interval '1 second'`)
	if result := f.collect(); result != (CollectResult{}) || f.entries() != 0 || f.blobExists(orphan) || !f.blobExists(shared) {
		t.Fatalf("pass after check_after = %+v with %d entries, orphan bytes stored %v, shared %v; want no entry, only the shared bytes",
			result, f.entries(), f.blobExists(orphan), f.blobExists(shared))
	}
	if attachment, found := resolve(t, f.writer, f.blobs, "sesn_race", kept.AttachmentID); !found || string(attachment.Data) != string(shared) {
		t.Fatalf("the stored attachment after the pass: found %v, %q", found, attachment.Data)
	}
}

// uploadFixture uploads and stores as the computer_hub role and collects as
// the bridge role, over a fresh MinIO bucket, for one seeded Session.
type uploadFixture struct {
	t         *testing.T
	ctx       context.Context
	admin     *sql.DB
	writer    *dbconnect.Client
	collector *dbconnect.Client
	blobs     *blob.S3BlobStore
}

func newUploadFixture(t *testing.T) *uploadFixture {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_race", "thr_race")
	roles := storagetest.OpenWorkloadDB(t, admin, "computer_hub")
	// Every racing upload holds its own connection while it waits for the
	// content's lock.
	roles.DB.SetMaxOpenConns(64)
	return &uploadFixture{t: t, ctx: context.Background(), admin: admin, writer: dbconnect.NewClientForTesting(roles.DB),
		collector: dbconnect.NewClientForTesting(roles.OpenWorkload(t, "bridge", nil)), blobs: minioStore(t)}
}

func (f *uploadFixture) upload(data []byte) Uploaded {
	f.t.Helper()
	upload, err := Upload(f.ctx, f.writer, f.blobs, workspace.DefaultID, "image/png", data)
	if err != nil {
		f.t.Error(err)
	}
	return upload
}

// store records upload in a committed transaction of the Session's thread.
func (f *uploadFixture) store(upload Uploaded) Reference {
	f.t.Helper()
	var reference Reference
	if err := f.writer.WithWorkspaceTx(f.ctx, "default", "test.store", func(tx *dbconnect.Tx) error {
		var err error
		reference, err = Store(f.ctx, tx, workspace.DefaultID, upload, "sesn_race", "thr_race")
		return err
	}); err != nil {
		f.t.Error(err)
	}
	return reference
}

// rolledBack records upload in a transaction that rolls back.
func (f *uploadFixture) rolledBack(upload Uploaded) {
	f.t.Helper()
	rollback := errors.New("roll back the caller's transaction")
	err := f.writer.WithWorkspaceTx(f.ctx, "default", "test.store_rolled_back", func(tx *dbconnect.Tx) error {
		if _, err := Store(f.ctx, tx, workspace.DefaultID, upload, "sesn_race", "thr_race"); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		f.t.Fatal(err)
	}
}

func (f *uploadFixture) collect() CollectResult {
	f.t.Helper()
	result, err := CollectExpired(f.ctx, f.collector, f.blobs, time.Minute, 100)
	if err != nil {
		f.t.Fatal(err)
	}
	return result
}

func (f *uploadFixture) entries() int {
	f.t.Helper()
	var entries int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_attachment_uploads`).Scan(&entries); err != nil {
		f.t.Fatal(err)
	}
	return entries
}

func (f *uploadFixture) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		f.t.Fatal(err)
	}
}

// blobExists reports whether the bytes data are stored for the Workspace.
func (f *uploadFixture) blobExists(data []byte) bool {
	f.t.Helper()
	_, err := f.blobs.HeadObject(f.ctx, blobKey("default", contentHash(data)))
	var notFound *blob.NotFoundError
	if errors.As(err, &notFound) {
		return false
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return true
}

// waitForLockWaiters returns once n sessions of the test database wait for
// an advisory lock.
func waitForLockWaiters(t *testing.T, admin *sql.DB, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting int
		if err := admin.QueryRow(`SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted
			AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions waited for the content's lock within 10s; want %d", waiting, n)
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
