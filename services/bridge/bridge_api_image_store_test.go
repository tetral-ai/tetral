package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/blob/blobtest"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/imagestore"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// An attachment resolves for its own Session until its lifetime ends: one
// second before, its bytes; at the end, unavailable, as for another
// Session's scope and an unknown ID. The next collection pass deletes its row
// and its bytes.
func TestResolveAttachmentServesItsSessionUntilTheLifetimeEnds(t *testing.T) {
	f := newImageStoreFixture(t)
	reference := f.store("sesn_image_one", []byte("one image"))

	f.age(reference, 59*time.Second)
	if got := f.resolve("sesn_image_one", reference.AttachmentID); got.GetResolved().GetMediaType() != "image/png" || string(got.GetResolved().GetData()) != "one image" {
		t.Fatalf("resolve 1 second before the end = %v; want the bytes", got)
	}
	for name, sessionAndID := range map[string][2]string{
		"another Session's scope": {"sesn_image_two", reference.AttachmentID},
		"an unknown ID":           {"sesn_image_one", "att_unknown"},
	} {
		if got := f.resolve(sessionAndID[0], sessionAndID[1]); got.GetUnavailable() == nil || got.GetResolved() != nil {
			t.Fatalf("resolve with %s = %v; want unavailable and no bytes", name, got)
		}
	}
	f.age(reference, 60*time.Second)
	if got := f.resolve("sesn_image_one", reference.AttachmentID); got.GetUnavailable() == nil {
		t.Fatalf("resolve at the end = %v; want unavailable", got)
	}
	if deleted := f.collectPass(); deleted != 1 || f.rows() != 0 || f.blobExists("one image") {
		t.Fatalf("the collection pass deleted %d rows, leaving %d rows and the bytes stored %v; want 1, 0 and false", deleted, f.rows(), f.blobExists("one image"))
	}
}

// Two rows of one content keep its bytes until the second expires: the pass
// that deletes the first leaves the bytes and the second resolves; the pass
// that deletes the second deletes them.
func TestAttachmentCollectionKeepsSharedBytesUntilTheLastRowExpires(t *testing.T) {
	f := newImageStoreFixture(t)
	first := f.store("sesn_image_one", []byte("shared image"))
	second := f.store("sesn_image_one", []byte("shared image"))

	f.age(first, time.Minute)
	if deleted := f.collectPass(); deleted != 1 || f.rows() != 1 || !f.blobExists("shared image") ||
		string(f.resolve("sesn_image_one", second.AttachmentID).GetResolved().GetData()) != "shared image" {
		t.Fatalf("the first row's pass deleted %d rows, leaving %d; want 1 and 1, with the shared bytes stored and the second row resolving", deleted, f.rows())
	}
	f.age(second, time.Minute)
	if deleted := f.collectPass(); deleted != 1 || f.rows() != 0 || f.blobExists("shared image") {
		t.Fatalf("the last row's pass deleted %d rows, leaving %d and the bytes stored %v; want 1, 0 and false", deleted, f.rows(), f.blobExists("shared image"))
	}
}

// imageStoreFixture is a Bridge store, as the bridge role, with a 60-second
// attachment lifetime over a fresh MinIO bucket, and two Sessions with a
// runtime binding each.
type imageStoreFixture struct {
	t      *testing.T
	admin  *sql.DB
	bridge *PostgreSQLBridgeAPIStore
	blobs  *blob.S3BlobStore
}

func newImageStoreFixture(t *testing.T) *imageStoreFixture {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	for _, session := range []string{"sesn_image_one", "sesn_image_two"} {
		sessionfixture.SeedBridgeAPISession(t, admin, "default", session, "thr_"+session)
		seedBridgeAPIRuntimeBinding(t, admin, "default", session, "bind_"+session, 1, "pod_"+session)
	}
	f := &imageStoreFixture{t: t, admin: admin, blobs: imageStoreMinIO(t)}
	f.bridge = NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(storagetest.OpenWorkloadDB(t, admin, "bridge").DB))
	f.bridge.AttachmentBlobStore = f.blobs
	f.bridge.AttachmentLifetime = time.Minute
	return f
}

// store stores data as a PNG attachment of session's thread, as Bridge
// stores an MCP Connector result.
func (f *imageStoreFixture) store(session string, data []byte) imagestore.Reference {
	f.t.Helper()
	var reference imagestore.Reference
	if err := f.bridge.Client.WithWorkspaceTx(context.Background(), "default", "test.store_attachment", func(tx *dbconnect.Tx) error {
		var err error
		reference, err = imagestore.Store(context.Background(), tx, f.blobs, workspace.DefaultID, session, "thr_"+session, "image/png", data)
		return err
	}); err != nil {
		f.t.Fatal(err)
	}
	return reference
}

// age sets the attachment's created_at that far before the database clock.
func (f *imageStoreFixture) age(reference imagestore.Reference, age time.Duration) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(context.Background(), `UPDATE session_attachments SET created_at=clock_timestamp()-make_interval(secs => $2) WHERE attachment_id=$1`, reference.AttachmentID, age.Seconds()); err != nil {
		f.t.Fatal(err)
	}
}

func (f *imageStoreFixture) resolve(session, attachmentID string) *bridgev1.ResolveAttachmentResponse {
	f.t.Helper()
	response, err := f.bridge.ResolveAttachment(context.Background(), &bridgev1.ResolveAttachmentRequest{
		Scope:        sessionfixture.BridgeAPIScope(session, "thr_"+session, "bind_"+session, 1, "pod_"+session),
		AttachmentId: attachmentID,
	})
	if err != nil {
		f.t.Fatalf("ResolveAttachment: %v", err)
	}
	return response
}

// rows counts the stored attachments.
func (f *imageStoreFixture) rows() int {
	f.t.Helper()
	var rows int
	if err := f.admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_attachments`).Scan(&rows); err != nil {
		f.t.Fatal(err)
	}
	return rows
}

// collectPass runs Bridge's collection task until a pass deletes
// attachments and returns how many that one pass deleted, from its log
// record; a pass that finds nothing expired writes none.
func (f *imageStoreFixture) collectPass() int {
	f.t.Helper()
	records := make(chan slog.Record, 16)
	stop := StartAttachmentGC(context.Background(), f.bridge, slog.New(recordHandler(records)), 20*time.Millisecond, 100)
	defer stop()
	select {
	case record := <-records:
		deleted := -1
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "attachment.deleted" {
				deleted = int(attr.Value.Int64())
			}
			return true
		})
		if record.Message != "bridge.attachment.gc_completed" {
			f.t.Fatalf("collection pass record %q; want bridge.attachment.gc_completed", record.Message)
		}
		return deleted
	case <-time.After(10 * time.Second):
		f.t.Fatalf("no collection pass deleted an attachment within 10s; %d rows", f.rows())
		return 0
	}
}

// recordHandler sends every log record to records.
type recordHandler chan<- slog.Record

func (h recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordHandler) Handle(_ context.Context, record slog.Record) error {
	h <- record.Clone()
	return nil
}
func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

// blobExists reports whether the bytes data are stored for the Workspace.
func (f *imageStoreFixture) blobExists(data string) bool {
	f.t.Helper()
	sum := sha256.Sum256([]byte(data))
	_, err := f.blobs.HeadObject(context.Background(), "attachments/default/"+hex.EncodeToString(sum[:]))
	var notFound *blob.NotFoundError
	if errors.As(err, &notFound) {
		return false
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return true
}

// imageStoreMinIO is an S3 blob store on a fresh bucket of the test MinIO.
func imageStoreMinIO(t *testing.T) *blob.S3BlobStore {
	t.Helper()
	endpoint := os.Getenv("TETRAL_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Fatal("TETRAL_TEST_MINIO_ENDPOINT is required")
	}
	cfg := blob.Config{Endpoint: endpoint, Region: os.Getenv("TETRAL_TEST_MINIO_REGION"), Bucket: fmt.Sprintf("bridge-images-%d", time.Now().UnixNano()),
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
