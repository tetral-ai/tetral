package imagestore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type retryDeleteStore struct {
	blob.BlobStore
	missing bool
	fail    bool
}

func (s *retryDeleteStore) Delete(ctx context.Context, key string) error {
	if s.fail {
		return errors.New("temporary delete failure")
	}
	if s.missing {
		return &blob.NotFoundError{Key: key}
	}
	return s.BlobStore.Delete(ctx, key)
}

// Attachment production writers are staged. These fixtures exercise the shared
// storage owner using installed computer_hub and bridge database roles.
func TestAttachmentDeleteFailurePreservesCustodyAndReleasesLock(t *testing.T) {
	for _, mode := range []string{"returned error", "deadline"} {
		for _, stored := range []bool{true, false} {
			for _, missing := range []bool{false, true} {
				name := mode + map[bool]string{true: "/reference", false: "/upload"}[stored] + map[bool]string{true: "/missing", false: "/success"}[missing]
				t.Run(name, func(t *testing.T) {
					_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
					sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_delete", "thr_delete")
					roles := storagetest.OpenWorkloadDB(t, admin, "computer_hub")
					writer := dbconnect.NewClientForTesting(roles.DB)
					collector := dbconnect.NewClientForTesting(roles.OpenWorkload(t, "bridge", nil))
					var store blob.BlobStore
					var retry atomic.Bool
					var calls atomic.Int32
					fake := &retryDeleteStore{BlobStore: blob.NewFakeBlobStore(), missing: missing, fail: true}
					if mode == "returned error" {
						store = fake
					} else {
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.Method == http.MethodPut {
								_, _ = io.Copy(io.Discard, r.Body)
								w.WriteHeader(http.StatusOK)
								return
							}
							calls.Add(1)
							if !retry.Load() {
								<-r.Context().Done()
								return
							}
							if missing {
								w.WriteHeader(http.StatusNotFound)
								return
							}
							w.WriteHeader(http.StatusNoContent)
						}))
						defer server.Close()
						var err error
						store, err = blob.NewS3BlobStore(context.Background(), &blob.Config{Endpoint: server.URL, Region: "us-east-1", Bucket: "bucket", AccessKey: "fixture", SecretKey: "fixture", AllowInsecure: true, LocalTestMode: true, DeleteTimeout: 50 * time.Millisecond})
						if err != nil {
							t.Fatal(err)
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					upload, err := Upload(ctx, writer, store, workspace.DefaultID, "image/png", []byte("fixture bytes"))
					if err != nil {
						t.Fatal(err)
					}
					table := "session_attachment_uploads"
					if stored {
						table = "session_attachments"
						if err := writer.WithWorkspaceTx(ctx, "default", "test.store", func(tx *dbconnect.Tx) error {
							_, err := Store(ctx, tx, workspace.DefaultID, upload, "sesn_delete", "thr_delete")
							return err
						}); err != nil {
							t.Fatal(err)
						}
						if _, err := admin.Exec(`UPDATE session_attachments SET created_at=now()-interval '2 minutes'`); err != nil {
							t.Fatal(err)
						}
					} else {
						if _, err := admin.Exec(`UPDATE session_attachment_uploads SET check_after=now()-interval '1 minute'`); err != nil {
							t.Fatal(err)
						}
					}
					result, err := CollectExpired(ctx, collector, store, time.Minute, 100)
					if err != nil || result.Failed != 1 || result.Deleted != 0 {
						t.Fatalf("first collection: %+v %v", result, err)
					}
					if mode == "deadline" && calls.Load() != 1 {
						t.Fatalf("stalled DELETE attempts %d", calls.Load())
					}
					var count int
					// table comes from the two fixed custody shapes above.
					if err := admin.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 1 {
						t.Fatalf("custody count %d %v", count, err)
					}
					tx, err := admin.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					var acquired bool
					if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1,$2)`, contentLockCategory, contentLockResource("default", upload.ContentSHA256)).Scan(&acquired); err != nil {
						t.Fatal(err)
					}
					_ = tx.Rollback()
					if !acquired {
						t.Fatal("collection retained content lock")
					}
					retry.Store(true)
					fake.fail = false
					result, err = CollectExpired(ctx, collector, store, time.Minute, 100)
					if err != nil || result.Failed != 0 {
						t.Fatalf("retry: %+v %v", result, err)
					}
					if err := admin.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("retry custody count %d %v", count, err)
					}
				})
			}
		}
	}
}
