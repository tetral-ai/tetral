package files_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestCreateSessionFileIdentityEnforcesIdentityQuotaBeforeBlobStore(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{
		MaxRetainedBytesPerWorkspace:  100,
		MaxFileIdentitiesPerWorkspace: 1,
	})
	countingBlob := newSessionIdentityCountingBlobStore()
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), countingBlob, files.StoreLimits{
		MaxRetainedBytesPerWorkspace:  100,
		MaxFileIdentitiesPerWorkspace: 1,
	})
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_quota_source", 3)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_quota_source", "fobj_session_identity_quota_source", "source.txt", "text/plain", false, "", "", "")
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)
	beforeObjects := objectCount(t, env.admin, workspace.DefaultID)

	_, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_quota_source",
		SessionID:     "sesn_identity_quota",
		SessionFileID: "file_session_identity_quota_created",
		CreatedAt:     time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
	})

	if err == nil {
		t.Fatal("CreateSessionFileIdentity succeeded past identity quota")
	}
	var quota *files.QuotaError
	if !errors.As(err, &quota) || quota.Kind != files.QuotaKindCount {
		t.Fatalf("CreateSessionFileIdentity error = %T %v; want count quota", err, err)
	}
	if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles {
		t.Fatalf("files rows changed after quota rejection")
	}
	if objectCount(t, env.admin, workspace.DefaultID) != beforeObjects {
		t.Fatalf("file_objects rows changed after quota rejection")
	}
	if fileIdentityExists(t, env.admin, workspace.DefaultID, "file_session_identity_quota_created") {
		t.Fatal("rejected session identity row persisted")
	}
	if countingBlob.totalCalls() != 0 {
		t.Fatalf("BlobStore calls during quota rejection = %+v; want none", countingBlob.snapshot())
	}
}

func TestSessionFileIdentityLifecycleDoesNotIncrementRetainedBytesOrTouchBlobStore(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{
		MaxRetainedBytesPerWorkspace:  5,
		MaxFileIdentitiesPerWorkspace: 2,
	})
	countingBlob := newSessionIdentityCountingBlobStore()
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), countingBlob, files.StoreLimits{
		MaxRetainedBytesPerWorkspace:  5,
		MaxFileIdentitiesPerWorkspace: 2,
	})
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_retained_source", 5)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_retained_source", "fobj_session_identity_retained_source", "source.txt", "text/plain", false, "", "", "")
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)
	beforeObjects := objectCount(t, env.admin, workspace.DefaultID)
	beforeRetainedBytes := retainedFileObjectBytes(t, env.admin, workspace.DefaultID)

	created, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_retained_source",
		SessionID:     "sesn_identity_retained",
		SessionFileID: "file_session_identity_retained_created",
		CreatedAt:     time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("CreateSessionFileIdentity: %v", err)
	}
	if created.SizeBytes != beforeRetainedBytes {
		t.Fatalf("created size = %d; want copied source size %d", created.SizeBytes, beforeRetainedBytes)
	}
	if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles+1 {
		t.Fatalf("files rows = %d; want %d", fileCount(t, env.admin, workspace.DefaultID), beforeFiles+1)
	}
	if objectCount(t, env.admin, workspace.DefaultID) != beforeObjects {
		t.Fatalf("file_objects rows = %d; want unchanged %d", objectCount(t, env.admin, workspace.DefaultID), beforeObjects)
	}
	if retainedFileObjectBytes(t, env.admin, workspace.DefaultID) != beforeRetainedBytes {
		t.Fatalf("retained file object bytes changed after metadata-only session identity")
	}
	if err := tombstoneSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, "sesn_identity_retained", "file_session_identity_retained_created"); err != nil {
		t.Fatalf("TombstoneSessionFileIdentity: %v", err)
	}
	sessionRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_retained_created")
	if !sessionRow.deletedAt.Valid {
		t.Fatal("session identity was not tombstoned")
	}
	if objectCount(t, env.admin, workspace.DefaultID) != beforeObjects {
		t.Fatalf("file_objects rows changed after session identity tombstone")
	}
	if retainedFileObjectBytes(t, env.admin, workspace.DefaultID) != beforeRetainedBytes {
		t.Fatalf("retained file object bytes changed after session identity tombstone")
	}
	if countingBlob.totalCalls() != 0 {
		t.Fatalf("BlobStore calls during session identity lifecycle = %+v; want none", countingBlob.snapshot())
	}
}

func TestCreateSessionFileIdentityAdvisoryLockSerializesSameWorkspaceOnly(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{
		MaxRetainedBytesPerWorkspace:  100,
		MaxFileIdentitiesPerWorkspace: 10,
	})
	countingBlob := newSessionIdentityCountingBlobStore()
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), countingBlob, files.StoreLimits{
		MaxRetainedBytesPerWorkspace:  100,
		MaxFileIdentitiesPerWorkspace: 10,
	})
	workspaceA := workspace.ID("workspace_session_identity_lock_a")
	workspaceB := workspace.ID("workspace_session_identity_lock_b")
	seedWorkspace(t, env.admin, workspaceA)
	seedWorkspace(t, env.admin, workspaceB)
	seedSessionIdentitySource(t, env.admin, workspaceA, "file_session_identity_lock_a_hold", "fobj_session_identity_lock_a_hold", 5)
	seedSessionIdentitySource(t, env.admin, workspaceA, "file_session_identity_lock_a_wait", "fobj_session_identity_lock_a_wait", 6)
	seedSessionIdentitySource(t, env.admin, workspaceB, "file_session_identity_lock_b", "fobj_session_identity_lock_b", 7)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := dbconnect.NewClientForTesting(env.runtime)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	firstReady := make(chan error, 1)
	firstDone := make(chan error, 1)
	go func() {
		err := client.WithWorkspaceTx(ctx, string(workspaceA), "files.session_identity_lock_hold", func(tx *dbconnect.Tx) error {
			_, err := store.CreateSessionFileIdentity(ctx, testSessionFilesTransaction{tx: tx}, workspaceA, files.SessionFileIdentityRequest{
				SourceFileID:  "file_session_identity_lock_a_hold",
				SessionID:     "sesn_identity_lock_a",
				SessionFileID: "file_session_identity_lock_a_created_hold",
				CreatedAt:     time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC),
			})
			firstReady <- err
			if err != nil {
				return err
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		firstDone <- err
	}()
	if err := <-firstReady; err != nil {
		t.Fatalf("first session identity create: %v", err)
	}

	sameWorkspaceDone := make(chan error, 1)
	go func() {
		sameWorkspaceDone <- createSessionIdentityInWorkspaceTx(ctx, env.runtime, store, workspaceA, "files.session_identity_lock_same", files.SessionFileIdentityRequest{
			SourceFileID:  "file_session_identity_lock_a_wait",
			SessionID:     "sesn_identity_lock_a",
			SessionFileID: "file_session_identity_lock_a_created_wait",
			CreatedAt:     time.Date(2026, 5, 11, 12, 0, 1, 0, time.UTC),
		})
	}()
	select {
	case err := <-sameWorkspaceDone:
		t.Fatalf("same-workspace create completed while first transaction held Files lock: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	differentWorkspaceDone := make(chan error, 1)
	go func() {
		differentWorkspaceDone <- createSessionIdentityInWorkspaceTx(ctx, env.runtime, store, workspaceB, "files.session_identity_lock_different", files.SessionFileIdentityRequest{
			SourceFileID:  "file_session_identity_lock_b",
			SessionID:     "sesn_identity_lock_b",
			SessionFileID: "file_session_identity_lock_b_created",
			CreatedAt:     time.Date(2026, 5, 11, 12, 0, 2, 0, time.UTC),
		})
	}()
	select {
	case err := <-differentWorkspaceDone:
		if err != nil {
			t.Fatalf("different-workspace create: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("different-workspace create blocked behind workspace A Files lock")
	}

	close(release)
	released = true
	select {
	case err := <-sameWorkspaceDone:
		if err != nil {
			t.Fatalf("same-workspace create after lock release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-workspace create did not complete after lock release")
	}
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first transaction commit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first transaction did not finish after release")
	}
	if fileCount(t, env.admin, workspaceA) != 4 {
		t.Fatalf("workspace A files rows = %d; want two sources plus two session identities", fileCount(t, env.admin, workspaceA))
	}
	if fileCount(t, env.admin, workspaceB) != 2 {
		t.Fatalf("workspace B files rows = %d; want source plus session identity", fileCount(t, env.admin, workspaceB))
	}
	if countingBlob.totalCalls() != 0 {
		t.Fatalf("BlobStore calls during advisory-lock session identity creates = %+v; want none", countingBlob.snapshot())
	}
}

func createSessionIdentityInWorkspaceTx(ctx context.Context, db *sql.DB, store *files.PostgreSQLFileStore, workspaceID workspace.ID, operation string, request files.SessionFileIdentityRequest) error {
	client := dbconnect.NewClientForTesting(db)
	return client.WithWorkspaceTx(ctx, string(workspaceID), operation, func(tx *dbconnect.Tx) error {
		_, err := store.CreateSessionFileIdentity(ctx, testSessionFilesTransaction{tx: tx}, workspaceID, request)
		return err
	})
}

func seedSessionIdentitySource(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string, objectID string, sizeBytes int64) {
	t.Helper()
	seedFileObject(t, db, workspaceID, objectID, sizeBytes)
	seedFileIdentityForObject(t, db, workspaceID, fileID, objectID, fileID+".txt", "text/plain", false, "", "", "")
}

func retainedFileObjectBytes(t *testing.T, db *sql.DB, workspaceID workspace.ID) int64 {
	t.Helper()
	var total int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT COALESCE(SUM(size_bytes), 0) FROM file_objects WHERE workspace_id = $1`,
		string(workspaceID),
	).Scan(&total); err != nil {
		t.Fatalf("sum retained file object bytes: %v", err)
	}
	return total
}

type sessionIdentityBlobCounts struct {
	put          int
	get          int
	head         int
	copy         int
	delete       int
	deletePrefix int
}

type sessionIdentityCountingBlobStore struct {
	mu     sync.Mutex
	inner  *blob.FakeBlobStore
	counts sessionIdentityBlobCounts
}

func newSessionIdentityCountingBlobStore() *sessionIdentityCountingBlobStore {
	return &sessionIdentityCountingBlobStore{inner: blob.NewFakeBlobStore()}
}

func (s *sessionIdentityCountingBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	s.mu.Lock()
	s.counts.put++
	s.mu.Unlock()
	return s.inner.Put(ctx, key, content, size)
}

func (s *sessionIdentityCountingBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.counts.get++
	s.mu.Unlock()
	return s.inner.Get(ctx, key)
}

func (s *sessionIdentityCountingBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	s.mu.Lock()
	s.counts.head++
	s.mu.Unlock()
	return s.inner.HeadObject(ctx, key)
}

func (s *sessionIdentityCountingBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	s.mu.Lock()
	s.counts.copy++
	s.mu.Unlock()
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *sessionIdentityCountingBlobStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	s.counts.delete++
	s.mu.Unlock()
	return s.inner.Delete(ctx, key)
}

func (s *sessionIdentityCountingBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	s.mu.Lock()
	s.counts.deletePrefix++
	s.mu.Unlock()
	return s.inner.DeletePrefix(ctx, prefix)
}

func (s *sessionIdentityCountingBlobStore) snapshot() sessionIdentityBlobCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts
}

func (s *sessionIdentityCountingBlobStore) totalCalls() int {
	counts := s.snapshot()
	return counts.put + counts.get + counts.head + counts.copy + counts.delete + counts.deletePrefix
}

func (c sessionIdentityBlobCounts) String() string {
	return fmt.Sprintf("put=%d get=%d head=%d copy=%d delete=%d delete_prefix=%d", c.put, c.get, c.head, c.copy, c.delete, c.deletePrefix)
}
