package files_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type filesStoreEnv struct {
	runtime  *sql.DB
	admin    *sql.DB
	blob     *blob.FakeBlobStore
	stageDir string
	store    *files.PostgreSQLFileStore
}

func newFilesStoreEnv(t *testing.T, limits files.StoreLimits) *filesStoreEnv {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	blobStore := blob.NewFakeBlobStore()
	stageDir := t.TempDir()
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(runtime), blobStore, limits)
	return &filesStoreEnv{
		runtime:  runtime,
		admin:    admin,
		blob:     blobStore,
		stageDir: stageDir,
		store:    store,
	}
}

func stagePlainFile(t *testing.T, stageDir, filename, mimeType, body string) *files.StagedUpload {
	t.Helper()
	staged, err := files.StageUpload(strings.NewReader(body), stageDir, filename, mimeType, files.UploadLimits{MaxFileBytes: 1024})
	if err != nil {
		t.Fatalf("StageUpload: %v", err)
	}
	t.Cleanup(func() { _ = staged.Cleanup() })
	return staged
}

func seedWorkspace(t *testing.T, admin *sql.DB, workspaceID workspace.ID) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`, string(workspaceID)); err != nil {
		t.Fatalf("seed workspace %s: %v", workspaceID, err)
	}
}

func seedFilesSession(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, archived bool) {
	t.Helper()
	seedWorkspace(t, db, workspaceID)
	agentID := "agent_" + sessionID
	envID := "env_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $2, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		string(workspaceID), "agv_"+sessionID, agentID, "hash_"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $2, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), envID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	var archivedAt any
	if archived {
		archivedAt = "2026-01-01T00:00:01Z"
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (workspace_id, id, type, metadata_json, status, lifecycle_state, archived_at, agent_id, agent_version, environment_id, vault_ids_json, created_at, updated_at)
		 VALUES ($1, $2, 'session', '{}', 'idle', 'active', $3, $4, 1, $5, '[]', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, archivedAt, agentID, envID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func isFilesNotFound(err error) bool {
	var notFound *files.NotFoundError
	return errors.As(err, &notFound)
}

func isFilesValidation(err error) bool {
	var validation *files.ValidationError
	return errors.As(err, &validation)
}

func TestProductionStoreQuotaConstantsMatchContract(t *testing.T) {
	if files.MaxRetainedBytesPerWorkspace != 500000000000 {
		t.Fatalf("MaxRetainedBytesPerWorkspace = %d; want 500000000000", files.MaxRetainedBytesPerWorkspace)
	}
	if files.MaxFileIdentitiesPerWorkspace != 100000 {
		t.Fatalf("MaxFileIdentitiesPerWorkspace = %d; want 100000", files.MaxFileIdentitiesPerWorkspace)
	}
}

func TestCreateUploadedFileStoresRowsAndBlobBytes(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	body := "alpha,beta\n1,2\n"
	staged := stagePlainFile(t, env.stageDir, "data.csv", "text/csv", body)
	tempPath := staged.TempPathForTest()

	created, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, staged)
	if err != nil {
		t.Fatalf("CreateUploadedFile: %v", err)
	}
	if _, statErr := os.Stat(tempPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("staged temp file after successful CreateUploadedFile err = %v; want not exist", statErr)
	}
	if !strings.HasPrefix(created.ID, files.IDPrefix) {
		t.Errorf("file id %q must start with %q", created.ID, files.IDPrefix)
	}
	if created.Type != "file" {
		t.Errorf("Type = %q; want file", created.Type)
	}
	if created.Filename != "data.csv" {
		t.Errorf("Filename = %q; want data.csv", created.Filename)
	}
	if created.MIMEType != "text/csv" {
		t.Errorf("MIMEType = %q; want text/csv", created.MIMEType)
	}
	if created.SizeBytes != int64(len(body)) {
		t.Errorf("SizeBytes = %d; want %d", created.SizeBytes, len(body))
	}
	if created.Downloadable {
		t.Error("uploaded originals must return downloadable=false")
	}
	if created.Scope != nil {
		t.Errorf("uploaded originals must not have scope; got %+v", created.Scope)
	}
	if created.CreatedAt.IsZero() {
		t.Error("CreatedAt must be populated")
	}

	row := loadFileRow(t, env.admin, workspace.DefaultID, created.ID)
	if !strings.HasPrefix(row.objectID, files.ObjectIDPrefix) {
		t.Errorf("object_id = %q; want prefix %q", row.objectID, files.ObjectIDPrefix)
	}
	wantKeyPrefix := "files/" + string(workspace.DefaultID) + "/" + files.ObjectIDPrefix
	if !strings.HasPrefix(row.blobKey, wantKeyPrefix) {
		t.Errorf("blob_key = %q; want prefix %q", row.blobKey, wantKeyPrefix)
	}
	if !env.blob.Has(row.blobKey) {
		t.Fatalf("blob missing at %q", row.blobKey)
	}
	gotBytes, _ := env.blob.Bytes(row.blobKey)
	if !bytes.Equal(gotBytes, []byte(body)) {
		t.Errorf("blob bytes = %q; want %q", gotBytes, body)
	}
	if objectCount(t, env.admin, workspace.DefaultID) != 1 {
		t.Errorf("file_objects rows = %d; want 1", objectCount(t, env.admin, workspace.DefaultID))
	}
	if fileCount(t, env.admin, workspace.DefaultID) != 1 {
		t.Errorf("files rows = %d; want 1", fileCount(t, env.admin, workspace.DefaultID))
	}
}

func TestCreateUploadedPDFStoresPageCountAndUnreadableSentinel(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	for _, test := range []struct {
		name      string
		body      []byte
		wantPages int64
	}{
		{name: "readable", body: minimalTestPDF(2), wantPages: 2},
		{name: "unreadable", body: []byte("not a pdf"), wantPages: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			staged, err := files.StageUpload(bytes.NewReader(test.body), env.stageDir, test.name+".pdf", "application/pdf", files.UploadLimits{})
			if err != nil {
				t.Fatalf("StageUpload: %v", err)
			}
			created, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, staged)
			if err != nil {
				t.Fatalf("CreateUploadedFile: %v", err)
			}
			var got int64
			if err := env.admin.QueryRowContext(context.Background(),
				`SELECT o.pdf_page_count
				   FROM files f
				   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
				  WHERE f.workspace_id = $1 AND f.file_id = $2`,
				string(workspace.DefaultID), created.ID,
			).Scan(&got); err != nil {
				t.Fatalf("read pdf_page_count: %v", err)
			}
			if got != test.wantPages {
				t.Fatalf("pdf_page_count = %d; want %d", got, test.wantPages)
			}
		})
	}
}

func TestCreateUploadedFileTreatsFilenameAsMetadataOnly(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	filename := "../tenant/files/default/fobj_bad.csv"
	staged := stagePlainFile(t, env.stageDir, filename, "text/csv", "csv")

	created, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, staged)
	if err != nil {
		t.Fatalf("CreateUploadedFile: %v", err)
	}
	row := loadFileRow(t, env.admin, workspace.DefaultID, created.ID)
	if created.Filename != filename {
		t.Errorf("returned filename = %q; want original metadata %q", created.Filename, filename)
	}
	if row.filename != filename {
		t.Errorf("stored filename = %q; want original metadata %q", row.filename, filename)
	}
	if strings.Contains(row.blobKey, "tenant") || strings.Contains(row.blobKey, "fobj_bad") || strings.Contains(row.blobKey, ".csv") {
		t.Fatalf("blob key %q contains uploaded filename fragments", row.blobKey)
	}
	if row.blobKey != "files/"+string(workspace.DefaultID)+"/"+row.objectID {
		t.Fatalf("blob key = %q; want files/<workspace>/<object_id>", row.blobKey)
	}
}

func TestCreateUploadedFileRejectsRetainedBytesQuotaBeforeBlobPut(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{MaxRetainedBytesPerWorkspace: 10, MaxFileIdentitiesPerWorkspace: 100})
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_seed_quota", 9)
	beforeBlobCount := env.blob.Len()
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)
	beforeObjects := objectCount(t, env.admin, workspace.DefaultID)

	_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, stagePlainFile(t, env.stageDir, "too-large.txt", "text/plain", "xx"))
	if err == nil {
		t.Fatal("expected retained-byte quota rejection")
	}
	var quota *files.QuotaError
	if !errors.As(err, &quota) {
		t.Fatalf("expected *files.QuotaError; got %T (%v)", err, err)
	}
	if quota.Kind != files.QuotaKindRetainedBytes {
		t.Fatalf("QuotaError.Kind = %d; want retained bytes", quota.Kind)
	}
	if env.blob.Len() != beforeBlobCount {
		t.Fatalf("BlobStore Put happened before quota rejection")
	}
	if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles {
		t.Fatalf("files row count changed before quota rejection")
	}
	if objectCount(t, env.admin, workspace.DefaultID) != beforeObjects {
		t.Fatalf("file_objects row count changed before quota rejection")
	}
}

func TestCreateUploadedFileIdentityQuotaCountsDeletedRowsBeforeBlobPut(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{MaxRetainedBytesPerWorkspace: 1000, MaxFileIdentitiesPerWorkspace: 1})
	seedDeletedFile(t, env.admin, workspace.DefaultID, "file_deleted_quota", "fobj_deleted_quota")
	beforeBlobCount := env.blob.Len()

	_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, stagePlainFile(t, env.stageDir, "new.txt", "text/plain", "x"))
	if err == nil {
		t.Fatal("expected file identity quota rejection")
	}
	var quota *files.QuotaError
	if !errors.As(err, &quota) {
		t.Fatalf("expected *files.QuotaError; got %T (%v)", err, err)
	}
	if quota.Kind != files.QuotaKindCount {
		t.Fatalf("QuotaError.Kind = %d; want count", quota.Kind)
	}
	if env.blob.Len() != beforeBlobCount {
		t.Fatalf("BlobStore Put happened before identity quota rejection")
	}
}

func TestCreateUploadedFileBlobPutFailureRollsBackRowsAndCleansStage(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	const uploadedPayload = "raw uploaded payload sentinel"
	staged := stagePlainFile(t, env.stageDir, "blobfail.txt", "text/plain", uploadedPayload)
	tempPath := staged.TempPathForTest()
	providerMessage := strings.Join([]string{
		"provider endpoint secret.internal",
		"bucket=tetral-private",
		"files/default/fobj_provider_leak",
		tempPath,
		"INSERT INTO file_objects",
		uploadedPayload,
	}, " ")
	env.blob.SetPutHook(func(_ context.Context, _ string) error { return errors.New(providerMessage) })

	_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, staged)
	if err == nil {
		t.Fatal("expected BlobStore Put failure")
	}
	for _, forbidden := range []string{
		"secret.internal",
		"tetral-private",
		"files/default/fobj_provider_leak",
		tempPath,
		"INSERT INTO file_objects",
		uploadedPayload,
	} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("public error %q leaked forbidden detail %q", err.Error(), forbidden)
		}
	}
	if fileCount(t, env.admin, workspace.DefaultID) != 0 {
		t.Fatalf("files rows persisted after BlobStore Put failure")
	}
	if objectCount(t, env.admin, workspace.DefaultID) != 0 {
		t.Fatalf("file_objects rows persisted after BlobStore Put failure")
	}
	if _, statErr := osStat(tempPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("staged temp file after failure err = %v; want not exist", statErr)
	}
	if len(env.blob.Deletes()) != 1 {
		t.Fatalf("best-effort delete calls = %v; want one attempted cleanup", env.blob.Deletes())
	}
}

func TestCreateUploadedFileCommitFailureDeletesJustCreatedBlobWithIndependentContext(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	inner := blob.NewFakeBlobStore()
	wrapped := &deleteContextCheckingBlobStore{inner: inner, cancelRequest: cancelRequest}
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(runtime), wrapped, files.StoreLimits{})
	store.SetTxRunner(func(ctx context.Context, workspaceID string, fn func(files.Transaction) error, onCommitFailure func()) error {
		tx, err := runtime.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, "SELECT set_config('tetral.workspace_id', $1, true)", workspaceID); err != nil {
			return err
		}
		filesTx := testFilesTransaction{tx: dbconnect.NewTxForTesting(tx, dbconnect.NewClientForTesting(runtime), "files.transaction")}
		if err := fn(filesTx); err != nil {
			return err
		}
		if onCommitFailure != nil {
			onCommitFailure()
		}
		return errors.New("synthetic commit failure")
	})
	staged := stagePlainFile(t, t.TempDir(), "commitfail.txt", "text/plain", "payload")

	_, err := store.CreateUploadedFile(requestCtx, workspace.DefaultID, staged)
	if err == nil || !strings.Contains(err.Error(), "synthetic commit failure") {
		t.Fatalf("expected synthetic commit failure; got %v", err)
	}
	if inner.Len() != 0 {
		t.Fatalf("blob store has orphan after commit failure: len=%d", inner.Len())
	}
	if len(wrapped.deleteKeys) != 1 {
		t.Fatalf("delete keys = %v; want one cleanup delete", wrapped.deleteKeys)
	}
	if len(wrapped.deleteFailures) != 0 {
		t.Fatalf("cleanup context failures: %v", wrapped.deleteFailures)
	}
	if fileCount(t, admin, workspace.DefaultID) != 0 || objectCount(t, admin, workspace.DefaultID) != 0 {
		t.Fatalf("SQL rows persisted after synthetic commit failure")
	}
}

func TestCreateUploadedFileDuplicateBlobKeyMapsToConflictAndRollsBack(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	env.blob.SetPutHook(func(_ context.Context, key string) error {
		return &blob.DuplicateKeyError{Key: key}
	})
	_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, stagePlainFile(t, env.stageDir, "dup.txt", "text/plain", "payload"))
	if err == nil {
		t.Fatal("expected duplicate blob key rejection")
	}
	var conflict *files.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected *files.ConflictError; got %T (%v)", err, err)
	}
	if errors.As(err, new(*blob.DuplicateKeyError)) {
		t.Fatalf("raw *blob.DuplicateKeyError leaked past files store boundary")
	}
	if fileCount(t, env.admin, workspace.DefaultID) != 0 || objectCount(t, env.admin, workspace.DefaultID) != 0 {
		t.Fatalf("SQL rows persisted after duplicate-key BlobStore failure")
	}
	if len(env.blob.Deletes()) != 0 {
		t.Fatalf("duplicate-key conflict must not delete another writer's key; deletes=%v", env.blob.Deletes())
	}
}

type testFilesTransaction struct {
	tx *dbconnect.Tx
}

func (t testFilesTransaction) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t testFilesTransaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t testFilesTransaction) Query(ctx context.Context, query string, args ...any) (files.Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t testFilesTransaction) QueryRow(ctx context.Context, query string, args ...any) files.Row {
	return t.tx.QueryRow(ctx, query, args...)
}

func TestCreateUploadedFileConcurrentIdentityQuotaSerializesSameWorkspace(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{MaxRetainedBytesPerWorkspace: 1000, MaxFileIdentitiesPerWorkspace: 1})
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		i := i
		go func() {
			<-start
			_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, stagePlainFile(t, env.stageDir, fmt.Sprintf("c%d.txt", i), "text/plain", "x"))
			results <- err
		}()
	}
	close(start)

	successes, quotaErrors := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			successes++
			continue
		}
		var quota *files.QuotaError
		if errors.As(err, &quota) && quota.Kind == files.QuotaKindCount {
			quotaErrors++
			continue
		}
		t.Fatalf("unexpected concurrent result: %T %v", err, err)
	}
	if successes != 1 || quotaErrors != 1 {
		t.Fatalf("successes=%d quotaErrors=%d; want 1 and 1", successes, quotaErrors)
	}
	if fileCount(t, env.admin, workspace.DefaultID) != 1 {
		t.Fatalf("file rows after quota race = %d; want 1", fileCount(t, env.admin, workspace.DefaultID))
	}
	if env.blob.Len() != 1 {
		t.Fatalf("blob count after quota race = %d; want 1", env.blob.Len())
	}
}

func TestCreateUploadedFileRetainedByteQuotaCountsUniqueObjectsNotFileIdentities(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{MaxRetainedBytesPerWorkspace: 10, MaxFileIdentitiesPerWorkspace: 100})
	seedVisibleFileWithBody(t, env, workspace.DefaultID, "file_quota_shared_a", "fobj_quota_shared", false, "", "", "123456789")
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_quota_shared_b", "fobj_quota_shared", "shared-b.txt", "text/plain", false, "", "", "")
	beforeBlobCount := env.blob.Len()
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)
	beforeObjects := objectCount(t, env.admin, workspace.DefaultID)

	created, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, stagePlainFile(t, env.stageDir, "fits.txt", "text/plain", "x"))
	if err != nil {
		t.Fatalf("CreateUploadedFile: %v", err)
	}
	row := loadFileRow(t, env.admin, workspace.DefaultID, created.ID)
	if !env.blob.Has(row.blobKey) {
		t.Fatalf("new blob %q missing", row.blobKey)
	}
	if env.blob.Len() != beforeBlobCount+1 {
		t.Fatalf("blob count after shared-object upload = %d; want %d", env.blob.Len(), beforeBlobCount+1)
	}
	if objectCount(t, env.admin, workspace.DefaultID) != beforeObjects+1 {
		t.Fatalf("file_objects rows after shared-object upload = %d; want %d", objectCount(t, env.admin, workspace.DefaultID), beforeObjects+1)
	}
	if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles+1 {
		t.Fatalf("files rows after shared-object upload = %d; want %d", fileCount(t, env.admin, workspace.DefaultID), beforeFiles+1)
	}
}

func TestCreateUploadedFileConcurrentRetainedByteQuotaSerializesSameWorkspace(t *testing.T) {
	limits := files.StoreLimits{MaxRetainedBytesPerWorkspace: 10, MaxFileIdentitiesPerWorkspace: 100}
	env := newFilesStoreEnv(t, limits)
	gatedBlob := &workspaceGatedBlobStore{inner: env.blob, gatedWorkspace: string(workspace.DefaultID), entered: make(chan struct{}), release: make(chan struct{})}
	env.store = files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), gatedBlob, limits)
	firstUpload := stagePlainFile(t, env.stageDir, "retained-a.txt", "text/plain", "123456")
	secondUpload := stagePlainFile(t, env.stageDir, "retained-b.txt", "text/plain", "abcdef")

	firstDone := make(chan error, 1)
	go func() {
		_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, firstUpload)
		firstDone <- err
	}()
	select {
	case <-gatedBlob.entered:
	case err := <-firstDone:
		t.Fatalf("first upload completed before gated BlobStore Put: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("first upload did not reach BlobStore Put")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, secondUpload)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second upload completed while first retained-byte transaction was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gatedBlob.release)

	successes, quotaErrors := 0, 0
	for _, result := range []<-chan error{firstDone, secondDone} {
		err := <-result
		if err == nil {
			successes++
			continue
		}
		var quota *files.QuotaError
		if errors.As(err, &quota) && quota.Kind == files.QuotaKindRetainedBytes {
			quotaErrors++
			continue
		}
		t.Fatalf("unexpected concurrent result: %T %v", err, err)
	}
	if successes != 1 || quotaErrors != 1 {
		t.Fatalf("successes=%d quotaErrors=%d; want 1 and 1", successes, quotaErrors)
	}
	if env.blob.Len() != 1 {
		t.Fatalf("blob count after retained-byte quota race = %d; want 1", env.blob.Len())
	}
	if objectCount(t, env.admin, workspace.DefaultID) != 1 {
		t.Fatalf("file_objects rows after retained-byte quota race = %d; want 1", objectCount(t, env.admin, workspace.DefaultID))
	}
	if fileCount(t, env.admin, workspace.DefaultID) != 1 {
		t.Fatalf("files rows after retained-byte quota race = %d; want 1", fileCount(t, env.admin, workspace.DefaultID))
	}
}

func TestCreateUploadedFileDifferentWorkspacesDoNotBlockOnFilesLock(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	workspaceA := workspace.ID("workspace_files_a")
	workspaceB := workspace.ID("workspace_files_b")
	seedWorkspace(t, env.admin, workspaceA)
	seedWorkspace(t, env.admin, workspaceB)
	gatedBlob := &workspaceGatedBlobStore{inner: env.blob, gatedWorkspace: string(workspaceA), entered: make(chan struct{}), release: make(chan struct{})}
	env.store = files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), gatedBlob, files.StoreLimits{})

	firstDone := make(chan error, 1)
	go func() {
		_, err := env.store.CreateUploadedFile(context.Background(), workspaceA, stagePlainFile(t, env.stageDir, "a.txt", "text/plain", "a"))
		firstDone <- err
	}()
	<-gatedBlob.entered

	secondDone := make(chan error, 1)
	go func() {
		_, err := env.store.CreateUploadedFile(context.Background(), workspaceB, stagePlainFile(t, env.stageDir, "b.txt", "text/plain", "b"))
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("workspace B upload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workspace B upload blocked behind workspace A Files lock")
	}
	close(gatedBlob.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("workspace A upload: %v", err)
	}
}

func TestListFilesEmptyAndDefaultLimitShape(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})

	empty, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{})
	if err != nil {
		t.Fatalf("ListFiles empty: %v", err)
	}
	if len(empty.Data) != 0 || empty.FirstID != nil || empty.LastID != nil || empty.HasMore {
		t.Fatalf("empty list = %+v; want empty data, nil cursors, has_more=false", empty)
	}

	for i := 0; i < 25; i++ {
		seedVisibleFile(t, env, workspace.DefaultID, fmt.Sprintf("file_list_%02d", i), fmt.Sprintf("fobj_list_%02d", i), false, "", "")
	}
	got, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{})
	if err != nil {
		t.Fatalf("ListFiles default: %v", err)
	}
	if len(got.Data) != 20 {
		t.Fatalf("default list length = %d; want 20", len(got.Data))
	}
	if got.FirstID == nil || *got.FirstID != "file_list_00" {
		t.Fatalf("first_id = %v; want file_list_00", got.FirstID)
	}
	if got.LastID == nil || *got.LastID != "file_list_19" {
		t.Fatalf("last_id = %v; want file_list_19", got.LastID)
	}
	if !got.HasMore {
		t.Fatal("has_more = false; want true")
	}
}

func TestListFilesLimitsOrderingDeletedAndCursors(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	for i := 0; i < 5; i++ {
		seedVisibleFile(t, env, workspace.DefaultID, fmt.Sprintf("file_page_%02d", i), fmt.Sprintf("fobj_page_%02d", i), false, "", "")
	}
	seedDeletedFile(t, env.admin, workspace.DefaultID, "file_page_deleted", "fobj_page_deleted")

	first, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListFiles limit 1: %v", err)
	}
	if idsOf(first.Data) != "file_page_00" || !first.HasMore {
		t.Fatalf("limit 1 result ids=%s has_more=%v; want first row and has_more", idsOf(first.Data), first.HasMore)
	}

	thousand, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("ListFiles limit 1000: %v", err)
	}
	if idsOf(thousand.Data) != "file_page_00,file_page_01,file_page_02,file_page_03,file_page_04" {
		t.Fatalf("limit 1000 ids = %s", idsOf(thousand.Data))
	}
	if thousand.HasMore {
		t.Fatal("limit 1000 has_more = true; want false")
	}
	if thousand.FirstID == nil || *thousand.FirstID != "file_page_00" || thousand.LastID == nil || *thousand.LastID != "file_page_04" {
		t.Fatalf("unexpected first/last: %+v", thousand)
	}

	after, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{Limit: 2, AfterID: "file_page_01"})
	if err != nil {
		t.Fatalf("ListFiles after: %v", err)
	}
	if idsOf(after.Data) != "file_page_02,file_page_03" || !after.HasMore {
		t.Fatalf("after ids=%s has_more=%v; want page_02,page_03 and has_more", idsOf(after.Data), after.HasMore)
	}

	before, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{BeforeID: "file_page_03"})
	if err != nil {
		t.Fatalf("ListFiles before: %v", err)
	}
	if idsOf(before.Data) != "file_page_00,file_page_01,file_page_02" || before.HasMore {
		t.Fatalf("before ids=%s has_more=%v; want first three and no more", idsOf(before.Data), before.HasMore)
	}

	between, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{AfterID: "file_page_01", BeforeID: "file_page_04"})
	if err != nil {
		t.Fatalf("ListFiles between: %v", err)
	}
	if idsOf(between.Data) != "file_page_02,file_page_03" || between.HasMore {
		t.Fatalf("between ids=%s has_more=%v; want page_02,page_03 and no more", idsOf(between.Data), between.HasMore)
	}

	inverted, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{AfterID: "file_page_04", BeforeID: "file_page_01"})
	if err != nil {
		t.Fatalf("ListFiles inverted: %v", err)
	}
	if len(inverted.Data) != 0 || inverted.FirstID != nil || inverted.LastID != nil || inverted.HasMore {
		t.Fatalf("inverted range = %+v; want empty", inverted)
	}
}

func TestListFilesCursorValidationAndScopedMetadata(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	workspaceB := workspace.ID("workspace_files_list_b")
	seedWorkspace(t, env.admin, workspaceB)
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_a", false)
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_b", false)
	seedVisibleFile(t, env, workspace.DefaultID, "file_original", "fobj_original", false, "", "")
	seedVisibleFile(t, env, workspace.DefaultID, "file_session_a", "fobj_session_a", false, "session", "sesn_a")
	seedVisibleFile(t, env, workspace.DefaultID, "file_session_b", "fobj_session_b", false, "session", "sesn_b")
	seedDeletedFile(t, env.admin, workspace.DefaultID, "file_deleted_cursor", "fobj_deleted_cursor")
	seedVisibleFile(t, env, workspaceB, "file_cross_cursor", "fobj_cross_cursor", false, "", "")

	scoped, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{ScopeID: "sesn_a"})
	if err != nil {
		t.Fatalf("ListFiles scoped: %v", err)
	}
	if idsOf(scoped.Data) != "file_session_a" {
		t.Fatalf("scoped ids = %s; want file_session_a", idsOf(scoped.Data))
	}
	if scoped.Data[0].Scope == nil || scoped.Data[0].Scope.Type != "session" || scoped.Data[0].Scope.ID != "sesn_a" {
		t.Fatalf("scoped metadata = %+v; want session/sesn_a", scoped.Data[0].Scope)
	}

	for _, tc := range []struct {
		name    string
		options files.ListOptions
	}{
		{name: "missing after", options: files.ListOptions{AfterID: "file_missing"}},
		{name: "deleted after", options: files.ListOptions{AfterID: "file_deleted_cursor"}},
		{name: "cross workspace after", options: files.ListOptions{AfterID: "file_cross_cursor"}},
		{name: "scope mismatch after", options: files.ListOptions{AfterID: "file_session_b", ScopeID: "sesn_a"}},
		{name: "missing before", options: files.ListOptions{BeforeID: "file_missing"}},
		{name: "deleted before", options: files.ListOptions{BeforeID: "file_deleted_cursor"}},
		{name: "cross workspace before", options: files.ListOptions{BeforeID: "file_cross_cursor"}},
		{name: "scope mismatch before", options: files.ListOptions{BeforeID: "file_session_b", ScopeID: "sesn_a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.store.ListFiles(context.Background(), workspace.DefaultID, tc.options)
			var validation *files.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("ListFiles error = %T %v; want ValidationError", err, err)
			}
		})
	}
}

func TestGetDeleteContentAndResolverBehavior(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	workspaceB := workspace.ID("workspace_files_get_b")
	seedWorkspace(t, env.admin, workspaceB)
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_download", false)
	seedVisibleFile(t, env, workspace.DefaultID, "file_original_get", "fobj_original_get", false, "", "")
	seedVisibleFile(t, env, workspace.DefaultID, "file_downloadable_get", "fobj_downloadable_get", true, "session", "sesn_download")
	seedVisibleFile(t, env, workspace.DefaultID, "file_shared_a", "fobj_shared", false, "", "")
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_shared_b", "fobj_shared", "shared-b.txt", "text/plain", false, "", "", "")
	seedDeletedFile(t, env.admin, workspace.DefaultID, "file_deleted_get", "fobj_deleted_get")
	seedVisibleFile(t, env, workspaceB, "file_cross_get", "fobj_cross_get", true, "", "")

	meta, err := env.store.GetFile(context.Background(), workspace.DefaultID, "file_downloadable_get")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if meta.Scope == nil || meta.Scope.Type != "session" || meta.Scope.ID != "sesn_download" {
		t.Fatalf("GetFile scope = %+v; want session/sesn_download", meta.Scope)
	}
	for _, id := range []string{"file_missing", "file_deleted_get", "file_cross_get"} {
		t.Run("get "+id, func(t *testing.T) {
			_, err := env.store.GetFile(context.Background(), workspace.DefaultID, id)
			var notFound *files.NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("GetFile error = %T %v; want NotFoundError", err, err)
			}
		})
	}

	countingBlob := &countingBlobStore{inner: env.blob}
	contentStore := files.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime), countingBlob)
	_, err = contentStore.OpenContent(context.Background(), workspace.DefaultID, "file_original_get")
	var permission *files.PermissionError
	if !errors.As(err, &permission) {
		t.Fatalf("OpenContent original error = %T %v; want PermissionError", err, err)
	}
	if countingBlob.getCalls != 0 {
		t.Fatalf("BlobStore Get calls for non-downloadable content = %d; want 0", countingBlob.getCalls)
	}

	stream, err := env.store.OpenContent(context.Background(), workspace.DefaultID, "file_downloadable_get")
	if err != nil {
		t.Fatalf("OpenContent downloadable: %v", err)
	}
	body, err := io.ReadAll(stream.Reader)
	if err != nil {
		t.Fatalf("read downloadable content: %v", err)
	}
	_ = stream.Reader.Close()
	if string(body) != "body:file_downloadable_get" {
		t.Fatalf("downloaded bytes = %q", string(body))
	}
	if stream.Metadata.MIMEType != "text/plain" || stream.Metadata.Scope == nil || stream.Metadata.Scope.ID != "sesn_download" {
		t.Fatalf("download metadata = %+v", stream.Metadata)
	}

	source, err := env.store.ResolveSource(context.Background(), workspace.DefaultID, "file_original_get")
	if err != nil {
		t.Fatalf("ResolveSource original: %v", err)
	}
	if source.Metadata.Downloadable {
		t.Fatal("resolver metadata should preserve downloadable=false for uploaded original")
	}
	sourceReader, err := source.Open(context.Background())
	if err != nil {
		t.Fatalf("source Open: %v", err)
	}
	sourceBytes, err := io.ReadAll(sourceReader)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	_ = sourceReader.Close()
	if string(sourceBytes) != "body:file_original_get" {
		t.Fatalf("source bytes = %q", string(sourceBytes))
	}
	for _, id := range []string{"file_missing", "file_deleted_get", "file_cross_get"} {
		t.Run("resolve "+id, func(t *testing.T) {
			_, err := env.store.ResolveSource(context.Background(), workspace.DefaultID, id)
			var notFound *files.NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("ResolveSource error = %T %v; want NotFoundError", err, err)
			}
		})
	}

	deleteCounting := &countingBlobStore{inner: env.blob}
	deleteStore := files.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime), deleteCounting)
	deleted, err := env.store.DeleteFile(context.Background(), workspace.DefaultID, "file_shared_a")
	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if deleted.ID != "file_shared_a" || deleted.Type != "file_deleted" {
		t.Fatalf("DeleteFile response = %+v", deleted)
	}
	if objectCount(t, env.admin, workspace.DefaultID) == 0 {
		t.Fatal("DeleteFile removed file_objects")
	}
	if !env.blob.Has("files/" + string(workspace.DefaultID) + "/fobj_shared") {
		t.Fatal("DeleteFile removed shared blob")
	}
	if _, err := env.store.GetFile(context.Background(), workspace.DefaultID, "file_shared_b"); err != nil {
		t.Fatalf("shared identity broke after deleting sibling: %v", err)
	}
	seedVisibleFile(t, env, workspace.DefaultID, "file_delete_side_effect", "fobj_delete_side_effect", false, "", "")
	if _, err := deleteStore.DeleteFile(context.Background(), workspace.DefaultID, "file_delete_side_effect"); err != nil {
		t.Fatalf("DeleteFile side-effect proof: %v", err)
	}
	if deleteCounting.deleteCalls != 0 || deleteCounting.deletePrefixCalls != 0 {
		t.Fatalf("DeleteFile called BlobStore cleanup: delete=%d deletePrefix=%d", deleteCounting.deleteCalls, deleteCounting.deletePrefixCalls)
	}
	for _, lookup := range []func(context.Context, workspace.ID, string) error{
		func(ctx context.Context, ws workspace.ID, id string) error {
			_, err := env.store.GetFile(ctx, ws, id)
			return err
		},
		func(ctx context.Context, ws workspace.ID, id string) error {
			_, err := env.store.OpenContent(ctx, ws, id)
			return err
		},
		func(ctx context.Context, ws workspace.ID, id string) error {
			_, err := env.store.ResolveSource(ctx, ws, id)
			return err
		},
	} {
		err := lookup(context.Background(), workspace.DefaultID, "file_shared_a")
		var notFound *files.NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("post-delete lookup error = %T %v; want NotFoundError", err, err)
		}
	}
	list, err := env.store.ListFiles(context.Background(), workspace.DefaultID, files.ListOptions{})
	if err != nil {
		t.Fatalf("post-delete list: %v", err)
	}
	if strings.Contains(idsOf(list.Data), "file_shared_a") {
		t.Fatalf("deleted identity appears in list: %s", idsOf(list.Data))
	}
	for _, id := range []string{"file_missing", "file_deleted_get", "file_cross_get", "file_shared_a"} {
		t.Run("delete "+id, func(t *testing.T) {
			_, err := env.store.DeleteFile(context.Background(), workspace.DefaultID, id)
			var notFound *files.NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("DeleteFile error = %T %v; want NotFoundError", err, err)
			}
		})
	}
}

func TestSessionScopedFilesUseDurableSessionVisibilityWithoutSandboxStateGate(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	ctx := context.Background()

	seedVisibleFile(t, env, workspace.DefaultID, "file_original_visible", "fobj_original_visible", false, "", "")
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_files_active", false)
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_files_archived", true)
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_files_deleted", false)
	if _, err := env.admin.ExecContext(ctx,
		`UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), "sesn_files_deleted"); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}

	seedVisibleFile(t, env, workspace.DefaultID, "file_session_active", "fobj_session_active", true, "session", "sesn_files_active")
	seedVisibleFile(t, env, workspace.DefaultID, "file_session_archived", "fobj_session_archived", true, "session", "sesn_files_archived")
	seedVisibleFile(t, env, workspace.DefaultID, "file_session_deleted", "fobj_session_deleted", true, "session", "sesn_files_deleted")

	for _, id := range []string{"file_session_deleted"} {
		t.Run("hidden "+id, func(t *testing.T) {
			if _, err := env.store.GetFile(ctx, workspace.DefaultID, id); !isFilesNotFound(err) {
				t.Fatalf("GetFile err = %T %v; want NotFoundError", err, err)
			}
			if _, err := env.store.OpenContent(ctx, workspace.DefaultID, id); !isFilesNotFound(err) {
				t.Fatalf("OpenContent err = %T %v; want NotFoundError", err, err)
			}
		})
	}
	for _, id := range []string{"file_original_visible", "file_session_active", "file_session_archived"} {
		if _, err := env.store.GetFile(ctx, workspace.DefaultID, id); err != nil {
			t.Fatalf("GetFile %s: %v", id, err)
		}
	}

	list, err := env.store.ListFiles(ctx, workspace.DefaultID, files.ListOptions{Limit: 3})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if idsOf(list.Data) != "file_original_visible,file_session_active,file_session_archived" || list.HasMore {
		t.Fatalf("ListFiles ids=%s has_more=%v; want only visible files without underfill", idsOf(list.Data), list.HasMore)
	}
	scoped, err := env.store.ListFiles(ctx, workspace.DefaultID, files.ListOptions{ScopeID: "sesn_files_deleted"})
	if err != nil {
		t.Fatalf("ListFiles hidden scope: %v", err)
	}
	if len(scoped.Data) != 0 || scoped.HasMore {
		t.Fatalf("hidden scoped ListFiles = %+v; want empty", scoped)
	}
	if _, err := env.store.ListFiles(ctx, workspace.DefaultID, files.ListOptions{AfterID: "file_session_deleted"}); !isFilesValidation(err) {
		t.Fatalf("hidden cursor err = %T %v; want ValidationError", err, err)
	}
}

func TestRetainedByteQuotaCountsObjectsAcrossSharedAndDeletedIdentities(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{MaxRetainedBytesPerWorkspace: 10, MaxFileIdentitiesPerWorkspace: 100})
	seedVisibleFileWithBody(t, env, workspace.DefaultID, "file_quota_a", "fobj_quota_shared", false, "", "", "123456789")
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_quota_b", "fobj_quota_shared", "shared-b.txt", "text/plain", false, "", "", "")

	if _, err := env.store.DeleteFile(context.Background(), workspace.DefaultID, "file_quota_a"); err != nil {
		t.Fatalf("DeleteFile quota seed: %v", err)
	}
	_, err := env.store.CreateUploadedFile(context.Background(), workspace.DefaultID, stagePlainFile(t, env.stageDir, "too-large.txt", "text/plain", "xx"))
	var quota *files.QuotaError
	if !errors.As(err, &quota) || quota.Kind != files.QuotaKindRetainedBytes {
		t.Fatalf("CreateUploadedFile error = %T %v; want retained-byte quota", err, err)
	}
}

func TestResolveSourceDoesNotGetBlobUntilOpenAndStreams(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	counting := &countingBlobStore{inner: env.blob}
	store := files.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime), counting)
	seedVisibleFileWithBody(t, env, workspace.DefaultID, "file_resolve_stream", "fobj_resolve_stream", false, "", "", "stream-body")

	source, err := store.ResolveSource(context.Background(), workspace.DefaultID, "file_resolve_stream")
	if err != nil {
		t.Fatalf("ResolveSource: %v", err)
	}
	if counting.getCalls != 0 {
		t.Fatalf("BlobStore Get calls after ResolveSource = %d; want 0", counting.getCalls)
	}
	reader, err := source.Open(context.Background())
	if err != nil {
		t.Fatalf("Open source: %v", err)
	}
	if counting.getCalls != 1 {
		t.Fatalf("BlobStore Get calls after Open = %d; want 1", counting.getCalls)
	}
	if counting.readCalls != 0 {
		t.Fatalf("reader read calls before caller reads = %d; want 0", counting.readCalls)
	}
	buf := make([]byte, 6)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("first stream read: %v", err)
	}
	if n == 0 || counting.readCalls == 0 {
		t.Fatalf("stream reader did not read lazily: n=%d readCalls=%d", n, counting.readCalls)
	}
	_ = reader.Close()
}

func TestResolveMountSourceReturnsOnlySessionOwnedObjectKey(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_mount_source", false)
	seedVisibleFileWithBody(t, env, workspace.DefaultID, "file_mount_original", "fobj_mount_original", false, "", "", "mount-body")
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_mount_session", "fobj_mount_original", "session.txt", "text/plain", false, "session", "sesn_mount_source", "")
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_mount_deleted_session", "fobj_mount_original", "deleted-session.txt", "text/plain", false, "session", "sesn_mount_source", "2026-01-01T00:00:01Z")
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_other_mount_source", false)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_mount_other_session", "fobj_mount_original", "other.txt", "text/plain", false, "session", "sesn_other_mount_source", "")
	otherWorkspace := workspace.ID("workspace_mount_source_b")
	seedFilesSession(t, env.admin, otherWorkspace, "sesn_mount_source_b", false)
	seedVisibleFileWithBody(t, env, otherWorkspace, "file_mount_cross_workspace", "fobj_mount_cross_workspace", false, "", "", "cross-body")
	// The scope_id intentionally matches the requested default-workspace
	// session so this negative case proves the workspace predicate.
	seedFileIdentityForObject(t, env.admin, otherWorkspace, "file_mount_cross_workspace_session", "fobj_mount_cross_workspace", "cross.txt", "text/plain", false, "session", "sesn_mount_source", "")

	source, err := env.store.ResolveMountSource(context.Background(), workspace.DefaultID, "sesn_mount_source", "file_mount_session")
	if err != nil {
		t.Fatalf("ResolveMountSource: %v", err)
	}
	if source.WorkspaceID != workspace.DefaultID || source.FileID != "file_mount_session" || source.ObjectKey != "files/default/fobj_mount_original" || source.SizeBytes != int64(len("mount-body")) || source.SHA256 == "" {
		t.Fatalf("mount source = %+v; want session-owned object key and metadata", source)
	}
	for _, tc := range []struct {
		name      string
		sessionID string
		fileID    string
	}{
		{name: "original is not session owned", sessionID: "sesn_mount_source", fileID: "file_mount_original"},
		{name: "wrong session", sessionID: "sesn_mount_source", fileID: "file_mount_other_session"},
		{name: "deleted session identity", sessionID: "sesn_mount_source", fileID: "file_mount_deleted_session"},
		{name: "wrong workspace", sessionID: "sesn_mount_source", fileID: "file_mount_cross_workspace_session"},
		{name: "missing", sessionID: "sesn_mount_source", fileID: "file_missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.store.ResolveMountSource(context.Background(), workspace.DefaultID, tc.sessionID, tc.fileID)
			if !isFilesNotFound(err) {
				t.Fatalf("ResolveMountSource err = %T %v; want NotFoundError", err, err)
			}
		})
	}
}

func TestResolveSourceNegativePathsDoNotCallBlobGet(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	workspaceB := workspace.ID("workspace_resolve_negative_b")
	seedWorkspace(t, env.admin, workspaceB)
	seedDeletedFile(t, env.admin, workspace.DefaultID, "file_resolve_deleted", "fobj_resolve_deleted")
	seedVisibleFile(t, env, workspaceB, "file_resolve_cross", "fobj_resolve_cross", false, "", "")
	counting := &countingBlobStore{inner: env.blob}
	store := files.NewPostgreSQLStore(dbconnect.NewClientForTesting(env.runtime), counting)

	for _, id := range []string{"file_resolve_missing", "file_resolve_deleted", "file_resolve_cross"} {
		t.Run(id, func(t *testing.T) {
			beforeGets := counting.getCalls
			_, err := store.ResolveSource(context.Background(), workspace.DefaultID, id)
			var notFound *files.NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("ResolveSource error = %T %v; want NotFoundError", err, err)
			}
			if counting.getCalls != beforeGets {
				t.Fatalf("ResolveSource negative path called BlobStore Get: before=%d after=%d", beforeGets, counting.getCalls)
			}
		})
	}
}

func TestCreateSessionFileIdentityLinksSameObjectWithoutBlobStore(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), nil, files.StoreLimits{})
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_source", 17)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_source", "fobj_session_identity_source", "source.csv", "text/csv", false, "", "", "")
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)
	beforeObjects := objectCount(t, env.admin, workspace.DefaultID)

	created, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_source",
		SessionID:     "sesn_identity_create",
		SessionFileID: "file_session_identity_created",
		CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("CreateSessionFileIdentity: %v", err)
	}
	if created.ID != "file_session_identity_created" || created.Filename != "source.csv" || created.MIMEType != "text/csv" || created.SizeBytes != 17 {
		t.Fatalf("created metadata = %+v; want source metadata copied", created)
	}
	if created.Scope == nil || created.Scope.Type != "session" || created.Scope.ID != "sesn_identity_create" {
		t.Fatalf("created scope = %+v; want session/sesn_identity_create", created.Scope)
	}
	sourceRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_source")
	sessionRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_created")
	if sessionRow.objectID != sourceRow.objectID {
		t.Fatalf("session object_id = %q; want source object_id %q", sessionRow.objectID, sourceRow.objectID)
	}
	if sessionRow.scopeType.String != "session" || sessionRow.scopeID.String != "sesn_identity_create" {
		t.Fatalf("session row scope = %q/%q; want session/sesn_identity_create", sessionRow.scopeType.String, sessionRow.scopeID.String)
	}
	if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles+1 {
		t.Fatalf("files rows = %d; want %d", fileCount(t, env.admin, workspace.DefaultID), beforeFiles+1)
	}
	if objectCount(t, env.admin, workspace.DefaultID) != beforeObjects {
		t.Fatalf("file_objects rows = %d; want unchanged %d", objectCount(t, env.admin, workspace.DefaultID), beforeObjects)
	}
}

func TestCreateSessionFileIdentityRejectsDeletedAndCrossWorkspaceSources(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), nil, files.StoreLimits{})
	workspaceB := workspace.ID("workspace_session_identity_b")
	seedWorkspace(t, env.admin, workspaceB)
	seedDeletedFile(t, env.admin, workspace.DefaultID, "file_session_identity_deleted", "fobj_session_identity_deleted")
	seedFileObject(t, env.admin, workspaceB, "fobj_session_identity_cross", 7)
	seedFileIdentityForObject(t, env.admin, workspaceB, "file_session_identity_cross", "fobj_session_identity_cross", "cross.txt", "text/plain", false, "", "", "")
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)

	for _, testCase := range []struct {
		name         string
		sourceFileID string
	}{
		{name: "deleted source", sourceFileID: "file_session_identity_deleted"},
		{name: "cross workspace source", sourceFileID: "file_session_identity_cross"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
				SourceFileID:  testCase.sourceFileID,
				SessionID:     "sesn_identity_reject",
				SessionFileID: "file_session_identity_rejected_" + strings.ReplaceAll(testCase.name, " ", "_"),
				CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
			})
			var notFound *files.NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("CreateSessionFileIdentity error = %T %v; want NotFoundError", err, err)
			}
			if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles {
				t.Fatalf("files rows changed after rejected source")
			}
		})
	}
}

func TestCreateSessionFileIdentityRejectsSessionScopedSource(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), nil, files.StoreLimits{})
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_scope_source", 11)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_scope_source_a", "fobj_session_identity_scope_source", "source.txt", "text/plain", false, "", "", "")
	if _, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_scope_source_a",
		SessionID:     "sesn_identity_scope",
		SessionFileID: "file_session_identity_scope_source_b",
		CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("CreateSessionFileIdentity source B: %v", err)
	}
	beforeFiles := fileCount(t, env.admin, workspace.DefaultID)

	_, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_scope_source_b",
		SessionID:     "sesn_identity_scope",
		SessionFileID: "file_session_identity_scope_source_c",
		CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	})
	var notFound *files.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("CreateSessionFileIdentity error = %T %v; want NotFoundError", err, err)
	}
	if fileCount(t, env.admin, workspace.DefaultID) != beforeFiles {
		t.Fatalf("files rows changed after rejected session-scoped source")
	}
}

func TestTombstoneSessionFileIdentityIsIndependentFromSourceIdentity(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), nil, files.StoreLimits{})
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_identity_tombstone", false)
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_tombstone", 9)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_tombstone_source", "fobj_session_identity_tombstone", "source.txt", "text/plain", false, "", "", "")
	if _, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_tombstone_source",
		SessionID:     "sesn_identity_tombstone",
		SessionFileID: "file_session_identity_tombstone_created",
		CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("CreateSessionFileIdentity: %v", err)
	}

	if err := tombstoneSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, "sesn_identity_tombstone", "file_session_identity_tombstone_source"); err == nil {
		t.Fatal("TombstoneSessionFileIdentity with original source id succeeded; want not found")
	}
	if err := tombstoneSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, "sesn_wrong", "file_session_identity_tombstone_created"); err == nil {
		t.Fatal("TombstoneSessionFileIdentity with wrong session id succeeded; want not found")
	}
	if _, err := store.DeleteFile(context.Background(), workspace.DefaultID, "file_session_identity_tombstone_source"); err != nil {
		t.Fatalf("DeleteFile source: %v", err)
	}
	if _, err := store.GetFile(context.Background(), workspace.DefaultID, "file_session_identity_tombstone_created"); err != nil {
		t.Fatalf("session identity should survive source tombstone: %v", err)
	}
	if err := tombstoneSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, "sesn_identity_tombstone", "file_session_identity_tombstone_created"); err != nil {
		t.Fatalf("TombstoneSessionFileIdentity: %v", err)
	}
	sessionRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_tombstone_created")
	sourceRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_tombstone_source")
	if !sessionRow.deletedAt.Valid {
		t.Fatalf("session identity deleted_at is null after tombstone")
	}
	if !sourceRow.deletedAt.Valid {
		t.Fatalf("source identity should remain independently tombstoned after DeleteFile")
	}
	if objectCount(t, env.admin, workspace.DefaultID) != 1 {
		t.Fatalf("file_objects rows = %d; want shared object retained", objectCount(t, env.admin, workspace.DefaultID))
	}
}

func TestDeleteFileRejectsSessionFileIdentityAndKeepsSessionTombstonePath(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), nil, files.StoreLimits{})
	seedFilesSession(t, env.admin, workspace.DefaultID, "sesn_identity_public_delete", false)
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_public_delete", 13)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_public_delete_source", "fobj_session_identity_public_delete", "source.txt", "text/plain", false, "", "", "")
	if _, err := createSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, files.SessionFileIdentityRequest{
		SourceFileID:  "file_session_identity_public_delete_source",
		SessionID:     "sesn_identity_public_delete",
		SessionFileID: "file_session_identity_public_delete_created",
		CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("CreateSessionFileIdentity: %v", err)
	}
	if _, err := store.DeleteFile(context.Background(), workspace.DefaultID, "file_session_identity_public_delete_source"); err != nil {
		t.Fatalf("DeleteFile original identity: %v", err)
	}
	if _, err := store.GetFile(context.Background(), workspace.DefaultID, "file_session_identity_public_delete_created"); err != nil {
		t.Fatalf("session identity should survive original identity delete: %v", err)
	}

	_, err := store.DeleteFile(context.Background(), workspace.DefaultID, "file_session_identity_public_delete_created")
	var validation *files.ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("DeleteFile session identity error = %T %v; want ValidationError", err, err)
	}
	sessionRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_public_delete_created")
	if sessionRow.deletedAt.Valid {
		t.Fatalf("session identity deleted_at = %q after rejected public delete; want null", sessionRow.deletedAt.String)
	}

	if err := tombstoneSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, "sesn_identity_public_delete", "file_session_identity_public_delete_created"); err != nil {
		t.Fatalf("TombstoneSessionFileIdentity after rejected public delete: %v", err)
	}
	if err := tombstoneSessionFileIdentityForTest(t, store, env.runtime, workspace.DefaultID, "sesn_identity_public_delete", "file_session_identity_public_delete_created"); err != nil {
		t.Fatalf("TombstoneSessionFileIdentity idempotent retry: %v", err)
	}
	sessionRow = loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_public_delete_created")
	sourceRow := loadFileRow(t, env.admin, workspace.DefaultID, "file_session_identity_public_delete_source")
	if !sessionRow.deletedAt.Valid {
		t.Fatalf("session identity deleted_at is null after session tombstone")
	}
	if !sourceRow.deletedAt.Valid {
		t.Fatalf("source identity should remain independently tombstoned after DeleteFile")
	}
}

func TestCreateSessionFileIdentityRollsBackWithTransactionFailure(t *testing.T) {
	env := newFilesStoreEnv(t, files.StoreLimits{})
	store := files.NewPostgreSQLStoreWithLimits(dbconnect.NewClientForTesting(env.runtime), nil, files.StoreLimits{})
	seedFileObject(t, env.admin, workspace.DefaultID, "fobj_session_identity_rollback", 5)
	seedFileIdentityForObject(t, env.admin, workspace.DefaultID, "file_session_identity_rollback_source", "fobj_session_identity_rollback", "source.txt", "text/plain", false, "", "", "")
	sentinel := errors.New("synthetic session resource failure")
	client := dbconnect.NewClientForTesting(env.runtime)

	err := client.WithWorkspaceTx(context.Background(), string(workspace.DefaultID), "files.session_identity_rollback", func(tx *dbconnect.Tx) error {
		if _, err := store.CreateSessionFileIdentity(context.Background(), testSessionFilesTransaction{tx: tx}, workspace.DefaultID, files.SessionFileIdentityRequest{
			SourceFileID:  "file_session_identity_rollback_source",
			SessionID:     "sesn_identity_rollback",
			SessionFileID: "file_session_identity_rollback_created",
			CreatedAt:     time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC),
		}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("transaction error = %v; want sentinel", err)
	}
	if fileIdentityExists(t, env.admin, workspace.DefaultID, "file_session_identity_rollback_created") {
		t.Fatal("session identity row persisted after transaction rollback")
	}
}

type fileRow struct {
	objectID  string
	blobKey   string
	filename  string
	scopeType sql.NullString
	scopeID   sql.NullString
	deletedAt sql.NullString
}

func loadFileRow(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string) fileRow {
	t.Helper()
	var row fileRow
	if err := db.QueryRowContext(context.Background(),
		`SELECT f.object_id, o.blob_key, f.filename, f.scope_type, f.scope_id, f.deleted_at
		   FROM files f
		   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
		  WHERE f.workspace_id = $1 AND f.file_id = $2`,
		string(workspaceID), fileID).Scan(&row.objectID, &row.blobKey, &row.filename, &row.scopeType, &row.scopeID, &row.deletedAt); err != nil {
		t.Fatalf("load file row: %v", err)
	}
	return row
}

func createSessionFileIdentityForTest(t *testing.T, store *files.PostgreSQLFileStore, db *sql.DB, workspaceID workspace.ID, request files.SessionFileIdentityRequest) (*files.FileMetadata, error) {
	t.Helper()
	client := dbconnect.NewClientForTesting(db)
	var created *files.FileMetadata
	err := client.WithWorkspaceTx(context.Background(), string(workspaceID), "files.session_identity_test", func(tx *dbconnect.Tx) error {
		var err error
		created, err = store.CreateSessionFileIdentity(context.Background(), testSessionFilesTransaction{tx: tx}, workspaceID, request)
		return err
	})
	return created, err
}

func tombstoneSessionFileIdentityForTest(t *testing.T, store *files.PostgreSQLFileStore, db *sql.DB, workspaceID workspace.ID, sessionID string, fileID string) error {
	t.Helper()
	client := dbconnect.NewClientForTesting(db)
	return client.WithWorkspaceTx(context.Background(), string(workspaceID), "files.session_identity_tombstone", func(tx *dbconnect.Tx) error {
		return store.TombstoneSessionFileIdentity(context.Background(), testSessionFilesTransaction{tx: tx}, workspaceID, sessionID, fileID)
	})
}

type testSessionFilesTransaction struct {
	tx *dbconnect.Tx
}

func (t testSessionFilesTransaction) ExecContext(ctx context.Context, query string, args ...any) (interface {
	RowsAffected() (int64, error)
}, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t testSessionFilesTransaction) Query(ctx context.Context, query string, args ...any) (files.Rows, error) {
	return t.tx.Query(ctx, query, args...)
}

func (t testSessionFilesTransaction) QueryRow(ctx context.Context, query string, args ...any) files.Row {
	return t.tx.QueryRow(ctx, query, args...)
}

func fileIdentityExists(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM files WHERE workspace_id = $1 AND file_id = $2)`,
		string(workspaceID), fileID).Scan(&exists); err != nil {
		t.Fatalf("check file identity exists: %v", err)
	}
	return exists
}

func objectCount(t *testing.T, db *sql.DB, workspaceID workspace.ID) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM file_objects WHERE workspace_id = $1`, string(workspaceID)).Scan(&count); err != nil {
		t.Fatalf("count file_objects: %v", err)
	}
	return count
}

func fileCount(t *testing.T, db *sql.DB, workspaceID workspace.ID) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM files WHERE workspace_id = $1`, string(workspaceID)).Scan(&count); err != nil {
		t.Fatalf("count files: %v", err)
	}
	return count
}

func seedFileObject(t *testing.T, db *sql.DB, workspaceID workspace.ID, objectID string, sizeBytes int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (object_id, workspace_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, $4, 'sha', '2026-01-01T00:00:00Z')`,
		objectID, string(workspaceID), "files/"+string(workspaceID)+"/"+objectID, sizeBytes); err != nil {
		t.Fatalf("seed file_object: %v", err)
	}
}

func minimalTestPDF(pageCount int) []byte {
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

func seedDeletedFile(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID, objectID string) {
	t.Helper()
	seedFileObject(t, db, workspaceID, objectID, 1)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at, deleted_at)
		 VALUES ($1, $2, $3, 'deleted.txt', 'text/plain', false, '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z')`,
		fileID, string(workspaceID), objectID); err != nil {
		t.Fatalf("seed deleted file: %v", err)
	}
}

func seedVisibleFile(t *testing.T, env *filesStoreEnv, workspaceID workspace.ID, fileID, objectID string, downloadable bool, scopeType, scopeID string) {
	t.Helper()
	seedVisibleFileWithBody(t, env, workspaceID, fileID, objectID, downloadable, scopeType, scopeID, "body:"+fileID)
}

func seedVisibleFileWithBody(t *testing.T, env *filesStoreEnv, workspaceID workspace.ID, fileID, objectID string, downloadable bool, scopeType, scopeID string, body string) {
	t.Helper()
	seedFileObject(t, env.admin, workspaceID, objectID, int64(len(body)))
	key := "files/" + string(workspaceID) + "/" + objectID
	if err := env.blob.Put(context.Background(), key, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("seed blob %s: %v", key, err)
	}
	seedFileIdentityForObject(t, env.admin, workspaceID, fileID, objectID, fileID+".txt", "text/plain", downloadable, scopeType, scopeID, "")
}

func seedFileIdentityForObject(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID, objectID, filename, mimeType string, downloadable bool, scopeType, scopeID, deletedAt string) {
	t.Helper()
	var err error
	if scopeType == "" && scopeID == "" {
		_, err = db.ExecContext(context.Background(),
			`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at, deleted_at)
			 VALUES ($1, $2, $3, $4, $5, $6, '2026-01-01T00:00:00Z', NULLIF($7, '')::timestamptz)`,
			fileID, string(workspaceID), objectID, filename, mimeType, downloadable, deletedAt)
	} else {
		_, err = db.ExecContext(context.Background(),
			`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at, deleted_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '2026-01-01T00:00:00Z', NULLIF($9, '')::timestamptz)`,
			fileID, string(workspaceID), objectID, filename, mimeType, downloadable, scopeType, scopeID, deletedAt)
	}
	if err != nil {
		t.Fatalf("seed file identity %s: %v", fileID, err)
	}
}

func idsOf(items []*files.FileMetadata) string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return strings.Join(ids, ",")
}

func osStat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

type deleteContextCheckingBlobStore struct {
	inner          *blob.FakeBlobStore
	cancelRequest  context.CancelFunc
	deleteKeys     []string
	deleteFailures []error
}

func (s *deleteContextCheckingBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *deleteContextCheckingBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *deleteContextCheckingBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *deleteContextCheckingBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *deleteContextCheckingBlobStore) Delete(ctx context.Context, key string) error {
	s.deleteKeys = append(s.deleteKeys, key)
	if s.cancelRequest != nil {
		s.cancelRequest()
	}
	if err := ctx.Err(); err != nil {
		s.deleteFailures = append(s.deleteFailures, err)
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		s.deleteFailures = append(s.deleteFailures, errors.New("cleanup delete context has no deadline"))
		return s.deleteFailures[len(s.deleteFailures)-1]
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 30*time.Second {
		err := fmt.Errorf("cleanup deadline remaining = %s", remaining)
		s.deleteFailures = append(s.deleteFailures, err)
		return err
	}
	return s.inner.Delete(ctx, key)
}

func (s *deleteContextCheckingBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

type workspaceGatedBlobStore struct {
	inner          *blob.FakeBlobStore
	gatedWorkspace string
	entered        chan struct{}
	release        chan struct{}
	once           sync.Once
}

func (s *workspaceGatedBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	if strings.Contains(key, "/"+s.gatedWorkspace+"/") {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.inner.Put(ctx, key, content, size)
}

func (s *workspaceGatedBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *workspaceGatedBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *workspaceGatedBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *workspaceGatedBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *workspaceGatedBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

type countingBlobStore struct {
	inner             *blob.FakeBlobStore
	getCalls          int
	headCalls         int
	copyCalls         int
	readCalls         int
	deleteCalls       int
	deletePrefixCalls int
}

func (s *countingBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *countingBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.getCalls++
	reader, err := s.inner.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &countingReadCloser{inner: reader, onRead: func() { s.readCalls++ }}, nil
}

func (s *countingBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	s.headCalls++
	return s.inner.HeadObject(ctx, key)
}

func (s *countingBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	s.copyCalls++
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *countingBlobStore) Delete(ctx context.Context, key string) error {
	s.deleteCalls++
	return s.inner.Delete(ctx, key)
}

func (s *countingBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	s.deletePrefixCalls++
	return s.inner.DeletePrefix(ctx, prefix)
}

type countingReadCloser struct {
	inner  io.ReadCloser
	onRead func()
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	if r.onRead != nil {
		r.onRead()
	}
	return r.inner.Read(p)
}

func (r *countingReadCloser) Close() error {
	return r.inner.Close()
}
