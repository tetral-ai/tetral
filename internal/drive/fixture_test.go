package drive_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

var (
	apiWriter     = drive.Writer{Kind: drive.WriterAPIKey, ID: "apikey_test"}
	sessionWriter = drive.Writer{Kind: drive.WriterSession, ID: "sesn_test", ComputerID: "comp_test"}
)

// fixture runs one drive service as the api workload and one as the
// computer_hub workload, each on its own pool of a private database.
type fixture struct {
	t     *testing.T
	ctx   context.Context
	admin *sql.DB
	api   *drive.Service
	hub   *drive.Service
	blobs *blob.FakeBlobStore
	ws    workspace.ID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "api")
	blobs := blob.NewFakeBlobStore()
	api, err := drive.NewService(dbconnect.NewClientForTesting(workload.DB), blobs, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hub, err := drive.NewService(dbconnect.NewClientForTesting(workload.OpenWorkload(t, "computer_hub", nil)), blobs, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, ctx: context.Background(), admin: admin, api: api, hub: hub, blobs: blobs, ws: workspace.DefaultID}
}

// on returns the fixture reporting to a subtest.
func (f *fixture) on(t *testing.T) *fixture {
	sub := *f
	sub.t = t
	return &sub
}

func (f *fixture) newDrive(settings drive.Settings) string {
	f.t.Helper()
	created, err := f.api.Create(f.ctx, f.ws, settings)
	if err != nil {
		f.t.Fatal(err)
	}
	return created.ID
}

// put stores content in the drive and returns its hash.
func (f *fixture) put(driveID, content string) string {
	f.t.Helper()
	stored, err := f.api.PutContent(f.ctx, f.ws, driveID, strings.NewReader(content), int64(len(content)), "")
	if err != nil {
		f.t.Fatal(err)
	}
	return stored.SHA256
}

// commit runs a commit through the api service and checks usage after it.
func (f *fixture) commit(driveID string, mode drive.Mode, changes ...drive.Change) drive.CommitResult {
	f.t.Helper()
	result, err := f.api.Commit(f.ctx, f.ws, driveID, apiWriter, mode, changes)
	if err != nil {
		f.t.Fatalf("commit: %v", err)
	}
	f.assertUsage()
	return result
}

// assertUsage compares every drive's usage, and the Workspace row, with sums
// over drive_files.
func (f *fixture) assertUsage() {
	f.t.Helper()
	rows, err := f.admin.QueryContext(f.ctx, `SELECT d.drive_id,d.usage_bytes,d.usage_files,COALESCE(sum(f.size_bytes),0),count(f.path)
		FROM drives d LEFT JOIN drive_files f ON f.workspace_id=d.workspace_id AND f.drive_id=d.drive_id
		WHERE d.workspace_id=$1 AND d.deleted_at IS NULL GROUP BY d.drive_id,d.usage_bytes,d.usage_files`, string(f.ws))
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var totalBytes, totalFiles int64
	for rows.Next() {
		var id string
		var bytes, files, sumBytes, countFiles int64
		if err := rows.Scan(&id, &bytes, &files, &sumBytes, &countFiles); err != nil {
			f.t.Fatal(err)
		}
		if bytes != sumBytes || files != countFiles {
			f.t.Fatalf("drive %s usage = %d bytes, %d files; current files hold %d bytes, %d files", id, bytes, files, sumBytes, countFiles)
		}
		totalBytes, totalFiles = totalBytes+bytes, totalFiles+files
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	var wsBytes, wsFiles int64
	err = f.admin.QueryRowContext(f.ctx, `SELECT COALESCE(sum(bytes),0),COALESCE(sum(files),0) FROM drive_workspace_usage WHERE workspace_id=$1`, string(f.ws)).Scan(&wsBytes, &wsFiles)
	if err != nil {
		f.t.Fatal(err)
	}
	if wsBytes != totalBytes || wsFiles != totalFiles {
		f.t.Fatalf("workspace usage = %d bytes, %d files; its drives hold %d bytes, %d files", wsBytes, wsFiles, totalBytes, totalFiles)
	}
}

// readContent reads content by hash and checks that it has that hash.
func (f *fixture) readContent(driveID, sha string) error {
	f.t.Helper()
	body, _, err := f.api.OpenContent(f.ctx, f.ws, driveID, sha)
	if err != nil {
		return err
	}
	f.checkBody(body, sha)
	return nil
}

// readVersion reads a version's content and checks it against the version.
func (f *fixture) readVersion(driveID, versionID string) error {
	f.t.Helper()
	body, version, err := f.api.OpenVersionContent(f.ctx, f.ws, driveID, versionID)
	if err != nil {
		return err
	}
	f.checkBody(body, *version.ContentSHA256)
	return nil
}

func (f *fixture) checkBody(body io.ReadCloser, sha string) {
	f.t.Helper()
	defer func() { _ = body.Close() }()
	content, err := io.ReadAll(body)
	if err != nil {
		f.t.Fatal(err)
	}
	if got := hashOf(string(content)); got != sha {
		f.t.Fatalf("content hash = %s, want %s", got, sha)
	}
}

// exec runs a fixture statement as the database owner.
func (f *fixture) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		f.t.Fatal(err)
	}
}

func write(path, sha string, base drive.Base) drive.Change {
	return drive.Change{Path: path, Base: base, ContentSHA256: sha}
}

func deletion(path, sha string) drive.Change {
	return drive.Change{Path: path, Delete: true, Base: drive.Base{Kind: drive.BaseHash, SHA256: sha}}
}

var (
	newFile = drive.Base{Kind: drive.BaseMustNotExist}
	anyBase = drive.Base{Kind: drive.BaseAny}
)

func basedOn(sha string) drive.Base { return drive.Base{Kind: drive.BaseHash, SHA256: sha} }

func hashOf(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}
