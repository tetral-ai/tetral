package tetralcleanup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// driveFixture writes drives as api does and runs the cleanup as the
// installed cleanup role, both over one blob store.
type driveFixture struct {
	t      *testing.T
	ctx    context.Context
	admin  *sql.DB
	api    *drive.Service
	blobs  *blob.FakeBlobStore
	runner *dbconnect.Client
}

func newDriveFixture(t *testing.T) *driveFixture {
	t.Helper()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	blobs := blob.NewFakeBlobStore()
	api, err := drive.NewService(dbconnect.NewClientForTesting(w.OpenWorkload(t, "api", nil)), blobs, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &driveFixture{t: t, ctx: context.Background(), admin: admin, api: api, blobs: blobs, runner: dbconnect.NewClientForTesting(w.DB)}
}

var fixtureWriter = drive.Writer{Kind: drive.WriterAPIKey, ID: "apikey_fixture"}

func (f *driveFixture) newDrive(ws workspace.ID) string {
	f.t.Helper()
	created, err := f.api.Create(f.ctx, ws, drive.Settings{})
	if err != nil {
		f.t.Fatal(err)
	}
	return created.ID
}

func (f *driveFixture) put(ws workspace.ID, driveID, body string) drive.Content {
	f.t.Helper()
	content, err := f.api.PutContent(f.ctx, ws, driveID, strings.NewReader(body), int64(len(body)), "")
	if err != nil {
		f.t.Fatal(err)
	}
	return content
}

// commit writes body at path, or deletes path when body is nil, and returns
// the commit's seq.
func (f *driveFixture) commit(ws workspace.ID, driveID, path string, body *string) int64 {
	f.t.Helper()
	change := drive.Change{Path: path, Base: drive.Base{Kind: drive.BaseAny}, Delete: body == nil}
	if body != nil {
		change.ContentSHA256 = f.put(ws, driveID, *body).SHA256
	}
	result, err := f.api.Commit(f.ctx, ws, driveID, fixtureWriter, drive.ModeAllOrNothing, []drive.Change{change})
	if err != nil || result.Seq == nil {
		f.t.Fatalf("commit %s: %+v, %v", path, result, err)
	}
	return *result.Seq
}

func (f *driveFixture) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, query, args...); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f *driveFixture) count(query string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.admin.QueryRowContext(f.ctx, query, args...).Scan(&n); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
	return n
}

// age sets the times of one commit's versions and change records to the
// database clock minus age, and the time its versions' predecessors stopped
// being current.
func (f *driveFixture) age(driveID string, seq int64, age string) {
	f.t.Helper()
	f.exec(`UPDATE drive_versions SET created_at=clock_timestamp()-$3::interval WHERE drive_id=$1 AND seq=$2`, driveID, seq, age)
	f.exec(`UPDATE drive_versions p SET superseded_at=clock_timestamp()-$3::interval FROM drive_versions v
		WHERE v.drive_id=$1 AND v.seq=$2 AND p.drive_id=v.drive_id AND p.path=v.path AND p.seq<v.seq AND p.superseded_at IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM drive_versions m WHERE m.drive_id=v.drive_id AND m.path=v.path AND m.seq>p.seq AND m.seq<v.seq)`, driveID, seq, age)
	f.exec(`UPDATE drive_changes SET created_at=clock_timestamp()-$3::interval WHERE drive_id=$1 AND seq=$2`, driveID, seq, age)
}

func (f *driveFixture) run() {
	f.t.Helper()
	if err := RunDrives(f.ctx, f.runner, f.blobs, nil); err != nil {
		f.t.Fatalf("drive cleanup run: %v", err)
	}
}

func text(s string) *string { return &s }

// DR-31 and DR-51 after one run: removed versions, change records and content
// are deleted, kept ones stay, and stored_bytes counts each named content
// once, current and old, while usage counts current files only.
func TestDriveCleanupDeletesWhatRetentionRemoved(t *testing.T) {
	f := newDriveFixture(t)
	ws := workspace.DefaultID
	id := f.newDrive(ws)
	// Commits in seq order with non-decreasing times: b.txt is deleted 41
	// days ago; a.txt's first version is replaced 31 days ago and shares its
	// content with a.txt's and copy.txt's current versions.
	f.age(id, f.commit(ws, id, "b.txt", text("bee")), "42 days")
	f.age(id, f.commit(ws, id, "b.txt", nil), "41 days")
	f.age(id, f.commit(ws, id, "a.txt", text("one")), "40 days")
	f.age(id, f.commit(ws, id, "a.txt", text("two-two")), "31 days")
	f.age(id, f.commit(ws, id, "a.txt", text("three-three-3")), "1 hour")
	f.commit(ws, id, "a.txt", text("one"))
	f.commit(ws, id, "copy.txt", text("one"))
	f.exec(`UPDATE drive_contents SET uploaded_at=clock_timestamp()-interval '30 days' WHERE drive_id=$1`, id)
	beeKey := blobKey(f, id, "bee")

	f.run()

	versions, err := f.api.ListVersions(f.ctx, ws, id, "a.txt")
	if err != nil || len(versions) != 3 {
		t.Fatalf("a.txt versions = %d, %v; want the 3 kept of 4", len(versions), err)
	}
	if n := f.count(`SELECT count(*) FROM drive_versions WHERE drive_id=$1`, id); n != 4 {
		t.Fatalf("drive_versions rows = %d; want a.txt's 3, copy.txt's and none of b.txt", n)
	}
	if n := f.count(`SELECT count(*) FROM drive_changes WHERE drive_id=$1`, id); n != 3 {
		t.Fatalf("drive_changes rows = %d; want the 3 younger than 30 days", n)
	}
	var removedSeq, removedIndex sql.NullInt64
	var stored sql.NullInt64
	var usage int64
	if err := f.admin.QueryRow(`SELECT removed_change_seq,removed_change_index,stored_bytes,usage_bytes FROM drives WHERE drive_id=$1`, id).Scan(&removedSeq, &removedIndex, &stored, &usage); err != nil {
		t.Fatal(err)
	}
	if removedSeq.Int64 != 4 || removedIndex.Int64 != 0 {
		t.Fatalf("removed change = (%v, %v); want (4, 0), the newest deleted record", removedSeq, removedIndex)
	}
	// one + two-two + three-three-3: "one" once, although a removed version
	// and two kept ones name it, while usage counts both current files.
	if !stored.Valid || stored.Int64 != 3+7+13 || usage != 3+3 {
		t.Fatalf("stored_bytes = %v, usage_bytes = %d; want 23 and 6", stored, usage)
	}
	if f.count(`SELECT count(*) FROM drive_contents WHERE drive_id=$1`, id) != 3 || f.blobs.Has(beeKey) {
		t.Fatalf("contents = %d, bee blob kept %v; want one, two-two and three-three-3, and b.txt's content gone", f.count(`SELECT count(*) FROM drive_contents WHERE drive_id=$1`, id), f.blobs.Has(beeKey))
	}
	restored, err := f.api.Restore(f.ctx, ws, id, "b.txt", time.Now().Add(-41*24*time.Hour-time.Hour), fixtureWriter)
	if err != nil || restored.Restored != 0 {
		t.Fatalf("restore of b.txt before its deletion = %+v, %v; want nothing restored", restored, err)
	}

	// DR-51: stored_bytes above max_bytes never refuses a write that fits by
	// current files.
	limit := int64(10)
	if _, err := f.api.Update(f.ctx, ws, id, drive.SettingsUpdate{Limits: &drive.LimitSettings{MaxBytes: &limit}}); err != nil {
		t.Fatal(err)
	}
	f.commit(ws, id, "c.txt", text("1234"))
}

// blobKey is the blob of the drive's content body.
func blobKey(f *driveFixture, driveID, body string) string {
	f.t.Helper()
	var key string
	digest := sha256.Sum256([]byte(body))
	sum := hex.EncodeToString(digest[:])
	if err := f.admin.QueryRow(`SELECT blob_key FROM drive_contents WHERE drive_id=$1 AND content_sha256=$2`, driveID, sum).Scan(&key); err != nil {
		f.t.Fatal(err)
	}
	return key
}

// Unnamed content survives until 24 hours after its upload, measured from the
// upload and not from the drive's last commit.
func TestDriveCleanupKeepsRecentUnnamedContent(t *testing.T) {
	f := newDriveFixture(t)
	id := f.newDrive(workspace.DefaultID)
	recent := f.put(workspace.DefaultID, id, "recent")
	old := f.put(workspace.DefaultID, id, "old")
	f.exec(`UPDATE drive_contents SET uploaded_at=clock_timestamp()-interval '23 hours' WHERE drive_id=$1 AND content_sha256=$2`, id, recent.SHA256)
	f.exec(`UPDATE drive_contents SET uploaded_at=clock_timestamp()-interval '24 hours 1 minute' WHERE drive_id=$1 AND content_sha256=$2`, id, old.SHA256)
	keys := map[string]string{}
	rows, err := f.admin.Query(`SELECT content_sha256,blob_key FROM drive_contents WHERE drive_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var sum, key string
		_ = rows.Scan(&sum, &key)
		keys[sum] = key
	}
	_ = rows.Close()

	f.run()

	if f.count(`SELECT count(*) FROM drive_contents WHERE drive_id=$1 AND content_sha256=$2`, id, recent.SHA256) != 1 || !f.blobs.Has(keys[recent.SHA256]) {
		t.Fatal("content uploaded 23 hours ago was deleted")
	}
	if f.count(`SELECT count(*) FROM drive_contents WHERE drive_id=$1 AND content_sha256=$2`, id, old.SHA256) != 0 || f.blobs.Has(keys[old.SHA256]) {
		t.Fatal("content uploaded 24 hours and 1 minute ago survived")
	}
	if n := f.count(`SELECT count(*) FROM drive_blob_cleanup`); n != 0 {
		t.Fatalf("drive_blob_cleanup rows = %d; want the deleted blob's entry gone", n)
	}
}

// A deleted drive in another Workspace leaves no row and no blob, and a blob
// left by a killed upload is deleted once its delete_after has passed, also
// in a Workspace that no longer has a drive; one already missing counts as
// deleted.
func TestDriveCleanupRemovesDeletedDrivesAndDueBlobs(t *testing.T) {
	f := newDriveFixture(t)
	other := workspace.ID("ws_drive_cleanup_other")
	f.exec(`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, clock_timestamp())`, string(other))
	id := f.newDrive(other)
	for _, body := range []string{"v1", "v2", "v3"} {
		f.commit(other, id, "x.txt", text(body))
	}
	var keys []string
	rows, err := f.admin.Query(`SELECT blob_key FROM drive_contents WHERE drive_id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var key string
		_ = rows.Scan(&key)
		keys = append(keys, key)
	}
	_ = rows.Close()
	if err := f.api.Delete(f.ctx, other, id); err != nil {
		t.Fatal(err)
	}

	live := f.newDrive(workspace.DefaultID)
	killed, pending, missing := "drives/default/"+live+"/killed", "drives/default/"+live+"/pending", "drives/default/"+live+"/missing"
	for _, key := range []string{killed, pending} {
		if err := f.blobs.Put(f.ctx, key, strings.NewReader("partial"), 7); err != nil {
			t.Fatal(err)
		}
	}
	f.exec(`INSERT INTO drive_blob_cleanup(workspace_id,blob_key,delete_after) VALUES ('default',$1,clock_timestamp()-interval '1 minute'),
		('default',$2,clock_timestamp()+interval '1 hour'),('default',$3,clock_timestamp()-interval '1 minute')`, killed, pending, missing)
	driveless := workspace.ID("ws_drive_cleanup_driveless")
	f.exec(`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, clock_timestamp())`, string(driveless))
	orphan := "drives/" + string(driveless) + "/drv_gone/killed"
	if err := f.blobs.Put(f.ctx, orphan, strings.NewReader("partial"), 7); err != nil {
		t.Fatal(err)
	}
	f.exec(`INSERT INTO drive_blob_cleanup(workspace_id,blob_key,delete_after) VALUES ($1,$2,clock_timestamp()-interval '1 minute')`, string(driveless), orphan)

	f.run()

	for _, table := range []string{"drives", "drive_contents"} {
		if n := f.count(`SELECT count(*) FROM `+table+` WHERE drive_id=$1`, id); n != 0 {
			t.Fatalf("%s rows of the deleted drive = %d", table, n)
		}
	}
	if n := f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE workspace_id=$1`, string(other)); n != 0 || len(keys) != 3 {
		t.Fatalf("drive_blob_cleanup rows = %d for %d contents; want none", n, len(keys))
	}
	for _, key := range keys {
		if f.blobs.Has(key) {
			t.Fatalf("blob %s of the deleted drive survived", key)
		}
	}
	if f.blobs.Has(killed) || f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key=$1`, killed) != 0 {
		t.Fatal("the due entry of a killed upload survived")
	}
	if !f.blobs.Has(pending) || f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key=$1`, pending) != 1 {
		t.Fatal("an entry not yet due was deleted")
	}
	if f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE blob_key=$1`, missing) != 0 {
		t.Fatal("a due entry whose blob is already missing survived")
	}
	if f.blobs.Has(orphan) || f.count(`SELECT count(*) FROM drive_blob_cleanup WHERE workspace_id=$1`, string(driveless)) != 0 {
		t.Fatal("the due entry of a Workspace with no drive survived")
	}
}
