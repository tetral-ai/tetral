package joint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// driveCleanupPublishesRetainedStorageAndCursorExpiry uses the real API
// and cleanup processes with their installed roles. It has no CLI
// participant; it reuses the harness's Engine composition.
func driveCleanupPublishesRetainedStorageAndCursorExpiry(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	record.boundary("no CLI machine participates; this is API/cleanup process evidence, not Drive mounting or synchronization evidence")
	d := e.developer()
	ctx := t.Context()
	var created struct{ ID string }
	d.post(ctx, "/v1/drives", map[string]any{}, http.StatusCreated, &created)
	base := "/v1/drives/" + created.ID
	upload := func(path, content string) {
		t.Helper()
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		if err := writer.WriteField("path", path); err != nil {
			t.Fatal(err)
		}
		part, err := writer.CreateFormFile("file", "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(part, content); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+base+"/files", &body)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Api-Key", d.apiKey)
		request.Header.Set("Content-Type", writer.FormDataContentType())
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		answer, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusCreated {
			t.Fatalf("upload: %d %s %v", response.StatusCode, answer, err)
		}
	}
	upload("deleted.txt", "deleted")
	deleteRequest, err := http.NewRequestWithContext(ctx, http.MethodDelete, d.baseURL+base+fmt.Sprintf("/files?path=deleted.txt&expected_content_sha256=%x", sha256.Sum256([]byte("deleted"))), nil)
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest.Header.Set("X-Api-Key", d.apiKey)
	deleteResponse, err := http.DefaultClient.Do(deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	deleteAnswer, err := io.ReadAll(deleteResponse.Body)
	_ = deleteResponse.Body.Close()
	if err != nil || deleteResponse.StatusCode != http.StatusOK {
		t.Fatalf("delete: %d %s %v", deleteResponse.StatusCode, deleteAnswer, err)
	}
	for _, content := range []string{"expired", "current", "retained", "current"} {
		upload("file.txt", content)
	}
	e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
		for _, statement := range []string{
			`UPDATE drive_versions SET created_at=now()-interval '43 days',superseded_at=now()-interval '41 days' WHERE drive_id=$1 AND seq=1`,
			`UPDATE drive_versions SET created_at=now()-interval '41 days' WHERE drive_id=$1 AND seq=2`,
			`UPDATE drive_versions SET created_at=now()-interval '40 days',superseded_at=now()-interval '31 days' WHERE drive_id=$1 AND seq=3`,
			`UPDATE drive_changes SET created_at=now()-interval '32 days' WHERE drive_id=$1 AND seq<=3`,
			`UPDATE drive_contents SET uploaded_at=now()-interval '32 days' WHERE drive_id=$1`,
		} {
			if _, err := tx.ExecContext(ctx, statement, created.ID); err != nil {
				return err
			}
		}
		return nil
	})
	for pass := 1; pass <= 2; pass++ {
		runCommand(t, builds.path(t, "tetral-cleanup"), e.cleanupEnv, nil, "drives")
		record.command("tetral-cleanup drives pass %d as the cleanup database role", pass)
		var drive struct {
			StoredBytes *int64 `json:"stored_bytes"`
			Usage       struct{ Bytes, Files int64 }
		}
		d.get(ctx, base, &drive)
		if drive.StoredBytes == nil || *drive.StoredBytes != 15 || drive.Usage.Bytes != 7 || drive.Usage.Files != 1 {
			t.Fatalf("storage after pass %d: %+v", pass, drive)
		}
		var versions, deletedVersions struct{ Data []json.RawMessage }
		var changes struct{ Data []struct{ Path string } }
		d.get(ctx, base+"/files/versions?path=file.txt", &versions)
		d.get(ctx, base+"/changes", &changes)
		d.get(ctx, base+"/files/versions?path=deleted.txt", &deletedVersions)
		if len(deletedVersions.Data) != 0 {
			t.Fatalf("deleted path retained versions: %s", deletedVersions.Data)
		}
		for _, change := range changes.Data {
			if change.Path != "file.txt" {
				t.Fatalf("expired deleted path retained change: %+v", change)
			}
		}
		if len(versions.Data) != 3 || len(changes.Data) != 3 {
			t.Fatalf("retained rows: versions=%d changes=%d", len(versions.Data), len(changes.Data))
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, d.baseURL+base+"/changes?after=0", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Api-Key", d.apiKey)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		answer, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusGone {
			t.Fatalf("expired cursor: %d %s %v", response.StatusCode, answer, err)
		}
		record.observe("pass %d: stored_bytes=15, current usage=7 bytes/1 file; three retained versions and changes; repeated current content counted once; deleted path has no versions or changes; after=0 rejected with 410", pass)
	}
	var restored struct {
		Restored     int
		RemovedPaths []string `json:"removed_paths"`
	}
	d.post(ctx, base+"/restore", map[string]string{"as_of": time.Now().Add(-42 * 24 * time.Hour).UTC().Format(time.RFC3339)}, http.StatusOK, &restored)
	if restored.Restored != 0 || len(restored.RemovedPaths) != 1 || restored.RemovedPaths[0] != "file.txt" {
		t.Fatalf("restore: %+v", restored)
	}
	record.observe("restore before the retained history restores zero files and reports only file.txt as removed; the expired deleted path is absent")
	var updated struct {
		Usage       struct{ Bytes, Files int64 }
		StoredBytes *int64 `json:"stored_bytes"`
	}
	d.post(ctx, base, map[string]any{"limits": map[string]any{"max_bytes": 10}}, http.StatusOK, &updated)
	if updated.StoredBytes == nil || *updated.StoredBytes != 15 {
		t.Fatalf("stored bytes before limit-fitting write: %+v", updated)
	}
	upload("extra.txt", "fit")
	d.get(ctx, base, &updated)
	if updated.Usage.Bytes != 10 || updated.Usage.Files != 2 {
		t.Fatalf("current usage after limit-fitting write: %+v", updated)
	}
	record.observe("with stored_bytes=15 and max_bytes=10, a 3-byte new file is accepted; current usage reaches 10 bytes/2 files")
}
