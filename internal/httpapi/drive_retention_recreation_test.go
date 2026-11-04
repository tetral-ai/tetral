package httpapi_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive"
)

func TestDriveRecreationDoesNotReviveExpiredDeletion(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	decodeDrive(t, api.upload(id, "recreated.txt", "old"), http.StatusCreated, nil)
	var deletion driveVersionAnswer
	decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path=recreated.txt&expected_content_sha256="+digest("old"), ""), http.StatusOK, &deletion)
	if _, err := api.env.admin.Exec(`UPDATE drive_versions SET created_at=now()-interval '32 days',superseded_at=CASE WHEN operation='deleted' THEN NULL ELSE now()-interval '31 days' END WHERE drive_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	var current driveVersionAnswer
	decodeDrive(t, api.upload(id, "recreated.txt", "new"), http.StatusCreated, &current)
	var versions struct{ Data []driveVersionAnswer }
	decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/files/versions?path=recreated.txt", ""), http.StatusOK, &versions)
	if len(versions.Data) != 1 || versions.Data[0].VersionID != current.VersionID {
		t.Errorf("versions after recreation: %+v", versions)
	}
	response := api.json(http.MethodGet, "/v1/drives/"+id+"/versions/"+deletion.VersionID, "")
	if response.status != http.StatusNotFound {
		t.Errorf("expired deletion: %d %s", response.status, response.body)
	}
	body, _ := json.Marshal(map[string]string{"as_of": time.Now().Add(-33 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)})
	var restored struct {
		Restored     int
		RemovedPaths []string `json:"removed_paths"`
	}
	decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", string(body)), http.StatusOK, &restored)
	if restored.Restored != 0 || len(restored.RemovedPaths) != 0 {
		t.Fatalf("expired deletion influenced restore: %+v", restored)
	}
	content := api.json(http.MethodGet, "/v1/drives/"+id+"/files/content?path=recreated.txt", "")
	if content.status != http.StatusOK || string(content.body) != "new" {
		t.Fatalf("recreated content: %d %s", content.status, content.body)
	}
}
