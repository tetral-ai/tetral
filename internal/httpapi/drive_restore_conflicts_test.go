package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDriveRestoreConflictsAreAtomicAndCanBeNarrowed(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	for _, tc := range []struct{ name, old, current string }{{"case fold", "name.txt", "Name.txt"}, {"file becomes folder", "node", "node/child"}, {"folder becomes file", "node/child", "node"}} {
		t.Run(tc.name, func(t *testing.T) {
			id := api.newDrive(`{}`)
			decodeDrive(t, api.upload(id, tc.old, "original"), http.StatusCreated, nil)
			decodeDrive(t, api.upload(id, "unrelated.txt", "before"), http.StatusCreated, nil)
			var asOf time.Time
			if err := api.env.admin.QueryRow(`SELECT max(created_at) FROM drive_versions WHERE drive_id=$1`, id).Scan(&asOf); err != nil {
				t.Fatal(err)
			}
			decodeDrive(t, api.json(http.MethodDelete, "/v1/drives/"+id+"/files?path="+url.QueryEscape(tc.old)+"&expected_content_sha256="+digest("original"), ""), http.StatusOK, nil)
			decodeDrive(t, api.upload(id, tc.current, "newer"), http.StatusCreated, nil)
			decodeDrive(t, api.upload(id, "unrelated.txt", "after"), http.StatusCreated, nil)
			before, err := api.drives.LastSeq(context.Background(), workspace.DefaultID, id)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(map[string]string{"as_of": asOf.UTC().Format(time.RFC3339Nano)})
			response := api.json(http.MethodPost, "/v1/drives/"+id+"/restore", string(body))
			if response.status != http.StatusConflict || response.header.Get("x-should-retry") != "false" {
				t.Fatalf("restore rejection: %d %s", response.status, response.body)
			}
			var failure struct{ Error struct{ Message string } }
			if err := json.Unmarshal(response.body, &failure); err != nil {
				t.Fatal(err)
			}
			if !strings.HasSuffix(failure.Error.Message, ": "+tc.old) {
				t.Fatalf("missing failing path: %s", failure.Error.Message)
			}
			after, err := api.drives.LastSeq(context.Background(), workspace.DefaultID, id)
			if err != nil {
				t.Fatal(err)
			}
			if after != before {
				t.Fatalf("rejected restore advanced seq %d -> %d", before, after)
			}
			for path, want := range map[string]string{tc.current: "newer", "unrelated.txt": "after"} {
				result := api.json(http.MethodGet, "/v1/drives/"+id+"/files/content?path="+url.QueryEscape(path), "")
				if result.status != http.StatusOK || string(result.body) != want {
					t.Fatalf("%s changed: %d %s", path, result.status, result.body)
				}
			}
			body, _ = json.Marshal(map[string]string{"as_of": asOf.UTC().Format(time.RFC3339Nano), "path_prefix": "unrelated.txt"})
			decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", string(body)), http.StatusOK, nil)
			result := api.json(http.MethodGet, "/v1/drives/"+id+"/files/content?path=unrelated.txt", "")
			if string(result.body) != "before" {
				t.Fatalf("narrow restore: %s", result.body)
			}
		})
	}
}

func TestDriveRestorePreservesHistoricalExecutableBit(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	content, err := api.drives.PutContent(context.Background(), workspace.DefaultID, id, strings.NewReader("script"), 6, "")
	if err != nil {
		t.Fatal(err)
	}
	writer := drive.Writer{Kind: drive.WriterSession, ID: "sesn_executable", ComputerID: "comp_executable"}
	for _, executable := range []bool{true, false} {
		result, err := api.drives.Commit(context.Background(), workspace.DefaultID, id, writer, drive.ModeMachine, []drive.Change{{Path: "run.sh", Base: drive.Base{Kind: drive.BaseAny}, ContentSHA256: content.SHA256, Executable: executable}})
		if err != nil || result.Seq == nil {
			t.Fatalf("executable commit: %+v %v", result, err)
		}
	}
	var first time.Time
	if err := api.env.admin.QueryRow(`SELECT min(created_at) FROM drive_versions WHERE drive_id=$1`, id).Scan(&first); err != nil {
		t.Fatal(err)
	}
	decodeDrive(t, api.json(http.MethodPost, "/v1/drives/"+id+"/restore", fmt.Sprintf(`{"as_of":%q}`, first.UTC().Format(time.RFC3339Nano))), http.StatusOK, nil)
	file, err := api.drives.CurrentFile(context.Background(), workspace.DefaultID, id, "run.sh")
	if err != nil || !file.Executable {
		t.Fatalf("restored executable: %+v %v", file, err)
	}
}
