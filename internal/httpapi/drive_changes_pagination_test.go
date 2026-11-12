package httpapi_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDriveChangesTraverseCommitPositions(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	ctx := context.Background()
	content, err := api.drives.PutContent(ctx, workspace.DefaultID, id, strings.NewReader("x"), 1, "")
	if err != nil {
		t.Fatal(err)
	}
	type position struct {
		Seq   int64
		Index int
		Path  string
	}
	var all, sessionOnly []position
	for commit, count := range []int{3, 1001, 2} {
		writer := drive.Writer{Kind: drive.WriterAPIKey, ID: "apikey_fixture"}
		if commit != 1 {
			writer = drive.Writer{Kind: drive.WriterSession, ID: "sesn_page", ComputerID: "comp_page"}
		}
		changes := make([]drive.Change, count)
		for i := range changes {
			path := fmt.Sprintf("c%d/f%04d", commit, i)
			changes[i] = drive.Change{Path: path, Base: drive.Base{Kind: drive.BaseMustNotExist}, ContentSHA256: content.SHA256}
			record := position{int64(commit + 1), i, path}
			all = append(all, record)
			if commit != 1 {
				sessionOnly = append(sessionOnly, record)
			}
		}
		if _, err := api.drives.Commit(ctx, workspace.DefaultID, id, writer, drive.ModeMachine, changes); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name, filter string
		limit        int
		want         []position
	}{
		{"small pages", "", 2, all}, {"maximum pages", "", 1000, all}, {"session filter", "&session_id=sesn_page", 2, sessionOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []position
			cursor := ""
			for pages := 0; ; pages++ {
				if pages > len(all) {
					t.Fatal("pagination did not terminate")
				}
				var page struct {
					Data    []position
					HasMore bool `json:"has_more"`
				}
				decodeDrive(t, api.json(http.MethodGet, fmt.Sprintf("/v1/drives/%s/changes?limit=%d%s%s", id, tc.limit, tc.filter, cursor), ""), http.StatusOK, &page)
				got = append(got, page.Data...)
				if !page.HasMore {
					break
				}
				if len(page.Data) == 0 {
					t.Fatal("empty page with has_more")
				}
				last := page.Data[len(page.Data)-1]
				cursor = fmt.Sprintf("&after=%d&after_index=%d", last.Seq, last.Index)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("traversal differs: got %d records, want %d", len(got), len(tc.want))
			}
		})
	}
	// after without after_index passes the whole commit of that seq.
	var beyond struct{ Data []position }
	decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?limit=1&after=2", ""), http.StatusOK, &beyond)
	if len(beyond.Data) != 1 || beyond.Data[0] != (position{3, 0, "c2/f0000"}) {
		t.Fatalf("changes after seq 2 start at %+v; want the first of seq 3", beyond.Data)
	}
	var legacy struct{ Data []position }
	decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?after=2", ""), http.StatusOK, &legacy)
	if fmt.Sprint(legacy.Data) != fmt.Sprint(all[len(all)-2:]) {
		t.Fatalf("legacy after: %+v", legacy.Data)
	}
	for _, query := range []string{"after_index=0", "after=1&after_index=-1", "after=1&after_index=1.5", "after=1&after_index=x", "after=1&after_index=", "after=1&after_index=0&after_index=1", "after=1&after=2&after_index=0"} {
		response := api.outside().json(http.MethodGet, "/v1/drives/"+id+"/changes?"+query, "")
		if response.status != http.StatusBadRequest {
			t.Fatalf("%s: %d %s", query, response.status, response.body)
		}
	}
	if _, err := api.env.admin.Exec(`UPDATE drive_changes SET created_at=now()-interval '31 days' WHERE drive_id=$1 AND seq=1`, id); err != nil {
		t.Fatal(err)
	}
	expectError(t, api.json(http.MethodGet, "/v1/drives/"+id+"/changes?after=1&after_index=0", ""), http.StatusGone, "drive_cursor_expired_error", "after is below the oldest change record the drive keeps")
}
