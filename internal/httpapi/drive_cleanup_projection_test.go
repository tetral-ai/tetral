package httpapi_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestDriveRetrieveReportsStoredBytesAfterCleanup(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	for _, body := range []string{"first", "current"} {
		decodeDrive(t, api.upload(id, "file.txt", body), http.StatusCreated, nil)
	}
	cleanerRole := storagetest.OpenWorkloadDB(t, api.env.admin, "cleanup")
	cleaner := drive.NewCleaner(dbconnect.NewClientForTesting(cleanerRole.DB), api.blobs)
	for range 2 {
		if err := cleaner.CleanDrive(context.Background(), workspace.DefaultID, id); err != nil {
			t.Fatal(err)
		}
		var got struct {
			StoredBytes *int64 `json:"stored_bytes"`
			Usage       struct{ Bytes int64 }
		}
		decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id, ""), http.StatusOK, &got)
		if got.StoredBytes == nil || *got.StoredBytes != 12 || got.Usage.Bytes != 7 {
			t.Fatalf("storage projection: %+v", got)
		}
	}
}
