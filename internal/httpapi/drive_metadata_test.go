package httpapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/tetral-ai/tetral/internal/drive"
)

func TestDriveMetadataMergeEnforcesStoredKeyLimit(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	metadata := map[string]string{}
	for i := range 16 {
		metadata[fmt.Sprintf("k%d", i)] = "original"
	}
	body, _ := json.Marshal(map[string]any{"metadata": metadata})
	id := api.newDrive(string(body))
	endpoint := "/v1/drives/" + id
	expectError(t, api.json(http.MethodPost, endpoint, `{"skills_path":"changed/","metadata":{"k0":"replaced","overflow":"new"}}`), http.StatusBadRequest, "invalid_request_error", "metadata must have at most 16 keys")
	var stored struct {
		Metadata   map[string]string
		SkillsPath string `json:"skills_path"`
	}
	decodeDrive(t, api.json(http.MethodGet, endpoint, ""), http.StatusOK, &stored)
	if len(stored.Metadata) != 16 || stored.Metadata["k0"] != "original" || stored.SkillsPath != "skills/" {
		t.Fatalf("overflow changed stored state: %+v", stored)
	}
	stored.Metadata = nil
	decodeDrive(t, api.json(http.MethodPost, endpoint, `{"metadata":{"k0":"replaced","k1":null,"new":"accepted"}}`), http.StatusOK, &stored)
	if len(stored.Metadata) != 16 || stored.Metadata["k0"] != "replaced" || stored.Metadata["new"] != "accepted" {
		t.Fatalf("delete/add merge: %+v", stored)
	}
	if _, exists := stored.Metadata["k1"]; exists {
		t.Fatal("deleted key remains")
	}
}
