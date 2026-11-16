package httpapi_test

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/drive"
)

// An upload whose storage write fails answers a server error that a client
// may send again, without the storage's diagnostic, and the same upload sent
// again is stored once the storage accepts it.
func TestDriveUploadServerFailuresRemainServerErrors(t *testing.T) {
	var attempts atomic.Int32
	storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		// Every other write fails: each upload's first, not its repeat.
		if attempts.Add(1)%2 == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>private-provider-diagnostic</Message></Error>`)
	}))
	defer storage.Close()
	ca := filepath.Join(t.TempDir(), "storage-ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: storage.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CA_BUNDLE", ca)
	store, err := blob.NewS3BlobStore(context.Background(), &blob.Config{Endpoint: storage.URL, Region: "us-east-1", Bucket: "upload-errors", AccessKey: "fixture", SecretKey: "fixture", LocalTestMode: true})
	if err != nil {
		t.Fatal(err)
	}
	api := newDriveAPIWithStore(t, drive.DefaultUploadBudgetBytesPerMinute, store, t.TempDir())
	id := api.newDrive(`{}`)
	for _, tc := range []struct {
		name string
		send func() driveResponse
	}{
		{"file", func() driveResponse { return api.upload(id, "file.txt", "contents") }},
		{"skill", func() driveResponse {
			return api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("description"))))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := attempts.Load()
			response := tc.send()
			expectError(t, response, http.StatusInternalServerError, "api_error", "internal error")
			if attempts.Load() <= before {
				t.Fatal("no request reached S3")
			}
			if response.header.Get("x-should-retry") == "false" {
				t.Fatal("the server failure tells the client not to send the upload again")
			}
			if strings.Contains(string(response.body), "private-provider") {
				t.Fatal("provider diagnostic leaked")
			}
			decodeDrive(t, tc.send(), http.StatusCreated, nil)
			if attempts.Load() != before+2 {
				t.Fatalf("%d storage writes for the upload and its repeat; want one each", attempts.Load()-before)
			}
		})
	}
}

func TestDriveUploadSourceAndStageFailuresAreDistinct(t *testing.T) {
	dir := t.TempDir()
	api := newDriveAPIWithStore(t, drive.DefaultUploadBudgetBytesPerMinute, blob.NewFakeBlobStore(), dir)
	id := api.newDrive(`{}`)
	for _, part := range []struct{ endpoint, name string }{{"files", "file"}, {"skills", "archive"}} {
		body := "--broken\r\nContent-Disposition: form-data; name=\"" + part.name + "\"; filename=\"upload\"\r\n\r\ntruncated"
		expectError(t, api.outside().request(http.MethodPost, "/v1/drives/"+id+"/"+part.endpoint, strings.NewReader(body), "multipart/form-data; boundary=broken"), http.StatusBadRequest, "invalid_request_error", "the multipart body could not be read")
	}
	if err := os.RemoveAll(filepath.Join(dir, "drive-upload-stage")); err != nil {
		t.Fatal(err)
	}
	expectError(t, api.upload(id, "file.txt", "contents"), http.StatusInternalServerError, "api_error", "internal error")
	expectError(t, api.uploadSkill(id, skillZip(t, file("report/SKILL.md", reportSkillMD("description")))), http.StatusInternalServerError, "api_error", "internal error")
}
