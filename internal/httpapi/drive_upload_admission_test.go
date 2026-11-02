package httpapi_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/httpapi"
)

func TestDriveUploadMultipartPreconditions(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	id := api.newDrive(`{}`)
	for _, tc := range []struct {
		name    string
		fields  []string
		message string
	}{
		{"unknown field", []string{"unknown", "value"}, "unknown multipart part unknown"},
		{"duplicate precondition", []string{"must_not_exist", "true", "must_not_exist", "true"}, "multipart part must_not_exist appears twice"},
		{"duplicate hash", []string{"expected_content_sha256", digest("x"), "expected_content_sha256", digest("y")}, "multipart part expected_content_sha256 appears twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectError(t, api.upload(id, "new.txt", "new", tc.fields...), http.StatusBadRequest, "invalid_request_error", tc.message)
		})
	}
	path := strings.Repeat("a/", 511) + "ab"
	decodeDrive(t, api.upload(id, path, "boundary"), http.StatusCreated, nil)
}

func TestDriveWriterFollowsVerifiedCredential(t *testing.T) {
	api := newDriveAPI(t, drive.DefaultUploadBudgetBytesPerMinute)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSigner(private)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifier(public)
	if err != nil {
		t.Fatal(err)
	}
	router := httpapi.NewRouter(nil, "", httpapi.WithInternalPrincipalVerifier(verifier), httpapi.WithDriveHandler(httpapi.NewDriveHandler(api.drives, api.budget, nil, []byte("page-test-key"))))
	key, err := auth.NewAuthorityResolver(api.env.runtime, "", nil).AuthenticateKey(context.Background(), api.env.envKey)
	if err != nil {
		t.Fatal(err)
	}
	derived := identityAdmissionFixture(auth.IdentityService)
	derived.Credential = auth.Credential{Kind: auth.CredentialAPIKey, ID: key.APIKeyID}
	derived.APIKeyID = key.APIKeyID
	for _, tc := range []struct {
		name      string
		principal auth.Principal
		want      drive.Writer
	}{
		{"API key", key, drive.Writer{Kind: drive.WriterAPIKey, ID: key.APIKeyID}},
		{"identity API key", derived, drive.Writer{Kind: drive.WriterAPIKey, ID: key.APIKeyID}},
		{"human token", identityAdmissionFixture(auth.IdentityHuman), drive.Writer{Kind: drive.WriterUser, ID: "identity_human"}},
		{"service token", identityAdmissionFixture(auth.IdentityService), drive.Writer{Kind: drive.WriterServiceAccount, ID: "identity_service"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := api.newDrive(`{}`)
			var body bytes.Buffer
			multipartWriter := multipart.NewWriter(&body)
			_ = multipartWriter.WriteField("path", "written.txt")
			part, _ := multipartWriter.CreateFormFile("file", "written.txt")
			_, _ = part.Write([]byte("contents"))
			_ = multipartWriter.Close()
			request := httptest.NewRequest(http.MethodPost, "/v1/drives/"+id+"/files", &body)
			token, err := signer.Mint(tc.principal, request.Method, request.URL.Path, "req_drive_actor", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("X-Tetral-Internal-Principal", token)
			request.Header.Set("X-Tetral-API-Key-ID", "forged")
			request.Header.Set("Content-Type", multipartWriter.FormDataContentType())
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			var uploaded driveVersionAnswer
			decodeDrive(t, driveResponse{response.Code, response.Header(), response.Body.Bytes()}, http.StatusCreated, &uploaded)
			var version struct{ Writer map[string]any }
			decodeDrive(t, api.json(http.MethodGet, "/v1/drives/"+id+"/versions/"+uploaded.VersionID, ""), http.StatusOK, &version)
			wantJSON, _ := json.Marshal(tc.want)
			var want map[string]any
			if err := json.Unmarshal(wantJSON, &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(version.Writer, want) {
				t.Fatalf("writer = %+v, want %+v", version.Writer, want)
			}
		})
	}
}
