package httpapi_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type fileHandlerEnv struct {
	blob     *blob.FakeBlobStore
	store    *files.PostgreSQLFileStore
	admin    *sql.DB
	handler  *httpapi.FileHandler
	stageDir string
}

func newFileHandlerEnv(t *testing.T, limits httpapi.FileHandlerLimits) *fileHandlerEnv {
	t.Helper()
	db, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	blobStore := blob.NewFakeBlobStore()
	store := files.NewPostgreSQLStore(dbconnect.NewClientForTesting(db), blobStore)
	stageDir, err := files.NewStageDir(t.TempDir())
	if err != nil {
		t.Fatalf("create Files stage dir: %v", err)
	}
	return &fileHandlerEnv{
		blob:     blobStore,
		store:    store,
		admin:    admin,
		handler:  httpapi.NewFileHandler(files.NewService(store), stageDir, limits),
		stageDir: stageDir,
	}
}

func (e *fileHandlerEnv) router(t *testing.T) http.Handler {
	t.Helper()
	return newFileRouter(e.handler)
}

func newFileRouter(handler *httpapi.FileHandler) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(_ context.Context, rawKey string) (auth.Principal, error) {
		if rawKey != testAPIKey {
			return auth.Principal{}, &auth.AuthenticationError{Message: "invalid api key"}
		}
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}, APIKeyID: "ak_test"}, nil
	})
	return httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithFileHandler(handler))
}

type fileUploadPart struct {
	name             string
	body             []byte
	fileName         string
	fileNameSet      bool
	shape            string
	contentType      string
	contentTypeSet   bool
	contentDispValue string
}

func buildFileMultipartBody(t *testing.T, parts []fileUploadPart) (*bytes.Buffer, string) {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	for _, part := range parts {
		shape := part.shape
		if shape == "" {
			shape = "file"
		}
		var (
			target io.Writer
			err    error
		)
		switch shape {
		case "file":
			header := make(textproto.MIMEHeader)
			if part.contentDispValue != "" {
				header.Set("Content-Disposition", part.contentDispValue)
			} else {
				fileName := part.fileName
				if fileName == "" && !part.fileNameSet {
					fileName = "upload.bin"
				}
				header.Set("Content-Disposition", `form-data; name="`+part.name+`"; filename="`+fileName+`"`)
			}
			if part.contentTypeSet {
				header.Set("Content-Type", part.contentType)
			}
			target, err = writer.CreatePart(header)
		case "text":
			target, err = writer.CreateFormField(part.name)
		default:
			t.Fatalf("unknown file upload part shape %q", part.shape)
		}
		if err != nil {
			t.Fatalf("create multipart part %q: %v", part.name, err)
		}
		if _, err := target.Write(part.body); err != nil {
			t.Fatalf("write multipart part %q: %v", part.name, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	return &buf, writer.FormDataContentType()
}

func performFileUpload(t *testing.T, h http.Handler, contentType string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	return performMultipartRequest(t, h, http.MethodPost, "/v1/files", contentType, body)
}

func decodeFileMetadata(t *testing.T, rec *httptest.ResponseRecorder) files.FileMetadata {
	t.Helper()
	var got files.FileMetadata
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode file metadata (status=%d, body=%q): %v", rec.Code, rec.Body.String(), err)
	}
	return got
}

func assertFileError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantType string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d; want %d body=%q", rec.Code, wantStatus, rec.Body.String())
	}
	errType, _, requestID := decodeError(t, rec)
	if errType != wantType {
		t.Errorf("error.type = %q; want %q", errType, wantType)
	}
	if requestID == "" {
		t.Error("error response must include request_id")
	}
}

func assertStageDirEmpty(t *testing.T, stageDir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(stageDir, "*"))
	if err != nil {
		t.Fatalf("glob stage dir: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("stage dir contains temp files after request: %v", matches)
	}
}

func TestFileUploadStoresBytesAndCleansStage(t *testing.T) {
	env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
	router := env.router(t)
	payload := []byte("hello files")
	body, contentType := buildFileMultipartBody(t, []fileUploadPart{{
		name:           "file",
		fileName:       "notes.txt",
		body:           payload,
		contentType:    "text/plain",
		contentTypeSet: true,
	}})

	rec := performFileUpload(t, router, contentType, body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
	}
	got := decodeFileMetadata(t, rec)
	if !strings.HasPrefix(got.ID, files.IDPrefix) {
		t.Fatalf("id = %q; want %q prefix", got.ID, files.IDPrefix)
	}
	if got.Type != "file" || got.Filename != "notes.txt" || got.MIMEType != "text/plain" || got.SizeBytes != int64(len(payload)) {
		t.Fatalf("unexpected metadata: %+v", got)
	}
	if got.Downloadable {
		t.Fatal("uploaded original files must not be downloadable")
	}
	assertUploadMetadataJSONShape(t, rec, got.ID, int64(len(payload)))
	if env.blob.Len() != 1 {
		t.Fatalf("blob object count = %d; want 1", env.blob.Len())
	}
	row := loadHTTPFileRow(t, env.admin, workspace.DefaultID, got.ID)
	stored, ok := env.blob.Bytes(row.blobKey)
	if !ok {
		t.Fatalf("blob key %q was not written", row.blobKey)
	}
	if !bytes.Equal(stored, payload) {
		t.Fatalf("stored blob bytes = %q; want exact upload payload %q", string(stored), string(payload))
	}
	assertStageDirEmpty(t, env.stageDir)
}

func TestFileUploadPreservesRawFilenameAsMetadataOnly(t *testing.T) {
	env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
	router := env.router(t)
	payload := []byte("csv")
	filename := "../tenant/data.csv"
	body, contentType := buildFileMultipartBody(t, []fileUploadPart{{
		name:           "file",
		fileName:       filename,
		body:           payload,
		contentType:    "text/csv",
		contentTypeSet: true,
	}})

	rec := performFileUpload(t, router, contentType, body)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
	}
	got := decodeFileMetadata(t, rec)
	if got.Filename != filename {
		t.Fatalf("returned filename = %q; want raw multipart filename %q", got.Filename, filename)
	}
	row := loadHTTPFileRow(t, env.admin, workspace.DefaultID, got.ID)
	if row.filename != filename {
		t.Fatalf("stored filename = %q; want raw multipart filename %q", row.filename, filename)
	}
	if row.blobKey != "files/"+string(workspace.DefaultID)+"/"+row.objectID {
		t.Fatalf("blob key = %q; want files/<workspace>/<object_id>", row.blobKey)
	}
	for _, fragment := range []string{"..", "tenant", "data.csv"} {
		if strings.Contains(row.blobKey, fragment) {
			t.Fatalf("blob key %q contains uploaded filename fragment %q", row.blobKey, fragment)
		}
	}
	if !env.blob.Has(row.blobKey) {
		t.Fatalf("blob key %q was not written", row.blobKey)
	}
	assertStageDirEmpty(t, env.stageDir)
}

func assertUploadMetadataJSONShape(t *testing.T, rec *httptest.ResponseRecorder, fileID string, sizeBytes int64) {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw upload metadata JSON: %v", err)
	}
	wantKeys := map[string]bool{
		"id":           true,
		"type":         true,
		"filename":     true,
		"mime_type":    true,
		"size_bytes":   true,
		"created_at":   true,
		"downloadable": true,
	}
	for key := range wantKeys {
		if _, ok := raw[key]; !ok {
			t.Fatalf("upload response missing required key %q in %s", key, rec.Body.String())
		}
	}
	for key := range raw {
		if !wantKeys[key] {
			t.Fatalf("upload response included unexpected key %q in %s", key, rec.Body.String())
		}
	}
	if len(raw) != len(wantKeys) {
		t.Fatalf("upload response key count = %d; want exactly %d in %s", len(raw), len(wantKeys), rec.Body.String())
	}
	if _, ok := raw["scope"]; ok {
		t.Fatalf("uploaded original response included unexpected scope: %s", raw["scope"])
	}
	var shape struct {
		ID           string `json:"id"`
		Type         string `json:"type"`
		Filename     string `json:"filename"`
		MIMEType     string `json:"mime_type"`
		SizeBytes    int64  `json:"size_bytes"`
		CreatedAt    string `json:"created_at"`
		Downloadable bool   `json:"downloadable"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &shape); err != nil {
		t.Fatalf("decode upload metadata shape: %v", err)
	}
	if shape.ID != fileID || shape.Type != "file" || shape.Filename != "notes.txt" || shape.MIMEType != "text/plain" || shape.SizeBytes != sizeBytes {
		t.Fatalf("unexpected upload JSON shape: %+v", shape)
	}
	if shape.Downloadable {
		t.Fatal("downloadable JSON field = true; want explicit false")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, shape.CreatedAt)
	if err != nil {
		t.Fatalf("created_at = %q; want RFC3339 timestamp: %v", shape.CreatedAt, err)
	}
	if createdAt.IsZero() {
		t.Fatal("created_at is zero")
	}
}

func loadHTTPFileRow(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string) fileRow {
	t.Helper()
	var row fileRow
	if err := db.QueryRowContext(context.Background(),
		`SELECT f.object_id, o.blob_key, f.filename
		   FROM files f
		   JOIN file_objects o ON o.workspace_id = f.workspace_id AND o.object_id = f.object_id
		  WHERE f.workspace_id = $1 AND f.file_id = $2`,
		string(workspaceID), fileID).Scan(&row.objectID, &row.blobKey, &row.filename); err != nil {
		t.Fatalf("load file row: %v", err)
	}
	return row
}

type fileRow struct {
	objectID string
	blobKey  string
	filename string
}

func TestFileUploadRequiresAuthentication(t *testing.T) {
	env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
	router := env.router(t)
	body, contentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", body: []byte("hello")}})
	req := httptest.NewRequest(http.MethodPost, "/v1/files", body)
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assertFileError(t, rec, http.StatusUnauthorized, "authentication_error")
	if env.blob.Len() != 0 {
		t.Fatalf("blob store was written for unauthenticated upload")
	}
	assertStageDirEmpty(t, env.stageDir)
}

func TestFileUploadRejectsMalformedPartsWithoutBlobWrites(t *testing.T) {
	tests := []struct {
		name        string
		parts       []fileUploadPart
		contentType string
		body        io.Reader
	}{
		{name: "missing file part", parts: nil},
		{name: "duplicate file part", parts: []fileUploadPart{{name: "file", body: []byte("a")}, {name: "file", body: []byte("b")}}},
		{name: "text file field", parts: []fileUploadPart{{name: "file", shape: "text", body: []byte("not a file")}}},
		{name: "unexpected purpose field", parts: []fileUploadPart{{name: "purpose", shape: "text", body: []byte("assistants")}, {name: "file", body: []byte("a")}}},
		{name: "unexpected extra file", parts: []fileUploadPart{{name: "file", body: []byte("a")}, {name: "extra", body: []byte("b")}}},
		{name: "missing filename", parts: []fileUploadPart{{name: "file", body: []byte("a"), contentDispValue: `form-data; name="file"`}}},
		{name: "empty filename", parts: []fileUploadPart{{name: "file", body: []byte("a"), fileNameSet: true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
			router := env.router(t)
			body := tt.body
			contentType := tt.contentType
			if body == nil {
				var buf *bytes.Buffer
				buf, contentType = buildFileMultipartBody(t, tt.parts)
				body = buf
			}

			rec := performFileUpload(t, router, contentType, body)

			assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
			if env.blob.Len() != 0 {
				t.Fatalf("blob store was written for rejected upload")
			}
			assertStageDirEmpty(t, env.stageDir)
		})
	}
}

func TestFileUploadRejectsMalformedMultipart(t *testing.T) {
	env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
	rec := performFileUpload(t, env.router(t), "multipart/form-data; boundary=missing", strings.NewReader("--wrong\r\n"))

	assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
	if env.blob.Len() != 0 {
		t.Fatal("blob store was written for malformed multipart")
	}
	assertStageDirEmpty(t, env.stageDir)
}

func TestFileUploadSizeCapsReturn413(t *testing.T) {
	t.Run("route multipart cap", func(t *testing.T) {
		env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{RouteMultipartBytes: 64})
		body, contentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", body: []byte(strings.Repeat("x", 128))}})

		rec := performFileUpload(t, env.router(t), contentType, body)

		assertFileError(t, rec, http.StatusRequestEntityTooLarge, "request_too_large")
		if env.blob.Len() != 0 {
			t.Fatal("blob store was written after route cap rejection")
		}
		assertStageDirEmpty(t, env.stageDir)
	})

	t.Run("per file cap", func(t *testing.T) {
		env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{RouteMultipartBytes: 4096, MaxFileBytes: 4})
		body, contentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", body: []byte("12345")}})

		rec := performFileUpload(t, env.router(t), contentType, body)

		assertFileError(t, rec, http.StatusRequestEntityTooLarge, "request_too_large")
		if env.blob.Len() != 0 {
			t.Fatal("blob store was written after per-file cap rejection")
		}
		assertStageDirEmpty(t, env.stageDir)
	})
}

func TestFileUploadValidatesFilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{name: "invalid utf8", filename: string([]byte{0xff})},
		{name: "nul", filename: "bad\x00name.txt"},
		{name: "control", filename: "bad\x01name.txt"},
		{name: "control in path fragment", filename: "bad\x01dir/data.csv"},
		{name: "unicode control", filename: "bad\u0085name.txt"},
		{name: "more than 1024 bytes", filename: strings.Repeat("a", 1025)},
		{name: "more than 255 code points", filename: strings.Repeat("a", 256)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
			body, contentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", fileName: tt.filename, body: []byte("a")}})

			rec := performFileUpload(t, env.router(t), contentType, body)

			assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
			if env.blob.Len() != 0 {
				t.Fatal("blob store was written after filename rejection")
			}
			assertStageDirEmpty(t, env.stageDir)
		})
	}
}

func TestFileUploadMIMEHandling(t *testing.T) {
	t.Run("missing defaults to octet stream", func(t *testing.T) {
		env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
		body, contentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", body: []byte("abc")}})

		rec := performFileUpload(t, env.router(t), contentType, body)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		got := decodeFileMetadata(t, rec)
		if got.MIMEType != "application/octet-stream" {
			t.Fatalf("mime_type = %q; want application/octet-stream", got.MIMEType)
		}
		assertStageDirEmpty(t, env.stageDir)
	})

	t.Run("parameterized normalizes media type", func(t *testing.T) {
		env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
		body, contentType := buildFileMultipartBody(t, []fileUploadPart{{
			name:           "file",
			body:           []byte("abc"),
			contentType:    "Text/Plain; charset=utf-8",
			contentTypeSet: true,
		}})

		rec := performFileUpload(t, env.router(t), contentType, body)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		got := decodeFileMetadata(t, rec)
		if got.MIMEType != "text/plain" {
			t.Fatalf("mime_type = %q; want text/plain", got.MIMEType)
		}
		assertStageDirEmpty(t, env.stageDir)
	})

	t.Run("malformed and overlong reject", func(t *testing.T) {
		tests := []struct {
			name        string
			contentType string
		}{
			{name: "malformed", contentType: `text/plain; charset="unterminated`},
			{name: "overlong", contentType: strings.Repeat("a", 256)},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
				body, formContentType := buildFileMultipartBody(t, []fileUploadPart{{
					name:           "file",
					body:           []byte("abc"),
					contentType:    tt.contentType,
					contentTypeSet: true,
				}})

				rec := performFileUpload(t, env.router(t), formContentType, body)

				assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
				if env.blob.Len() != 0 {
					t.Fatal("blob store was written after MIME rejection")
				}
				assertStageDirEmpty(t, env.stageDir)
			})
		}
	})
}

type recordingFileService struct {
	createCalls int
	listCalls   int
	getCalls    int
	deleteCalls int
	openCalls   int

	createResult *files.FileMetadata
	listResult   files.ListResult
	getResult    *files.FileMetadata
	deleteResult *files.DeleteResponse
	openResult   *files.ContentStream
	err          error
	lastOptions  files.ListOptions

	lastStagedFilename string
}

var _ httpapi.FileService = (*recordingFileService)(nil)

func (s *recordingFileService) CreateUploadedFile(_ context.Context, _ workspace.ID, staged *files.StagedUpload) (*files.FileMetadata, error) {
	s.createCalls++
	if staged != nil {
		s.lastStagedFilename = staged.Filename
		_ = staged.Cleanup()
	}
	if s.err != nil {
		return nil, s.err
	}
	if s.createResult != nil {
		return s.createResult, nil
	}
	return &files.FileMetadata{ID: "file_fake", Type: "file", Filename: "upload.bin", MIMEType: "application/octet-stream"}, nil
}

func TestFileUploadParsesRawContentDispositionFilename(t *testing.T) {
	t.Run("path fragments are staged as metadata", func(t *testing.T) {
		store := &recordingFileService{}
		stageDir, err := files.NewStageDir(t.TempDir())
		if err != nil {
			t.Fatalf("create stage dir: %v", err)
		}
		handler := httpapi.NewFileHandler(store, stageDir, httpapi.FileHandlerLimits{})
		router := newFileRouter(handler)
		filename := "../tenant/data.csv"
		body, contentType := buildFileMultipartBody(t, []fileUploadPart{{
			name:     "file",
			fileName: filename,
			body:     []byte("csv"),
		}})

		rec := performFileUpload(t, router, contentType, body)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		if store.lastStagedFilename != filename {
			t.Fatalf("staged filename = %q; want raw multipart filename %q", store.lastStagedFilename, filename)
		}
		assertStageDirEmpty(t, stageDir)
	})

	t.Run("control character in stripped path fragment rejects before store", func(t *testing.T) {
		store := &recordingFileService{}
		stageDir, err := files.NewStageDir(t.TempDir())
		if err != nil {
			t.Fatalf("create stage dir: %v", err)
		}
		handler := httpapi.NewFileHandler(store, stageDir, httpapi.FileHandlerLimits{})
		router := newFileRouter(handler)
		body, contentType := buildFileMultipartBody(t, []fileUploadPart{{
			name:     "file",
			fileName: "bad\x01dir/data.csv",
			body:     []byte("csv"),
		}})

		rec := performFileUpload(t, router, contentType, body)

		assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
		if store.createCalls != 0 {
			t.Fatalf("store create calls = %d; want 0", store.createCalls)
		}
		assertStageDirEmpty(t, stageDir)
	})
}

func (s *recordingFileService) ListFiles(_ context.Context, _ workspace.ID, options files.ListOptions) (files.ListResult, error) {
	s.listCalls++
	s.lastOptions = options
	return s.listResult, s.err
}

func (s *recordingFileService) GetFile(_ context.Context, _ workspace.ID, _ string) (*files.FileMetadata, error) {
	s.getCalls++
	if s.err != nil {
		return nil, s.err
	}
	if s.getResult != nil {
		return s.getResult, nil
	}
	return &files.FileMetadata{ID: "file_fake", Type: "file", Filename: "stored.bin", MIMEType: "application/octet-stream"}, nil
}

func (s *recordingFileService) DeleteFile(_ context.Context, _ workspace.ID, fileID string) (*files.DeleteResponse, error) {
	s.deleteCalls++
	if s.err != nil {
		return nil, s.err
	}
	if s.deleteResult != nil {
		return s.deleteResult, nil
	}
	return &files.DeleteResponse{ID: fileID, Type: "file_deleted"}, nil
}

func (s *recordingFileService) OpenContent(_ context.Context, _ workspace.ID, _ string) (*files.ContentStream, error) {
	s.openCalls++
	if s.err != nil {
		return nil, s.err
	}
	if s.openResult != nil {
		return s.openResult, nil
	}
	return &files.ContentStream{
		Metadata: &files.FileMetadata{ID: "file_fake", Type: "file", Filename: "stored.bin", MIMEType: "application/octet-stream"},
		Reader:   io.NopCloser(strings.NewReader("content")),
	}, nil
}

func TestFilePathIDValidationRejectsBadIDsBeforeStore(t *testing.T) {
	badIDs := []string{
		"wrong_test",
		"file_bad%00id",
		"file_bad%C2%85id",
		"file_" + strings.Repeat("a", 252),
	}
	for _, badID := range badIDs {
		for _, route := range []struct {
			method string
			path   string
		}{
			{http.MethodGet, "/v1/files/" + badID},
			{http.MethodDelete, "/v1/files/" + badID},
			{http.MethodGet, "/v1/files/" + badID + "/content"},
		} {
			t.Run(route.method+" "+badID, func(t *testing.T) {
				store := &recordingFileService{}
				handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
				router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
				req := httptest.NewRequest(route.method, route.path, nil)
				setAuthHeader(req)
				rec := httptest.NewRecorder()

				router.ServeHTTP(rec, req)

				assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
				if store.getCalls != 0 || store.deleteCalls != 0 || store.openCalls != 0 {
					t.Fatalf("store reached for invalid id: get=%d delete=%d open=%d", store.getCalls, store.deleteCalls, store.openCalls)
				}
			})
		}
	}
}

func TestFileRoutesDoNotRequireAnthropicHeaders(t *testing.T) {
	for _, withHeaders := range []bool{false, true} {
		name := "absent"
		if withHeaders {
			name = "present"
		}
		t.Run(name, func(t *testing.T) {
			store := &recordingFileService{
				createResult: &files.FileMetadata{ID: "file_create", Type: "file", Filename: "created.bin", MIMEType: "application/octet-stream"},
				listResult: files.ListResult{Data: []*files.FileMetadata{{
					ID: "file_list", Type: "file", Filename: "listed.bin", MIMEType: "application/octet-stream",
				}}},
				getResult:    &files.FileMetadata{ID: "file_get", Type: "file", Filename: "got.bin", MIMEType: "application/octet-stream"},
				deleteResult: &files.DeleteResponse{ID: "file_delete", Type: "file_deleted"},
				openResult: &files.ContentStream{
					Metadata: &files.FileMetadata{ID: "file_content", Type: "file", Filename: "content.txt", MIMEType: "text/plain"},
					Reader:   io.NopCloser(strings.NewReader("content")),
				},
			}
			handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
			router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
			uploadBody, uploadContentType := buildFileMultipartBody(t, []fileUploadPart{{name: "file", body: []byte("upload")}})
			requests := []struct {
				method      string
				path        string
				contentType string
				body        io.Reader
			}{
				{method: http.MethodPost, path: "/v1/files", contentType: uploadContentType, body: uploadBody},
				{method: http.MethodGet, path: "/v1/files"},
				{method: http.MethodGet, path: "/v1/files/file_get"},
				{method: http.MethodDelete, path: "/v1/files/file_delete"},
				{method: http.MethodGet, path: "/v1/files/file_content/content"},
			}
			for _, tc := range requests {
				t.Run(tc.method+" "+tc.path, func(t *testing.T) {
					req := httptest.NewRequest(tc.method, tc.path, tc.body)
					if tc.contentType != "" {
						req.Header.Set("Content-Type", tc.contentType)
					}
					if withHeaders {
						req.Header.Set("anthropic-version", "2023-06-01")
						req.Header.Set("anthropic-beta", "files-api-test")
					}
					setAuthHeader(req)
					rec := httptest.NewRecorder()

					router.ServeHTTP(rec, req)

					if rec.Code != http.StatusOK {
						t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
					}
				})
			}
		})
	}
}

func TestFilePathUnknownWellFormedIDReturns404(t *testing.T) {
	store := &recordingFileService{err: &files.NotFoundError{Message: "file not found"}}
	handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
	req := httptest.NewRequest(http.MethodGet, "/v1/files/file_missing", nil)
	setAuthHeader(req)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assertFileError(t, rec, http.StatusNotFound, "not_found_error")
	if store.getCalls != 1 {
		t.Fatalf("get calls = %d; want 1", store.getCalls)
	}
}

func TestFileHandlerRoutesListMetadataDeleteAndContent(t *testing.T) {
	store := &recordingFileService{
		listResult: files.ListResult{Data: []*files.FileMetadata{{
			ID: "file_listed", Type: "file", Filename: "listed.bin", MIMEType: "application/octet-stream", CreatedAt: time.Unix(0, 0).UTC(),
		}}, HasMore: false},
		getResult:    &files.FileMetadata{ID: "file_meta", Type: "file", Filename: "meta.bin", MIMEType: "application/octet-stream"},
		deleteResult: &files.DeleteResponse{ID: "file_delete", Type: "file_deleted"},
		openResult: &files.ContentStream{
			Metadata: &files.FileMetadata{ID: "file_download", Type: "file", Filename: "download.bin", MIMEType: "text/plain"},
			Reader:   io.NopCloser(strings.NewReader("download bytes")),
		},
	}
	handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))

	for _, route := range []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodGet, "/v1/files", http.StatusOK},
		{http.MethodGet, "/v1/files/file_meta", http.StatusOK},
		{http.MethodDelete, "/v1/files/file_delete", http.StatusOK},
		{http.MethodGet, "/v1/files/file_download/content", http.StatusOK},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			req := httptest.NewRequest(route.method, route.path, nil)
			setAuthHeader(req)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != route.want {
				t.Fatalf("status = %d; want %d body=%q", rec.Code, route.want, rec.Body.String())
			}
		})
	}
	if store.listCalls != 1 || store.getCalls != 1 || store.deleteCalls != 1 || store.openCalls != 1 {
		t.Fatalf("route calls = list:%d get:%d delete:%d open:%d; want all 1", store.listCalls, store.getCalls, store.deleteCalls, store.openCalls)
	}
}

func TestFileListResponseShapeAndQueryValidation(t *testing.T) {
	t.Run("empty real store shape", func(t *testing.T) {
		env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
		req := httptest.NewRequest(http.MethodGet, "/v1/files", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		env.router(t).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatalf("decode empty list response: %v", err)
		}
		if string(raw["data"]) != "[]" || string(raw["first_id"]) != "null" || string(raw["last_id"]) != "null" || string(raw["has_more"]) != "false" {
			t.Fatalf("empty list response = %s; want data=[], first_id=null, last_id=null, has_more=false", rec.Body.String())
		}
	})

	t.Run("shape and options", func(t *testing.T) {
		firstID := "file_first"
		lastID := "file_last"
		store := &recordingFileService{
			listResult: files.ListResult{
				Data: []*files.FileMetadata{{
					ID: "file_first", Type: "file", Filename: "first.txt", MIMEType: "text/plain", SizeBytes: 5,
					CreatedAt: time.Unix(1, 0).UTC(),
					Scope:     &files.FileScope{Type: "session", ID: "sesn_123"},
				}},
				FirstID: &firstID,
				LastID:  &lastID,
				HasMore: true,
			},
		}
		handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
		router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=1000&after_id=file_after&before_id=file_before&scope_id=sesn_123", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatalf("decode list response: %v", err)
		}
		for _, key := range []string{"data", "first_id", "last_id", "has_more"} {
			if _, ok := raw[key]; !ok {
				t.Fatalf("list response missing %q in %s", key, rec.Body.String())
			}
		}
		if len(raw) != 4 {
			t.Fatalf("list response keys = %v; want exact Files list envelope", raw)
		}
		if store.lastOptions.Limit != 1000 || store.lastOptions.AfterID != "file_after" || store.lastOptions.BeforeID != "file_before" || store.lastOptions.ScopeID != "sesn_123" {
			t.Fatalf("list options = %+v", store.lastOptions)
		}
		var decoded files.ListResult
		if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode Files list result: %v", err)
		}
		if decoded.Data[0].Scope == nil || decoded.Data[0].Scope.Type != "session" || decoded.Data[0].Scope.ID != "sesn_123" {
			t.Fatalf("scope metadata = %+v", decoded.Data[0].Scope)
		}
	})

	tests := []string{
		"limit=0",
		"limit=-1",
		"limit=1001",
		"limit=abc",
		"after_id=wrong",
		"after_id=" + strings.Repeat("a", 257),
		"before_id=wrong",
		"before_id=" + strings.Repeat("a", 257),
		"scope_id=",
		"scope_id=bad%2Fscope",
		"scope_id=" + strings.Repeat("a", 257),
	}
	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			store := &recordingFileService{}
			handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
			router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
			req := httptest.NewRequest(http.MethodGet, "/v1/files?"+query, nil)
			setAuthHeader(req)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
			if store.listCalls != 0 {
				t.Fatalf("store ListFiles reached for invalid query %q", query)
			}
		})
	}
}

func TestFileHTTPContentDeleteAndPostDeleteBehavior(t *testing.T) {
	env := newFileHandlerEnv(t, httpapi.FileHandlerLimits{})
	router := env.router(t)
	seedHTTPFileSession(t, env.admin, workspace.DefaultID, "sesn_http")
	seedHTTPFile(t, env, workspace.DefaultID, "file_http_original", "fobj_http_original", false, "", "", "original bytes")
	seedHTTPFile(t, env, workspace.DefaultID, "file_http_download", "fobj_http_download", true, "session", "sesn_http", "download bytes")
	seedHTTPDeletedFile(t, env.admin, workspace.DefaultID, "file_http_deleted", "fobj_http_deleted")
	workspaceB := workspace.ID("workspace_http_files_b")
	seedHTTPWorkspace(t, env.admin, workspaceB)
	seedHTTPFile(t, env, workspaceB, "file_http_cross", "fobj_http_cross", true, "", "", "cross bytes")

	t.Run("content downloadable streams exact bytes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files/file_http_download/content", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Content-Type") != "text/plain" {
			t.Fatalf("Content-Type = %q; want text/plain", rec.Header().Get("Content-Type"))
		}
		if rec.Body.String() != "download bytes" {
			t.Fatalf("content = %q; want exact bytes", rec.Body.String())
		}
	})

	t.Run("metadata includes scope", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files/file_http_download", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		got := decodeFileMetadata(t, rec)
		if got.Scope == nil || got.Scope.Type != "session" || got.Scope.ID != "sesn_http" {
			t.Fatalf("metadata scope = %+v", got.Scope)
		}
	})

	t.Run("scoped list includes scope", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?scope_id=sesn_http", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		var result files.ListResult
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		if len(result.Data) != 1 || result.Data[0].ID != "file_http_download" || result.Data[0].Scope == nil || result.Data[0].Scope.ID != "sesn_http" {
			t.Fatalf("scoped list = %+v", result)
		}
	})

	t.Run("list uses durable session visibility without sandbox state gate", func(t *testing.T) {
		seedHTTPFileSession(t, env.admin, workspace.DefaultID, "sesn_http_active_later")
		seedHTTPFileSession(t, env.admin, workspace.DefaultID, "sesn_http_deleted_later")
		if _, err := env.admin.ExecContext(context.Background(),
			`UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = $1 AND id = $2`,
			string(workspace.DefaultID), "sesn_http_deleted_later"); err != nil {
			t.Fatalf("mark session deleted: %v", err)
		}
		seedHTTPFile(t, env, workspace.DefaultID, "file_http_visible_later_a", "fobj_http_visible_later_a", false, "", "", "later a")
		seedHTTPFile(t, env, workspace.DefaultID, "file_http_session_active_later", "fobj_http_session_active_later", true, "session", "sesn_http_active_later", "active session")
		seedHTTPFile(t, env, workspace.DefaultID, "file_http_visible_later_b", "fobj_http_visible_later_b", false, "", "", "later b")
		seedHTTPFile(t, env, workspace.DefaultID, "file_http_session_deleted_later", "fobj_http_session_deleted_later", true, "session", "sesn_http_deleted_later", "deleted session")

		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=3&after_id=file_http_download", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		var result files.ListResult
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		got := fileMetadataIDs(result.Data)
		if strings.Join(got, ",") != "file_http_visible_later_a,file_http_session_active_later,file_http_visible_later_b" || result.HasMore {
			t.Fatalf("list ids=%v has_more=%v; want non-deleted session files without underfill", got, result.HasMore)
		}
	})

	t.Run("uploaded original content forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files/file_http_original/content", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		assertFileError(t, rec, http.StatusForbidden, "permission_error")
	})

	for _, id := range []string{"file_missing", "file_http_deleted", "file_http_cross"} {
		t.Run("content not found "+id, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/files/"+id+"/content", nil)
			setAuthHeader(req)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assertFileError(t, rec, http.StatusNotFound, "not_found_error")
			if strings.Contains(rec.Body.String(), "cross bytes") {
				t.Fatalf("not-found response exposed blob bytes: %s", rec.Body.String())
			}
		})
	}

	t.Run("delete original exact shape and post-delete not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/files/file_http_original", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("delete status = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatalf("decode delete response: %v", err)
		}
		if len(raw) != 2 {
			t.Fatalf("delete response keys = %v; want exact id/type", raw)
		}
		var deleted files.DeleteResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &deleted); err != nil {
			t.Fatalf("decode delete envelope: %v", err)
		}
		if deleted.ID != "file_http_original" || deleted.Type != "file_deleted" {
			t.Fatalf("delete response = %+v", deleted)
		}
		for _, path := range []string{"/v1/files/file_http_original", "/v1/files/file_http_original/content"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			setAuthHeader(req)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assertFileError(t, rec, http.StatusNotFound, "not_found_error")
		}
	})

	t.Run("delete session scoped file rejected and remains visible", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/files/file_http_download", nil)
		setAuthHeader(req)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		assertFileError(t, rec, http.StatusBadRequest, "invalid_request_error")
		req = httptest.NewRequest(http.MethodGet, "/v1/files/file_http_download", nil)
		setAuthHeader(req)
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("session scoped file metadata status after rejected delete = %d; want 200 body=%q", rec.Code, rec.Body.String())
		}
	})

	for _, id := range []string{"file_missing", "file_http_deleted", "file_http_cross"} {
		t.Run("delete not found "+id, func(t *testing.T) {
			beforeDeletes := env.blob.Deletes()
			req := httptest.NewRequest(http.MethodDelete, "/v1/files/"+id, nil)
			setAuthHeader(req)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assertFileError(t, rec, http.StatusNotFound, "not_found_error")
			afterDeletes := env.blob.Deletes()
			if len(afterDeletes) != len(beforeDeletes) {
				t.Fatalf("delete not-found called BlobStore delete: before=%v after=%v", beforeDeletes, afterDeletes)
			}
		})
	}
}

func TestFileHTTPPublicErrorsDoNotLeakStorageDetails(t *testing.T) {
	leakyErr := errors.New("blob_key=files/default/fobj_secret bucket=tetral-private endpoint=s3.secret.local temp=/tmp/upload-secret INSERT INTO files raw uploaded bytes")
	store := &recordingFileService{err: leakyErr}
	handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
	req := httptest.NewRequest(http.MethodGet, "/v1/files/file_leaky", nil)
	setAuthHeader(req)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	assertFileError(t, rec, http.StatusInternalServerError, "api_error")
	for _, forbidden := range []string{"files/default/fobj_secret", "tetral-private", "s3.secret.local", "/tmp/upload-secret", "INSERT INTO files", "raw uploaded bytes"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("public error leaked %q in %s", forbidden, rec.Body.String())
		}
	}
}

func TestFileHandlerSourceGuard(t *testing.T) {
	body, err := os.ReadFile("file_handler.go")
	if err != nil {
		t.Fatalf("read file_handler.go: %v", err)
	}
	for _, forbidden := range []string{"ParseMultipartForm", "FormFile", "MultipartForm", "io.ReadAll"} {
		if bytes.Contains(body, []byte(forbidden)) {
			t.Fatalf("file_handler.go must not contain %q", forbidden)
		}
	}
}

func TestFileErrorsMapThroughCentralEnvelope(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantType   string
	}{
		{name: "validation", err: &files.ValidationError{Message: "bad file request"}, wantStatus: http.StatusBadRequest, wantType: "invalid_request_error"},
		{name: "too large", err: &files.RequestTooLargeError{Message: "file too large"}, wantStatus: http.StatusRequestEntityTooLarge, wantType: "request_too_large"},
		{name: "not found", err: &files.NotFoundError{Message: "file not found"}, wantStatus: http.StatusNotFound, wantType: "not_found_error"},
		{name: "permission", err: &files.PermissionError{Message: "file content is not downloadable"}, wantStatus: http.StatusForbidden, wantType: "permission_error"},
		{name: "conflict", err: &files.ConflictError{Message: "file object conflict"}, wantStatus: http.StatusConflict, wantType: "conflict_error"},
		{name: "count quota", err: &files.QuotaError{Kind: files.QuotaKindCount, Message: "file identity quota exceeded"}, wantStatus: http.StatusBadRequest, wantType: "invalid_request_error"},
		{name: "retained byte quota", err: &files.QuotaError{Kind: files.QuotaKindRetainedBytes, Message: "retained bytes quota exceeded"}, wantStatus: http.StatusRequestEntityTooLarge, wantType: "request_too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &recordingFileService{err: tt.err}
			handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
			router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
			req := httptest.NewRequest(http.MethodGet, "/v1/files/file_test", nil)
			setAuthHeader(req)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assertFileError(t, rec, tt.wantStatus, tt.wantType)
		})
	}
}

func TestFileHandlerClosesContentStreamOnCopyFailure(t *testing.T) {
	store := &recordingFileService{openResult: &files.ContentStream{
		Metadata: &files.FileMetadata{ID: "file_stream", Type: "file", Filename: "stream.bin", MIMEType: "application/octet-stream"},
		Reader:   &recordingReadCloser{reader: strings.NewReader("bytes")},
	}}
	handler := httpapi.NewFileHandler(store, t.TempDir(), httpapi.FileHandlerLimits{})
	router := newAuthenticatedRouter(t, newTestHandler(t), httpapi.WithFileHandler(handler))
	req := httptest.NewRequest(http.MethodGet, "/v1/files/file_stream/content", nil)
	setAuthHeader(req)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%q", rec.Code, rec.Body.String())
	}
	reader := store.openResult.Reader.(*recordingReadCloser)
	if !reader.closed {
		t.Fatal("content reader was not closed")
	}
}

type recordingReadCloser struct {
	reader io.Reader
	closed bool
}

func (r *recordingReadCloser) Read(p []byte) (int, error) {
	if r.reader == nil {
		return 0, errors.New("missing reader")
	}
	return r.reader.Read(p)
}

func (r *recordingReadCloser) Close() error {
	r.closed = true
	return nil
}

func seedHTTPWorkspace(t *testing.T, db *sql.DB, workspaceID workspace.ID) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`, string(workspaceID)); err != nil {
		t.Fatalf("seed workspace %s: %v", workspaceID, err)
	}
}

func seedHTTPFileSession(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) {
	t.Helper()
	seedHTTPWorkspace(t, db, workspaceID)
	agentID := "agent_" + sessionID
	environmentID := "env_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $2, 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, '{}', $4, '2026-01-01T00:00:00Z')
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		string(workspaceID), "agv_"+sessionID, agentID, "hash_"+sessionID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $2, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID), environmentID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (workspace_id, id, type, metadata_json, status, lifecycle_state, agent_id, agent_version, environment_id, vault_ids_json, created_at, updated_at)
		 VALUES ($1, $2, 'session', '{}', 'idle', 'active', $3, 1, $4, '[]', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		string(workspaceID), sessionID, agentID, environmentID); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func seedHTTPFile(t *testing.T, env *fileHandlerEnv, workspaceID workspace.ID, fileID, objectID string, downloadable bool, scopeType, scopeID, body string) {
	t.Helper()
	seedHTTPWorkspace(t, env.admin, workspaceID)
	key := "files/" + string(workspaceID) + "/" + objectID
	if _, err := env.admin.ExecContext(context.Background(),
		`INSERT INTO file_objects (object_id, workspace_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, $4, 'sha', '2026-01-01T00:00:00Z')`,
		objectID, string(workspaceID), key, int64(len(body))); err != nil {
		t.Fatalf("seed file object %s: %v", objectID, err)
	}
	if err := env.blob.Put(context.Background(), key, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("seed blob %s: %v", key, err)
	}
	if scopeType == "" && scopeID == "" {
		if _, err := env.admin.ExecContext(context.Background(),
			`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at)
			 VALUES ($1, $2, $3, $4, 'text/plain', $5, '2026-01-01T00:00:00Z')`,
			fileID, string(workspaceID), objectID, fileID+".txt", downloadable); err != nil {
			t.Fatalf("seed file %s: %v", fileID, err)
		}
		return
	}
	if _, err := env.admin.ExecContext(context.Background(),
		`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at)
		 VALUES ($1, $2, $3, $4, 'text/plain', $5, $6, $7, '2026-01-01T00:00:00Z')`,
		fileID, string(workspaceID), objectID, fileID+".txt", downloadable, scopeType, scopeID); err != nil {
		t.Fatalf("seed file %s: %v", fileID, err)
	}
}

func fileMetadataIDs(items []*files.FileMetadata) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func seedHTTPDeletedFile(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID, objectID string) {
	t.Helper()
	key := "files/" + string(workspaceID) + "/" + objectID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (object_id, workspace_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, 1, 'sha', '2026-01-01T00:00:00Z')`,
		objectID, string(workspaceID), key); err != nil {
		t.Fatalf("seed deleted object %s: %v", objectID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (file_id, workspace_id, object_id, filename, mime_type, downloadable, created_at, deleted_at)
		 VALUES ($1, $2, $3, $4, 'text/plain', false, '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z')`,
		fileID, string(workspaceID), objectID, fileID+".txt"); err != nil {
		t.Fatalf("seed deleted file %s: %v", fileID, err)
	}
}
