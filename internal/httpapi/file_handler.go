package httpapi

import (
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// FileHandlerLimits carries HTTP-layer Files upload caps. Zero values
// select the production constants pinned by the Files domain.
type FileHandlerLimits struct {
	RouteMultipartBytes int64
	MaxFileBytes        int64
}

// FileService is the workspace-aware Files service surface consumed by
// the HTTP handler. Handler tests substitute fakes to pin routing,
// validation, and error mapping behavior.
type FileService interface {
	CreateUploadedFile(ctx context.Context, ws workspace.ID, staged *files.StagedUpload) (*files.FileMetadata, error)
	ListFiles(ctx context.Context, ws workspace.ID, options files.ListOptions) (files.ListResult, error)
	GetFile(ctx context.Context, ws workspace.ID, fileID string) (*files.FileMetadata, error)
	DeleteFile(ctx context.Context, ws workspace.ID, fileID string) (*files.DeleteResponse, error)
	OpenContent(ctx context.Context, ws workspace.ID, fileID string) (*files.ContentStream, error)
}

// FileHandler groups dependencies for the `/v1/files` route family.
type FileHandler struct {
	service  FileService
	stageDir string
	limits   FileHandlerLimits
}

// NewFileHandler constructs the Files route handler.
func NewFileHandler(service FileService, stageDir string, limits FileHandlerLimits) *FileHandler {
	return &FileHandler{service: service, stageDir: stageDir, limits: normalizeFileHandlerLimits(limits)}
}

func normalizeFileHandlerLimits(limits FileHandlerLimits) FileHandlerLimits {
	if limits.RouteMultipartBytes == 0 {
		limits.RouteMultipartBytes = files.RouteMultipartBytes
	}
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = files.MaxFileBytes
	}
	return limits
}

// createFile handles POST /v1/files. The request must contain exactly
// one file part named `file`; every other part is rejected so uploads
// stay unambiguous and stream directly to server-owned staging.
func (h *FileHandler) createFile(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	staged, err := h.parseUploadMultipart(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = staged.Cleanup() }()
	created, err := h.service.CreateUploadedFile(r.Context(), ws, staged)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, created)
}

func (h *FileHandler) parseUploadMultipart(w http.ResponseWriter, r *http.Request) (*files.StagedUpload, error) {
	r.Body = http.MaxBytesReader(w, r.Body, h.limits.RouteMultipartBytes)
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, &files.ValidationError{Message: "request must be multipart/form-data"}
	}
	var staged *files.StagedUpload
	cleanupOnError := func() {
		if staged != nil {
			_ = staged.Cleanup()
			staged = nil
		}
	}
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			cleanupOnError()
			if isMaxBytesError(err) {
				return nil, &files.RequestTooLargeError{Message: "upload exceeds the size cap"}
			}
			return nil, &files.ValidationError{Message: "multipart body is malformed"}
		}
		if part.FormName() != "file" {
			cleanupOnError()
			return nil, &files.ValidationError{Message: "request contains an unexpected form part"}
		}
		filename, ok := uploadMultipartFilename(part)
		if !ok {
			cleanupOnError()
			return nil, &files.ValidationError{Message: "file form field must be a file part"}
		}
		if staged != nil {
			cleanupOnError()
			return nil, &files.ValidationError{Message: "request contains multiple file form fields"}
		}
		if err := validateUploadFilename(filename); err != nil {
			cleanupOnError()
			return nil, err
		}
		mimeType, err := normalizeUploadedMIMEType(part.Header.Get("Content-Type"))
		if err != nil {
			cleanupOnError()
			return nil, err
		}
		staged, err = files.StageUpload(maxBytesMappingReader{reader: part}, h.stageDir, filename, mimeType, files.UploadLimits{MaxFileBytes: h.limits.MaxFileBytes})
		if err != nil {
			cleanupOnError()
			return nil, err
		}
	}
	if staged == nil {
		return nil, &files.ValidationError{Message: "request is missing the file form field"}
	}
	return staged, nil
}

func uploadMultipartFilename(part *multipart.Part) (string, bool) {
	_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil {
		return "", false
	}
	filename, ok := params["filename"]
	return filename, ok
}

type maxBytesMappingReader struct {
	reader io.Reader
}

func (r maxBytesMappingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && isMaxBytesError(err) {
		return n, &files.RequestTooLargeError{Message: "upload exceeds the size cap"}
	}
	return n, err
}

func validateUploadFilename(filename string) error {
	if filename == "" {
		return &files.ValidationError{Message: "filename is required"}
	}
	if !utf8.ValidString(filename) {
		return &files.ValidationError{Message: "filename must be valid UTF-8"}
	}
	if len(filename) > 1024 {
		return &files.ValidationError{Message: "filename exceeds 1024 bytes"}
	}
	if utf8.RuneCountInString(filename) > 255 {
		return &files.ValidationError{Message: "filename exceeds 255 characters"}
	}
	for _, r := range filename {
		if unicode.IsControl(r) {
			return &files.ValidationError{Message: "filename contains a control character"}
		}
	}
	return nil
}

func normalizeUploadedMIMEType(raw string) (string, error) {
	if raw == "" {
		return "application/octet-stream", nil
	}
	if len(raw) > 255 {
		return "", &files.ValidationError{Message: "file content type exceeds 255 bytes"}
	}
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil || mediaType == "" {
		return "", &files.ValidationError{Message: "file content type is malformed"}
	}
	return strings.ToLower(mediaType), nil
}

// listFiles handles GET /v1/files.
func (h *FileHandler) listFiles(w http.ResponseWriter, r *http.Request) {
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	options, err := parseFileListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListFiles(r.Context(), ws, options)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func parseFileListOptions(r *http.Request) (files.ListOptions, error) {
	query, err := requireBetaQuery(r, "limit", "after_id", "before_id", "scope_id")
	if err != nil {
		return files.ListOptions{}, err
	}
	limit := 20
	if values, ok := query["limit"]; ok {
		if len(values) != 1 || values[0] == "" {
			return files.ListOptions{}, &files.ValidationError{Message: "invalid file list limit"}
		}
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed < 1 || parsed > 1000 {
			return files.ListOptions{}, &files.ValidationError{Message: "invalid file list limit"}
		}
		limit = parsed
	}
	afterID := query.Get("after_id")
	if afterID != "" {
		if err := validateFileID(afterID); err != nil {
			return files.ListOptions{}, &files.ValidationError{Message: "invalid after_id cursor"}
		}
	}
	beforeID := query.Get("before_id")
	if beforeID != "" {
		if err := validateFileID(beforeID); err != nil {
			return files.ListOptions{}, &files.ValidationError{Message: "invalid before_id cursor"}
		}
	}
	scopeID := ""
	if values, ok := query["scope_id"]; ok {
		if len(values) != 1 || !validFileScopeID(values[0]) {
			return files.ListOptions{}, &files.ValidationError{Message: "invalid scope_id"}
		}
		scopeID = values[0]
	}
	return files.ListOptions{Limit: limit, AfterID: afterID, BeforeID: beforeID, ScopeID: scopeID}, nil
}

func validFileScopeID(scopeID string) bool {
	if scopeID == "" || len(scopeID) > 256 {
		return false
	}
	for _, r := range scopeID {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

// getFile handles GET /v1/files/{file_id}.
func (h *FileHandler) getFile(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	fileID, err := validatedFileIDFromRequest(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	got, err := h.service.GetFile(r.Context(), ws, fileID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, got)
}

// deleteFile handles DELETE /v1/files/{file_id}.
func (h *FileHandler) deleteFile(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	fileID, err := validatedFileIDFromRequest(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	deleted, err := h.service.DeleteFile(r.Context(), ws, fileID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, deleted)
}

// getFileContent handles GET /v1/files/{file_id}/content.
func (h *FileHandler) getFileContent(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	fileID, err := validatedFileIDFromRequest(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	stream, err := h.service.OpenContent(r.Context(), ws, fileID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = stream.Reader.Close() }()
	mimeType := "application/octet-stream"
	if stream.Metadata != nil && stream.Metadata.MIMEType != "" {
		mimeType = stream.Metadata.MIMEType
	}
	w.Header().Set("Content-Type", mimeType)
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, stream.Reader)
}

func validatedFileIDFromRequest(r *http.Request) (string, error) {
	fileID := chi.URLParam(r, "file_id")
	if err := validateFileID(fileID); err != nil {
		return "", err
	}
	return fileID, nil
}

func validateFileID(fileID string) error {
	if fileID == "" {
		return &files.ValidationError{Message: "file_id is required"}
	}
	if len(fileID) > 256 {
		return &files.ValidationError{Message: "file_id exceeds 256 bytes"}
	}
	if !strings.HasPrefix(fileID, files.IDPrefix) {
		return &files.ValidationError{Message: "file_id is invalid"}
	}
	if !utf8.ValidString(fileID) {
		return &files.ValidationError{Message: "file_id must be valid UTF-8"}
	}
	for _, r := range fileID {
		if unicode.IsControl(r) {
			return &files.ValidationError{Message: "file_id contains a control character"}
		}
	}
	return nil
}
