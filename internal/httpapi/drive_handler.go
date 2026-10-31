package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	driveBodyByteCap = 1 << 20
	// driveUploadByteCap bounds an upload's whole multipart body: one file of
	// drive.MaxFileBytes plus its other parts.
	driveUploadByteCap     = 501_000_000
	driveDefaultPageLimit  = 20
	driveMaxPageLimit      = 100
	driveMaxChangeLimit    = 1000
	driveMaxInstructions   = 4096
	driveMaxMetadataKeys   = 16
	driveMaxMetadataKey    = 64
	driveMaxMetadataValue  = 512
	driveMaxCapBytes       = drive.WorkspaceMaxBytes
	driveMaxCapFiles       = drive.WorkspaceMaxFiles
	driveChangeEvent       = "drive.changed"
	driveDefaultSkillsPath = "skills/"
	driveDefaultMemoryPath = "memory/"
)

// DriveHandler serves the developer drive API on internal/drive.
type DriveHandler struct {
	drives  *drive.Service
	budget  *drive.UploadBudget
	changes *DriveChanges
	pageKey []byte
}

// NewDriveHandler signs list page tokens with pageKey.
func NewDriveHandler(drives *drive.Service, budget *drive.UploadBudget, changes *DriveChanges, pageKey []byte) *DriveHandler {
	return &DriveHandler{drives: drives, budget: budget, changes: changes, pageKey: pageKey}
}

// drivePrincipal returns the request's Workspace and the writer its
// credential records: the API key, or the human or service identity of a
// direct token.
func drivePrincipal(r *http.Request) (workspace.ID, drive.Writer, error) {
	principal, ok := auth.PrincipalFromContext(r.Context())
	if !ok {
		return "", drive.Writer{}, &auth.AuthenticationError{Message: "missing authenticated principal"}
	}
	if err := principal.Validate(); err != nil {
		return "", drive.Writer{}, err
	}
	ws, err := authorizeWorkspace(r.Context())
	if err != nil {
		return "", drive.Writer{}, err
	}
	switch {
	case principal.Credential.Kind == auth.CredentialAPIKey:
		return ws, drive.Writer{Kind: drive.WriterAPIKey, ID: principal.APIKeyID}, nil
	case principal.Identity.Kind == auth.IdentityHuman:
		return ws, drive.Writer{Kind: drive.WriterUser, ID: principal.Identity.ID}, nil
	default:
		return ws, drive.Writer{Kind: drive.WriterServiceAccount, ID: principal.Identity.ID}, nil
	}
}

// Response objects.

type driveObject struct {
	ID          string            `json:"id"`
	SkillsPath  string            `json:"skills_path"`
	Memory      driveMemoryObject `json:"memory"`
	Limits      driveLimitsObject `json:"limits"`
	Usage       driveUsageObject  `json:"usage"`
	StoredBytes *int64            `json:"stored_bytes"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   string            `json:"created_at"`
}

type driveMemoryObject struct {
	Path         string  `json:"path"`
	Instructions *string `json:"instructions"`
}

type driveLimitsObject struct {
	MaxBytes *int64 `json:"max_bytes"`
	MaxFiles *int64 `json:"max_files"`
}

type driveUsageObject struct {
	Bytes int64 `json:"bytes"`
	Files int64 `json:"files"`
}

func newDriveObject(d drive.Drive) driveObject {
	return driveObject{
		ID:          d.ID,
		SkillsPath:  d.SkillsPath,
		Memory:      driveMemoryObject{Path: d.MemoryPath, Instructions: d.MemoryInstructions},
		Limits:      driveLimitsObject{MaxBytes: d.MaxBytes, MaxFiles: d.MaxFiles},
		Usage:       driveUsageObject{Bytes: d.UsageBytes, Files: d.UsageFiles},
		StoredBytes: d.StoredBytes,
		Metadata:    d.Metadata,
		CreatedAt:   driveTime(d.CreatedAt),
	}
}

type driveFileObject struct {
	Path          string `json:"path"`
	SizeBytes     int64  `json:"size_bytes"`
	ContentSHA256 string `json:"content_sha256"`
	VersionID     string `json:"version_id"`
	Executable    bool   `json:"executable"`
}

type driveVersionObject struct {
	VersionID     string        `json:"version_id"`
	Path          string        `json:"path"`
	Operation     string        `json:"operation"`
	SizeBytes     int64         `json:"size_bytes"`
	ContentSHA256 *string       `json:"content_sha256"`
	Executable    bool          `json:"executable"`
	Writer        drive.Writer  `json:"writer"`
	CreatedAt     string        `json:"created_at"`
	Redacted      bool          `json:"redacted"`
	RedactedBy    *drive.Writer `json:"redacted_by"`
}

func newDriveVersionObject(v drive.Version) driveVersionObject {
	return driveVersionObject{
		VersionID:     v.VersionID,
		Path:          v.Path,
		Operation:     string(v.Operation),
		SizeBytes:     v.SizeBytes,
		ContentSHA256: v.ContentSHA256,
		Executable:    v.Executable,
		Writer:        v.Writer,
		CreatedAt:     driveTime(v.CreatedAt),
		Redacted:      v.RedactedAt != nil,
		RedactedBy:    v.RedactedBy,
	}
}

type driveChangeObject struct {
	Seq       int64        `json:"seq"`
	Path      string       `json:"path"`
	Operation string       `json:"operation"`
	VersionID string       `json:"version_id"`
	Writer    drive.Writer `json:"writer"`
	CreatedAt string       `json:"created_at"`
}

type drivePage[T any] struct {
	Data     []T     `json:"data"`
	NextPage *string `json:"next_page"`
}

func driveTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// Drives.

func (h *DriveHandler) createDrive(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	fields, err := readDriveFields(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	settings := drive.Settings{SkillsPath: driveDefaultSkillsPath, MemoryPath: driveDefaultMemoryPath}
	update, err := decodeDriveSettings(fields, false)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if update.SkillsPath != nil {
		settings.SkillsPath = *update.SkillsPath
	}
	if update.Memory != nil {
		settings.MemoryPath, settings.MemoryInstructions = update.Memory.Path, update.Memory.Instructions
	}
	if update.Limits != nil {
		settings.MaxBytes, settings.MaxFiles = update.Limits.MaxBytes, update.Limits.MaxFiles
	}
	settings.Metadata = map[string]string{}
	for key, value := range update.Metadata {
		settings.Metadata[key] = *value
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	created, err := h.drives.Create(r.Context(), ws, settings)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, newDriveObject(created))
}

func (h *DriveHandler) getDrive(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	found, err := h.drives.Get(r.Context(), ws, chi.URLParam(r, "drive_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", found.ID) {
		return
	}
	writeJSON(w, http.StatusOK, newDriveObject(found))
}

func (h *DriveHandler) listDrives(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"limit", "page"}); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	binding := drivePageBinding{WorkspaceID: string(ws), Route: "drives.list"}
	limit, after, err := h.parsePage(r, binding)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var cursor *drive.DriveCursor
	if after != "" {
		at, id, ok := strings.Cut(after, " ")
		parsed, parseErr := time.Parse(time.RFC3339Nano, at)
		if !ok || parseErr != nil {
			writeError(w, r, &ValidationError{Message: "page is not a page token of this list"})
			return
		}
		cursor = &drive.DriveCursor{CreatedAt: parsed, ID: id}
	}
	drives, more, err := h.drives.List(r.Context(), ws, cursor, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	page := drivePage[driveObject]{Data: make([]driveObject, 0, len(drives))}
	for _, d := range drives {
		page.Data = append(page.Data, newDriveObject(d))
	}
	if more {
		last := drives[len(drives)-1]
		page.NextPage = h.nextPage(binding, driveTime(last.CreatedAt)+" "+last.ID)
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *DriveHandler) updateDrive(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	fields, err := readDriveFields(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	update, err := decodeDriveSettings(fields, true)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	updated, err := h.drives.Update(r.Context(), ws, chi.URLParam(r, "drive_id"), update)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newDriveObject(updated))
}

func (h *DriveHandler) deleteDrive(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	if err := h.drives.Delete(r.Context(), ws, chi.URLParam(r, "drive_id")); err != nil {
		writeError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *DriveHandler) getUsage(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	bytes, files, err := h.drives.WorkspaceUsage(r.Context(), ws)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Bytes    int64 `json:"bytes"`
		Files    int64 `json:"files"`
		MaxBytes int64 `json:"max_bytes"`
		MaxFiles int64 `json:"max_files"`
	}{bytes, files, drive.WorkspaceMaxBytes, drive.WorkspaceMaxFiles})
}

// readDriveFields reads a JSON object body of at most driveBodyByteCap.
func readDriveFields(w http.ResponseWriter, r *http.Request) (map[string]json.RawMessage, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, driveBodyByteCap))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &RequestTooLargeError{Message: fmt.Sprintf("request body exceeds the limit of %d bytes", driveBodyByteCap)}
		}
		return nil, err
	}
	if err := rejectUnstorableJSONText(body); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		return nil, &ValidationError{Message: "request body must be a JSON object"}
	}
	return fields, nil
}

// decodeDriveSettings applies the field rules of drives.create and
// drives.update. memory and limits are replaced as a whole: a field they
// leave out takes its default. Only an update may set a metadata key to null,
// which deletes it.
func decodeDriveSettings(fields map[string]json.RawMessage, update bool) (drive.SettingsUpdate, error) {
	var settings drive.SettingsUpdate
	for name, raw := range fields {
		switch name {
		case "skills_path":
			path, err := decodeDriveFolder("skills_path", raw)
			if err != nil {
				return settings, err
			}
			settings.SkillsPath = &path
		case "memory":
			memory, err := decodeDriveMemory(raw)
			if err != nil {
				return settings, err
			}
			settings.Memory = &memory
		case "limits":
			limits, err := decodeDriveLimits(raw)
			if err != nil {
				return settings, err
			}
			settings.Limits = &limits
		case "metadata":
			metadata, err := decodeDriveMetadata(raw, update)
			if err != nil {
				return settings, err
			}
			settings.Metadata = metadata
		default:
			return settings, &ValidationError{Message: "unknown field " + name}
		}
	}
	return settings, nil
}

// decodeDriveFolder reads a folder path: without a trailing "/" it follows
// path rules 1 to 6, and it is stored with one.
func decodeDriveFolder(name string, raw json.RawMessage) (string, error) {
	var path string
	if err := json.Unmarshal(raw, &path); err != nil {
		return "", &ValidationError{Message: name + " must be a string"}
	}
	path = strings.TrimSuffix(path, "/")
	if rule := drive.CheckPath(path); rule != "" {
		return "", &ValidationError{Message: name + ": " + rule}
	}
	return path + "/", nil
}

func decodeDriveMemory(raw json.RawMessage) (drive.MemorySettings, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return drive.MemorySettings{}, &ValidationError{Message: "memory must be an object"}
	}
	memory := drive.MemorySettings{Path: driveDefaultMemoryPath}
	for name, value := range fields {
		switch name {
		case "path":
			path, err := decodeDriveFolder("memory.path", value)
			if err != nil {
				return memory, err
			}
			memory.Path = path
		case "instructions":
			if string(value) == "null" {
				continue
			}
			var instructions string
			if err := json.Unmarshal(value, &instructions); err != nil || utf8.RuneCountInString(instructions) > driveMaxInstructions {
				return memory, &ValidationError{Message: fmt.Sprintf("memory.instructions must be a string of at most %d characters, or null", driveMaxInstructions)}
			}
			memory.Instructions = &instructions
		default:
			return memory, &ValidationError{Message: "unknown field memory." + name}
		}
	}
	return memory, nil
}

func decodeDriveLimits(raw json.RawMessage) (drive.LimitSettings, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return drive.LimitSettings{}, &ValidationError{Message: "limits must be an object"}
	}
	var limits drive.LimitSettings
	for name, value := range fields {
		var bound int64
		var target **int64
		switch name {
		case "max_bytes":
			bound, target = driveMaxCapBytes, &limits.MaxBytes
		case "max_files":
			bound, target = driveMaxCapFiles, &limits.MaxFiles
		default:
			return limits, &ValidationError{Message: "unknown field limits." + name}
		}
		if string(value) == "null" {
			continue
		}
		cap, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil || cap < 1 || cap > bound {
			return limits, &ValidationError{Message: fmt.Sprintf("limits.%s must be an integer from 1 to %d, or null", name, bound)}
		}
		*target = &cap
	}
	return limits, nil
}

func decodeDriveMetadata(raw json.RawMessage, update bool) (map[string]*string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	if len(fields) > driveMaxMetadataKeys {
		return nil, &ValidationError{Message: fmt.Sprintf("metadata must have at most %d keys", driveMaxMetadataKeys)}
	}
	metadata := make(map[string]*string, len(fields))
	for key, value := range fields {
		if utf8.RuneCountInString(key) > driveMaxMetadataKey {
			return nil, &ValidationError{Message: fmt.Sprintf("metadata keys must be at most %d characters", driveMaxMetadataKey)}
		}
		if update && string(value) == "null" {
			metadata[key] = nil
			continue
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil || string(value) == "null" || utf8.RuneCountInString(text) > driveMaxMetadataValue {
			return nil, &ValidationError{Message: fmt.Sprintf("metadata values must be strings of at most %d characters", driveMaxMetadataValue)}
		}
		metadata[key] = &text
	}
	return metadata, nil
}

// Files.

// uploadFile streams the file part into the drive's content and writes it to
// path in one all_or_nothing commit. The budget is taken after the route's
// declared-length check and before any byte of the body is read.
func (h *DriveHandler) uploadFile(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, writer, err := drivePrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	if r.ContentLength > driveUploadByteCap {
		writeError(w, r, driveUploadTooLarge())
		return
	}
	if err := h.budget.Take(r.Context(), ws, r.ContentLength); err != nil {
		writeDriveBudgetError(w, r, err)
		return
	}
	upload, err := h.readUpload(w, r, ws, driveID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	executable := false
	current, err := h.drives.CurrentFile(r.Context(), ws, driveID, upload.path)
	switch {
	case err == nil:
		executable = current.Executable
	case !errors.Is(err, drive.ErrNotFound):
		writeError(w, r, err)
		return
	}
	result, err := h.drives.Commit(r.Context(), ws, driveID, writer, drive.ModeAllOrNothing, []drive.Change{
		{Path: upload.path, Base: upload.base, ContentSHA256: upload.content.SHA256, Executable: executable},
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if result.Results[0].NewVersion {
		status = http.StatusCreated
	}
	writeJSON(w, status, struct {
		VersionID     string `json:"version_id"`
		ContentSHA256 string `json:"content_sha256"`
	}{*result.Results[0].VersionID, upload.content.SHA256})
}

type driveUpload struct {
	path    string
	base    drive.Base
	content drive.Content
}

// readUpload reads the parts path, file and at most one of
// expected_content_sha256 and must_not_exist, each once, in any order.
func (h *DriveHandler) readUpload(w http.ResponseWriter, r *http.Request, ws workspace.ID, driveID string) (driveUpload, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		return driveUpload{}, &ValidationError{Message: "the body must be multipart/form-data"}
	}
	reader := multipart.NewReader(http.MaxBytesReader(w, r.Body, driveUploadByteCap), params["boundary"])
	upload := driveUpload{base: drive.Base{Kind: drive.BaseAny}}
	seen := map[string]bool{}
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return driveUpload{}, driveUploadReadError(err)
		}
		name := part.FormName()
		if seen[name] {
			return driveUpload{}, &ValidationError{Message: "multipart part " + name + " appears twice"}
		}
		seen[name] = true
		switch name {
		case "file":
			upload.content, err = h.drives.PutContent(r.Context(), ws, driveID, part, -1, "")
			if err != nil {
				return driveUpload{}, driveUploadReadError(err)
			}
		case "path", "expected_content_sha256", "must_not_exist":
			value, err := io.ReadAll(io.LimitReader(part, 4096))
			if err != nil {
				return driveUpload{}, driveUploadReadError(err)
			}
			switch name {
			case "path":
				upload.path = string(value)
			case "expected_content_sha256":
				if !driveSHA256(string(value)) {
					return driveUpload{}, &ValidationError{Message: "expected_content_sha256 must be 64 lowercase hexadecimal characters"}
				}
				upload.base = drive.Base{Kind: drive.BaseHash, SHA256: string(value)}
			case "must_not_exist":
				if string(value) != "true" {
					return driveUpload{}, &ValidationError{Message: "must_not_exist must be true"}
				}
				upload.base = drive.Base{Kind: drive.BaseMustNotExist}
			}
		default:
			return driveUpload{}, &ValidationError{Message: "unknown multipart part " + name}
		}
	}
	switch {
	case seen["expected_content_sha256"] && seen["must_not_exist"]:
		return driveUpload{}, &ValidationError{Message: "give at most one of expected_content_sha256 and must_not_exist"}
	case !seen["path"] || upload.path == "":
		return driveUpload{}, &ValidationError{Message: "multipart part path is required"}
	case !seen["file"]:
		return driveUpload{}, &ValidationError{Message: "multipart part file is required"}
	}
	return upload, nil
}

func driveUploadTooLarge() error {
	return &RequestTooLargeError{Message: fmt.Sprintf("request body exceeds the limit of %d bytes", driveUploadByteCap)}
}

func driveUploadReadError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return driveUploadTooLarge()
	}
	if errors.Is(err, drive.ErrContentTooLarge) || errors.Is(err, drive.ErrDriveNotFound) {
		return err
	}
	return &ValidationError{Message: "the multipart body could not be read"}
}

// writeDriveBudgetError answers a refused take with its Retry-After.
func writeDriveBudgetError(w http.ResponseWriter, r *http.Request, err error) {
	var refused *drive.UploadRefusedError
	if errors.As(err, &refused) {
		w.Header().Set("Retry-After", strconv.Itoa(refused.RetryAfterSeconds))
	}
	writeError(w, r, err)
}

func (h *DriveHandler) listFiles(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"limit", "page", "path_prefix"}); err != nil {
		writeError(w, r, err)
		return
	}
	prefix, err := drivePathPrefix(r.URL.Query().Get("path_prefix"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	binding := drivePageBinding{WorkspaceID: string(ws), Route: "drives.files.list", DriveID: driveID, Filter: prefix}
	limit, after, err := h.parsePage(r, binding)
	if err != nil {
		writeError(w, r, err)
		return
	}
	files, more, err := h.drives.ListFiles(r.Context(), ws, driveID, drive.FileQuery{After: after, Prefix: prefix, Limit: limit})
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	page := drivePage[driveFileObject]{Data: make([]driveFileObject, 0, len(files))}
	for _, f := range files {
		page.Data = append(page.Data, driveFileObject{Path: f.Path, SizeBytes: f.SizeBytes, ContentSHA256: f.ContentSHA256, VersionID: f.VersionID, Executable: f.Executable})
	}
	if more {
		page.NextPage = h.nextPage(binding, files[len(files)-1].Path)
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *DriveHandler) downloadFile(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"path"}); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, r, &ValidationError{Message: "path is required"})
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	file, err := h.drives.CurrentFile(r.Context(), ws, driveID, path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	body, content, err := h.drives.OpenContent(r.Context(), ws, driveID, file.ContentSHA256)
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = body.Close() }()
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	writeDriveContent(w, body, content.SizeBytes)
}

func writeDriveContent(w http.ResponseWriter, body io.Reader, size int64) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, body)
}

func (h *DriveHandler) deleteFile(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"path", "expected_content_sha256"}); err != nil {
		writeError(w, r, err)
		return
	}
	query := r.URL.Query()
	if query.Get("path") == "" {
		writeError(w, r, &ValidationError{Message: "path is required"})
		return
	}
	expected := query.Get("expected_content_sha256")
	if !driveSHA256(expected) {
		writeError(w, r, &ValidationError{Message: "expected_content_sha256 must be 64 lowercase hexadecimal characters"})
		return
	}
	ws, writer, err := drivePrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.drives.Commit(r.Context(), ws, chi.URLParam(r, "drive_id"), writer, drive.ModeAllOrNothing, []drive.Change{
		{Path: query.Get("path"), Delete: true, Base: drive.Base{Kind: drive.BaseHash, SHA256: expected}},
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	if result.Results[0].VersionID == nil {
		writeError(w, r, &NotFoundError{Message: "file not found"})
		return
	}
	writeJSON(w, http.StatusOK, struct {
		VersionID string `json:"version_id"`
	}{*result.Results[0].VersionID})
}

// Versions.

func (h *DriveHandler) listVersions(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"limit", "page", "path"}); err != nil {
		writeError(w, r, err)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, r, &ValidationError{Message: "path is required"})
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	binding := drivePageBinding{WorkspaceID: string(ws), Route: "drives.files.versions.list", DriveID: driveID, Filter: path}
	limit, after, err := h.parsePage(r, binding)
	if err != nil {
		writeError(w, r, err)
		return
	}
	afterSeq := int64(-1)
	if after != "" {
		var ok bool
		if afterSeq, ok = parseDriveCount(after); !ok {
			writeError(w, r, &ValidationError{Message: "page is not a page token of this list"})
			return
		}
	}
	versions, err := h.drives.ListVersions(r.Context(), ws, driveID, path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	// A path has one version per seq, and its versions' seq order is their
	// created_at order.
	page := drivePage[driveVersionObject]{Data: []driveVersionObject{}}
	var last int64
	for _, v := range versions {
		if v.Seq <= afterSeq {
			continue
		}
		if len(page.Data) == limit {
			page.NextPage = h.nextPage(binding, strconv.FormatInt(last, 10))
			break
		}
		page.Data = append(page.Data, newDriveVersionObject(v))
		last = v.Seq
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *DriveHandler) getVersion(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	version, err := h.drives.GetVersion(r.Context(), ws, chi.URLParam(r, "drive_id"), chi.URLParam(r, "version_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive_version", version.VersionID) {
		return
	}
	writeJSON(w, http.StatusOK, newDriveVersionObject(version))
}

func (h *DriveHandler) getVersionContent(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	body, version, err := h.drives.OpenVersionContent(r.Context(), ws, chi.URLParam(r, "drive_id"), chi.URLParam(r, "version_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	defer func() { _ = body.Close() }()
	if !authorizeReadResource(w, r, ws, "drive_version", version.VersionID) {
		return
	}
	writeDriveContent(w, body, version.SizeBytes)
}

func (h *DriveHandler) redactVersion(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, writer, err := drivePrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	redacted, err := h.drives.Redact(r.Context(), ws, chi.URLParam(r, "drive_id"), chi.URLParam(r, "version_id"), writer)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newDriveVersionObject(redacted))
}

// Changes and restore.

func (h *DriveHandler) listChanges(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, []string{"after", "session_id", "limit"}); err != nil {
		writeError(w, r, err)
		return
	}
	query := r.URL.Query()
	changes := drive.ChangeQuery{Limit: driveDefaultPageLimit, SessionID: query.Get("session_id")}
	if raw, ok := query["after"]; ok {
		after, ok := parseDriveCount(raw[0])
		if !ok {
			writeError(w, r, &ValidationError{Message: "after must be an integer of at least 0"})
			return
		}
		changes.AfterSeq = &after
	}
	if raw, ok := query["limit"]; ok {
		limit, ok := parseDriveCount(raw[0])
		if !ok || limit < 1 || limit > driveMaxChangeLimit {
			writeError(w, r, &ValidationError{Message: fmt.Sprintf("limit must be an integer from 1 to %d", driveMaxChangeLimit)})
			return
		}
		changes.Limit = int(limit)
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	records, more, err := h.drives.ListChanges(r.Context(), ws, driveID, changes)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	page := struct {
		Data    []driveChangeObject `json:"data"`
		HasMore bool                `json:"has_more"`
	}{Data: make([]driveChangeObject, 0, len(records)), HasMore: more}
	for _, c := range records {
		page.Data = append(page.Data, driveChangeObject{Seq: c.Seq, Path: c.Path, Operation: string(c.Operation), VersionID: c.VersionID, Writer: c.Writer, CreatedAt: driveTime(c.CreatedAt)})
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *DriveHandler) restore(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	fields, err := readDriveFields(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var prefix string
	var asOf time.Time
	for name, raw := range fields {
		switch name {
		case "path_prefix":
			if err := json.Unmarshal(raw, &prefix); err != nil {
				writeError(w, r, &ValidationError{Message: "path_prefix must be a string"})
				return
			}
			if prefix, err = drivePathPrefix(prefix); err != nil {
				writeError(w, r, err)
				return
			}
		case "as_of":
			var text string
			if json.Unmarshal(raw, &text) == nil {
				asOf, err = time.Parse(time.RFC3339Nano, text)
			}
			if text == "" || err != nil {
				writeError(w, r, &ValidationError{Message: "as_of must be an RFC 3339 time"})
				return
			}
		default:
			writeError(w, r, &ValidationError{Message: "unknown field " + name})
			return
		}
	}
	if asOf.IsZero() {
		writeError(w, r, &ValidationError{Message: "as_of is required"})
		return
	}
	ws, writer, err := drivePrincipal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.drives.Restore(r.Context(), ws, chi.URLParam(r, "drive_id"), prefix, asOf, writer)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Seq           *int64   `json:"seq"`
		Restored      int      `json:"restored"`
		RedactedPaths []string `json:"redacted_paths"`
		RemovedPaths  []string `json:"removed_paths"`
	}{result.Seq, result.Restored, result.RedactedPaths, result.RemovedPaths})
}

// streamChanges answers drives.changes.stream: the drive's last_seq when it
// is above 0, then a hint per commit notice, until the client leaves or the
// pod shuts down.
func (h *DriveHandler) streamChanges(w http.ResponseWriter, r *http.Request) {
	if err := requireDriveQuery(r, nil); err != nil {
		writeError(w, r, err)
		return
	}
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	driveID := chi.URLParam(r, "drive_id")
	subscriber := h.changes.subscribe(driveKey{ws, driveID})
	if subscriber == nil {
		writeError(w, r, &UnavailableError{Message: "the change stream is shutting down"})
		return
	}
	defer h.changes.unsubscribe(driveKey{ws, driveID}, subscriber)
	// Subscribing first means a commit after this read is noticed.
	lastSeq, err := h.drives.LastSeq(r.Context(), ws, driveID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !authorizeReadResource(w, r, ws, "drive", driveID) {
		return
	}
	subscriber.offer(lastSeq)
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-subscriber.done:
			return
		case <-subscriber.wake:
			seq := subscriber.take()
			if seq == 0 {
				continue
			}
			data, _ := json.Marshal(struct {
				DriveID string `json:"drive_id"`
				Seq     int64  `json:"seq"`
			}{driveID, seq})
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", driveChangeEvent, data); err != nil {
				return
			}
			if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return
			}
		}
	}
}

// DriveChanges fans the commit notices of drive.NoticeChannel out to this
// pod's drives.changes.stream subscribers. A subscriber holds only the highest
// seq not yet written, so the notices a slow client has not received merge
// into one.
type DriveChanges struct {
	database *dbconnect.Client
	drives   *drive.Service
	logger   *slog.Logger

	mu          sync.Mutex
	closed      bool
	subscribers map[driveKey]map[*driveSubscriber]struct{}
}

type driveKey struct {
	ws      workspace.ID
	driveID string
}

type driveSubscriber struct {
	mu   sync.Mutex
	seq  int64
	wake chan struct{}
	done chan struct{}
}

// offer records seq, when above what is pending, and wakes the writer.
func (s *driveSubscriber) offer(seq int64) {
	if seq <= 0 {
		return
	}
	s.mu.Lock()
	s.seq = max(s.seq, seq)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// take returns the pending seq, or 0, and clears it.
func (s *driveSubscriber) take() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.seq
	s.seq = 0
	return seq
}

// NewDriveChanges reads each drive's last_seq through drives after a LISTEN
// (re)connect; Run receives the notices on database.
func NewDriveChanges(database *dbconnect.Client, drives *drive.Service, logger *slog.Logger) *DriveChanges {
	if logger == nil {
		logger = workload.ComponentLogger("api")
	}
	return &DriveChanges{database: database, drives: drives, logger: logger, subscribers: map[driveKey]map[*driveSubscriber]struct{}{}}
}

// Run holds one LISTEN connection on drive.NoticeChannel, reconnecting after
// a failure, until ctx ends; then it ends every subscriber's stream. After
// each (re)connect every subscriber gets its drive's last_seq, since notices
// may have been missed.
func (c *DriveChanges) Run(ctx context.Context) error {
	defer c.closeAll()
	listener := queue.PostgreSQLNotificationListener{Client: c.database, Operation: "api.drive_changes"}
	return queue.RunListener(ctx, listener, drive.NoticeChannel, func() { c.refresh(ctx) }, c.deliver, func(error) {
		c.logger.WarnContext(ctx, "drive_changes.listener_disconnected",
			slog.String("operation", "api.drive_changes"),
			slog.String("error.class", "dependency_unavailable"),
			slog.String("error.code", "notification_listener_disconnected"),
			slog.String("error.message_safe", "drive change listener disconnected"))
	})
}

func (c *DriveChanges) subscribe(key driveKey) *driveSubscriber {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	subscriber := &driveSubscriber{wake: make(chan struct{}, 1), done: make(chan struct{})}
	if c.subscribers[key] == nil {
		c.subscribers[key] = map[*driveSubscriber]struct{}{}
	}
	c.subscribers[key][subscriber] = struct{}{}
	return subscriber
}

func (c *DriveChanges) unsubscribe(key driveKey, subscriber *driveSubscriber) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropLocked(key, subscriber)
}

func (c *DriveChanges) dropLocked(key driveKey, subscriber *driveSubscriber) {
	if _, ok := c.subscribers[key][subscriber]; !ok {
		return
	}
	delete(c.subscribers[key], subscriber)
	if len(c.subscribers[key]) == 0 {
		delete(c.subscribers, key)
	}
	close(subscriber.done)
}

func (c *DriveChanges) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for key, subscribers := range c.subscribers {
		for subscriber := range subscribers {
			c.dropLocked(key, subscriber)
		}
	}
}

// deliver offers a notice's seq to the subscribers of its drive; a malformed
// notice is skipped.
func (c *DriveChanges) deliver(payload string) {
	var notice struct {
		WorkspaceID string `json:"workspace_id"`
		DriveID     string `json:"drive_id"`
		Seq         int64  `json:"seq"`
	}
	if json.Unmarshal([]byte(payload), &notice) != nil {
		return
	}
	c.offer(driveKey{workspace.ID(notice.WorkspaceID), notice.DriveID}, notice.Seq)
}

func (c *DriveChanges) offer(key driveKey, seq int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for subscriber := range c.subscribers[key] {
		subscriber.offer(seq)
	}
}

// refresh offers every subscribed drive's last_seq.
func (c *DriveChanges) refresh(ctx context.Context) {
	c.mu.Lock()
	keys := make([]driveKey, 0, len(c.subscribers))
	for key := range c.subscribers {
		keys = append(keys, key)
	}
	c.mu.Unlock()
	for _, key := range keys {
		if seq, err := c.drives.LastSeq(ctx, key.ws, key.driveID); err == nil {
			c.offer(key, seq)
		}
	}
}

// Query, paths and page tokens.

// requireDriveQuery admits only the allowed parameters and beta, each once.
func requireDriveQuery(r *http.Request, allowed []string) error {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return &ValidationError{Message: "invalid query string"}
	}
	for key, given := range values {
		if len(given) != 1 {
			return &ValidationError{Message: "query parameter " + key + " must be given at most once"}
		}
		if key != "beta" && !containsString(allowed, key) {
			return &ValidationError{Message: "unknown query parameter " + key}
		}
	}
	return nil
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// drivePathPrefix checks a path prefix: without a trailing "/" it follows
// path rules 1 to 6. It matches every path that starts with its bytes.
func drivePathPrefix(prefix string) (string, error) {
	if prefix == "" {
		return "", nil
	}
	if rule := drive.CheckPath(strings.TrimSuffix(prefix, "/")); rule != "" {
		return "", &ValidationError{Message: "path_prefix: " + rule}
	}
	return prefix, nil
}

// parseDriveCount reads a decimal integer of at least 0, digits only.
func parseDriveCount(raw string) (int64, bool) {
	if raw == "" || strings.Trim(raw, "0123456789") != "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	return value, err == nil
}

func driveSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// drivePageBinding is what a page token is valid for: a token of one list,
// Workspace, drive or filter is refused by every other.
type drivePageBinding struct {
	WorkspaceID string `json:"workspace_id"`
	Route       string `json:"route"`
	DriveID     string `json:"drive_id"`
	Filter      string `json:"filter"`
}

type drivePageToken struct {
	After     string `json:"after"`
	Signature string `json:"signature"`
}

// parsePage reads limit (default 20, above 100 treated as 100) and page,
// returning the place after which the page starts, or "".
func (h *DriveHandler) parsePage(r *http.Request, binding drivePageBinding) (int, string, error) {
	query := r.URL.Query()
	limit := driveDefaultPageLimit
	if raw, ok := query["limit"]; ok {
		digits := strings.TrimLeft(raw[0], "0")
		if digits == "" || strings.Trim(digits, "0123456789") != "" {
			return 0, "", &ValidationError{Message: "limit must be a positive integer"}
		}
		limit = driveMaxPageLimit
		if parsed, err := strconv.Atoi(digits); err == nil && parsed < driveMaxPageLimit {
			limit = parsed
		}
	}
	raw := query.Get("page")
	if raw == "" {
		return limit, "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	var token drivePageToken
	if err != nil || json.Unmarshal(decoded, &token) != nil || !hmac.Equal([]byte(token.Signature), []byte(h.signPage(binding, token.After))) {
		return 0, "", &ValidationError{Message: "page is not a page token of this list"}
	}
	return limit, token.After, nil
}

func (h *DriveHandler) nextPage(binding drivePageBinding, after string) *string {
	raw, _ := json.Marshal(drivePageToken{After: after, Signature: h.signPage(binding, after)})
	token := base64.RawURLEncoding.EncodeToString(raw)
	return &token
}

func (h *DriveHandler) signPage(binding drivePageBinding, after string) string {
	encoded, _ := json.Marshal(binding)
	mac := hmac.New(sha256.New, h.pageKey)
	mac.Write(encoded)
	mac.Write([]byte{0})
	mac.Write([]byte(after))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
