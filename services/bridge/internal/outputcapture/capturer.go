package outputcapture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/pathvalidation"
	"github.com/tetral-ai/tetral/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	OutputRoot = "/mnt/session/outputs"

	defaultMaxFiles      = 100
	defaultMaxFileBytes  = 10 * 1024 * 1024
	defaultMaxTotalBytes = 50 * 1024 * 1024
)

type Scanner interface {
	ScanOutputs(context.Context, SandboxOutputTarget) (SandboxOutputScan, error)
}

type SandboxOutputTarget struct {
	WorkspaceID       string
	SessionID         string
	SessionThreadID   string
	BindingID         string
	BindingGeneration int64
	SandboxID         string
	ProviderSandboxID string
	MaxFiles          int
	MaxFileBytes      int64
	MaxTotalBytes     int64
}

type SandboxOutputScan struct {
	Files                []SandboxOutputFile
	Truncated            bool
	UnrepresentableNames int
	Records              []ScanRecord
}

type SandboxOutputFile struct {
	SourcePath string
	Kind       string
	LinkCount  uint64
	SizeBytes  int64
	SHA256     string
	MIMEType   string
	Skipped    bool
	SkipReason string
	Open       func(context.Context) (io.ReadCloser, error)
}

type Request struct {
	WorkspaceID    string
	SessionID      string
	RuntimeWriteID string
	Target         SandboxOutputTarget
	CapturedAt     time.Time
}

type Result struct {
	Cleanup     func()
	Skipped     []SkippedFile
	ScanRecords []ScanRecord
}

// CaptureScanError marks a failure that happened before output persistence
// began. FinishIdle may record and downgrade only this type; database, quota,
// and object-store failures remain settlement failures.
type CaptureScanError struct {
	kind string
}

func (e *CaptureScanError) Error() string {
	return "output capture scan failed"
}

func (e *CaptureScanError) Kind() string {
	if e == nil {
		return ""
	}
	return e.kind
}

type SkippedFile struct {
	SourcePath string
	Reason     string
	SizeBytes  int64
}

type ScanRecord struct {
	ParentPath string
	Reason     string
	Count      int
}

type Limits struct {
	MaxFiles                      int
	MaxFileBytes                  int64
	MaxTotalBytes                 int64
	MaxRetainedBytesPerWorkspace  int64
	MaxFileIdentitiesPerWorkspace int
}

type Capturer struct {
	BlobStore blob.BlobStore
	Scanner   Scanner
	Limits    Limits
}

func NewCapturer(blobStore blob.BlobStore, scanner Scanner) *Capturer {
	return &Capturer{BlobStore: blobStore, Scanner: scanner}
}

func (c *Capturer) CaptureOutputs(ctx context.Context, tx *dbconnect.Tx, request Request) (Result, error) {
	if c == nil || c.BlobStore == nil || c.Scanner == nil {
		return Result{}, status.Error(codes.FailedPrecondition, "output capture is not installed")
	}
	if tx == nil {
		return Result{}, status.Error(codes.FailedPrecondition, "output capture transaction is required")
	}
	if request.WorkspaceID == "" || request.SessionID == "" || request.RuntimeWriteID == "" {
		return Result{}, status.Error(codes.InvalidArgument, "output capture request is incomplete")
	}
	capturedAt := request.CapturedAt.UTC()
	if capturedAt.IsZero() {
		capturedAt = storage.Now()
	}
	limits := normalizeLimits(c.Limits)
	target := request.Target
	target.MaxFiles = limits.MaxFiles
	target.MaxFileBytes = limits.MaxFileBytes
	target.MaxTotalBytes = limits.MaxTotalBytes
	scan, err := c.Scanner.ScanOutputs(ctx, target)
	if err != nil {
		return Result{}, &CaptureScanError{kind: "scan_outputs"}
	}
	entries, skipped, err := normalizeScan(scan, limits)
	if err != nil {
		return Result{}, &CaptureScanError{kind: "normalize_scan"}
	}
	if err := storage.AcquireWorkspaceFilesLock(ctx, tx, request.WorkspaceID); err != nil {
		return Result{}, err
	}
	existing, err := loadCaptureIndex(ctx, tx, request.WorkspaceID, request.SessionID)
	if err != nil {
		return Result{}, err
	}
	neededFiles, neededBytes := captureNeeds(entries, existing)
	if err := assertFileQuota(ctx, tx, request.WorkspaceID, limits, neededFiles, neededBytes); err != nil {
		return Result{}, err
	}

	uploaded := make([]string, 0, neededFiles)
	failed := true
	defer func() {
		if failed {
			c.cleanupUploaded(uploaded)()
		}
	}()

	for _, entry := range entries {
		row, ok := existing[entry.SourcePath]
		if ok && row.LastFileID.Valid && row.LastSizeBytes.Valid &&
			row.LastSizeBytes.Int64 == entry.SizeBytes && row.LastSHA256.Valid &&
			strings.EqualFold(row.LastSHA256.String, entry.SHA256) {
			if err := markCaptureSeenTx(ctx, tx, request.WorkspaceID, request.SessionID, entry.SourcePath, capturedAt); err != nil {
				return Result{}, err
			}
			continue
		}
		body, digest, err := readCaptureBody(ctx, entry, limits.MaxFileBytes)
		if err != nil {
			skipped = appendSkippedFile(skipped, entry.SourcePath, captureReadSkipReason(err), entry.SizeBytes)
			continue
		}
		if !strings.EqualFold(digest, entry.SHA256) {
			skipped = appendSkippedFile(skipped, entry.SourcePath, "changed_during_capture", entry.SizeBytes)
			continue
		}
		fileID := id.New(files.IDPrefix)
		objectID := id.New(files.ObjectIDPrefix)
		blobKey := blobKeyFor(request.WorkspaceID, objectID)
		now := capturedAt
		if _, err := tx.Exec(ctx,
			`INSERT INTO file_objects (object_id, workspace_id, blob_key, size_bytes, sha256, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			objectID, request.WorkspaceID, blobKey, entry.SizeBytes, entry.SHA256, now,
		); err != nil {
			return Result{}, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO files (
				file_id, workspace_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at
			) VALUES ($1, $2, $3, $4, $5, true, 'session', $6, $7)`,
			fileID, request.WorkspaceID, objectID, entry.Filename, entry.MIMEType, request.SessionID, now,
		); err != nil {
			return Result{}, err
		}
		if err := c.BlobStore.Put(ctx, blobKey, bytes.NewReader(body), entry.SizeBytes); err != nil {
			_ = c.BlobStore.Delete(context.WithoutCancel(ctx), blobKey)
			return Result{}, status.Error(codes.Internal, "output capture blob write failed")
		}
		uploaded = append(uploaded, blobKey)
		if err := upsertCaptureRowTx(ctx, tx, request.WorkspaceID, request.SessionID, entry.SourcePath, fileID, entry.SizeBytes, entry.SHA256, capturedAt); err != nil {
			return Result{}, err
		}
	}
	failed = false
	return Result{
		Cleanup:     c.cleanupUploaded(uploaded),
		Skipped:     skipped,
		ScanRecords: append([]ScanRecord(nil), scan.Records...),
	}, nil
}

type normalizedOutputFile struct {
	SourcePath string
	Filename   string
	SizeBytes  int64
	SHA256     string
	MIMEType   string
	Open       func(context.Context) (io.ReadCloser, error)
}

type captureRow struct {
	LastFileID    sql.NullString
	LastSizeBytes sql.NullInt64
	LastSHA256    sql.NullString
}

func normalizeLimits(limits Limits) Limits {
	if limits.MaxFiles == 0 {
		limits.MaxFiles = defaultMaxFiles
	}
	if limits.MaxFileBytes == 0 {
		limits.MaxFileBytes = defaultMaxFileBytes
	}
	if limits.MaxTotalBytes == 0 {
		limits.MaxTotalBytes = defaultMaxTotalBytes
	}
	if limits.MaxRetainedBytesPerWorkspace == 0 {
		limits.MaxRetainedBytesPerWorkspace = files.MaxRetainedBytesPerWorkspace
	}
	if limits.MaxFileIdentitiesPerWorkspace == 0 {
		limits.MaxFileIdentitiesPerWorkspace = files.MaxFileIdentitiesPerWorkspace
	}
	return limits
}

func normalizeScan(scan SandboxOutputScan, limits Limits) ([]normalizedOutputFile, []SkippedFile, error) {
	seen := make(map[string]struct{}, len(scan.Files))
	totalBytes := int64(0)
	entries := make([]normalizedOutputFile, 0, len(scan.Files))
	skipped := make([]SkippedFile, 0)
	for _, file := range scan.Files {
		sourcePath, err := canonicalOutputPathStructure(file.SourcePath)
		if err != nil {
			return nil, nil, err
		}
		if _, ok := seen[sourcePath]; ok {
			return nil, nil, status.Error(codes.FailedPrecondition, "duplicate sandbox output path")
		}
		seen[sourcePath] = struct{}{}
		if file.SizeBytes < 0 {
			return nil, nil, status.Error(codes.FailedPrecondition, "sandbox output size is invalid")
		}
		if file.Skipped != (strings.TrimSpace(file.SkipReason) != "") {
			return nil, nil, status.Error(codes.FailedPrecondition, "sandbox output skip disposition is inconsistent")
		}
		digest := strings.ToLower(strings.TrimSpace(file.SHA256))
		if digest != "" && !validSHA256Hex(digest) {
			return nil, nil, status.Error(codes.FailedPrecondition, "sandbox output hash is invalid")
		}
		if file.Skipped {
			if strings.TrimSpace(file.SkipReason) == "" {
				return nil, nil, status.Error(codes.FailedPrecondition, "sandbox output skip reason is missing")
			}
			skipped = appendSkippedFile(skipped, sourcePath, file.SkipReason, file.SizeBytes)
			continue
		}
		filename, validName := outputFilename(file.SourcePath, sourcePath)
		if !validName {
			return nil, nil, status.Error(codes.FailedPrecondition, "unmarked sandbox output filename is invalid")
		}
		if file.Kind != "regular" {
			return nil, nil, status.Error(codes.FailedPrecondition, "unmarked sandbox output file type is unsafe")
		}
		if file.LinkCount != 1 {
			return nil, nil, status.Error(codes.FailedPrecondition, "unmarked sandbox output link count is unsafe")
		}
		if file.SizeBytes > limits.MaxFileBytes {
			return nil, nil, status.Error(codes.FailedPrecondition, "unmarked sandbox output exceeds file byte cap")
		}
		if file.Open == nil {
			return nil, nil, status.Error(codes.FailedPrecondition, "unmarked sandbox output stream is missing")
		}
		if digest == "" {
			return nil, nil, status.Error(codes.FailedPrecondition, "unmarked sandbox output hash is missing")
		}
		if len(entries) >= limits.MaxFiles {
			return nil, nil, status.Error(codes.FailedPrecondition, "sandbox output file count exceeds driver budget")
		}
		if file.SizeBytes > limits.MaxTotalBytes-totalBytes {
			return nil, nil, status.Error(codes.FailedPrecondition, "sandbox output total bytes exceed driver budget")
		}
		totalBytes += file.SizeBytes
		mimeType := strings.TrimSpace(file.MIMEType)
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		entries = append(entries, normalizedOutputFile{
			SourcePath: sourcePath,
			Filename:   filename,
			SizeBytes:  file.SizeBytes,
			SHA256:     digest,
			MIMEType:   mimeType,
			Open:       file.Open,
		})
	}
	return entries, skipped, nil
}

func canonicalOutputPathStructure(raw string) (string, error) {
	if raw == "" || !utf8.ValidString(raw) || strings.ContainsRune(raw, 0) {
		return "", status.Error(codes.FailedPrecondition, "sandbox output path is invalid")
	}
	if !strings.HasPrefix(raw, "/") {
		return "", status.Error(codes.FailedPrecondition, "sandbox output path must be absolute")
	}
	cleaned := path.Clean(raw)
	if cleaned == OutputRoot || !strings.HasPrefix(cleaned, OutputRoot+"/") {
		return "", status.Error(codes.FailedPrecondition, "sandbox output path escapes output root")
	}
	return cleaned, nil
}

func outputFilename(raw string, canonical string) (string, bool) {
	if raw == "" || !utf8.ValidString(raw) || strings.ContainsRune(raw, 0) || !pathvalidation.IsNFC(raw) {
		return "", false
	}
	base := path.Base(canonical)
	if base == "." || base == "/" || base == "" {
		return "", false
	}
	return base, true
}

func appendSkippedFile(skipped []SkippedFile, sourcePath string, reason string, sizeBytes int64) []SkippedFile {
	return append(skipped, SkippedFile{SourcePath: sourcePath, Reason: reason, SizeBytes: sizeBytes})
}

func captureReadSkipReason(err error) string {
	switch status.Code(err) {
	case codes.FailedPrecondition, codes.ResourceExhausted:
		return "changed_during_capture"
	default:
		return "capture_failed"
	}
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readCaptureBody(ctx context.Context, entry normalizedOutputFile, maxBytes int64) ([]byte, string, error) {
	reader, err := entry.Open(ctx)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = reader.Close() }()
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(body)) > maxBytes {
		return nil, "", status.Error(codes.ResourceExhausted, "output capture file byte cap exceeded")
	}
	if int64(len(body)) != entry.SizeBytes {
		return nil, "", status.Error(codes.FailedPrecondition, "sandbox output size changed during capture")
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if entry.SHA256 == "" {
		return nil, "", status.Error(codes.FailedPrecondition, "sandbox output hash is missing")
	}
	return body, digest, nil
}

func loadCaptureIndex(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (map[string]captureRow, error) {
	rows, err := tx.Query(ctx,
		`SELECT source_path, last_file_id, last_size_bytes, last_sha256
		   FROM session_output_captures
		  WHERE workspace_id = $1
		    AND session_id = $2
		  FOR UPDATE`,
		workspaceID, sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	index := make(map[string]captureRow)
	for rows.Next() {
		var sourcePath string
		var row captureRow
		if err := rows.Scan(&sourcePath, &row.LastFileID, &row.LastSizeBytes, &row.LastSHA256); err != nil {
			return nil, err
		}
		index[sourcePath] = row
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return index, nil
}

func captureNeeds(entries []normalizedOutputFile, existing map[string]captureRow) (int, int64) {
	var neededFiles int
	var neededBytes int64
	for _, entry := range entries {
		row, ok := existing[entry.SourcePath]
		if ok && row.LastFileID.Valid && row.LastSizeBytes.Valid &&
			row.LastSizeBytes.Int64 == entry.SizeBytes && row.LastSHA256.Valid &&
			strings.EqualFold(row.LastSHA256.String, entry.SHA256) {
			continue
		}
		neededFiles++
		neededBytes += entry.SizeBytes
	}
	return neededFiles, neededBytes
}

func assertFileQuota(ctx context.Context, tx *dbconnect.Tx, workspaceID string, limits Limits, addFiles int, addBytes int64) error {
	if addFiles == 0 && addBytes == 0 {
		return nil
	}
	var fileCount int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM files WHERE workspace_id = $1`,
		workspaceID,
	).Scan(&fileCount); err != nil {
		return err
	}
	if fileCount+addFiles > limits.MaxFileIdentitiesPerWorkspace {
		return status.Error(codes.ResourceExhausted, "workspace file identity quota exceeded")
	}
	var retainedBytes int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(SUM(size_bytes), 0) FROM file_objects WHERE workspace_id = $1`,
		workspaceID,
	).Scan(&retainedBytes); err != nil {
		return err
	}
	if retainedBytes+addBytes > limits.MaxRetainedBytesPerWorkspace {
		return status.Error(codes.ResourceExhausted, "workspace retained file bytes quota exceeded")
	}
	return nil
}

func markCaptureSeenTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, sourcePath string, capturedAt time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_output_captures
		    SET last_captured_at = $4,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND source_path = $3`,
		workspaceID, sessionID, sourcePath, capturedAt,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.Internal, "output capture index update failed")
	}
	return nil
}

func upsertCaptureRowTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, sourcePath string, fileID string, sizeBytes int64, digest string, capturedAt time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_output_captures (
			workspace_id, session_id, source_path, last_file_id, last_size_bytes,
			last_sha256, last_captured_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $7)
		ON CONFLICT (workspace_id, session_id, source_path) DO UPDATE SET
			last_file_id = EXCLUDED.last_file_id,
			last_size_bytes = EXCLUDED.last_size_bytes,
			last_sha256 = EXCLUDED.last_sha256,
			last_captured_at = EXCLUDED.last_captured_at,
			updated_at = EXCLUDED.updated_at`,
		workspaceID, sessionID, sourcePath, fileID, sizeBytes, digest, capturedAt,
	)
	return err
}

func (c *Capturer) cleanupUploaded(keys []string) func() {
	if c == nil || c.BlobStore == nil || len(keys) == 0 {
		return func() {}
	}
	copied := append([]string(nil), keys...)
	var once sync.Once
	return func() {
		once.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for _, key := range copied {
				if err := c.BlobStore.Delete(cleanupCtx, key); err != nil && !isBlobNotFound(err) {
					continue
				}
			}
		})
	}
}

func isBlobNotFound(err error) bool {
	var notFound *blob.NotFoundError
	return errors.As(err, &notFound)
}

func blobKeyFor(workspaceID string, objectID string) string {
	return "files/" + workspaceID + "/" + objectID
}

func rowsAffected(result sql.Result) bool {
	if result == nil {
		return false
	}
	rows, err := result.RowsAffected()
	return err == nil && rows > 0
}
