package files

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// MaxFileBytes is the production per-file upload cap.
const MaxFileBytes int64 = 500000000

// RouteMultipartBytes is the HTTP route cap: one 500 MB file plus a
// 1 MB multipart envelope allowance.
const RouteMultipartBytes int64 = 501000000

// UploadLimits carries upload-size limits. Zero values select the
// production constants.
type UploadLimits struct {
	MaxFileBytes int64
}

// StagedUpload owns the server-named temporary file holding exact
// uploaded bytes plus measured metadata.
type StagedUpload struct {
	Filename  string
	MIMEType  string
	SizeBytes int64
	SHA256    string

	tempPath string
}

// NewStageDir creates the owner-only Files staging directory beneath
// Engine's data dir.
func NewStageDir(dataDir string) (string, error) {
	if dataDir == "" {
		return "", errors.New("files: dataDir is required")
	}
	stageDir := filepath.Join(dataDir, "files-upload-stage")
	if err := os.MkdirAll(stageDir, 0o700); err != nil {
		return "", fmt.Errorf("files: create stage dir: %w", err)
	}
	info, err := os.Stat(stageDir)
	if err != nil {
		return "", fmt.Errorf("files: stat stage dir: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("files: stage path is not a directory")
	}
	if info.Mode().Perm() != 0o700 {
		if err := os.Chmod(stageDir, 0o700); err != nil { //nolint:gosec // directory must be owner-only, not 0600
			return "", fmt.Errorf("files: chmod stage dir: %w", err)
		}
	}
	return stageDir, nil
}

// StageUpload streams reader into a server-named temp file while
// counting exact bytes and computing SHA-256. On overflow or read
// failure, the partial temp file is removed before return.
func StageUpload(reader io.Reader, stageDir string, filename string, mimeType string, limits UploadLimits) (*StagedUpload, error) {
	if reader == nil {
		return nil, errors.New("files: upload reader is required")
	}
	if stageDir == "" {
		return nil, errors.New("files: stageDir is required")
	}
	maxFileBytes := limits.MaxFileBytes
	if maxFileBytes == 0 {
		maxFileBytes = MaxFileBytes
	}
	if maxFileBytes < 0 {
		return nil, errors.New("files: MaxFileBytes must be non-negative")
	}

	temp, err := os.CreateTemp(stageDir, "upload-*")
	if err != nil {
		return nil, fmt.Errorf("files: stage temp file: %w", err)
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}

	hasher := sha256.New()
	limited := &overflowReader{reader: reader, remaining: maxFileBytes}
	written, copyErr := io.Copy(io.MultiWriter(temp, hasher), limited)
	if copyErr != nil {
		cleanup()
		var requestTooLarge *RequestTooLargeError
		if errors.As(copyErr, &requestTooLarge) {
			return nil, requestTooLarge
		}
		if errors.Is(copyErr, errFileCapExceeded) {
			return nil, &RequestTooLargeError{Message: "file exceeds the size cap"}
		}
		return nil, &ValidationError{Message: "upload read failed"}
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return nil, fmt.Errorf("files: stage sync: %w", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("files: stage close: %w", err)
	}
	return &StagedUpload{
		Filename:  filename,
		MIMEType:  mimeType,
		SizeBytes: written,
		SHA256:    hex.EncodeToString(hasher.Sum(nil)),
		tempPath:  tempPath,
	}, nil
}

// Open returns a fresh reader positioned at the start of the staged bytes.
func (s *StagedUpload) Open() (io.ReadCloser, error) {
	if s == nil {
		return nil, errors.New("files: StagedUpload is nil")
	}
	if s.tempPath == "" {
		return nil, errors.New("files: StagedUpload has no staged bytes")
	}
	return os.Open(s.tempPath) //nolint:gosec // path is server-generated, never client input
}

// Cleanup removes the temporary staged upload. It is idempotent.
func (s *StagedUpload) Cleanup() error {
	if s == nil || s.tempPath == "" {
		return nil
	}
	err := os.Remove(s.tempPath)
	s.tempPath = ""
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// TempPathForTest exposes the server-generated temp path to tests.
func (s *StagedUpload) TempPathForTest() string {
	if s == nil {
		return ""
	}
	return s.tempPath
}

var errFileCapExceeded = errors.New("files: file size cap exceeded")

type overflowReader struct {
	reader    io.Reader
	remaining int64
}

func (r *overflowReader) Read(p []byte) (int, error) {
	if r.remaining < 0 {
		return 0, errFileCapExceeded
	}
	if int64(len(p)) > r.remaining+1 {
		p = p[:r.remaining+1]
	}
	n, err := r.reader.Read(p)
	if n > 0 {
		r.remaining -= int64(n)
		if r.remaining < 0 {
			return n - 1, errFileCapExceeded
		}
	}
	return n, err
}
