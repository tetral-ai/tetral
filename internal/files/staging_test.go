package files_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/files"
)

func TestProductionUploadConstantsMatchContract(t *testing.T) {
	if files.MaxFileBytes != 500000000 {
		t.Fatalf("MaxFileBytes = %d; want 500000000", files.MaxFileBytes)
	}
	if files.RouteMultipartBytes != 501000000 {
		t.Fatalf("RouteMultipartBytes = %d; want 501000000", files.RouteMultipartBytes)
	}
}

func TestNewStageDirCreatesOwnerOnlyFilesDirectory(t *testing.T) {
	dataDir := t.TempDir()
	stageDir, err := files.NewStageDir(dataDir)
	if err != nil {
		t.Fatalf("NewStageDir: %v", err)
	}
	if !strings.HasPrefix(stageDir, dataDir+string(os.PathSeparator)) {
		t.Fatalf("stage dir %q must live under data dir %q", stageDir, dataDir)
	}
	info, err := os.Stat(stageDir)
	if err != nil {
		t.Fatalf("stat stage dir: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("stage path %q is not a directory", stageDir)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("stage dir mode = %04o; want 0700", info.Mode().Perm())
	}
}

func TestStageUploadStoresExactBytesSHAAndCanReopen(t *testing.T) {
	stageDir := t.TempDir()
	body := []byte("alpha,beta\n1,2\n")
	staged, err := files.StageUpload(bytes.NewReader(body), stageDir, "../tenant/data.csv", "text/csv", files.UploadLimits{MaxFileBytes: 1024})
	if err != nil {
		t.Fatalf("StageUpload: %v", err)
	}
	defer func() { _ = staged.Cleanup() }()

	if staged.Filename != "../tenant/data.csv" {
		t.Errorf("Filename = %q; want uploaded metadata preserved", staged.Filename)
	}
	if staged.MIMEType != "text/csv" {
		t.Errorf("MIMEType = %q; want text/csv", staged.MIMEType)
	}
	if staged.SizeBytes != int64(len(body)) {
		t.Errorf("SizeBytes = %d; want %d", staged.SizeBytes, len(body))
	}
	sum := sha256.Sum256(body)
	if staged.SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("SHA256 = %q; want %q", staged.SHA256, hex.EncodeToString(sum[:]))
	}

	rc, err := staged.Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll staged: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("staged bytes = %q; want %q", got, body)
	}
}

func TestStageUploadCleanupRemovesTempFileAndIsIdempotent(t *testing.T) {
	stageDir := t.TempDir()
	staged, err := files.StageUpload(strings.NewReader("abc"), stageDir, "data.txt", "text/plain", files.UploadLimits{MaxFileBytes: 10})
	if err != nil {
		t.Fatalf("StageUpload: %v", err)
	}
	tempPath := staged.TempPathForTest()
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("expected staged temp file before cleanup: %v", err)
	}
	if err := staged.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if err := staged.Cleanup(); err != nil {
		t.Fatalf("second Cleanup: %v", err)
	}
	if _, err := os.Stat(tempPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file after cleanup err = %v; want not exist", err)
	}
}

func TestStageUploadRejectsOverPerFileCapAndRemovesPartialTemp(t *testing.T) {
	stageDir := t.TempDir()
	_, err := files.StageUpload(strings.NewReader("123456"), stageDir, "data.txt", "text/plain", files.UploadLimits{MaxFileBytes: 5})
	if err == nil {
		t.Fatal("expected over-cap upload to reject")
	}
	var tooLarge *files.RequestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("expected *files.RequestTooLargeError, got %T (%v)", err, err)
	}
	entries, readErr := os.ReadDir(stageDir)
	if readErr != nil {
		t.Fatalf("ReadDir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("stage dir contains partial temp files after reject: %v", entries)
	}
}

func TestStageUploadTempNameIsServerGenerated(t *testing.T) {
	stageDir := t.TempDir()
	uploadedName := "../tenant/files/ws/fobj_bad.csv"
	staged, err := files.StageUpload(strings.NewReader("abc"), stageDir, uploadedName, "text/csv", files.UploadLimits{MaxFileBytes: 10})
	if err != nil {
		t.Fatalf("StageUpload: %v", err)
	}
	defer func() { _ = staged.Cleanup() }()
	base := filepath.Base(staged.TempPathForTest())
	if strings.Contains(base, "tenant") || strings.Contains(base, "files") || strings.Contains(base, "fobj_bad") || strings.Contains(base, "csv") {
		t.Fatalf("temp basename %q contains uploaded filename fragments from %q", base, uploadedName)
	}
}
