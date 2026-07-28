package outputcapture

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

type staticOutputScanner struct {
	files []SandboxOutputFile
	err   error
}

func (s *staticOutputScanner) ScanOutputs(context.Context, SandboxOutputTarget) (SandboxOutputScan, error) {
	if s.err != nil {
		return SandboxOutputScan{}, s.err
	}
	return SandboxOutputScan{Files: s.files}, nil
}

func TestCapturerPassesThroughMarkedSkipsAndUploadsOnlyLegalFiles(t *testing.T) {
	tests := []struct {
		name   string
		file   SandboxOutputFile
		reason string
	}{
		{name: "non-regular", file: SandboxOutputFile{SourcePath: OutputRoot + "/pipe", Kind: "fifo", Skipped: true, SkipReason: "non_regular"}, reason: "non_regular"},
		{name: "multiple-links", file: SandboxOutputFile{SourcePath: OutputRoot + "/linked.txt", Kind: "regular", LinkCount: 2, SizeBytes: 2, Skipped: true, SkipReason: "multiple_links"}, reason: "multiple_links"},
		{name: "file-too-large", file: SandboxOutputFile{SourcePath: OutputRoot + "/large.txt", Kind: "regular", LinkCount: 1, SizeBytes: 5, Skipped: true, SkipReason: "file_too_large"}, reason: "file_too_large"},
		{name: "budget", file: SandboxOutputFile{SourcePath: OutputRoot + "/budget.txt", Kind: "regular", LinkCount: 1, SizeBytes: 2, Skipped: true, SkipReason: "budget"}, reason: "budget"},
		{name: "non-nfc", file: SandboxOutputFile{SourcePath: OutputRoot + "/e\u0301.txt", Kind: "regular", SizeBytes: 2, Skipped: true, SkipReason: "invalid_filename"}, reason: "invalid_filename"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedOutputCaptureSession(t, admin, "sesn_output_skip_"+strings.ReplaceAll(test.name, "-", "_"))
			blobStore := blob.NewFakeBlobStore()
			capturer := NewCapturer(blobStore, &staticOutputScanner{files: []SandboxOutputFile{test.file, captureFile(OutputRoot+"/legal.txt", "ok")}})
			capturer.Limits = Limits{MaxFiles: 2, MaxFileBytes: 4, MaxTotalBytes: 8}

			result := runCapture(t, runtime, capturer, "sesn_output_skip_"+strings.ReplaceAll(test.name, "-", "_"), "rwrite_1", time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC))
			if len(result.Skipped) != 1 || result.Skipped[0].Reason != test.reason || result.Skipped[0].SizeBytes != test.file.SizeBytes {
				t.Fatalf("skipped = %+v; want one %s record", result.Skipped, test.reason)
			}
			if blobStore.Len() != 1 {
				t.Fatalf("stored blobs = %d; want only the legal file", blobStore.Len())
			}
			var files int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM files WHERE workspace_id='default' AND scope_id=$1`, "sesn_output_skip_"+strings.ReplaceAll(test.name, "-", "_")).Scan(&files); err != nil {
				t.Fatalf("count captured files: %v", err)
			}
			if files != 1 {
				t.Fatalf("captured file rows = %d; want 1", files)
			}
		})
	}
}

func TestNormalizeScanRejectsDriverBudgetViolationsAndUnmarkedDisqualifications(t *testing.T) {
	tests := []struct {
		name  string
		files []SandboxOutputFile
		limit Limits
	}{
		{name: "count", files: []SandboxOutputFile{captureFile(OutputRoot+"/a.txt", "a"), captureFile(OutputRoot+"/b.txt", "b")}, limit: Limits{MaxFiles: 1, MaxFileBytes: 10, MaxTotalBytes: 10}},
		{name: "total-bytes", files: []SandboxOutputFile{captureFile(OutputRoot+"/a.txt", "aa"), captureFile(OutputRoot+"/b.txt", "bb")}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 3}},
		{name: "non-regular", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/pipe", Kind: "fifo"}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 10}},
		{name: "multiple-links", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/linked", Kind: "regular", LinkCount: 2}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 10}},
		{name: "file-too-large", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/large", Kind: "regular", LinkCount: 1, SizeBytes: 11}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
		{name: "missing-stream", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/missing", Kind: "regular", LinkCount: 1}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
		{name: "missing-hash", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/missing-hash", Kind: "regular", LinkCount: 1, SizeBytes: 1, Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("x")), nil }}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
		{name: "contradictory-skip-reason", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/skip-reason", Kind: "regular", LinkCount: 1, SizeBytes: 1, SHA256: digestFor("x"), SkipReason: "budget", Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("x")), nil }}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
		{name: "invalid-utf8", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/bad\xff.txt", Kind: "regular", LinkCount: 1, SizeBytes: 1, SHA256: digestFor("x"), Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("x")), nil }}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
		{name: "marked-invalid-utf8", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/bad\xff.txt", Kind: "regular", SizeBytes: 1, Skipped: true, SkipReason: "invalid_filename"}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
		{name: "non-nfc", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/e\u0301.txt", Kind: "regular", LinkCount: 1, SizeBytes: 1, SHA256: digestFor("x"), Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("x")), nil }}}, limit: Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 20}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := normalizeScan(SandboxOutputScan{Files: test.files}, test.limit); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("normalize err = %v; want FailedPrecondition", err)
			}
		})
	}
}

func TestCapturerClassifiesOnlyPrePersistenceScanFailures(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_output_scan_boundary"
	seedOutputCaptureSession(t, admin, sessionID)
	client := dbconnect.NewClientForTesting(runtime)

	tests := []struct {
		name     string
		capturer *Capturer
		wantKind string
	}{
		{
			name:     "scanner",
			capturer: NewCapturer(blob.NewFakeBlobStore(), &staticOutputScanner{err: errors.New("scanner transport failed")}),
			wantKind: "scan_outputs",
		},
		{
			name: "normalization",
			capturer: NewCapturer(blob.NewFakeBlobStore(), &staticOutputScanner{files: []SandboxOutputFile{
				captureFile("relative.txt", "x"),
			}}),
			wantKind: "normalize_scan",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := client.WithWorkspaceTx(context.Background(), "default", "agentruntimebridge.finish_idle", func(tx *dbconnect.Tx) error {
				_, err := test.capturer.CaptureOutputs(context.Background(), tx, Request{
					WorkspaceID: "default", SessionID: sessionID, RuntimeWriteID: "rwrite_" + test.name,
				})
				return err
			})
			var scanErr *CaptureScanError
			if !errors.As(err, &scanErr) || scanErr.Kind() != test.wantKind {
				t.Fatalf("capture error = %T %v; want CaptureScanError kind %q", err, err, test.wantKind)
			}
		})
	}

	blobStore := blob.NewFakeBlobStore()
	blobStore.SetPutHook(func(context.Context, string) error { return errors.New("blob unavailable") })
	capturer := NewCapturer(blobStore, &staticOutputScanner{files: []SandboxOutputFile{
		captureFile(OutputRoot+"/report.txt", "body"),
	}})
	err := client.WithWorkspaceTx(context.Background(), "default", "agentruntimebridge.finish_idle", func(tx *dbconnect.Tx) error {
		_, err := capturer.CaptureOutputs(context.Background(), tx, Request{
			WorkspaceID: "default", SessionID: sessionID, RuntimeWriteID: "rwrite_blob_failure",
		})
		return err
	})
	var scanErr *CaptureScanError
	if err == nil || errors.As(err, &scanErr) {
		t.Fatalf("blob persistence error = %T %v; want non-scan failure", err, err)
	}
}

func TestCapturerPreservesTypedDriverScanFailureCause(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_output_scan_cause"
	seedOutputCaptureSession(t, admin, sessionID)
	client := dbconnect.NewClientForTesting(runtime)
	capturer := NewCapturer(blob.NewFakeBlobStore(), &staticOutputScanner{err: &sandboxdriver.OutputCaptureEntryError{
		Kind:    "permission_denied",
		Message: "capture mode requires root",
	}})

	err := client.WithWorkspaceTx(context.Background(), "default", "agentruntimebridge.finish_idle", func(tx *dbconnect.Tx) error {
		_, err := capturer.CaptureOutputs(context.Background(), tx, Request{
			WorkspaceID: "default", SessionID: sessionID, RuntimeWriteID: "rwrite_scan_cause",
		})
		return err
	})
	var scanErr *CaptureScanError
	if !errors.As(err, &scanErr) || scanErr.Kind() != "scan_outputs" {
		t.Fatalf("capture error = %T %v; want CaptureScanError", err, err)
	}
	if scanErr.CaptureKind != "permission_denied" || scanErr.CaptureDetail != "capture mode requires root" {
		t.Fatalf("capture cause = %q/%q; want typed driver cause", scanErr.CaptureKind, scanErr.CaptureDetail)
	}
}

func TestCapturerConvertsPerEntryReadAndHashFailuresToSkips(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_output_per_entry_failures"
	seedOutputCaptureSession(t, admin, sessionID)
	readFailure := captureFile(OutputRoot+"/read-failure.txt", "read")
	readFailure.Open = func(context.Context) (io.ReadCloser, error) {
		return nil, errors.New("read failed")
	}
	hashFailure := captureFile(OutputRoot+"/hash-failure.txt", "hash")
	hashFailure.SHA256 = digestFor("different")
	blobStore := blob.NewFakeBlobStore()
	capturer := NewCapturer(blobStore, &staticOutputScanner{files: []SandboxOutputFile{
		readFailure,
		hashFailure,
		captureFile(OutputRoot+"/kept.txt", "kept"),
	}})
	capturer.Limits = Limits{MaxFiles: 3, MaxFileBytes: 16, MaxTotalBytes: 32}

	result := runCapture(t, runtime, capturer, sessionID, "rwrite_per_entry", time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC))
	if got := skippedReasons(result.Skipped); !slices.Equal(got, []string{"capture_failed", "changed_during_capture"}) {
		t.Fatalf("per-entry skip reasons = %v; want read/hash dispositions", got)
	}
	if blobStore.Len() != 1 {
		t.Fatalf("stored blobs = %d; want only surviving sibling", blobStore.Len())
	}
	var fileRows int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM files WHERE workspace_id='default' AND scope_id=$1`, sessionID).Scan(&fileRows); err != nil {
		t.Fatalf("count captured files: %v", err)
	}
	if fileRows != 1 {
		t.Fatalf("captured file rows = %d; want 1", fileRows)
	}
}

func TestCapturerRetainsCapturedIdentityWhenLaterScanSkipsPath(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_output_skip_retention"
	seedOutputCaptureSession(t, admin, sessionID)
	blobStore := blob.NewFakeBlobStore()
	scanner := &staticOutputScanner{files: []SandboxOutputFile{captureFile(OutputRoot+"/report.txt", "old")}}
	capturer := NewCapturer(blobStore, scanner)
	capturer.Limits = Limits{MaxFiles: 2, MaxFileBytes: 4, MaxTotalBytes: 8}

	runCapture(t, runtime, capturer, sessionID, "rwrite_1", time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC))
	var fileID string
	if err := admin.QueryRowContext(context.Background(), `SELECT last_file_id FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, OutputRoot+"/report.txt").Scan(&fileID); err != nil {
		t.Fatalf("read initial capture: %v", err)
	}
	if fileID == "" || blobStore.Len() != 1 {
		t.Fatalf("initial capture file/blobs = %q/%d", fileID, blobStore.Len())
	}

	scanner.files = []SandboxOutputFile{{SourcePath: OutputRoot + "/report.txt", Kind: "regular", LinkCount: 1, SizeBytes: 5, Skipped: true, SkipReason: "file_too_large"}}
	result := runCapture(t, runtime, capturer, sessionID, "rwrite_2", time.Date(2026, 7, 22, 1, 1, 0, 0, time.UTC))
	if len(result.Skipped) != 1 || result.Skipped[0].Reason != "file_too_large" {
		t.Fatalf("second capture skipped = %+v", result.Skipped)
	}
	var retainedFileID string
	if err := admin.QueryRowContext(context.Background(), `SELECT last_file_id FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, OutputRoot+"/report.txt").Scan(&retainedFileID); err != nil {
		t.Fatalf("read retained capture: %v", err)
	}
	if retainedFileID != fileID || blobStore.Len() != 1 {
		t.Fatalf("retained capture file/blobs = %q/%d; want %q/1", retainedFileID, blobStore.Len(), fileID)
	}
}

func TestCapturerEmptyScanLeavesPriorCaptureUntouched(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_output_empty_scan"
	seedOutputCaptureSession(t, admin, sessionID)
	scanner := &staticOutputScanner{files: []SandboxOutputFile{captureFile(OutputRoot+"/retained.txt", "old")}}
	blobStore := blob.NewFakeBlobStore()
	capturer := NewCapturer(blobStore, scanner)
	runCapture(t, runtime, capturer, sessionID, "rwrite_1", time.Date(2026, 7, 22, 1, 0, 0, 0, time.UTC))
	var initialFileID string
	if err := admin.QueryRowContext(context.Background(), `SELECT last_file_id FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, OutputRoot+"/retained.txt").Scan(&initialFileID); err != nil {
		t.Fatalf("read initial capture: %v", err)
	}

	scanner.files = nil
	runCapture(t, runtime, capturer, sessionID, "rwrite_2", time.Date(2026, 7, 22, 1, 1, 0, 0, time.UTC))

	var retainedFileID string
	if err := admin.QueryRowContext(context.Background(), `SELECT last_file_id FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, OutputRoot+"/retained.txt").Scan(&retainedFileID); err != nil {
		t.Fatalf("read retained capture: %v", err)
	}
	if retainedFileID != initialFileID || blobStore.Len() != 1 {
		t.Fatalf("retained capture file/blobs = %q/%d; want %q/1", retainedFileID, blobStore.Len(), initialFileID)
	}
}

func TestNormalizeScanKeepsStructuralAndIntegrityFailuresHard(t *testing.T) {
	tests := []struct {
		name  string
		files []SandboxOutputFile
	}{
		{name: "relative", files: []SandboxOutputFile{captureFile("relative.txt", "x")}},
		{name: "escape", files: []SandboxOutputFile{captureFile(OutputRoot+"/../escape.txt", "x")}},
		{name: "duplicate", files: []SandboxOutputFile{captureFile(OutputRoot+"/same.txt", "x"), captureFile(OutputRoot+"/same.txt", "x")}},
		{name: "negative-size", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/negative.txt", Kind: "regular", LinkCount: 1, SizeBytes: -1, SHA256: digestFor("x")}}},
		{name: "invalid-hash", files: []SandboxOutputFile{{SourcePath: OutputRoot + "/hash.txt", Kind: "regular", LinkCount: 1, SizeBytes: 1, SHA256: "not-a-hash"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := normalizeScan(SandboxOutputScan{Files: test.files}, Limits{MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 10}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("normalize err = %v; want FailedPrecondition", err)
			}
		})
	}
}

func runCapture(t *testing.T, runtime *sql.DB, capturer *Capturer, sessionID string, writeID string, capturedAt time.Time) Result {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtime)
	var result Result
	if err := client.WithWorkspaceTx(context.Background(), "default", "agentruntimebridge.finish_idle", func(tx *dbconnect.Tx) error {
		var err error
		result, err = capturer.CaptureOutputs(context.Background(), tx, Request{
			WorkspaceID: "default", SessionID: sessionID, RuntimeWriteID: writeID, CapturedAt: capturedAt,
		})
		return err
	}); err != nil {
		t.Fatalf("capture outputs: %v", err)
	}
	return result
}

func seedOutputCaptureSession(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	now := "2026-07-22T00:00:00Z"
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	threadID := "thr_" + sessionID
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ('default', 'workspace', 'default', $1) ON CONFLICT (id) DO NOTHING`, []any{now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('default', $1, $1, 1, $2, $2)`, []any{agentID, now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('default', $1, $2, 1, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $3, $4)`, []any{agentVersionID, agentID, "hash_" + sessionID, now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ('default', $1, $1, '{}', $2, $2)`, []any{environmentID, now}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ('default', $1, $2, 'session', 'idle', 'active', $3, 1, $4, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $5, $5)`, []any{sessionID, threadID, agentID, environmentID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ('default', $1, $2, 'main', 'public', 'idle', $3, $3, $3)`, []any{threadID, sessionID, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed output capture session: %v", err)
		}
	}
}

func captureFile(sourcePath string, body string) SandboxOutputFile {
	return SandboxOutputFile{
		SourcePath: sourcePath,
		Kind:       "regular",
		LinkCount:  1,
		SizeBytes:  int64(len(body)),
		SHA256:     digestFor(body),
		MIMEType:   "text/plain",
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(body)), nil
		},
	}
}

func digestFor(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func skippedReasons(entries []SkippedFile) []string {
	reasons := make([]string, 0, len(entries))
	for _, entry := range entries {
		reasons = append(reasons, entry.Reason)
	}
	return reasons
}
