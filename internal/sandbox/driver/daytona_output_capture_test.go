package driver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	daytonaerrors "github.com/daytonaio/daytona/libs/sdk-go/pkg/errors"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"
	"golang.org/x/sys/unix"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestDaytonaOutputCaptureUsesOneHelperInvocationPerVisitedPath(t *testing.T) {
	process := &captureProcess{}
	client := captureSandboxGetter{handle: daytonaSandboxHandle{Process: process}}
	executor := NewDaytonaHelperExecutorForClient(client)

	scan, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_sandbox_1",
		MaxFiles:          100,
		MaxFileBytes:      1024,
		MaxTotalBytes:     4096,
	})
	if err != nil {
		t.Fatalf("CaptureOutputs: %v", err)
	}
	if len(scan.Files) != 1 || len(process.commands) != 2 {
		t.Fatalf("capture files/commands = %d/%d; want root and file helper invocations", len(scan.Files), len(process.commands))
	}
	if command := process.commands[1]; !strings.Contains(command, "sudo -n -u 'root' '/usr/local/bin/sandbox' __capture --path '/mnt/session/outputs/report.txt' --max-bytes 1024 --max-entries 9998 --max-name-bytes 524288") || strings.Contains(command, "sh -") {
		t.Fatalf("capture command = %q; want privileged helper mode", command)
	}
	file := scan.Files[0]
	reader, err := file.Open(context.Background())
	if err != nil {
		t.Fatalf("Open captured file: %v", err)
	}
	body, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(body) != "captured body" || file.SHA256 != "343e5564b69e7e1d964f81995f66c0836156ac283ea7601921051d299145dcf3" {
		t.Fatalf("captured body/hash = %q/%q err=%v", body, file.SHA256, err)
	}
}

func TestDaytonaOutputCaptureClassifiesProviderTransportErrors(t *testing.T) {
	tests := []struct {
		name   string
		client captureSandboxGetter
		stage  sandbox.ProviderStage
	}{
		{
			name:   "inspect",
			client: captureSandboxGetter{err: daytonaerrors.NewDaytonaRateLimitError("busy", nil)},
			stage:  sandbox.StageStatus,
		},
		{
			name: "helper command",
			client: captureSandboxGetter{handle: daytonaSandboxHandle{Process: captureErrorProcess{
				err: daytonaerrors.NewDaytonaRateLimitError("busy", nil),
			}}},
			stage: sandbox.StageExecuteTool,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewDaytonaHelperExecutorForClient(test.client)
			_, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{ProviderSandboxID: "provider_transport_error"})
			var providerErr *sandbox.ProviderError
			if !errors.As(err, &providerErr) || providerErr.Stage != test.stage || !providerErr.Retryable {
				t.Fatalf("CaptureOutputs error = %#v; want retryable %s ProviderError", err, test.stage)
			}
		})
	}
}

func TestDaytonaOutputCaptureRealHelperEnumeratesMarkersBeyondCaptureBudgets(t *testing.T) {
	helper := buildOutputCaptureHelper(t)
	root := t.TempDir()
	mustWriteOutputFixture(t, filepath.Join(root, "a-large.txt"), "large")
	mustWriteOutputFixture(t, filepath.Join(root, "b.txt"), "ok")
	if err := unix.Mkfifo(filepath.Join(root, "c.pipe"), 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "d-dir"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	mustWriteOutputFixture(t, filepath.Join(root, "d-dir", "after.txt"), "later")
	if err := os.Symlink(".", filepath.Join(root, "e-loop")); err != nil {
		t.Fatalf("symlink loop: %v", err)
	}
	if err := os.Symlink("a-large.txt", filepath.Join(root, "f-link")); err != nil {
		t.Fatalf("ordinary symlink: %v", err)
	}
	mustWriteOutputFixture(t, filepath.Join(root, "g-e\u0301.txt"), "nfc")

	observedMaxBytes := map[string]int64{}
	services := localOutputCaptureServices{root: root, helper: helper, observedMaxBytes: observedMaxBytes}
	executor := NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (DaytonaSandboxServices, error) {
		return DaytonaSandboxServices{Process: services}, nil
	})
	scan, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_local", MaxFiles: 10, MaxFileBytes: 4, MaxTotalBytes: 4,
	})
	if err != nil {
		t.Fatalf("CaptureOutputs with real helper: %v", err)
	}
	if scan.Truncated {
		t.Fatal("finite mixed tree was marked truncated")
	}
	got := make(map[string]OutputCaptureFile, len(scan.Files))
	for _, file := range scan.Files {
		got[file.SourcePath] = file
	}
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/a-large.txt", true, "file_too_large")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/b.txt", false, "")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/c.pipe", true, "non_regular")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/d-dir/after.txt", true, "budget")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/e-loop", true, "symlink")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/f-link", true, "symlink")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/g-e\u0301.txt", true, "invalid_filename")
	if got := observedMaxBytes[outputCaptureRoot+"/d-dir/after.txt"]; got != 2 {
		t.Fatalf("boundary-file helper cap = %d; want remaining total-byte budget 2", got)
	}
}

func TestDaytonaOutputCaptureEnumerationCeilingReturnsTruncatedPartialScan(t *testing.T) {
	helper := buildOutputCaptureHelper(t)
	root := t.TempDir()
	mustWriteOutputFixture(t, filepath.Join(root, "a.txt"), "a")
	mustWriteOutputFixture(t, filepath.Join(root, "b.txt"), "b")
	mustWriteOutputFixture(t, filepath.Join(root, "c.txt"), "c")
	services := localOutputCaptureServices{root: root, helper: helper}
	executor := NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (DaytonaSandboxServices, error) {
		return DaytonaSandboxServices{Process: services}, nil
	})
	scan, err := executor.captureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_local", MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 10,
	}, 3, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("capture bounded enumeration: %v", err)
	}
	if !scan.Truncated || len(scan.Files) != 2 ||
		scan.Files[0].SourcePath >= scan.Files[1].SourcePath {
		t.Fatalf("bounded scan = %+v; want two-file truncated partial result", scan)
	}
}

func TestDaytonaOutputCaptureRealHelperRecordsUnrepresentableNamesWithoutAborting(t *testing.T) {
	helper := buildOutputCaptureHelper(t)
	root := t.TempDir()
	mustWriteOutputFixture(t, filepath.Join(root, "good.txt"), "good")
	badName := string([]byte{'b', 'a', 'd', 0xff})
	mustWriteOutputFixture(t, filepath.Join(root, badName), "bad")
	services := localOutputCaptureServices{root: root, helper: helper}
	executor := NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (DaytonaSandboxServices, error) {
		return DaytonaSandboxServices{Process: services}, nil
	})

	scan, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_unrepresentable", MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 10,
	})
	if err != nil {
		t.Fatalf("CaptureOutputs with invalid UTF-8 name: %v", err)
	}
	if scan.UnrepresentableNames != 1 || len(scan.Files) != 1 || scan.Files[0].SourcePath != outputCaptureRoot+"/good.txt" ||
		len(scan.Records) != 1 || scan.Records[0] != (OutputCaptureScanRecord{
		ParentPath: outputCaptureRoot,
		Reason:     "unrepresentable_names",
		Count:      1,
	}) {
		t.Fatalf("scan = %+v; want good sibling and one scan-level unrepresentable name", scan)
	}
}

func TestDaytonaOutputCaptureSkipsNonNFCDirectoryWithoutDescending(t *testing.T) {
	helper := buildOutputCaptureHelper(t)
	root := t.TempDir()
	mustWriteOutputFixture(t, filepath.Join(root, "a-good.txt"), "good")
	invalidDirectory := "b-e\u0301"
	if err := os.Mkdir(filepath.Join(root, invalidDirectory), 0o755); err != nil {
		t.Fatalf("mkdir non-NFC directory: %v", err)
	}
	mustWriteOutputFixture(t, filepath.Join(root, invalidDirectory, "hidden.txt"), "hidden")
	services := localOutputCaptureServices{root: root, helper: helper}
	executor := NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (DaytonaSandboxServices, error) {
		return DaytonaSandboxServices{Process: services}, nil
	})

	scan, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_non_nfc_directory", MaxFiles: 10, MaxFileBytes: 16, MaxTotalBytes: 32,
	})
	if err != nil {
		t.Fatalf("CaptureOutputs with non-NFC directory: %v", err)
	}
	got := make(map[string]OutputCaptureFile, len(scan.Files))
	for _, file := range scan.Files {
		got[file.SourcePath] = file
	}
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/a-good.txt", false, "")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/"+invalidDirectory, true, "invalid_filename")
	if _, descended := got[outputCaptureRoot+"/"+invalidDirectory+"/hidden.txt"]; descended {
		t.Fatalf("scan descended into non-NFC directory: %+v", scan.Files)
	}
}

func TestDaytonaOutputCaptureValidatorTreatsAbsentDirectoryEntriesAsEmpty(t *testing.T) {
	const body = `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs","kind":"directory","link_count":2,"size_bytes":0}}`
	var envelope protocol.CaptureEnvelope
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("decode additive directory envelope: %v", err)
	}
	if envelope.Result == nil || !validCaptureResult(*envelope.Result, 1024, 10, 4096) {
		t.Fatalf("absent Entries rejected by validator: %+v", envelope)
	}
	if len(envelope.Result.Entries) != 0 || envelope.Result.EntriesTruncated || envelope.Result.UnrepresentableNames != 0 {
		t.Fatalf("absent Entries shape = %+v; want empty directory", envelope.Result)
	}
}

func TestDaytonaOutputCaptureUnreadableRootIsScanLevelOnly(t *testing.T) {
	const body = `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs","kind":"directory","link_count":2,"size_bytes":0,"skipped":true,"skip_reason":"unreadable"}}`
	executor := NewDaytonaHelperExecutorForClient(captureSandboxGetter{
		handle: daytonaSandboxHandle{Process: staticCaptureResponseProcess{body: body}},
	})

	scan, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{ProviderSandboxID: "provider_unreadable_root"})
	if err != nil {
		t.Fatalf("CaptureOutputs unreadable root: %v", err)
	}
	if len(scan.Files) != 0 ||
		len(scan.Records) != 1 || scan.Records[0] != (OutputCaptureScanRecord{
		ParentPath: outputCaptureRoot,
		Reason:     "root_unreadable",
		Count:      1,
	}) {
		t.Fatalf("unreadable-root scan = %+v; want empty scan-level record", scan)
	}
}

func TestDaytonaOutputCaptureMissingRootAbortsInsteadOfProvingAbsence(t *testing.T) {
	const body = `{"schema_version":1,"status":"error","error":{"kind":"root_unavailable","message":"output root unavailable"}}`
	client := captureSandboxGetter{handle: daytonaSandboxHandle{Process: staticCaptureResponseProcess{body: body}}}
	executor := NewDaytonaHelperExecutorForClient(client)
	_, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{ProviderSandboxID: "provider_missing_root"})
	var captureErr *OutputCaptureEntryError
	if err == nil || !errors.As(err, &captureErr) {
		t.Fatal("missing-root capture succeeded; want abort")
	}
	if captureErr.Kind != "root_unavailable" || captureErr.Message != "output root unavailable" {
		t.Fatalf("missing-root capture error = %+v; want helper kind and message", captureErr)
	}
}

func TestDaytonaOutputCaptureTransportFailureCarriesOnlyExitCode(t *testing.T) {
	const secretOutput = "CAPTURE_TRANSPORT_OUTPUT_MUST_NOT_LEAK"
	sandbox := daytonaSandboxHandle{Process: staticCaptureResponseProcess{
		body:     secretOutput,
		exitCode: 23,
	}}
	_, err := daytonaCaptureOutputEntry(context.Background(), sandbox, outputCaptureRoot, 10, 10, 1024)
	if err == nil || !strings.Contains(err.Error(), "exit code 23") {
		t.Fatalf("transport error = %v; want exit code", err)
	}
	if strings.Contains(err.Error(), secretOutput) {
		t.Fatalf("transport error leaked helper output: %v", err)
	}
}

func TestDaytonaOutputCaptureRejectsContradictoryAndMalformedEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "success-with-error", body: `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"regular","link_count":1,"size_bytes":1,"sha256":"00","data_base64":"YQ=="},"error":{"kind":"capture_failed","message":"bad"}}`},
		{name: "error-with-result", body: `{"schema_version":1,"status":"error","result":{"source_path":"/mnt/session/outputs/a","kind":"regular","link_count":1,"size_bytes":1},"error":{"kind":"not_found","message":"gone"}}`},
		{name: "skip-without-reason", body: `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"fifo","skipped":true}}`},
		{name: "unmarked-special", body: `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"fifo"}}`},
		{name: "error-without-message", body: `{"schema_version":1,"status":"error","error":{"kind":"not_found"}}`},
		{name: "unknown-skip", body: `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"fifo","skipped":true,"skip_reason":"invented"}}`},
		{name: "bad-hash", body: `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"regular","link_count":1,"size_bytes":1,"sha256":"bad","mime_type":"text/plain","data_base64":"YQ=="}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sandbox := daytonaSandboxHandle{Process: staticCaptureResponseProcess{body: test.body}}
			if _, err := daytonaCaptureOutputEntry(context.Background(), sandbox, outputCaptureRoot+"/a", 10, 10, 1024); err == nil {
				t.Fatalf("malformed envelope accepted: %s", test.body)
			}
		})
	}
	if _, err := outputCaptureFileFromResult(outputCaptureRoot+"/a", protocol.CaptureResult{Kind: "regular", LinkCount: 1, SizeBytes: 1, DataBase64: "%%%"}); err == nil {
		t.Fatal("malformed captured bytes were accepted")
	}
	if _, err := outputCaptureFileFromResult(outputCaptureRoot+"/a", protocol.CaptureResult{
		Kind: "regular", LinkCount: 1, SizeBytes: 1, SHA256: strings.Repeat("0", 64), DataBase64: "YQ==",
	}); err == nil {
		t.Fatal("captured bytes with a mismatched hash were accepted")
	}
}

func TestDaytonaOutputCaptureAcceptsZeroLinkUnlinkRaceAsSkipped(t *testing.T) {
	const body = `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"regular","link_count":0,"size_bytes":1,"skipped":true,"skip_reason":"multiple_links"}}`
	sandbox := daytonaSandboxHandle{Process: staticCaptureResponseProcess{body: body}}
	result, err := daytonaCaptureOutputEntry(context.Background(), sandbox, outputCaptureRoot+"/a", 10, 10, 1024)
	if err != nil {
		t.Fatalf("zero-link skip: %v", err)
	}
	if !result.Skipped || result.SkipReason != "multiple_links" || result.LinkCount != 0 {
		t.Fatalf("zero-link result = %+v; want multiple_links skip", result)
	}
}

func TestDaytonaOutputCaptureAcceptsZeroLinkDirectoryRaceAsRecursionSignal(t *testing.T) {
	const body = `{"schema_version":1,"status":"success","result":{"source_path":"/mnt/session/outputs/a","kind":"directory","link_count":0,"size_bytes":0}}`
	sandbox := daytonaSandboxHandle{Process: staticCaptureResponseProcess{body: body}}
	result, err := daytonaCaptureOutputEntry(context.Background(), sandbox, outputCaptureRoot+"/a", 10, 10, 1024)
	if err != nil {
		t.Fatalf("zero-link directory: %v", err)
	}
	if result.Kind != "directory" || result.LinkCount != 0 || result.Skipped {
		t.Fatalf("zero-link directory result = %+v; want recursion signal", result)
	}
}

func TestDaytonaOutputCaptureRealHelperContinuesAfterMaxFiles(t *testing.T) {
	helper := buildOutputCaptureHelper(t)
	root := t.TempDir()
	mustWriteOutputFixture(t, filepath.Join(root, "a.txt"), "a")
	if err := os.Mkdir(filepath.Join(root, "b-dir"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	mustWriteOutputFixture(t, filepath.Join(root, "b-dir", "b.txt"), "b")
	mustWriteOutputFixture(t, filepath.Join(root, "c.txt"), "c")
	services := localOutputCaptureServices{root: root, helper: helper}
	executor := NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (DaytonaSandboxServices, error) {
		return DaytonaSandboxServices{Process: services}, nil
	})

	scan, err := executor.CaptureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_max_files", MaxFiles: 1, MaxFileBytes: 10, MaxTotalBytes: 10,
	})
	if err != nil {
		t.Fatalf("capture MaxFiles overflow: %v", err)
	}
	got := make(map[string]OutputCaptureFile, len(scan.Files))
	for _, file := range scan.Files {
		got[file.SourcePath] = file
	}
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/a.txt", false, "")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/b-dir/b.txt", true, "budget")
	assertOutputCaptureDisposition(t, got, outputCaptureRoot+"/c.txt", true, "budget")
}

func TestDaytonaOutputCaptureDoesNotHideAbortBehindExpiredScanBudget(t *testing.T) {
	client := captureSandboxGetter{handle: daytonaSandboxHandle{Process: delayedErrorCaptureProcess{}}}
	executor := NewDaytonaHelperExecutorForClient(client)
	_, err := executor.captureOutputs(context.Background(), OutputCaptureTarget{
		ProviderSandboxID: "provider_late_abort", MaxFiles: 10, MaxFileBytes: 10, MaxTotalBytes: 10,
	}, 10, time.Now().Add(time.Millisecond))
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Stage != sandbox.StageExecuteTool {
		t.Fatalf("late abort err = %#v; want classified execute-tool failure", err)
	}
}

type captureSandboxGetter struct {
	handle daytonaSandboxHandle
	err    error
}

func (g captureSandboxGetter) Get(context.Context, string) (daytonaSandboxHandle, error) {
	return g.handle, g.err
}

type captureErrorProcess struct {
	err error
}

func (p captureErrorProcess) ExecuteCommand(context.Context, string, ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	return nil, p.err
}

type captureProcess struct {
	commands []string
}

func (p *captureProcess) ExecuteCommand(_ context.Context, command string, _ ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	p.commands = append(p.commands, command)
	remote, _, _, _, err := parseOutputCaptureCommand(command)
	if err != nil {
		return nil, err
	}
	if remote == outputCaptureRoot {
		result := fmt.Sprintf(`{"schema_version":1,"status":"success","result":{"source_path":%q,"kind":"directory","link_count":2,"size_bytes":0,"entries":["report.txt"]}}`, remote)
		return &types.ExecuteResponse{ExitCode: 0, Result: result}, nil
	}
	path := "/mnt/session/outputs/report.txt"
	body := "captured body"
	result := fmt.Sprintf(`{"schema_version":1,"status":"success","result":{"source_path":%q,"kind":"regular","link_count":1,"size_bytes":%d,"sha256":"343e5564b69e7e1d964f81995f66c0836156ac283ea7601921051d299145dcf3","mime_type":"text/plain; charset=utf-8","data_base64":%q}}`, path, len(body), base64.StdEncoding.EncodeToString([]byte(body)))
	return &types.ExecuteResponse{ExitCode: 0, Result: result}, nil
}

type staticCaptureResponseProcess struct {
	body     string
	exitCode int
}

type delayedErrorCaptureProcess struct{}

func (delayedErrorCaptureProcess) ExecuteCommand(context.Context, string, ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	time.Sleep(5 * time.Millisecond)
	return nil, errors.New("transport failed")
}

func (p staticCaptureResponseProcess) ExecuteCommand(context.Context, string, ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	return &types.ExecuteResponse{ExitCode: p.exitCode, Result: p.body}, nil
}

type localOutputCaptureServices struct {
	root             string
	helper           string
	observedMaxBytes map[string]int64
}

func (s localOutputCaptureServices) ExecuteCommand(ctx context.Context, command string, _ ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	remote, maxBytes, maxEntries, maxNameBytes, err := parseOutputCaptureCommand(command)
	if err != nil {
		return nil, err
	}
	if s.observedMaxBytes != nil {
		s.observedMaxBytes[remote] = maxBytes
	}
	script := "mount -t tmpfs tmpfs /mnt && mkdir -p /mnt/session/outputs && mount --bind \"$1\" /mnt/session/outputs && exec \"$2\" __capture --path \"$3\" --max-bytes \"$4\" --max-entries \"$5\" --max-name-bytes \"$6\""
	cmd := exec.CommandContext(ctx, "unshare", "-Ur", "-m", "sh", "-c", script, "capture-test", s.root, s.helper, remote, strconv.FormatInt(maxBytes, 10), strconv.Itoa(maxEntries), strconv.FormatInt(maxNameBytes, 10))
	output, runErr := cmd.Output()
	if runErr == nil {
		return &types.ExecuteResponse{ExitCode: 0, Result: string(output)}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return &types.ExecuteResponse{ExitCode: exitErr.ExitCode(), Result: string(output)}, nil
	}
	return nil, runErr
}

func parseOutputCaptureCommand(command string) (string, int64, int, int64, error) {
	const pathMarker = " --path '"
	const maxMarker = "' --max-bytes "
	const entriesMarker = " --max-entries "
	const namesMarker = " --max-name-bytes "
	pathStart := strings.Index(command, pathMarker)
	if pathStart < 0 {
		return "", 0, 0, 0, fmt.Errorf("capture command lacks path: %q", command)
	}
	pathStart += len(pathMarker)
	pathEnd := strings.Index(command[pathStart:], maxMarker)
	if pathEnd < 0 {
		return "", 0, 0, 0, fmt.Errorf("capture command lacks max bytes: %q", command)
	}
	remote := command[pathStart : pathStart+pathEnd]
	bounds := command[pathStart+pathEnd+len(maxMarker):]
	entriesAt := strings.Index(bounds, entriesMarker)
	namesAt := strings.Index(bounds, namesMarker)
	if entriesAt < 0 || namesAt < 0 || namesAt <= entriesAt {
		return "", 0, 0, 0, fmt.Errorf("capture command lacks directory bounds: %q", command)
	}
	maxBytes, err := strconv.ParseInt(strings.TrimSpace(bounds[:entriesAt]), 10, 64)
	if err != nil {
		return "", 0, 0, 0, err
	}
	maxEntries, err := strconv.Atoi(strings.TrimSpace(bounds[entriesAt+len(entriesMarker) : namesAt]))
	if err != nil {
		return "", 0, 0, 0, err
	}
	maxNameBytes, err := strconv.ParseInt(strings.TrimSpace(bounds[namesAt+len(namesMarker):]), 10, 64)
	return remote, maxBytes, maxEntries, maxNameBytes, err
}

func buildOutputCaptureHelper(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve output capture test source")
	}
	engineRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
	binary := filepath.Join(t.TempDir(), "sandbox")
	cmd := exec.Command("go", "build", "-o", binary, "./internal/sandbox/helper/cmd/sandbox")
	cmd.Dir = engineRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build sandbox helper: %v\n%s", err, output)
	}
	return binary
}

func mustWriteOutputFixture(t *testing.T, path string, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write output fixture %s: %v", filepath.Base(path), err)
	}
}

func assertOutputCaptureDisposition(t *testing.T, files map[string]OutputCaptureFile, path string, skipped bool, reason string) {
	t.Helper()
	file, ok := files[path]
	if !ok {
		t.Fatalf("output scan omitted %s; got %d paths", path, len(files))
	}
	if file.Skipped != skipped || file.SkipReason != reason {
		t.Fatalf("output %s disposition = skipped:%t reason:%q; want %t/%q", path, file.Skipped, file.SkipReason, skipped, reason)
	}
}
