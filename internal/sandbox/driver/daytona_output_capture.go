package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/pathvalidation"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	outputCaptureRoot                = "/mnt/session/outputs"
	outputCaptureEnumerationCeiling  = 10_000
	outputCaptureNameBytesCeiling    = 512 * 1024
	outputCaptureWallClockBudget     = 2 * time.Minute
	outputCaptureBudgetSkipReason    = "budget"
	outputCaptureSymlinkSkipReason   = "symlink"
	outputCaptureRootUnavailableKind = "root_unavailable"
)

type outputCaptureWalk struct {
	target               OutputCaptureTarget
	files                []OutputCaptureFile
	capturedFiles        int
	capturedBytes        int64
	workUnits            int
	workUnitLimit        int
	deadline             time.Time
	truncated            bool
	unrepresentableNames int
	records              []OutputCaptureScanRecord
}

// OutputCaptureEntryError carries a helper-authored capture classification
// without exposing it through an unstructured error string.
type OutputCaptureEntryError struct {
	Kind    string
	Message string
}

func (e *OutputCaptureEntryError) Error() string {
	return "daytona capture failed with " + e.Kind
}

func (e *DaytonaHelperExecutor) CaptureOutputs(ctx context.Context, target OutputCaptureTarget) (OutputCaptureScan, error) {
	return e.captureOutputs(ctx, target, outputCaptureEnumerationCeiling, time.Now().Add(outputCaptureWallClockBudget))
}

func (e *DaytonaHelperExecutor) captureOutputs(ctx context.Context, target OutputCaptureTarget, workUnitLimit int, deadline time.Time) (OutputCaptureScan, error) {
	if e == nil || e.client == nil {
		return OutputCaptureScan{}, errors.New("daytona sandbox client is unavailable")
	}
	if target.ProviderSandboxID == "" {
		return OutputCaptureScan{}, errors.New("provider sandbox id is required")
	}
	sandbox, err := e.client.Get(ctx, target.ProviderSandboxID)
	if err != nil {
		return OutputCaptureScan{}, err
	}
	if sandbox.Process == nil {
		return OutputCaptureScan{}, errors.New("daytona sandbox is missing process service")
	}
	walk := outputCaptureWalk{target: target, workUnitLimit: workUnitLimit, deadline: deadline}
	if err := walk.scanPath(ctx, sandbox, outputCaptureRoot); err != nil {
		return OutputCaptureScan{}, err
	}
	return OutputCaptureScan{
		Files:                walk.files,
		Truncated:            walk.truncated,
		UnrepresentableNames: walk.unrepresentableNames,
		Records:              walk.records,
	}, nil
}

func (w *outputCaptureWalk) scanPath(ctx context.Context, sandbox daytonaSandboxHandle, remotePath string) error {
	if !w.consumeWorkUnit() {
		return nil
	}
	maxBytes, captureBudgetLimited := w.captureMaxBytes()
	remainingEntries := max(w.workUnitLimit-w.workUnits, 0)
	capture, err := daytonaCaptureOutputEntry(
		ctx,
		sandbox,
		remotePath,
		maxBytes,
		remainingEntries,
		outputCaptureNameBytesCeiling,
	)
	if err != nil {
		var captureErr *OutputCaptureEntryError
		if !errors.As(err, &captureErr) {
			return err
		}
		if remotePath == outputCaptureRoot {
			return err
		}
		switch captureErr.Kind {
		case "not_found":
			return nil
		case "unsafe_path", "changed_during_capture", "permission_denied", "capture_failed":
			w.files = append(w.files, skippedOutputCaptureFile(remotePath, "", 0, captureErr.Kind))
			return nil
		case outputCaptureRootUnavailableKind, "invalid_input", "path_escape":
			return err
		default:
			return fmt.Errorf("daytona capture returned unsupported error kind for %s", remotePath)
		}
	}
	if !w.withinWallClock() {
		return nil
	}
	w.unrepresentableNames += capture.UnrepresentableNames
	if capture.UnrepresentableNames > 0 {
		w.records = append(w.records, OutputCaptureScanRecord{
			ParentPath: remotePath,
			Reason:     "unrepresentable_names",
			Count:      capture.UnrepresentableNames,
		})
	}
	if remotePath != outputCaptureRoot {
		filename := path.Base(remotePath)
		if !utf8.ValidString(filename) || !pathvalidation.IsNFC(filename) {
			w.files = append(w.files, skippedOutputCaptureFile(remotePath, capture.Kind, capture.SizeBytes, "invalid_filename"))
			return nil
		}
	}
	if capture.Kind == "directory" {
		if capture.Skipped {
			if remotePath == outputCaptureRoot {
				w.records = append(w.records, OutputCaptureScanRecord{
					ParentPath: remotePath,
					Reason:     "root_unreadable",
					Count:      1,
				})
			} else {
				w.files = append(w.files, skippedOutputCaptureFile(remotePath, capture.Kind, capture.SizeBytes, capture.SkipReason))
			}
			return nil
		}
		if capture.EntriesTruncated {
			w.truncated = true
		}
		for _, name := range capture.Entries {
			if !w.withinWallClock() {
				return nil
			}
			childPath, err := outputCaptureChildPath(remotePath, name)
			if err != nil {
				return err
			}
			if w.workUnits >= w.workUnitLimit {
				w.truncated = true
				return nil
			}
			if err := w.scanPath(ctx, sandbox, childPath); err != nil {
				return err
			}
		}
		return nil
	}
	if remotePath == outputCaptureRoot {
		return fmt.Errorf("daytona capture root classified as %s", capture.Kind)
	}
	if capture.Skipped {
		reason := capture.SkipReason
		if capture.Kind == "symlink" {
			reason = outputCaptureSymlinkSkipReason
		}
		if captureBudgetLimited && capture.Kind == "regular" && capture.SkipReason == "file_too_large" {
			reason = outputCaptureBudgetSkipReason
		}
		w.files = append(w.files, skippedOutputCaptureFile(remotePath, capture.Kind, capture.SizeBytes, reason))
		return nil
	}
	file, err := outputCaptureFileFromResult(remotePath, capture)
	if err != nil {
		return err
	}
	if w.target.MaxFiles > 0 && w.capturedFiles >= w.target.MaxFiles ||
		w.target.MaxTotalBytes > 0 && capture.SizeBytes > w.target.MaxTotalBytes-w.capturedBytes {
		w.files = append(w.files, skippedOutputCaptureFile(remotePath, capture.Kind, capture.SizeBytes, outputCaptureBudgetSkipReason))
		return nil
	}
	w.files = append(w.files, file)
	w.capturedFiles++
	w.capturedBytes += file.SizeBytes
	return nil
}

func (w *outputCaptureWalk) consumeWorkUnit() bool {
	if !w.withinWallClock() {
		return false
	}
	if w.workUnits >= w.workUnitLimit {
		w.truncated = true
		return false
	}
	w.workUnits++
	return true
}

func (w *outputCaptureWalk) withinWallClock() bool {
	if time.Now().Before(w.deadline) {
		return true
	}
	w.truncated = true
	return false
}

func (w *outputCaptureWalk) captureMaxBytes() (int64, bool) {
	if w.target.MaxFiles > 0 && w.capturedFiles >= w.target.MaxFiles {
		return 0, true
	}
	maxBytes := w.target.MaxFileBytes
	if w.target.MaxTotalBytes <= 0 {
		return maxBytes, false
	}
	remaining := w.target.MaxTotalBytes - w.capturedBytes
	if remaining <= 0 {
		return 0, true
	}
	if maxBytes <= 0 || remaining < maxBytes {
		return remaining, true
	}
	return maxBytes, false
}

func skippedOutputCaptureFile(sourcePath string, kind string, sizeBytes int64, reason string) OutputCaptureFile {
	return OutputCaptureFile{SourcePath: sourcePath, Kind: kind, SizeBytes: sizeBytes, Skipped: true, SkipReason: reason}
}

func outputCaptureFileFromResult(childPath string, capture protocol.CaptureResult) (OutputCaptureFile, error) {
	body, err := base64.StdEncoding.DecodeString(capture.DataBase64)
	if err != nil {
		return OutputCaptureFile{}, fmt.Errorf("daytona capture returned invalid base64 for %s", childPath)
	}
	if int64(len(body)) != capture.SizeBytes {
		return OutputCaptureFile{}, fmt.Errorf("daytona capture returned mismatched bytes for %s", childPath)
	}
	digest := sha256.Sum256(body)
	if !strings.EqualFold(hex.EncodeToString(digest[:]), capture.SHA256) {
		return OutputCaptureFile{}, fmt.Errorf("daytona capture returned mismatched hash for %s", childPath)
	}
	return OutputCaptureFile{
		SourcePath: childPath,
		Kind:       capture.Kind,
		LinkCount:  capture.LinkCount,
		SizeBytes:  capture.SizeBytes,
		SHA256:     capture.SHA256,
		MIMEType:   capture.MIMEType,
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		},
	}, nil
}

func outputCaptureChildPath(directory string, name string) (string, error) {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("unsafe output capture entry name %q", name)
	}
	candidate := path.Clean(path.Join(directory, name))
	if candidate != outputCaptureRoot && !strings.HasPrefix(candidate, outputCaptureRoot+"/") {
		return "", fmt.Errorf("output capture path %q escapes %s", candidate, outputCaptureRoot)
	}
	return candidate, nil
}

func daytonaCaptureOutputEntry(
	ctx context.Context,
	sandbox daytonaSandboxHandle,
	remotePath string,
	maxBytes int64,
	maxEntries int,
	maxNameBytes int64,
) (protocol.CaptureResult, error) {
	command := "sudo -n -u " + shellQuote(helperUser) + " " + shellQuote(helperPath) + " __capture --path " + shellQuote(remotePath) +
		" --max-bytes " + strconv.FormatInt(maxBytes, 10) +
		" --max-entries " + strconv.Itoa(maxEntries) +
		" --max-name-bytes " + strconv.FormatInt(maxNameBytes, 10)
	response, err := sandbox.Process.ExecuteCommand(ctx, command)
	if err != nil {
		return protocol.CaptureResult{}, err
	}
	if response == nil {
		return protocol.CaptureResult{}, errors.New("daytona capture returned no response")
	}
	if response.ExitCode != 0 {
		return protocol.CaptureResult{}, fmt.Errorf("daytona capture transport failed for %s with exit code %d", remotePath, response.ExitCode)
	}
	var envelope protocol.CaptureEnvelope
	if err := json.Unmarshal([]byte(response.Result), &envelope); err != nil {
		return protocol.CaptureResult{}, fmt.Errorf("daytona capture returned malformed output for %s", remotePath)
	}
	if envelope.SchemaVersion != protocol.SchemaVersion {
		return protocol.CaptureResult{}, fmt.Errorf("daytona capture returned unsupported schema for %s", remotePath)
	}
	if envelope.Status == protocol.ToolStatusError && validCaptureErrorEnvelope(envelope) {
		return protocol.CaptureResult{}, &OutputCaptureEntryError{
			Kind:    envelope.Error.Kind,
			Message: envelope.Error.Message,
		}
	}
	if envelope.Status != protocol.ToolStatusSuccess || envelope.Result == nil || envelope.Error != nil {
		return protocol.CaptureResult{}, fmt.Errorf("daytona capture returned invalid envelope for %s", remotePath)
	}
	if envelope.Result.SourcePath != remotePath {
		return protocol.CaptureResult{}, fmt.Errorf("daytona capture returned mismatched path for %s", remotePath)
	}
	if !validCaptureResult(*envelope.Result, maxBytes, maxEntries, maxNameBytes) {
		return protocol.CaptureResult{}, fmt.Errorf("daytona capture returned malformed result for %s", remotePath)
	}
	return *envelope.Result, nil
}

func validCaptureErrorEnvelope(envelope protocol.CaptureEnvelope) bool {
	if envelope.Error == nil || envelope.Result != nil || strings.TrimSpace(envelope.Error.Message) == "" {
		return false
	}
	switch envelope.Error.Kind {
	case "invalid_input", "path_escape", "permission_denied", outputCaptureRootUnavailableKind,
		"not_found", "unsafe_path", "capture_failed", "changed_during_capture":
		return true
	default:
		return false
	}
}

func validCaptureResult(result protocol.CaptureResult, maxBytes int64, maxEntries int, maxNameBytes int64) bool {
	if result.Kind == "" || result.SizeBytes < 0 || result.UnrepresentableNames < 0 || result.Skipped != (result.SkipReason != "") {
		return false
	}
	if result.Skipped {
		if result.SHA256 != "" || result.MIMEType != "" || result.DataBase64 != "" ||
			len(result.Entries) != 0 || result.EntriesTruncated || result.UnrepresentableNames != 0 {
			return false
		}
		if result.Kind == "directory" {
			return result.SkipReason == "unreadable"
		}
		switch result.SkipReason {
		case "non_regular":
			switch result.Kind {
			case "fifo", "symlink", "socket", "character_device", "block_device", "non_regular":
				return true
			default:
				return false
			}
		case "multiple_links":
			return result.Kind == "regular" && result.LinkCount != 1
		case "file_too_large":
			return result.Kind == "regular" && result.LinkCount == 1 && (maxBytes == 0 || result.SizeBytes > maxBytes)
		default:
			return false
		}
	}
	if result.SkipReason != "" {
		return false
	}
	if result.Kind == "directory" {
		if result.SizeBytes != 0 || result.SHA256 != "" || result.MIMEType != "" || result.DataBase64 != "" ||
			len(result.Entries)+result.UnrepresentableNames > maxEntries ||
			!sort.StringsAreSorted(result.Entries) {
			return false
		}
		var nameBytes int64
		previous := ""
		for index, name := range result.Entries {
			if _, err := outputCaptureChildPath(result.SourcePath, name); err != nil {
				return false
			}
			if index > 0 && name == previous {
				return false
			}
			previous = name
			nameBytes += int64(len(name))
		}
		return nameBytes <= maxNameBytes
	}
	if len(result.Entries) != 0 || result.EntriesTruncated || result.UnrepresentableNames != 0 {
		return false
	}
	return maxBytes > 0 && result.Kind == "regular" && result.LinkCount == 1 && result.SizeBytes <= maxBytes && validCaptureSHA256(result.SHA256) && result.MIMEType != "" && (result.DataBase64 != "" || result.SizeBytes == 0)
}

func validCaptureSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
