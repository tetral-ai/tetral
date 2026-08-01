package tetralsandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/pathvalidation"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const (
	outputCaptureRoot          = "/mnt/session/outputs"
	outputCaptureMaxFiles      = 100
	outputCaptureMaxFileBytes  = 10 * 1024 * 1024
	outputCaptureMaxTotalBytes = 50 * 1024 * 1024
)

type SandboxOutputCaptureJob struct {
	JobID             string
	LeaseToken        string
	WorkspaceID       string
	SessionID         string
	FinishIdleWriteID string
	CaptureGeneration int64
}

type SandboxOutputCaptureWork struct {
	SandboxOutputCaptureJob
	SessionThreadID    string
	BindingID          string
	BindingGeneration  int64
	LogicalSandboxID   string
	Provider           string
	ProviderResourceID string
	BindingRevision    int64
	ProviderAvailable  bool
	Existing           map[string]SandboxOutputCaptureIndexEntry
}

type SandboxOutputCaptureIndexEntry struct {
	FileID    string
	SizeBytes int64
	SHA256    string
}

type SandboxOutputCaptureManifestEntry struct {
	SourcePath     string `json:"source_path"`
	Filename       string `json:"filename"`
	MIMEType       string `json:"mime_type"`
	SizeBytes      int64  `json:"size_bytes"`
	SHA256         string `json:"sha256"`
	ExistingFileID string `json:"existing_file_id,omitempty"`
	BlobPointer    string `json:"blob_pointer,omitempty"`
}

type SandboxOutputCaptureSkippedFile struct {
	SourcePath string `json:"source_path"`
	Reason     string `json:"reason"`
	SizeBytes  int64  `json:"size_bytes"`
}

type SandboxOutputCaptureScanRecord struct {
	ParentPath string `json:"parent_path"`
	Reason     string `json:"reason"`
	Count      int    `json:"count"`
}

type SandboxOutputCaptureStore interface {
	LoadCapture(context.Context, SandboxOutputCaptureJob, time.Time) (SandboxOutputCaptureWork, bool, error)
	EnsureBlobStage(context.Context, SandboxOutputCaptureWork, SandboxOutputCaptureManifestEntry, time.Time) (string, error)
	MarkBlobUploaded(context.Context, SandboxOutputCaptureWork, string, time.Time) error
	StageCapture(context.Context, SandboxOutputCaptureWork, []SandboxOutputCaptureManifestEntry, []SandboxOutputCaptureSkippedFile, []SandboxOutputCaptureScanRecord, bool, string, string, time.Time) error
	FailCapture(context.Context, SandboxOutputCaptureWork, string, string, time.Time) error
	FinalizeCaptureExhaustion(context.Context, *queuev1.QueueJob, time.Time) error
}

type SandboxOutputCaptureRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type SandboxOutputCaptureJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxOutputCaptureStore
	Providers *ProviderRegistry
	BlobStore blob.BlobStore
	Logger    *slog.Logger
	Config    SandboxOutputCaptureRunnerConfig
	Clock     func() time.Time
}

func (r *SandboxOutputCaptureJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxOutputCaptureJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil || r.BlobStore == nil {
		return false, errors.New("sandbox output capture dependencies are required")
	}
	cfg := r.Config
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.WorkspaceID == "" || cfg.LeaseDuration <= cfg.HeartbeatInterval || cfg.HeartbeatInterval <= 0 {
		return false, errors.New("sandbox output capture runner configuration is invalid")
	}
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxOutputCapture},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxOutputCaptureJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxOutputCaptureRunnerConfig) error {
	if queueJob.GetMaxAttempts() <= 0 || queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := r.Store.FinalizeCaptureExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_output_capture_exhausted")
	}
	job, err := DecodeSandboxOutputCaptureJob(queueJob)
	if err != nil {
		if err := r.Store.FinalizeCaptureExhaustion(ctx, queueJob, r.now()); err != nil {
			return err
		}
		return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "invalid_sandbox_output_capture_payload")
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	err = r.capture(workCtx, job)
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if err != nil {
		if queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts() {
			if err := r.Store.FinalizeCaptureExhaustion(ctx, queueJob, r.now()); err != nil {
				return err
			}
			return deadLetterBackgroundJob(ctx, r.Queue, queueJob, "sandbox_output_capture_exhausted")
		}
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
			ErrorKind: "sandbox_output_capture_retryable", ErrorMessage: "sandbox output capture will be retried",
		}))
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxOutputCaptureJobRunner) capture(ctx context.Context, job SandboxOutputCaptureJob) error {
	work, current, err := r.Store.LoadCapture(ctx, job, r.now())
	if err != nil || !current {
		return err
	}
	if !work.ProviderAvailable {
		return r.Store.StageCapture(ctx, work, nil, nil, nil, true, "", "", r.now())
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.Store.FailCapture(ctx, work, "provider_not_registered", "sandbox provider is not registered", r.now())
	}
	readiness := adapter.InspectForExecution(ctx, work.ProviderResourceID)
	if readiness.Failed() {
		if readiness.EffectBoundary == ProviderProvedNotStarted && readiness.Disposition == ProviderRetryable {
			return errors.New("sandbox output capture inspection is retryable")
		}
		return r.Store.FailCapture(ctx, work, valueOrDefault(readiness.ErrorKind, "sandbox_output_capture_inspection_failed"), valueOrDefault(readiness.SafeMessage, "sandbox output capture inspection failed"), r.now())
	}
	if readiness.Value != ExecutionReady {
		return r.Store.StageCapture(ctx, work, nil, nil, nil, true, "", "", r.now())
	}
	capturer, ok := adapter.(OutputCaptureAdapter)
	if !ok {
		return r.Store.FailCapture(ctx, work, "provider_configuration_invalid", "sandbox output capture adapter is unavailable", r.now())
	}
	outcome := capturer.CaptureOutputs(ctx, sandboxdriver.OutputCaptureTarget{
		WorkspaceID: work.WorkspaceID, SessionID: work.SessionID, SessionThreadID: work.SessionThreadID,
		BindingID: work.BindingID, BindingGeneration: work.BindingGeneration, SandboxID: work.LogicalSandboxID,
		ProviderSandboxID: work.ProviderResourceID, MaxFiles: outputCaptureMaxFiles,
		MaxFileBytes: outputCaptureMaxFileBytes, MaxTotalBytes: outputCaptureMaxTotalBytes,
	})
	if outcome.Failed() {
		if strings.HasPrefix(outcome.ErrorKind, "output_capture_scan_") {
			r.logScanFailure(work, outcome.ErrorKind, outcome.SafeMessage)
			return r.Store.StageCapture(ctx, work, nil, nil, nil, false, outcome.ErrorKind, outcome.SafeMessage, r.now())
		}
		if outcome.EffectBoundary == ProviderProvedNotStarted && outcome.Disposition == ProviderRetryable {
			return errors.New("sandbox output capture provider call is retryable")
		}
		return r.Store.FailCapture(ctx, work, valueOrDefault(outcome.ErrorKind, "sandbox_output_capture_failed"), valueOrDefault(outcome.SafeMessage, "sandbox output capture failed"), r.now())
	}
	scan := outcome.Value
	entries, skipped, records, err := normalizeOutputCaptureScan(scan)
	if err != nil {
		r.logScanFailure(work, "normalize_scan", "sandbox output capture scan is malformed")
		return r.Store.StageCapture(ctx, work, nil, nil, nil, false, "normalize_scan", "sandbox output capture scan is malformed", r.now())
	}
	manifest := make([]SandboxOutputCaptureManifestEntry, 0, len(entries))
	for _, entry := range entries {
		manifestEntry := entry.manifest
		if existing, ok := work.Existing[entry.manifest.SourcePath]; ok && existing.FileID != "" && existing.SizeBytes == entry.manifest.SizeBytes && strings.EqualFold(existing.SHA256, entry.manifest.SHA256) {
			manifestEntry.ExistingFileID = existing.FileID
			manifest = append(manifest, manifestEntry)
			continue
		}
		body, digest, readErr := readOutputCaptureBody(ctx, entry.open, entry.manifest.SizeBytes)
		if readErr != nil || !strings.EqualFold(digest, entry.manifest.SHA256) {
			skipped = append(skipped, SandboxOutputCaptureSkippedFile{SourcePath: entry.manifest.SourcePath, Reason: "changed_during_capture", SizeBytes: entry.manifest.SizeBytes})
			continue
		}
		pointer := outputCaptureBlobPointer(work, entry.manifest.SourcePath)
		manifestEntry.BlobPointer = pointer
		state, err := r.Store.EnsureBlobStage(ctx, work, manifestEntry, r.now())
		if err != nil {
			return err
		}
		if state != "uploaded" {
			err = r.BlobStore.Put(ctx, pointer, bytes.NewReader(body), int64(len(body)))
			if err != nil {
				var duplicate *blob.DuplicateKeyError
				if !errors.As(err, &duplicate) {
					return err
				}
				metadata, headErr := r.BlobStore.HeadObject(ctx, pointer)
				if headErr != nil || metadata.SizeBytes != int64(len(body)) {
					return errors.New("sandbox output capture staged blob could not be verified")
				}
			}
			if err := r.Store.MarkBlobUploaded(ctx, work, entry.manifest.SourcePath, r.now()); err != nil {
				return err
			}
		}
		manifest = append(manifest, manifestEntry)
	}
	return r.Store.StageCapture(ctx, work, manifest, skipped, records, false, "", "", r.now())
}

func DecodeSandboxOutputCaptureJob(job *queuev1.QueueJob) (SandboxOutputCaptureJob, error) {
	identity, err := decodeSandboxOutputCaptureTransportIdentity(job)
	if err != nil || job.GetLeaseToken() == "" {
		return SandboxOutputCaptureJob{}, errors.New("sandbox output capture transport identity is incomplete")
	}
	var payload struct {
		WorkspaceID       string `json:"workspace_id"`
		SessionID         string `json:"session_id"`
		FinishIdleWriteID string `json:"finish_idle_write_id"`
		CaptureGeneration int64  `json:"capture_generation"`
	}
	decoder := json.NewDecoder(strings.NewReader(job.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SandboxOutputCaptureJob{}, err
	}
	if payload.WorkspaceID != identity.WorkspaceID || payload.SessionID != identity.SessionID || payload.FinishIdleWriteID != identity.FinishIdleWriteID || payload.CaptureGeneration != identity.CaptureGeneration {
		return SandboxOutputCaptureJob{}, errors.New("sandbox output capture identity is invalid")
	}
	identity.LeaseToken = job.GetLeaseToken()
	return identity, nil
}

type normalizedOutputCaptureEntry struct {
	manifest SandboxOutputCaptureManifestEntry
	open     func(context.Context) (io.ReadCloser, error)
}

func normalizeOutputCaptureScan(scan sandboxdriver.OutputCaptureScan) ([]normalizedOutputCaptureEntry, []SandboxOutputCaptureSkippedFile, []SandboxOutputCaptureScanRecord, error) {
	seen := make(map[string]struct{}, len(scan.Files))
	entries := make([]normalizedOutputCaptureEntry, 0, len(scan.Files))
	skipped := make([]SandboxOutputCaptureSkippedFile, 0)
	totalBytes := int64(0)
	for _, file := range scan.Files {
		sourcePath, err := canonicalOutputCapturePath(file.SourcePath)
		if err != nil {
			return nil, nil, nil, err
		}
		if _, exists := seen[sourcePath]; exists {
			return nil, nil, nil, errors.New("duplicate sandbox output path")
		}
		seen[sourcePath] = struct{}{}
		if file.SizeBytes < 0 || file.Skipped != (strings.TrimSpace(file.SkipReason) != "") {
			return nil, nil, nil, errors.New("sandbox output disposition is invalid")
		}
		if file.Skipped {
			skipped = append(skipped, SandboxOutputCaptureSkippedFile{SourcePath: sourcePath, Reason: file.SkipReason, SizeBytes: file.SizeBytes})
			continue
		}
		digest := strings.ToLower(strings.TrimSpace(file.SHA256))
		filename := path.Base(sourcePath)
		if file.Kind != "regular" || file.LinkCount != 1 || file.Open == nil || file.SizeBytes > outputCaptureMaxFileBytes || !validOutputCaptureSHA256(digest) || !pathvalidation.IsNFC(file.SourcePath) || filename == "" || filename == "." || filename == "/" {
			return nil, nil, nil, errors.New("sandbox output entry is unsafe")
		}
		if len(entries) >= outputCaptureMaxFiles || file.SizeBytes > outputCaptureMaxTotalBytes-totalBytes {
			return nil, nil, nil, errors.New("sandbox output capture budget exceeded")
		}
		totalBytes += file.SizeBytes
		mimeType := strings.TrimSpace(file.MIMEType)
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		entries = append(entries, normalizedOutputCaptureEntry{manifest: SandboxOutputCaptureManifestEntry{SourcePath: sourcePath, Filename: filename, MIMEType: mimeType, SizeBytes: file.SizeBytes, SHA256: digest}, open: file.Open})
	}
	records := make([]SandboxOutputCaptureScanRecord, 0, len(scan.Records))
	for _, record := range scan.Records {
		records = append(records, SandboxOutputCaptureScanRecord{ParentPath: record.ParentPath, Reason: record.Reason, Count: record.Count})
	}
	return entries, skipped, records, nil
}

func canonicalOutputCapturePath(raw string) (string, error) {
	if raw == "" || !utf8.ValidString(raw) || strings.ContainsRune(raw, 0) || !strings.HasPrefix(raw, "/") {
		return "", errors.New("sandbox output path is invalid")
	}
	cleaned := path.Clean(raw)
	if cleaned == outputCaptureRoot || !strings.HasPrefix(cleaned, outputCaptureRoot+"/") {
		return "", errors.New("sandbox output path escapes output root")
	}
	return cleaned, nil
}

func validOutputCaptureSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readOutputCaptureBody(ctx context.Context, open func(context.Context) (io.ReadCloser, error), expected int64) ([]byte, string, error) {
	reader, err := open(ctx)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = reader.Close() }()
	body, err := io.ReadAll(io.LimitReader(reader, outputCaptureMaxFileBytes+1))
	if err != nil || int64(len(body)) != expected || int64(len(body)) > outputCaptureMaxFileBytes {
		return nil, "", errors.New("sandbox output changed during capture")
	}
	digest := sha256.Sum256(body)
	return body, hex.EncodeToString(digest[:]), nil
}

func outputCaptureBlobPointer(work SandboxOutputCaptureWork, sourcePath string) string {
	identity := sha256.Sum256([]byte(work.WorkspaceID + "\x00" + work.SessionID + "\x00" + work.FinishIdleWriteID + "\x00" + sourcePath))
	return "output-captures/" + work.WorkspaceID + "/" + hex.EncodeToString(identity[:]) + "/" + strconv.FormatInt(work.CaptureGeneration, 10)
}

func boundedSandboxOutputCaptureDetail(detail string) string {
	const maxBytes = 512
	if len(detail) <= maxBytes {
		return detail
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(detail[:end]) {
		end--
	}
	return detail[:end]
}

func (r *SandboxOutputCaptureJobRunner) logScanFailure(work SandboxOutputCaptureWork, kind string, detail string) {
	if r.Logger == nil {
		return
	}
	r.Logger.Error("sandbox.output_capture.scan_failed",
		slog.String("operation", "output_capture.finish_idle"), slog.String("event.kind", "output_capture.scan_failed"),
		slog.String("component", ServiceName), slog.String("workspace.id", work.WorkspaceID), slog.String("session.id", work.SessionID),
		slog.String("error.class", "output_capture_scan_error"), slog.String("error.code", kind),
		slog.String("error.capture_detail", boundedSandboxOutputCaptureDetail(detail)), slog.Bool("retryable", false), slog.String("alert.family", "output_capture"),
	)
}

func (r *SandboxOutputCaptureJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}
