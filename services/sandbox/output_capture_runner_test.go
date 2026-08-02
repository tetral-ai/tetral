package tetralsandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSandboxOutputCaptureRunnerStagesBlobBeforeCaptureResult(t *testing.T) {
	body := []byte("captured output")
	digest := sha256.Sum256(body)
	queueJob := &queuev1.QueueJob{
		Id: "qjob_capture", WorkspaceId: "ws_capture", Kind: queue.KindSandboxOutputCapture,
		PartitionKey: queue.FormatSandboxCapturePartitionKey("ws_capture", "sesn_capture", "rwrite_idle"),
		DedupeKey:    queue.FormatSandboxOutputCaptureDedupeKey("ws_capture", "sesn_capture", "rwrite_idle", 1),
		PayloadJson:  `{"workspace_id":"ws_capture","session_id":"sesn_capture","finish_idle_write_id":"rwrite_idle","capture_generation":1}`,
		LeaseToken:   "lease_capture", AttemptCount: 1, MaxAttempts: queue.SandboxOutputCaptureMaxAttempts,
	}
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{queueJob}}
	store := &recordingOutputCaptureStore{current: true, work: SandboxOutputCaptureWork{
		SandboxOutputCaptureJob: SandboxOutputCaptureJob{WorkspaceID: "ws_capture", SessionID: "sesn_capture", FinishIdleWriteID: "rwrite_idle", CaptureGeneration: 1},
		SessionThreadID:         "thr_capture", BindingID: "bind_capture", BindingGeneration: 1,
		LogicalSandboxID: "sbox_capture", Provider: sandboxdriver.DaytonaProviderName, ProviderResourceID: "provider_capture", ProviderAvailable: true,
		Existing: map[string]SandboxOutputCaptureIndexEntry{},
	}}
	adapter := &recordingOutputCaptureAdapter{scan: sandboxdriver.OutputCaptureScan{Files: []sandboxdriver.OutputCaptureFile{{
		SourcePath: "/mnt/session/outputs/result.txt", Kind: "regular", LinkCount: 1,
		SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(digest[:]), MIMEType: "text/plain",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil },
	}}}}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	blobStore := blob.NewFakeBlobStore()
	runner := &SandboxOutputCaptureJobRunner{
		Queue: queueClient, Store: store, Providers: registry, BlobStore: blobStore,
		Config: SandboxOutputCaptureRunnerConfig{WorkspaceID: "ws_capture", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"load", "ensure", "uploaded", "stage"}) {
		t.Fatalf("store calls = %v; want blob stage before parent stage", store.calls)
	}
	if len(store.manifest) != 1 || store.manifest[0].BlobPointer == "" || !blobStore.Has(store.manifest[0].BlobPointer) {
		t.Fatalf("staged manifest/blob = %#v; want one durable blob reference", store.manifest)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_capture"}) {
		t.Fatalf("queue transitions = %v; want capture ack", queueClient.transitions)
	}
}

func TestSandboxOutputCaptureRunnerDoesNotDowngradeProviderFailureToEmptyCapture(t *testing.T) {
	store := &recordingOutputCaptureStore{current: true, work: SandboxOutputCaptureWork{
		SandboxOutputCaptureJob: SandboxOutputCaptureJob{WorkspaceID: "ws_capture", SessionID: "sesn_capture", FinishIdleWriteID: "rwrite_idle", CaptureGeneration: 1},
		SessionThreadID:         "thr_capture", BindingID: "bind_capture", BindingGeneration: 1,
		LogicalSandboxID: "sbox_capture", Provider: sandboxdriver.DaytonaProviderName, ProviderResourceID: "provider_capture", ProviderAvailable: true,
	}}
	failed := terminalProviderFailure[sandboxdriver.OutputCaptureScan]("provider_response_malformed", "provider returned an unclassified failure")
	adapter := &recordingOutputCaptureAdapter{outcome: &failed}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	runner := &SandboxOutputCaptureJobRunner{Store: store, Providers: registry, BlobStore: blob.NewFakeBlobStore()}
	if err := runner.capture(context.Background(), store.work.SandboxOutputCaptureJob); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"load", "failed"}) {
		t.Fatalf("store calls = %v; want terminal failure without staged empty result", store.calls)
	}
}

func TestSandboxOutputCaptureCleanupDeletesStagedBlobsBeforeClosingCustody(t *testing.T) {
	blobStore := blob.NewFakeBlobStore()
	if err := blobStore.Put(context.Background(), "output-captures/staged", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	queueJob := &queuev1.QueueJob{
		Id: "qjob_capture_cleanup", WorkspaceId: "ws_capture", Kind: queue.KindSandboxOutputCaptureCleanup,
		PartitionKey: queue.FormatSandboxCapturePartitionKey("ws_capture", "sesn_capture", "rwrite_idle"),
		DedupeKey:    queue.FormatSandboxOutputCaptureCleanupDedupeKey("ws_capture", "sesn_capture", "rwrite_idle", 1, 1),
		PayloadJson:  `{"workspace_id":"ws_capture","session_id":"sesn_capture","finish_idle_write_id":"rwrite_idle","capture_generation":1,"cleanup_generation":1}`,
		LeaseToken:   "lease_cleanup", AttemptCount: 1, MaxAttempts: queue.SandboxOutputCaptureCleanupMaxAttempts,
	}
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{queueJob}}
	store := &recordingOutputCaptureCleanupStore{current: true, work: SandboxOutputCaptureCleanupWork{
		SandboxOutputCaptureCleanupJob: SandboxOutputCaptureCleanupJob{WorkspaceID: "ws_capture", SessionID: "sesn_capture", FinishIdleWriteID: "rwrite_idle", CaptureGeneration: 1, CleanupGeneration: 1},
		BlobPointers:                   []string{"output-captures/staged"},
	}}
	runner := &SandboxOutputCaptureCleanupRunner{
		Queue: queueClient, Store: store, BlobStore: blobStore,
		Config: SandboxOutputCaptureRunnerConfig{WorkspaceID: "ws_capture", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if _, err := runner.RunOnceWithActivity(context.Background()); err != nil {
		t.Fatalf("RunOnceWithActivity: %v", err)
	}
	if blobStore.Has("output-captures/staged") || !store.completed {
		t.Fatalf("cleanup blob/completion = %t/%t; want deleted then completed", blobStore.Has("output-captures/staged"), store.completed)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_capture_cleanup"}) {
		t.Fatalf("queue transitions = %v; want cleanup ack", queueClient.transitions)
	}
}

func TestSandboxOutputCaptureCleanupTreatsMissingBlobAsAlreadyDeleted(t *testing.T) {
	job := sandboxOutputCaptureCleanupQueueJob()
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingOutputCaptureCleanupStore{current: true, work: SandboxOutputCaptureCleanupWork{
		SandboxOutputCaptureCleanupJob: SandboxOutputCaptureCleanupJob{WorkspaceID: "ws_capture", SessionID: "sesn_capture", FinishIdleWriteID: "rwrite_idle", CaptureGeneration: 1, CleanupGeneration: 1},
		BlobPointers:                   []string{"output-captures/already-missing"},
	}}
	runner := &SandboxOutputCaptureCleanupRunner{
		Queue: queueClient, Store: store, BlobStore: &missingDeleteBlobStore{FakeBlobStore: blob.NewFakeBlobStore()},
		Config: SandboxOutputCaptureRunnerConfig{WorkspaceID: "ws_capture", LeaseDuration: 2 * time.Minute, HeartbeatInterval: 15 * time.Second},
	}
	if _, err := runner.RunOnceWithActivity(context.Background()); err != nil {
		t.Fatalf("RunOnceWithActivity: %v", err)
	}
	if !store.completed || !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_capture_cleanup"}) {
		t.Fatalf("missing-blob cleanup = completed %t transitions %v; want completed and acknowledged", store.completed, queueClient.transitions)
	}
}

func TestSandboxOutputCaptureRunnersKeepHeartbeatThroughLiveExhaustion(t *testing.T) {
	t.Run("capture", func(t *testing.T) {
		job := sandboxOutputCaptureQueueJob()
		job.AttemptCount = job.MaxAttempts
		finalizing := make(chan struct{})
		heartbeatObserved := make(chan struct{}, 1)
		queueClient := &observingSandboxFinalizerQueue{
			recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{job}},
			finalizing:            finalizing, heartbeatObserved: heartbeatObserved,
		}
		store := &recordingOutputCaptureStore{
			current: true,
			work:    SandboxOutputCaptureWork{SandboxOutputCaptureJob: SandboxOutputCaptureJob{WorkspaceID: "ws_capture", SessionID: "sesn_capture", FinishIdleWriteID: "rwrite_idle", CaptureGeneration: 1}, ProviderAvailable: true, Provider: sandboxdriver.DaytonaProviderName, ProviderResourceID: "provider_capture"},
			loadErr: errors.New("capture store unavailable"), finalizeStarted: finalizing, heartbeatObserved: heartbeatObserved,
		}
		registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: &recordingOutputCaptureAdapter{}})
		if err != nil {
			t.Fatalf("NewProviderRegistry: %v", err)
		}
		runner := &SandboxOutputCaptureJobRunner{
			Queue: queueClient, Store: store, Providers: registry, BlobStore: blob.NewFakeBlobStore(),
			Config: SandboxOutputCaptureRunnerConfig{WorkspaceID: "ws_capture", LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond},
		}
		if err := runner.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_capture:sandbox_output_capture_exhausted"}) {
			t.Fatalf("capture transitions = %v; want dead letter after live finalizer", queueClient.transitions)
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		job := sandboxOutputCaptureCleanupQueueJob()
		job.AttemptCount = job.MaxAttempts
		finalizing := make(chan struct{})
		heartbeatObserved := make(chan struct{}, 1)
		queueClient := &observingSandboxFinalizerQueue{
			recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{job}},
			finalizing:            finalizing, heartbeatObserved: heartbeatObserved,
		}
		store := &recordingOutputCaptureCleanupStore{
			current: true, loadErr: errors.New("cleanup store unavailable"),
			finalizeStarted: finalizing, heartbeatObserved: heartbeatObserved,
		}
		runner := &SandboxOutputCaptureCleanupRunner{
			Queue: queueClient, Store: store, BlobStore: blob.NewFakeBlobStore(),
			Config: SandboxOutputCaptureRunnerConfig{WorkspaceID: "ws_capture", LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond},
		}
		if _, err := runner.RunOnceWithActivity(context.Background()); err != nil {
			t.Fatalf("RunOnceWithActivity: %v", err)
		}
		if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_capture_cleanup:sandbox_output_capture_cleanup_exhausted"}) {
			t.Fatalf("cleanup transitions = %v; want dead letter after live finalizer", queueClient.transitions)
		}
	})
}

func sandboxOutputCaptureQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_capture", WorkspaceId: "ws_capture", Kind: queue.KindSandboxOutputCapture,
		PartitionKey: queue.FormatSandboxCapturePartitionKey("ws_capture", "sesn_capture", "rwrite_idle"),
		DedupeKey:    queue.FormatSandboxOutputCaptureDedupeKey("ws_capture", "sesn_capture", "rwrite_idle", 1),
		PayloadJson:  `{"workspace_id":"ws_capture","session_id":"sesn_capture","finish_idle_write_id":"rwrite_idle","capture_generation":1}`,
		LeaseToken:   "lease_capture", AttemptCount: 1, MaxAttempts: queue.SandboxOutputCaptureMaxAttempts,
	}
}

func sandboxOutputCaptureCleanupQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id: "qjob_capture_cleanup", WorkspaceId: "ws_capture", Kind: queue.KindSandboxOutputCaptureCleanup,
		PartitionKey: queue.FormatSandboxCapturePartitionKey("ws_capture", "sesn_capture", "rwrite_idle"),
		DedupeKey:    queue.FormatSandboxOutputCaptureCleanupDedupeKey("ws_capture", "sesn_capture", "rwrite_idle", 1, 1),
		PayloadJson:  `{"workspace_id":"ws_capture","session_id":"sesn_capture","finish_idle_write_id":"rwrite_idle","capture_generation":1,"cleanup_generation":1}`,
		LeaseToken:   "lease_cleanup", AttemptCount: 1, MaxAttempts: queue.SandboxOutputCaptureCleanupMaxAttempts,
	}
}

type missingDeleteBlobStore struct {
	*blob.FakeBlobStore
}

func (s *missingDeleteBlobStore) Delete(context.Context, string) error {
	return &blob.NotFoundError{}
}

type failingDeleteBlobStore struct {
	*blob.FakeBlobStore
}

func (s *failingDeleteBlobStore) Delete(context.Context, string) error {
	return errors.New("blob delete unavailable")
}

type recordingOutputCaptureAdapter struct {
	recordingProviderAdapter
	scan    sandboxdriver.OutputCaptureScan
	outcome *ProviderOutcome[sandboxdriver.OutputCaptureScan]
}

func (a *recordingOutputCaptureAdapter) InspectForExecution(context.Context, string) ProviderOutcome[ExecutionReadiness] {
	return ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady}
}
func (a *recordingOutputCaptureAdapter) InspectForRelease(context.Context, string) ProviderOutcome[bool] {
	return ProviderOutcome[bool]{Value: true}
}

func (a *recordingOutputCaptureAdapter) CaptureOutputs(context.Context, sandboxdriver.OutputCaptureTarget) ProviderOutcome[sandboxdriver.OutputCaptureScan] {
	if a.outcome != nil {
		return *a.outcome
	}
	return ProviderOutcome[sandboxdriver.OutputCaptureScan]{Value: a.scan}
}

type recordingOutputCaptureStore struct {
	current           bool
	work              SandboxOutputCaptureWork
	calls             []string
	manifest          []SandboxOutputCaptureManifestEntry
	loadErr           error
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
}

func (s *recordingOutputCaptureStore) LoadCapture(context.Context, SandboxOutputCaptureJob, time.Time) (SandboxOutputCaptureWork, bool, error) {
	s.calls = append(s.calls, "load")
	return s.work, s.current, s.loadErr
}
func (s *recordingOutputCaptureStore) EnsureBlobStage(_ context.Context, _ SandboxOutputCaptureWork, entry SandboxOutputCaptureManifestEntry, _ time.Time) (string, error) {
	s.calls = append(s.calls, "ensure")
	s.manifest = []SandboxOutputCaptureManifestEntry{entry}
	return "pending", nil
}
func (s *recordingOutputCaptureStore) MarkBlobUploaded(context.Context, SandboxOutputCaptureWork, string, time.Time) error {
	s.calls = append(s.calls, "uploaded")
	return nil
}
func (s *recordingOutputCaptureStore) StageCapture(_ context.Context, _ SandboxOutputCaptureWork, manifest []SandboxOutputCaptureManifestEntry, _ []SandboxOutputCaptureSkippedFile, _ []SandboxOutputCaptureScanRecord, _ bool, _ string, _ string, _ time.Time) error {
	s.calls = append(s.calls, "stage")
	s.manifest = manifest
	return nil
}
func (s *recordingOutputCaptureStore) FailCapture(context.Context, SandboxOutputCaptureWork, string, string, time.Time) error {
	s.calls = append(s.calls, "failed")
	return nil
}
func (s *recordingOutputCaptureStore) FinalizeCaptureExhaustion(context.Context, *queuev1.QueueJob, time.Time) error {
	s.calls = append(s.calls, "exhausted")
	if s.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(s.finalizeStarted, s.heartbeatObserved)
	}
	return nil
}

type recordingOutputCaptureCleanupStore struct {
	current           bool
	work              SandboxOutputCaptureCleanupWork
	completed         bool
	loadErr           error
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
}

func (s *recordingOutputCaptureCleanupStore) SweepExpiredCaptures(context.Context, string, time.Time, int) (int, error) {
	return 0, nil
}
func (s *recordingOutputCaptureCleanupStore) LoadCaptureCleanup(context.Context, SandboxOutputCaptureCleanupJob) (SandboxOutputCaptureCleanupWork, bool, error) {
	return s.work, s.current, s.loadErr
}
func (s *recordingOutputCaptureCleanupStore) CompleteCaptureCleanup(context.Context, SandboxOutputCaptureCleanupWork, time.Time) error {
	s.completed = true
	return nil
}
func (s *recordingOutputCaptureCleanupStore) FinalizeCaptureCleanupExhaustion(context.Context, *queuev1.QueueJob, time.Time) error {
	if s.finalizeStarted != nil {
		return requireHeartbeatDuringFinalizer(s.finalizeStarted, s.heartbeatObserved)
	}
	return nil
}

var _ OutputCaptureAdapter = (*recordingOutputCaptureAdapter)(nil)
