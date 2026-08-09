package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestEnvironmentBuildRunnerMarksReadyEnqueuesFanoutAndAcks(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{environmentBuildQueueJob()}}
	store := &recordingEnvironmentBuildStore{
		input: EnvironmentArtifactBuildInput{
			WorkspaceID:        workspace.ID("ws_env"),
			EnvironmentID:      "env_build",
			Generation:         7,
			Provider:           sandboxdriver.DaytonaProviderName,
			ArtifactInputHash:  "hash_packages",
			NormalizedPackages: sandbox.PackageSetup{"pip": []string{"pandas==2.2.0"}},
		},
		claimed: true,
	}
	builder := &recordingArtifactBuilder{result: sandbox.BuildArtifactResult{ProviderArtifactRef: "snapshot_ref"}}
	runner := &EnvironmentBuildJobRunner{
		Queue:     queueClient,
		Store:     store,
		Providers: artifactProviderRegistry(t, builder),
		Config:    EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:     fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_env_build"}) {
		t.Fatalf("transitions = %v; want ack", queueClient.transitions)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "authorize-create", "ready:snapshot_ref"}) {
		t.Fatalf("store calls = %v; want claim, provider-create authorization, then ready", store.calls)
	}
	if len(builder.requests) != 1 || builder.requests[0].ArtifactInputHash != "hash_packages" {
		t.Fatalf("builder requests = %+v; want durable artifact input", builder.requests)
	}
}

func TestEnvironmentBuildRunnerFinalizesBeforeCancellingWorkContext(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{environmentBuildQueueJob()}}
	store := &recordingEnvironmentBuildStore{
		input: EnvironmentArtifactBuildInput{
			WorkspaceID: workspace.ID("ws_env"), EnvironmentID: "env_build", Generation: 7,
			Provider: sandboxdriver.DaytonaProviderName, ArtifactInputHash: "hash_packages",
		},
		claimed:                true,
		rejectCancelledContext: true,
	}
	builder := &recordingArtifactBuilder{result: sandbox.BuildArtifactResult{ProviderArtifactRef: "snapshot_ref"}}
	runner := &EnvironmentBuildJobRunner{
		Queue: queueClient, Store: store, Providers: artifactProviderRegistry(t, builder),
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "authorize-create", "ready:snapshot_ref"}) {
		t.Fatalf("store calls = %v; want live-context finalization", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_env_build"}) {
		t.Fatalf("transitions = %v; want ack", queueClient.transitions)
	}
}

func TestEnvironmentBuildRunnerFinalizesInvalidAndExhaustedBudgetsBeforeProviderWork(t *testing.T) {
	for _, test := range []struct {
		name          string
		attemptCount  int32
		maxAttempts   int32
		wantErrorKind string
	}{
		{name: "missing budget", attemptCount: 1, maxAttempts: 0, wantErrorKind: "sandbox_queue_integrity_error"},
		{name: "past budget", attemptCount: 4, maxAttempts: 3, wantErrorKind: "environment_build_attempts_exhausted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := environmentBuildQueueJob()
			job.AttemptCount = test.attemptCount
			job.MaxAttempts = test.maxAttempts
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
			store := &recordingEnvironmentBuildStore{claimed: true}
			builder := &recordingArtifactBuilder{}
			runner := &EnvironmentBuildJobRunner{
				Queue: queueClient, Store: store, Providers: artifactProviderRegistry(t, builder),
				Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
				Clock:  fixedEnvironmentRunnerClock,
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(store.calls, []string{"claim", "terminal:" + test.wantErrorKind}) {
				t.Fatalf("store calls = %v; want fenced claim then terminal finalizer", store.calls)
			}
			if len(builder.requests) != 0 {
				t.Fatalf("provider requests = %d; want none", len(builder.requests))
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_env_build:" + test.wantErrorKind}) {
				t.Fatalf("queue transitions = %v; want terminal dead letter", queueClient.transitions)
			}
		})
	}
}

func TestEnvironmentBuildRunnerFinalizesMalformedPayloadBeforeDeadLetter(t *testing.T) {
	job := environmentBuildQueueJob()
	job.PayloadJson = `{"workspace_id":`
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingEnvironmentBuildStore{claimed: true}
	builder := &recordingArtifactBuilder{}
	runner := &EnvironmentBuildJobRunner{
		Queue: queueClient, Store: store, Providers: artifactProviderRegistry(t, builder),
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "terminal:invalid_environment_build_payload"}) {
		t.Fatalf("store calls = %v; want fenced claim then payload finalizer", store.calls)
	}
	if len(builder.requests) != 0 {
		t.Fatalf("provider requests = %d; want none", len(builder.requests))
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_env_build:invalid_environment_build_payload"}) {
		t.Fatalf("queue transitions = %v; want terminal dead letter", queueClient.transitions)
	}
}

func TestEnvironmentBuildRunnerRetriesRetryableFailureAndFinalizesBeforeExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name            string
		job             *queuev1.QueueJob
		err             error
		wantStoreSuffix string
		wantTransition  string
	}{
		{
			name:            "retryable",
			job:             environmentBuildQueueJob(),
			err:             &sandbox.ProviderError{Provider: "daytona", Stage: sandbox.StageBuildArtifact, Kind: sandbox.ProviderErrorUnavailable, Retryable: true, SafeMessage: "temporarily unavailable"},
			wantStoreSuffix: "retryable:unavailable",
			wantTransition:  "retry:qjob_env_build:unavailable",
		},
		{
			name: "explicit create rejection rearms authorization",
			job:  environmentBuildQueueJob(),
			err: sandboxdriver.MarkProviderOperationNotSubmitted(&sandbox.ProviderError{
				Provider: "daytona", Stage: sandbox.StageBuildArtifact, Kind: sandbox.ProviderErrorUnavailable,
				Retryable: true, SafeMessage: "temporarily unavailable",
			}),
			wantStoreSuffix: "retryable:unavailable:rearm-create",
			wantTransition:  "retry:qjob_env_build:unavailable",
		},
		{
			name: "retryable exhausted",
			job: func() *queuev1.QueueJob {
				job := environmentBuildQueueJob()
				job.AttemptCount = 3
				job.MaxAttempts = 3
				return job
			}(),
			err:             &sandbox.ProviderError{Provider: "daytona", Stage: sandbox.StageBuildArtifact, Kind: sandbox.ProviderErrorUnavailable, Retryable: true, SafeMessage: "temporarily unavailable"},
			wantStoreSuffix: "terminal:unavailable",
			wantTransition:  "retry:qjob_env_build:unavailable",
		},
		{
			name:            "terminal",
			job:             environmentBuildQueueJob(),
			err:             &sandbox.ProviderError{Provider: "daytona", Stage: sandbox.StageBuildArtifact, Kind: sandbox.ProviderErrorConfigInvalid, Retryable: false, SafeMessage: "bad config"},
			wantStoreSuffix: "terminal:config_invalid",
			wantTransition:  "dead:qjob_env_build:config_invalid",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{tc.job}}
			store := &recordingEnvironmentBuildStore{input: EnvironmentArtifactBuildInput{WorkspaceID: workspace.ID("ws_env"), EnvironmentID: "env_build", Generation: 7, Provider: sandboxdriver.DaytonaProviderName}, claimed: true}
			builder := &recordingArtifactBuilder{err: tc.err}
			runner := &EnvironmentBuildJobRunner{
				Queue:     queueClient,
				Store:     store,
				Providers: artifactProviderRegistry(t, builder),
				Config:    EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
				Clock:     fixedEnvironmentRunnerClock,
			}

			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if got := store.calls[len(store.calls)-1]; got != tc.wantStoreSuffix {
				t.Fatalf("store calls = %v; want suffix %s", store.calls, tc.wantStoreSuffix)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{tc.wantTransition}) {
				t.Fatalf("transitions = %v; want %s", queueClient.transitions, tc.wantTransition)
			}
		})
	}
}

func TestEnvironmentBuildRunnerPreservesAuthorizationStoreFailureAsControlError(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{environmentBuildQueueJob()}}
	storeErr := errors.New("authorization store unavailable")
	store := &recordingEnvironmentBuildStore{
		input: EnvironmentArtifactBuildInput{
			WorkspaceID: workspace.ID("ws_env"), EnvironmentID: "env_build", Generation: 7,
			Provider: sandboxdriver.DaytonaProviderName,
		},
		claimed: true, authorizeErr: storeErr,
	}
	runner := &EnvironmentBuildJobRunner{
		Queue: queueClient, Store: store,
		Providers: artifactProviderRegistry(t, &DaytonaAdapter{Artifacts: authorizationArtifactBuilder{}}),
		Config: EnvironmentRunnerConfig{
			WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
		Clock: fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); !errors.Is(err, storeErr) {
		t.Fatalf("RunOnce error = %v; want authorization store failure", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "authorize-create"}) {
		t.Fatalf("store calls = %v; want no artifact terminalization after control error", store.calls)
	}
	if len(queueClient.transitions) != 0 {
		t.Fatalf("Queue transitions = %v; want none after control error", queueClient.transitions)
	}
}

func TestEnvironmentBuildRunnerLeaseLossCancelsBuildWithoutDurableOutcome(t *testing.T) {
	for _, tc := range []struct {
		name string
		lost bool
		err  error
	}{
		{name: "stale token", lost: true},
		{name: "transport error", err: errors.New("queue unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{
				leased:        []*queuev1.QueueJob{environmentBuildQueueJob()},
				heartbeatLost: tc.lost,
				heartbeatErr:  tc.err,
			}
			store := &recordingEnvironmentBuildStore{
				input:   EnvironmentArtifactBuildInput{WorkspaceID: workspace.ID("ws_env"), EnvironmentID: "env_build", Generation: 7, Provider: sandboxdriver.DaytonaProviderName},
				claimed: true,
			}
			builder := &recordingArtifactBuilder{block: make(chan struct{}), cancelled: make(chan struct{})}
			runner := &EnvironmentBuildJobRunner{
				Queue:     queueClient,
				Store:     store,
				Providers: artifactProviderRegistry(t, builder),
				Config: EnvironmentRunnerConfig{
					WorkspaceID:       "ws_env",
					LeaseDuration:     100 * time.Millisecond,
					HeartbeatInterval: 5 * time.Millisecond,
				},
				Clock: fixedEnvironmentRunnerClock,
			}

			if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
				t.Fatalf("RunOnce err = %v; want queue lease lost", err)
			}
			if !reflect.DeepEqual(store.calls, []string{"claim", "authorize-create"}) {
				t.Fatalf("store calls after lease loss = %v; want only pre-loss claim and provider-create authorization", store.calls)
			}
			if len(queueClient.transitions) != 0 {
				t.Fatalf("transitions after lease loss = %v; want none", queueClient.transitions)
			}
			select {
			case <-builder.cancelled:
			default:
				t.Fatal("lease loss did not cancel artifact build")
			}
		})
	}
}

func TestEnvironmentReadyFanoutRunnerAcksAfterDurableFanout(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{environmentReadyFanoutQueueJob()}}
	store := &recordingEnvironmentFanoutStore{advanced: 2}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue:  queueClient,
		Store:  store,
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_env_fanout"}) {
		t.Fatalf("transitions = %v; want ack", queueClient.transitions)
	}
	if !reflect.DeepEqual(store.calls, []string{"fanout:env_build:7"}) {
		t.Fatalf("fanout calls = %v; want durable fanout", store.calls)
	}
}

func TestEnvironmentReadyFanoutRunnerRetriesStoreError(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{environmentReadyFanoutQueueJob()}}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue:  queueClient,
		Store:  &recordingEnvironmentFanoutStore{err: errors.New("database unavailable")},
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_env_fanout:environment_ready_fanout_error"}) {
		t.Fatalf("transitions = %v; want retry", queueClient.transitions)
	}
}

func TestEnvironmentReadyFanoutRunnerDoesNotTransitionAfterStoreLosesAuthority(t *testing.T) {
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{environmentReadyFanoutQueueJob()}}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue:  queueClient,
		Store:  &recordingEnvironmentFanoutStore{err: errQueueLeaseLost},
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
		t.Fatalf("RunOnce error = %v; want Queue authority loss", err)
	}
	if len(queueClient.transitions) != 0 {
		t.Fatalf("transitions after store authority loss = %v; want none", queueClient.transitions)
	}
}

func TestEnvironmentReadyFanoutRunnerFinalizesWaitingActivationsAtExhaustion(t *testing.T) {
	job := environmentReadyFanoutQueueJob()
	job.AttemptCount = 3
	job.MaxAttempts = 3
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingEnvironmentFanoutStore{err: errors.New("database unavailable")}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue: queueClient, Store: store,
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"fanout:env_build:7", "finalize:env_build:7"}) {
		t.Fatalf("store calls = %v; want fanout then dependency finalization", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_env_fanout:environment_ready_fanout_attempts_exhausted"}) {
		t.Fatalf("transitions = %v; want named dead letter", queueClient.transitions)
	}
}

func TestEnvironmentReadyFanoutRunnerFinalizesBeforeRunningAReclaimedOverBudgetJob(t *testing.T) {
	job := environmentReadyFanoutQueueJob()
	job.AttemptCount = 4
	job.MaxAttempts = 3
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingEnvironmentFanoutStore{}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue: queueClient, Store: store,
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"finalize:env_build:7"}) {
		t.Fatalf("store calls = %v; want finalization without fanout", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_env_fanout:environment_ready_fanout_attempts_exhausted"}) {
		t.Fatalf("transitions = %v; want exhausted dead letter", queueClient.transitions)
	}
}

func TestEnvironmentReadyFanoutRunnerFinalizesCanonicalJobWithInvalidPayload(t *testing.T) {
	job := environmentReadyFanoutQueueJob()
	job.PayloadJson = `{`
	queueClient := &recordingSandboxQueue{leased: []*queuev1.QueueJob{job}}
	store := &recordingEnvironmentFanoutStore{}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue: queueClient, Store: store,
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second},
		Clock:  fixedEnvironmentRunnerClock,
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(store.calls, []string{"finalize:env_build:7"}) {
		t.Fatalf("store calls = %v; want dependency finalization from transport identity", store.calls)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_env_fanout:invalid_environment_ready_fanout_payload"}) {
		t.Fatalf("transitions = %v; want invalid payload dead letter", queueClient.transitions)
	}
}

func TestEnvironmentReadyFanoutRunnerKeepsHeartbeatThroughExhaustionFinalizer(t *testing.T) {
	job := environmentReadyFanoutQueueJob()
	job.AttemptCount = 3
	job.MaxAttempts = 3
	finalizeStarted := make(chan struct{})
	heartbeatObserved := make(chan struct{}, 1)
	queueClient := &observingEnvironmentFanoutQueue{
		recordingSandboxQueue: recordingSandboxQueue{leased: []*queuev1.QueueJob{job}},
		finalizing:            finalizeStarted,
		heartbeatObserved:     heartbeatObserved,
	}
	store := &recordingEnvironmentFanoutStore{
		err:             errors.New("database unavailable"),
		finalizeStarted: finalizeStarted, heartbeatObserved: heartbeatObserved,
	}
	runner := &EnvironmentReadyFanoutJobRunner{
		Queue: queueClient, Store: store,
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond},
		Clock:  fixedEnvironmentRunnerClock,
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_env_fanout:environment_ready_fanout_attempts_exhausted"}) {
		t.Fatalf("transitions = %v; want dead letter after fenced finalizer", queueClient.transitions)
	}
}

func TestEnvironmentReadyFanoutRunnerLeaseLossCancelsFanoutWithoutQueueTransition(t *testing.T) {
	for _, tc := range []struct {
		name string
		lost bool
		err  error
	}{
		{name: "stale token", lost: true},
		{name: "transport error", err: errors.New("queue unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingSandboxQueue{
				leased:        []*queuev1.QueueJob{environmentReadyFanoutQueueJob()},
				heartbeatLost: tc.lost,
				heartbeatErr:  tc.err,
			}
			store := &recordingEnvironmentFanoutStore{
				block:     make(chan struct{}),
				cancelled: make(chan struct{}),
			}
			runner := &EnvironmentReadyFanoutJobRunner{
				Queue: queueClient,
				Store: store,
				Config: EnvironmentRunnerConfig{
					WorkspaceID:       "ws_env",
					LeaseDuration:     100 * time.Millisecond,
					HeartbeatInterval: 5 * time.Millisecond,
				},
				Clock: fixedEnvironmentRunnerClock,
			}

			if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
				t.Fatalf("RunOnce err = %v; want queue lease lost", err)
			}
			if len(queueClient.transitions) != 0 {
				t.Fatalf("transitions after lease loss = %v; want none", queueClient.transitions)
			}
			select {
			case <-store.cancelled:
			default:
				t.Fatal("lease loss did not cancel environment fanout")
			}
		})
	}
}

func environmentBuildQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:           "qjob_env_build",
		WorkspaceId:  "ws_env",
		Kind:         "environment_build",
		LeaseToken:   "lease_env_build",
		AttemptCount: 1,
		MaxAttempts:  3,
		PartitionKey: queue.FormatEnvironmentPartitionKey("ws_env", "env_build"),
		DedupeKey:    queue.FormatEnvironmentBuildDedupeKey("ws_env", "env_build", "7"),
		PayloadJson:  `{"workspace_id":"ws_env","environment_id":"env_build","generation":"7"}`,
	}
}

func environmentReadyFanoutQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:           "qjob_env_fanout",
		WorkspaceId:  "ws_env",
		Kind:         "environment_ready_fanout",
		PartitionKey: queue.FormatEnvironmentPartitionKey("ws_env", "env_build"),
		DedupeKey:    queue.FormatEnvironmentReadyFanoutDedupeKey("ws_env", "env_build", "7"),
		LeaseToken:   "lease_env_fanout",
		AttemptCount: 1,
		MaxAttempts:  3,
		PayloadJson:  `{"workspace_id":"ws_env","environment_id":"env_build","generation":"7"}`,
	}
}

func fixedEnvironmentRunnerClock() time.Time {
	return time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
}

func artifactProviderRegistry(t *testing.T, builder EnvironmentArtifactAdapter) *ProviderRegistry {
	t.Helper()
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{
		sandboxdriver.DaytonaProviderName: &recordingEnvironmentProviderAdapter{builder: builder},
	})
	if err != nil {
		t.Fatalf("NewProviderRegistry: %v", err)
	}
	return registry
}

type recordingEnvironmentProviderAdapter struct {
	recordingProviderAdapter
	builder EnvironmentArtifactAdapter
}

func (a *recordingEnvironmentProviderAdapter) EnvironmentArtifactAdapter() EnvironmentArtifactAdapter {
	return a.builder
}

type recordingEnvironmentBuildStore struct {
	input                  EnvironmentArtifactBuildInput
	claimed                bool
	rejectCancelledContext bool
	authorizeErr           error
	calls                  []string
}

func (s *recordingEnvironmentBuildStore) AuthorizeEnvironmentArtifactCreate(context.Context, EnvironmentBuildJob, time.Time) (bool, error) {
	s.calls = append(s.calls, "authorize-create")
	return s.authorizeErr == nil, s.authorizeErr
}

func (s *recordingEnvironmentBuildStore) ClaimEnvironmentBuild(context.Context, EnvironmentBuildJob, time.Time) (EnvironmentArtifactBuildInput, bool, error) {
	s.calls = append(s.calls, "claim")
	return s.input, s.claimed, nil
}

func (s *recordingEnvironmentBuildStore) MarkEnvironmentBuildReady(ctx context.Context, _ EnvironmentBuildJob, providerArtifactRef string, _ time.Time) error {
	if s.rejectCancelledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.calls = append(s.calls, "ready:"+providerArtifactRef)
	return nil
}

func (s *recordingEnvironmentBuildStore) MarkEnvironmentBuildRetryableFailure(_ context.Context, _ EnvironmentBuildJob, failure EnvironmentArtifactFailure, rearmCreate bool, _ time.Time) error {
	call := "retryable:" + failure.LastErrorKind
	if rearmCreate {
		call += ":rearm-create"
	}
	s.calls = append(s.calls, call)
	return nil
}

func (s *recordingEnvironmentBuildStore) MarkEnvironmentBuildTerminalFailure(_ context.Context, _ EnvironmentBuildJob, failure EnvironmentArtifactFailure, _ time.Time) error {
	s.calls = append(s.calls, "terminal:"+failure.LastErrorKind)
	return nil
}

type recordingArtifactBuilder struct {
	result    sandbox.BuildArtifactResult
	err       error
	requests  []sandbox.BuildArtifactRequest
	block     <-chan struct{}
	cancelled chan struct{}
}

type authorizationArtifactBuilder struct{}

func (authorizationArtifactBuilder) BuildArtifact(ctx context.Context, request sandbox.BuildArtifactRequest) (sandbox.BuildArtifactResult, error) {
	if request.AuthorizeProviderCreate == nil {
		return sandbox.BuildArtifactResult{}, errors.New("authorization callback is required")
	}
	if _, err := request.AuthorizeProviderCreate(ctx); err != nil {
		return sandbox.BuildArtifactResult{}, err
	}
	return sandbox.BuildArtifactResult{ProviderArtifactRef: "snapshot_authorized"}, nil
}

func (b *recordingArtifactBuilder) BuildArtifact(ctx context.Context, request sandbox.BuildArtifactRequest) (sandbox.BuildArtifactResult, error) {
	b.requests = append(b.requests, request)
	if b.block != nil {
		select {
		case <-b.block:
		case <-ctx.Done():
			if b.cancelled != nil {
				close(b.cancelled)
			}
			return sandbox.BuildArtifactResult{}, ctx.Err()
		}
	}
	return b.result, b.err
}

func (b *recordingArtifactBuilder) BuildEnvironmentArtifact(ctx context.Context, request sandbox.BuildArtifactRequest) (ProviderOutcome[sandbox.BuildArtifactResult], error) {
	if request.AuthorizeProviderCreate != nil {
		authorized, err := request.AuthorizeProviderCreate(ctx)
		if err != nil {
			return ProviderOutcome[sandbox.BuildArtifactResult]{}, err
		}
		if !authorized {
			return ProviderOutcome[sandbox.BuildArtifactResult]{
				EffectBoundary: ProviderProvedNotStarted,
				Disposition:    ProviderRetryable,
				ErrorKind:      "environment_build_lost_authority",
			}, nil
		}
	}
	result, err := b.BuildArtifact(ctx, request)
	if err == nil {
		return ProviderOutcome[sandbox.BuildArtifactResult]{Value: result}, nil
	}
	boundary := ProviderSubmitted
	if sandboxdriver.ProviderOperationWasNotSubmitted(err) {
		boundary = ProviderProvedNotStarted
	}
	return outcomeFromProviderError[sandbox.BuildArtifactResult](err, boundary), nil
}

type recordingEnvironmentFanoutStore struct {
	advanced          int
	err               error
	calls             []string
	block             <-chan struct{}
	cancelled         chan struct{}
	finalizeStarted   chan struct{}
	heartbeatObserved <-chan struct{}
}

func (s *recordingEnvironmentFanoutStore) FinalizeReadyEnvironmentFanout(_ context.Context, job EnvironmentReadyFanoutJob, _ time.Time) error {
	s.calls = append(s.calls, "finalize:"+job.EnvironmentID+":"+strconvInt64(job.Generation))
	if s.finalizeStarted != nil {
		close(s.finalizeStarted)
		select {
		case <-s.heartbeatObserved:
		case <-time.After(time.Second):
			return errors.New("queue heartbeat stopped before environment fanout finalization")
		}
	}
	return nil
}

func (s *recordingEnvironmentFanoutStore) FanoutReadyEnvironment(ctx context.Context, job EnvironmentReadyFanoutJob, _ time.Time) (int, error) {
	s.calls = append(s.calls, "fanout:"+job.EnvironmentID+":"+strconvInt64(job.Generation))
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			if s.cancelled != nil {
				close(s.cancelled)
			}
			return 0, ctx.Err()
		}
	}
	return s.advanced, s.err
}

type observingEnvironmentFanoutQueue struct {
	recordingSandboxQueue
	finalizing        <-chan struct{}
	heartbeatObserved chan<- struct{}
}

func (q *observingEnvironmentFanoutQueue) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	select {
	case <-q.finalizing:
		select {
		case q.heartbeatObserved <- struct{}{}:
		default:
		}
	default:
	}
	return q.recordingSandboxQueue.Heartbeat(ctx, request)
}

func strconvInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
