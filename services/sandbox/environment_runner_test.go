package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

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
		Config:    EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		Clock:     fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_env_build"}) {
		t.Fatalf("transitions = %v; want ack", queueClient.transitions)
	}
	if !reflect.DeepEqual(store.calls, []string{"claim", "ready:snapshot_ref"}) {
		t.Fatalf("store calls = %v; want claim then ready", store.calls)
	}
	if len(builder.requests) != 1 || builder.requests[0].ArtifactInputHash != "hash_packages" {
		t.Fatalf("builder requests = %+v; want durable artifact input", builder.requests)
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
				Config:    EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
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
			if !reflect.DeepEqual(store.calls, []string{"claim"}) {
				t.Fatalf("store calls after lease loss = %v; want claim only", store.calls)
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
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
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
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		Clock:  fixedEnvironmentRunnerClock,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_env_fanout:environment_ready_fanout_error"}) {
		t.Fatalf("transitions = %v; want retry", queueClient.transitions)
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
		Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
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
		PayloadJson:  `{"workspace_id":"ws_env","environment_id":"env_build","generation":"7"}`,
	}
}

func environmentReadyFanoutQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:          "qjob_env_fanout",
		WorkspaceId: "ws_env",
		Kind:        "environment_ready_fanout",
		LeaseToken:  "lease_env_fanout",
		PayloadJson: `{"workspace_id":"ws_env","environment_id":"env_build","generation":"7"}`,
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
	input   EnvironmentArtifactBuildInput
	claimed bool
	calls   []string
}

func (s *recordingEnvironmentBuildStore) AuthorizeEnvironmentArtifactCreate(context.Context, EnvironmentBuildJob, time.Time) (bool, error) {
	s.calls = append(s.calls, "authorize-create")
	return true, nil
}

func (s *recordingEnvironmentBuildStore) ClaimEnvironmentBuild(context.Context, EnvironmentBuildJob, time.Time) (EnvironmentArtifactBuildInput, bool, error) {
	s.calls = append(s.calls, "claim")
	return s.input, s.claimed, nil
}

func (s *recordingEnvironmentBuildStore) MarkEnvironmentBuildReady(_ context.Context, _ EnvironmentBuildJob, providerArtifactRef string, _ time.Time) error {
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

func (b *recordingArtifactBuilder) BuildEnvironmentArtifact(ctx context.Context, request sandbox.BuildArtifactRequest) ProviderOutcome[sandbox.BuildArtifactResult] {
	result, err := b.BuildArtifact(ctx, request)
	if err == nil {
		return ProviderOutcome[sandbox.BuildArtifactResult]{Value: result}
	}
	boundary := ProviderSubmitted
	if sandboxdriver.ProviderOperationWasNotSubmitted(err) {
		boundary = ProviderProvedNotStarted
	}
	return outcomeFromProviderError[sandbox.BuildArtifactResult](err, boundary)
}

type recordingEnvironmentFanoutStore struct {
	advanced  int
	err       error
	calls     []string
	block     <-chan struct{}
	cancelled chan struct{}
}

func (s *recordingEnvironmentFanoutStore) FinalizeReadyEnvironmentFanout(_ context.Context, job EnvironmentReadyFanoutJob, _ time.Time) error {
	s.calls = append(s.calls, "finalize:"+job.EnvironmentID+":"+strconvInt64(job.Generation))
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

func strconvInt64(value int64) string {
	return strconv.FormatInt(value, 10)
}
