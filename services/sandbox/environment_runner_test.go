package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestEnvironmentBuildRunnerMarksReadyEnqueuesFanoutAndAcks(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{environmentBuildQueueJob()}}
	store := &recordingEnvironmentBuildStore{
		input: EnvironmentArtifactBuildInput{
			WorkspaceID:        workspace.ID("ws_env"),
			EnvironmentID:      "env_build",
			Generation:         7,
			ArtifactInputHash:  "hash_packages",
			NormalizedPackages: sandbox.PackageSetup{"pip": []string{"pandas==2.2.0"}},
		},
		claimed: true,
	}
	builder := &recordingArtifactBuilder{result: sandbox.BuildArtifactResult{ProviderArtifactRef: "snapshot_ref"}}
	runner := &EnvironmentBuildJobRunner{
		Queue:   queueClient,
		Store:   store,
		Builder: builder,
		Config:  EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		Clock:   fixedEnvironmentRunnerClock,
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
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{tc.job}}
			store := &recordingEnvironmentBuildStore{input: EnvironmentArtifactBuildInput{WorkspaceID: workspace.ID("ws_env"), EnvironmentID: "env_build", Generation: 7}, claimed: true}
			runner := &EnvironmentBuildJobRunner{
				Queue:   queueClient,
				Store:   store,
				Builder: &recordingArtifactBuilder{err: tc.err},
				Config:  EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
				Clock:   fixedEnvironmentRunnerClock,
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
			queueClient := &recordingSessionPrepareQueue{
				leased:        []*queuev1.QueueJob{environmentBuildQueueJob()},
				heartbeatLost: tc.lost,
				heartbeatErr:  tc.err,
			}
			store := &recordingEnvironmentBuildStore{
				input:   EnvironmentArtifactBuildInput{WorkspaceID: workspace.ID("ws_env"), EnvironmentID: "env_build", Generation: 7},
				claimed: true,
			}
			builder := &recordingArtifactBuilder{block: make(chan struct{}), cancelled: make(chan struct{})}
			runner := &EnvironmentBuildJobRunner{
				Queue:   queueClient,
				Store:   store,
				Builder: builder,
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
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{environmentReadyFanoutQueueJob()}}
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
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{environmentReadyFanoutQueueJob()}}
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

func TestEnvironmentFailedFanoutRunnerSettlesQueueFromStoreOutcome(t *testing.T) {
	for _, tc := range []struct {
		name           string
		storeErr       error
		wantTransition string
	}{
		{name: "ack after durable fanout", wantTransition: "ack:qjob_env_failed_fanout"},
		{name: "retry store failure", storeErr: errors.New("database unavailable"), wantTransition: "retry:qjob_env_failed_fanout:environment_failed_fanout_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{environmentFailedFanoutQueueJob()}}
			store := &recordingEnvironmentFailedFanoutStore{err: tc.storeErr}
			runner := &EnvironmentFailedFanoutJobRunner{
				Queue:  queueClient,
				Store:  store,
				Config: EnvironmentRunnerConfig{WorkspaceID: "ws_env", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
				Clock:  fixedEnvironmentRunnerClock,
			}

			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{tc.wantTransition}) {
				t.Fatalf("transitions = %v; want %s", queueClient.transitions, tc.wantTransition)
			}
			if !reflect.DeepEqual(store.calls, []string{"fanout:env_build:7"}) {
				t.Fatalf("fanout calls = %v; want failed-environment fanout", store.calls)
			}
		})
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
			queueClient := &recordingSessionPrepareQueue{
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
		Id:          "qjob_env_build",
		WorkspaceId: "ws_env",
		Kind:        "environment_build",
		LeaseToken:  "lease_env_build",
		PayloadJson: `{"workspace_id":"ws_env","environment_id":"env_build","generation":"7"}`,
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

func environmentFailedFanoutQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:          "qjob_env_failed_fanout",
		WorkspaceId: "ws_env",
		Kind:        "environment_failed_fanout",
		LeaseToken:  "lease_env_failed_fanout",
		PayloadJson: `{"workspace_id":"ws_env","environment_id":"env_build","generation":"7"}`,
	}
}

func fixedEnvironmentRunnerClock() time.Time {
	return time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
}

type recordingEnvironmentBuildStore struct {
	input   EnvironmentArtifactBuildInput
	claimed bool
	calls   []string
}

func (s *recordingEnvironmentBuildStore) ClaimEnvironmentBuild(context.Context, EnvironmentBuildJob, time.Time) (EnvironmentArtifactBuildInput, bool, error) {
	s.calls = append(s.calls, "claim")
	return s.input, s.claimed, nil
}

func (s *recordingEnvironmentBuildStore) MarkEnvironmentBuildReady(_ context.Context, _ EnvironmentBuildJob, providerArtifactRef string, _ time.Time) error {
	s.calls = append(s.calls, "ready:"+providerArtifactRef)
	return nil
}

func (s *recordingEnvironmentBuildStore) MarkEnvironmentBuildRetryableFailure(_ context.Context, _ EnvironmentBuildJob, failure EnvironmentArtifactFailure, _ time.Time) error {
	s.calls = append(s.calls, "retryable:"+failure.LastErrorKind)
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

type recordingEnvironmentFanoutStore struct {
	advanced  int
	err       error
	calls     []string
	block     <-chan struct{}
	cancelled chan struct{}
}

type recordingEnvironmentFailedFanoutStore struct {
	err   error
	calls []string
}

func (s *recordingEnvironmentFailedFanoutStore) FanoutFailedEnvironment(_ context.Context, job EnvironmentFailedFanoutJob, _ time.Time) (int, error) {
	s.calls = append(s.calls, "fanout:"+job.EnvironmentID+":"+strconvInt64(job.Generation))
	return 1, s.err
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
