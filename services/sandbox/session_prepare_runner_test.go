package tetralsandbox

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestSessionPrepareRunnerTransitionsDurableOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		result          sandbox.SessionPrepareResult
		wantTransitions []string
	}{
		{
			name:            "ready",
			result:          sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusReady},
			wantTransitions: []string{"ack:qjob_prepare"},
		},
		{
			name:            "failed",
			result:          sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusFailed, FailureReason: sandbox.GitHubCredentialRequiredReason},
			wantTransitions: []string{"dead:qjob_prepare:github_credential_required"},
		},
		{
			name:            "stale noop ack",
			result:          sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusNoop},
			wantTransitions: []string{"ack:qjob_prepare"},
		},
		{
			name:            "waiting on machine preserves retry budget",
			result:          sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusWaitingOnMachine},
			wantTransitions: []string{"defer:qjob_prepare"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sessionPrepareQueueJob()}}
			handler := &recordingSessionPrepareHandler{result: tc.result}
			runner := &SessionPrepareJobRunner{
				Queue:   queueClient,
				Handler: handler,
				Config:  SessionPrepareRunnerConfig{WorkspaceID: "ws_prepare", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
			}

			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if len(handler.requests) != 1 || handler.requests[0].PreparationAttemptID != "prep_1" {
				t.Fatalf("handler requests = %+v; want decoded preparation identity", handler.requests)
			}
			if !reflect.DeepEqual(queueClient.transitions, tc.wantTransitions) {
				t.Fatalf("transitions = %v; want %v", queueClient.transitions, tc.wantTransitions)
			}
		})
	}
}

func TestSessionPrepareRunnerRetriesHandlerErrorWithoutAck(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{sessionPrepareQueueJob()}}
	handler := &recordingSessionPrepareHandler{err: errors.New("database unavailable")}
	runner := &SessionPrepareJobRunner{
		Queue:   queueClient,
		Handler: handler,
		Config:  SessionPrepareRunnerConfig{WorkspaceID: "ws_prepare", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_prepare:session_prepare_error"}) {
		t.Fatalf("transitions = %v; want retry", queueClient.transitions)
	}
}

func TestSessionPrepareRunnerFailsPreparationBeforeRetryExhaustionDeadLetter(t *testing.T) {
	job := sessionPrepareQueueJob()
	job.AttemptCount = 3
	job.MaxAttempts = 3
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	handler := &recordingSessionPrepareHandler{err: errors.New("database unavailable")}
	runner := &SessionPrepareJobRunner{
		Queue:   queueClient,
		Handler: handler,
		Config:  SessionPrepareRunnerConfig{WorkspaceID: "ws_prepare", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(handler.exhausted, []string{"sesn_1:prep_1:session_prepare_error"}) {
		t.Fatalf("retry exhaustion finalizer calls = %+v; want active attempt failed", handler.exhausted)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_prepare:session_prepare_error"}) {
		t.Fatalf("transitions = %v; want retry transition to dead-letter exhausted lease", queueClient.transitions)
	}
}

func TestSessionPrepareRunnerAcksStaleRetryExhaustionAttempt(t *testing.T) {
	job := sessionPrepareQueueJob()
	job.AttemptCount = 3
	job.MaxAttempts = 3
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	handler := &recordingSessionPrepareHandler{err: errors.New("database unavailable"), exhaustedStatus: sandbox.SessionPrepareStatusNoop}
	runner := &SessionPrepareJobRunner{
		Queue:   queueClient,
		Handler: handler,
		Config:  SessionPrepareRunnerConfig{WorkspaceID: "ws_prepare", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_prepare"}) {
		t.Fatalf("transitions = %v; want stale exhausted attempt ack", queueClient.transitions)
	}
}

func TestSessionPrepareRunnerDeadLettersNonIdentityPayloadFields(t *testing.T) {
	job := sessionPrepareQueueJob()
	job.PayloadJson = `{"workspace_id":"ws_prepare","session_id":"sesn_1","preparation_attempt_id":"prep_1","unexpected_control_field":true}`
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{job}}
	handler := &recordingSessionPrepareHandler{result: sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusReady}}
	runner := &SessionPrepareJobRunner{
		Queue:   queueClient,
		Handler: handler,
		Config:  SessionPrepareRunnerConfig{WorkspaceID: "ws_prepare", LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(handler.requests) != 0 {
		t.Fatalf("handler requests = %+v; want none for payload with control fields", handler.requests)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_prepare:invalid_session_prepare_payload"}) {
		t.Fatalf("transitions = %v; want invalid payload dead letter", queueClient.transitions)
	}
}

func TestSessionPrepareRunnerDeadLettersInvalidPayloadBeforeHandler(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{leased: []*queuev1.QueueJob{{
		Id:          "qjob_bad",
		WorkspaceId: "ws_prepare",
		Kind:        "session_prepare",
		LeaseToken:  "lease_bad",
		PayloadJson: `{"workspace_id":"ws_prepare","session_id":"sesn_1"}`,
	}}}
	handler := &recordingSessionPrepareHandler{}
	runner := &SessionPrepareJobRunner{Queue: queueClient, Handler: handler, Config: SessionPrepareRunnerConfig{WorkspaceID: "ws_prepare"}}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(handler.requests) != 0 {
		t.Fatalf("handler requests = %+v; want none", handler.requests)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_bad:invalid_session_prepare_payload"}) {
		t.Fatalf("transitions = %v; want invalid payload dead letter", queueClient.transitions)
	}
}

func TestSessionPrepareRunnerHeartbeatsDuringLongPreparation(t *testing.T) {
	queueClient := &recordingSessionPrepareQueue{
		leased:      []*queuev1.QueueJob{sessionPrepareQueueJob()},
		heartbeatCh: make(chan string, 1),
	}
	release := make(chan struct{})
	handler := &recordingSessionPrepareHandler{block: release, result: sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusReady}}
	runner := &SessionPrepareJobRunner{
		Queue:   queueClient,
		Handler: handler,
		Config: SessionPrepareRunnerConfig{
			WorkspaceID:       "ws_prepare",
			LeaseDuration:     time.Minute,
			HeartbeatInterval: 5 * time.Millisecond,
		},
	}
	done := make(chan error, 1)
	go func() { done <- runner.RunOnce(context.Background()) }()
	deadline := time.After(500 * time.Millisecond)
	var heartbeat string
	select {
	case err := <-done:
		t.Fatalf("RunOnce finished before heartbeat: %v", err)
	case <-deadline:
		t.Fatal("timed out waiting for heartbeat")
	case heartbeat = <-queueClient.heartbeatCh:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if heartbeat != "qjob_prepare" {
		t.Fatalf("heartbeat = %q; want job heartbeat", heartbeat)
	}
}

func TestSessionPrepareRunnerLeaseLossCancelsWorkWithoutTransition(t *testing.T) {
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
				leased:        []*queuev1.QueueJob{sessionPrepareQueueJob()},
				heartbeatLost: tc.lost,
				heartbeatErr:  tc.err,
			}
			handler := &recordingSessionPrepareHandler{block: make(chan struct{}), cancelled: make(chan struct{})}
			runner := &SessionPrepareJobRunner{
				Queue:   queueClient,
				Handler: handler,
				Config: SessionPrepareRunnerConfig{
					WorkspaceID:       "ws_prepare",
					LeaseDuration:     100 * time.Millisecond,
					HeartbeatInterval: 5 * time.Millisecond,
				},
			}

			if err := runner.RunOnce(context.Background()); !errors.Is(err, errQueueLeaseLost) {
				t.Fatalf("RunOnce err = %v; want queue lease lost", err)
			}
			if len(queueClient.transitions) != 0 {
				t.Fatalf("transitions after lease loss = %v; want none", queueClient.transitions)
			}
			select {
			case <-handler.cancelled:
			default:
				t.Fatal("lease loss did not cancel session preparation")
			}
		})
	}
}

func sessionPrepareQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:          "qjob_prepare",
		WorkspaceId: "ws_prepare",
		Kind:        "session_prepare",
		LeaseToken:  "lease_prepare",
		PayloadJson: `{"workspace_id":"ws_prepare","session_id":"sesn_1","preparation_attempt_id":"prep_1"}`,
	}
}

type recordingSessionPrepareHandler struct {
	result          sandbox.SessionPrepareResult
	err             error
	block           <-chan struct{}
	cancelled       chan struct{}
	exhaustedStatus string
	requests        []sandbox.SessionPrepareRequest
	exhausted       []string
}

func (h *recordingSessionPrepareHandler) PrepareSession(ctx context.Context, request sandbox.SessionPrepareRequest) (sandbox.SessionPrepareResult, error) {
	h.requests = append(h.requests, request)
	if h.block != nil {
		select {
		case <-h.block:
		case <-ctx.Done():
			if h.cancelled != nil {
				close(h.cancelled)
			}
			return sandbox.SessionPrepareResult{}, ctx.Err()
		}
	}
	return h.result, h.err
}

func (h *recordingSessionPrepareHandler) FailSessionPreparationAfterRetryExhaustion(_ context.Context, request sandbox.SessionPrepareRequest, failureReason string) (sandbox.SessionPrepareResult, error) {
	h.exhausted = append(h.exhausted, request.SessionID+":"+request.PreparationAttemptID+":"+failureReason)
	if h.exhaustedStatus != "" {
		return sandbox.SessionPrepareResult{Status: h.exhaustedStatus}, nil
	}
	return sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusFailed, FailureReason: failureReason}, nil
}

type recordingSessionPrepareQueue struct {
	leased        []*queuev1.QueueJob
	transitions   []string
	heartbeats    []string
	heartbeatCh   chan string
	heartbeatLost bool
	heartbeatErr  error
}

func (q *recordingSessionPrepareQueue) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	return &queuev1.LeaseResponse{Jobs: q.leased}, nil
}

func (q *recordingSessionPrepareQueue) Heartbeat(_ context.Context, request *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error) {
	q.heartbeats = append(q.heartbeats, request.GetJobId())
	if q.heartbeatCh != nil {
		select {
		case q.heartbeatCh <- request.GetJobId():
		default:
		}
	}
	if q.heartbeatErr != nil {
		return nil, q.heartbeatErr
	}
	return &queuev1.TransitionResponse{Updated: !q.heartbeatLost}, nil
}

func (q *recordingSessionPrepareQueue) Ack(_ context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "ack:"+request.GetJobId())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSessionPrepareQueue) Retry(_ context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "retry:"+request.GetJobId()+":"+request.GetErrorKind())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSessionPrepareQueue) Defer(_ context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "defer:"+request.GetJobId())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSessionPrepareQueue) DeadLetter(_ context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "dead:"+request.GetJobId()+":"+request.GetErrorKind())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

// A dead-lettered preparation is the only durable trace an operator gets, so
// the provider's stage, kind, and safe message must survive into it. This
// pins the classification that a fixed error string previously discarded.
func TestSessionPrepareErrorClassificationSurvivesIntoQueueRow(t *testing.T) {
	providerErr := &sandbox.ProviderError{
		Provider:    "daytona",
		Stage:       sandbox.StageCreateSandbox,
		Kind:        sandbox.ProviderErrorAuthFailed,
		SafeMessage: "provider rejected the credential",
	}
	if got := sessionPrepareErrorKind(providerErr); got != string(sandbox.ProviderErrorAuthFailed) {
		t.Fatalf("kind = %q; want %q", got, sandbox.ProviderErrorAuthFailed)
	}
	message := sessionPrepareErrorMessage(providerErr)
	if !strings.Contains(message, string(sandbox.StageCreateSandbox)) {
		t.Fatalf("message %q does not name the failing stage", message)
	}
	if !strings.Contains(message, "provider rejected the credential") {
		t.Fatalf("message %q dropped the provider's safe message", message)
	}
	if got := sessionPrepareErrorKind(errors.New("opaque")); got != "session_prepare_error" {
		t.Fatalf("unclassified kind = %q; want session_prepare_error", got)
	}
}

// The handler can also settle a preparation as a structured failed RESULT
// instead of returning an error; that path dead-letters on the first attempt
// and must name the stage and provider detail the same way.
func TestSessionPrepareFailedResultMessageNamesStageAndDetail(t *testing.T) {
	full := sessionPrepareFailedResultMessage(sandbox.SessionPrepareResult{
		Status:        sandbox.SessionPrepareStatusFailed,
		FailureReason: "sandbox_preparation_failed",
		FailureStage:  "mount_resources",
		FailureDetail: "sandbox base directory preparation failed",
	})
	if full != "session_prepare failed at mount_resources: sandbox base directory preparation failed" {
		t.Fatalf("message = %q; want stage and detail", full)
	}
	stageOnly := sessionPrepareFailedResultMessage(sandbox.SessionPrepareResult{
		Status:       sandbox.SessionPrepareStatusFailed,
		FailureStage: "session_prepare",
	})
	if stageOnly != "session_prepare failed at session_prepare" {
		t.Fatalf("message = %q; want stage-only form", stageOnly)
	}
	if bare := sessionPrepareFailedResultMessage(sandbox.SessionPrepareResult{Status: sandbox.SessionPrepareStatusFailed}); bare != "session_prepare failed" {
		t.Fatalf("message = %q; want bare fallback", bare)
	}
}
