package agentruntimebridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestJobRunnerAcksRuntimeInputOnlyAfterRuntimeAccepts(t *testing.T) {
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{runtimeInputQueueJob()}}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}

	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
		Config: JobRunnerConfig{
			LeaseOwner:    "bridge",
			MaxJobs:       1,
			LeaseDuration: time.Second,
		},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 1 {
		t.Fatalf("delivered jobs = %d; want 1", len(deliverer.jobs))
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_1"}) {
		t.Fatalf("queue transitions = %v; want ack after runtime accepts", queueClient.transitions)
	}
	if deliverer.jobs[0].CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES {
		t.Fatalf("command kind = %v; want messages", deliverer.jobs[0].CommandKind)
	}
	if !reflect.DeepEqual(queueClient.leaseKinds, []string{"runtime_input", "runtime_config_update", "cleanup_session", "session_delete_cleanup"}) {
		t.Fatalf("lease kinds = %v; want Bridge runtime-facing kinds", queueClient.leaseKinds)
	}
}

func TestDecodeRuntimeInputJobAcceptsEventlessAgentMail(t *testing.T) {
	job, err := DecodeRuntimeJob(&queuev1.QueueJob{
		Id:          "qjob_agent_mail",
		WorkspaceId: "ws_bridge",
		Kind:        "runtime_input",
		LeaseToken:  "qlt_agent_mail",
		PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_agent_mail","session_thread_id":"thr_agent_mail_main","runtime_input_id":"agent_mail:delivery_1","preparation_attempt_id":"prep_agent_mail","event_ids":[],"sequence_from":0,"sequence_to":0,"input_kind":"agent_mail"}`,
	})
	if err != nil {
		t.Fatalf("DecodeRuntimeJob agent_mail: %v", err)
	}
	if job.InputKind != "agent_mail" || job.CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_AGENT_MAIL {
		t.Fatalf("decoded agent_mail = kind %q command %s; want agent_mail command", job.InputKind, job.CommandKind)
	}
}

func TestJobRunnerHeartbeatsLongDeliveryBeforeAck(t *testing.T) {
	queueClient := &recordingQueueClient{
		leased:          []*queuev1.QueueJob{runtimeInputQueueJob()},
		heartbeatNotify: make(chan struct{}, 1),
	}
	deliverer := newBlockingDeliverer()
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
		Config: JobRunnerConfig{
			LeaseDuration:     100 * time.Millisecond,
			HeartbeatInterval: 5 * time.Millisecond,
		},
	}
	done := make(chan error, 1)
	go func() { done <- runner.RunOnce(context.Background()) }()

	select {
	case <-queueClient.heartbeatNotify:
	case <-time.After(time.Second):
		t.Fatal("job runner did not heartbeat a blocked delivery")
	}
	close(deliverer.release)
	if err := <-done; err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if transitions := queueClient.transitionSnapshot(); !reflect.DeepEqual(transitions, []string{"ack:qjob_1"}) {
		t.Fatalf("queue transitions = %v; want ACK after heartbeat and delivery", transitions)
	}
}

func TestJobRunnerLeaseLossCancelsDeliveryWithoutTransition(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		lost bool
	}{
		{name: "stale token", lost: true},
		{name: "transport error", err: errors.New("queue unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingQueueClient{
				leased:        []*queuev1.QueueJob{runtimeInputQueueJob()},
				heartbeatLost: tc.lost,
				heartbeatErr:  tc.err,
			}
			deliverer := newBlockingDeliverer()
			runner := &JobRunner{
				Queue:      queueClient,
				Workspaces: staticWorkspaceLister{"ws_bridge"},
				Deliverer:  deliverer,
				Config: JobRunnerConfig{
					LeaseDuration:     100 * time.Millisecond,
					HeartbeatInterval: 5 * time.Millisecond,
				},
			}

			if err := runner.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "queue lease lost") {
				t.Fatalf("RunOnce err = %v; want queue lease lost", err)
			}
			if transitions := queueClient.transitionSnapshot(); len(transitions) != 0 {
				t.Fatalf("queue transitions after lease loss = %v; want none", transitions)
			}
			select {
			case <-deliverer.cancelled:
			default:
				t.Fatal("lease loss did not cancel blocked delivery")
			}
		})
	}
}

func TestJobRunnerRepairsRuntimeInboxAndCompletionMailBeforeLeasingJobs(t *testing.T) {
	queueClient := &recordingQueueClient{}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if deliverer.repairCalls != 1 || deliverer.repairWorkspaceID != "ws_bridge" || deliverer.repairLimit != defaultRuntimeInboxRepairBatch {
		t.Fatalf("repair calls = %d workspace=%q limit=%d; want one workspace repair before lease", deliverer.repairCalls, deliverer.repairWorkspaceID, deliverer.repairLimit)
	}
	if deliverer.mailRepairCalls != 1 || deliverer.mailRepairWorkspaceID != "ws_bridge" || deliverer.mailRepairLimit != defaultRuntimeInboxRepairBatch {
		t.Fatalf("mail repair calls = %d workspace=%q limit=%d; want one workspace repair before lease", deliverer.mailRepairCalls, deliverer.mailRepairWorkspaceID, deliverer.mailRepairLimit)
	}
	if len(queueClient.leaseKinds) == 0 {
		t.Fatal("queue was not leased after runtime inbox and completion-mail repair")
	}
}

func TestJobRunnerDiscoversAndLeasesEveryWorkspace(t *testing.T) {
	queueClient := &recordingQueueClient{}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_alpha", "ws_beta"},
		Deliverer:  deliverer,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.leaseWorkspaceIDs, []string{"ws_alpha", "ws_beta"}) {
		t.Fatalf("lease workspaces = %v; want every discovered workspace", queueClient.leaseWorkspaceIDs)
	}
	if !reflect.DeepEqual(deliverer.repairWorkspaceIDs, []string{"ws_alpha", "ws_beta"}) {
		t.Fatalf("repair workspaces = %v; want every discovered workspace", deliverer.repairWorkspaceIDs)
	}
	if !reflect.DeepEqual(deliverer.mailRepairWorkspaceIDs, []string{"ws_alpha", "ws_beta"}) {
		t.Fatalf("mail repair workspaces = %v; want every discovered workspace", deliverer.mailRepairWorkspaceIDs)
	}
}

func TestJobRunnerSweepContinuesPastAFailingWorkspace(t *testing.T) {
	queueClient := &recordingQueueClient{leaseErrWorkspace: "ws_beta"}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_alpha", "ws_beta", "ws_gamma"},
		Deliverer:  deliverer,
	}

	err := runner.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce returned nil; the failing workspace must still be reported")
	}
	if !strings.Contains(err.Error(), "ws_beta") {
		t.Fatalf("RunOnce error = %v; want the failing workspace named", err)
	}
	if !reflect.DeepEqual(queueClient.leaseWorkspaceIDs, []string{"ws_alpha", "ws_beta", "ws_gamma"}) {
		t.Fatalf("lease workspaces = %v; want the sweep to reach every workspace despite the failure", queueClient.leaseWorkspaceIDs)
	}
	if !reflect.DeepEqual(deliverer.repairWorkspaceIDs, []string{"ws_alpha", "ws_beta", "ws_gamma"}) {
		t.Fatalf("repair workspaces = %v; want workspaces after the failure still repaired", deliverer.repairWorkspaceIDs)
	}
}

func TestJobRunnerRetriesTransportFailureWithoutAck(t *testing.T) {
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{runtimeInputQueueJob()}}
	deliverer := &recordingDeliverer{err: errors.New("pod unavailable")}

	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
		Config:     JobRunnerConfig{},
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"retry:qjob_1:runtime_transport_error"}) {
		t.Fatalf("queue transitions = %v; want retry", queueClient.transitions)
	}
}

func TestJobRunnerFinalAttemptCommitsBridgeFenceBeforeQueueDeadLetter(t *testing.T) {
	steps := []string{}
	job := runtimeInputQueueJob()
	job.AttemptCount = 2
	job.MaxAttempts = 2
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}, steps: &steps}
	deliverer := &recordingDeliverer{
		result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy"},
		finalizeResult: RuntimeDeliveryResult{
			Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted",
		},
		steps: &steps,
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.finalizations) != 1 {
		t.Fatalf("finalizations = %#v; want one final-attempt Bridge fence", deliverer.finalizations)
	}
	if got := deliverer.finalizations[0].job; got.AttemptCount != 2 || got.MaxAttempts != 2 {
		t.Fatalf("finalized queue attempt = %d/%d; want leased 2/2", got.AttemptCount, got.MaxAttempts)
	}
	if got := deliverer.finalizations[0].result; !got.Retryable || got.ErrorKind != "runtime_busy" {
		t.Fatalf("finalized delivery result = %#v; want original retryable result", got)
	}
	if !reflect.DeepEqual(steps, []string{"replay:qjob_1", "deliver:qjob_1", "finalize:qjob_1", "dead:qjob_1"}) {
		t.Fatalf("steps = %v; want Bridge finalization before Queue dead-letter", steps)
	}
}

func TestJobRunnerFinalAttemptTransportFailureCommitsBridgeFenceBeforeQueueDeadLetter(t *testing.T) {
	steps := []string{}
	job := runtimeInputQueueJob()
	job.AttemptCount = 3
	job.MaxAttempts = 3
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}, steps: &steps}
	deliverer := &recordingDeliverer{
		err: errors.New("pod unavailable"),
		finalizeResult: RuntimeDeliveryResult{
			Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted",
		},
		steps: &steps,
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.finalizations) != 1 ||
		!deliverer.finalizations[0].result.Retryable ||
		deliverer.finalizations[0].result.ErrorKind != "runtime_transport_error" {
		t.Fatalf("transport finalization = %#v; want one retryable transport result", deliverer.finalizations)
	}
	if !reflect.DeepEqual(steps, []string{"replay:qjob_1", "deliver:qjob_1", "finalize:qjob_1", "dead:qjob_1"}) {
		t.Fatalf("steps = %v; want Bridge finalization before Queue dead-letter", steps)
	}
}

func TestJobRunnerFinalizationStoredStaleDispositionAcksQueue(t *testing.T) {
	job := runtimeInputQueueJob()
	job.AttemptCount = 2
	job.MaxAttempts = 2
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}}
	deliverer := &recordingDeliverer{
		result:         RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy"},
		finalizeResult: RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate},
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_1"}) {
		t.Fatalf("queue transitions = %v; want stale finalization ACK", queueClient.transitions)
	}
}

func TestJobRunnerReplaysStoredFinalizationBeforeRuntimeRedelivery(t *testing.T) {
	steps := []string{}
	job := runtimeInputQueueJob()
	job.AttemptCount = 2
	job.MaxAttempts = 2
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}, steps: &steps}
	deliverer := &recordingDeliverer{
		replayFound: true,
		replayResult: RuntimeDeliveryResult{
			Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted",
		},
		steps: &steps,
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 0 || len(deliverer.finalizations) != 0 {
		t.Fatalf("deliveries=%d finalizations=%d; want stored disposition replay without Runtime delivery or second finalization", len(deliverer.jobs), len(deliverer.finalizations))
	}
	if !reflect.DeepEqual(steps, []string{"replay:qjob_1", "dead:qjob_1"}) {
		t.Fatalf("steps = %v; want stored Bridge disposition before Queue dead-letter", steps)
	}
}

func TestJobRunnerReplaysStoredFinalizationForNonFinalReadmissionBeforeRuntimeDelivery(t *testing.T) {
	for _, test := range []struct {
		name        string
		disposition RuntimeDeliveryResult
		transition  string
	}{
		{
			name:        "exhausted disposition dead-letters",
			disposition: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted"},
			transition:  "dead:qjob_1:runtime_delivery_exhausted",
		},
		{
			name:        "stale disposition ACKs",
			disposition: RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate},
			transition:  "ack:qjob_1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			steps := []string{}
			job := runtimeInputQueueJob()
			job.AttemptCount = 1
			job.MaxAttempts = 3
			queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}, steps: &steps}
			deliverer := &recordingDeliverer{
				replayFound:  true,
				replayResult: test.disposition,
				steps:        &steps,
			}
			runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if len(deliverer.jobs) != 0 || len(deliverer.finalizations) != 0 {
				t.Fatalf("deliveries=%d finalizations=%d; want stored non-final disposition with zero Runtime delivery and no new fence", len(deliverer.jobs), len(deliverer.finalizations))
			}
			if len(deliverer.replayJobs) != 1 || deliverer.replayJobs[0].AttemptCount != 1 || deliverer.replayJobs[0].MaxAttempts != 3 {
				t.Fatalf("replayed leased attempts = %#v; want exactly 1/3", deliverer.replayJobs)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.transition}) {
				t.Fatalf("queue transitions = %v; want only %s and no retry budget", queueClient.transitions, test.transition)
			}
			if !reflect.DeepEqual(steps, []string{"replay:qjob_1", strings.Split(test.transition, ":")[0] + ":qjob_1"}) {
				t.Fatalf("steps = %v; want replay before terminal Queue disposition", steps)
			}
		})
	}
}

func TestJobRunnerReplaysStoredStaleTaskDispositionAsQueueAckWithoutRuntimeDelivery(t *testing.T) {
	job := runtimeInputQueueJob()
	job.AttemptCount = 2
	job.MaxAttempts = 2
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}}
	deliverer := &recordingDeliverer{replayFound: true, replayResult: RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 0 || len(deliverer.finalizations) != 0 {
		t.Fatalf("deliveries=%d finalizations=%d; want stale ACK without Runtime delivery or mutation", len(deliverer.jobs), len(deliverer.finalizations))
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_1"}) {
		t.Fatalf("queue transitions = %v; want stale ACK", queueClient.transitions)
	}
}

func TestJobRunnerFinalizationFailureLeavesQueueLeaseUnchanged(t *testing.T) {
	job := runtimeInputQueueJob()
	job.AttemptCount = 2
	job.MaxAttempts = 2
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}}
	deliverer := &recordingDeliverer{
		result:      RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy"},
		finalizeErr: errors.New("bridge finalization unavailable"),
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce succeeded; want Bridge finalization error")
	}
	if len(queueClient.transitions) != 0 {
		t.Fatalf("queue transitions = %v; want none before Bridge ACK", queueClient.transitions)
	}
}

func TestJobRunnerNonFinalRetryAndAcceptedFinalDeliveryRemainUnchanged(t *testing.T) {
	for _, test := range []struct {
		name       string
		result     RuntimeDeliveryResult
		attempt    int32
		max        int32
		transition string
	}{
		{name: "non-final retryable", result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy"}, attempt: 1, max: 2, transition: "retry:qjob_1:runtime_busy"},
		{name: "accepted final attempt", result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, attempt: 2, max: 2, transition: "ack:qjob_1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := runtimeInputQueueJob()
			job.AttemptCount = test.attempt
			job.MaxAttempts = test.max
			queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}}
			deliverer := &recordingDeliverer{result: test.result}
			runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if len(deliverer.finalizations) != 0 {
				t.Fatalf("finalizations = %#v; want none", deliverer.finalizations)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.transition}) {
				t.Fatalf("queue transitions = %v; want %s", queueClient.transitions, test.transition)
			}
		})
	}
}

func TestJobRunnerNonRetryableRuntimeInputFinalizesBeforeQueueDeadLetter(t *testing.T) {
	steps := []string{}
	job := runtimeInputQueueJob()
	job.AttemptCount = 1
	job.MaxAttempts = 4
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}, steps: &steps}
	deliverer := &recordingDeliverer{
		result:         RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_contract_failure"},
		finalizeResult: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_contract_failure"},
		steps:          &steps,
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(steps, []string{"replay:qjob_1", "deliver:qjob_1", "finalize:qjob_1", "dead:qjob_1"}) {
		t.Fatalf("steps = %v; want terminal Bridge finalization before Queue dead-letter", steps)
	}
}

func TestJobRunnerSessionDeleteCleanupReleaseOutcomesTransitionQueueExactly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     RuntimeDeliveryResult
		err        error
		transition string
	}{
		{name: "released ACKs", result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, transition: "ack:qjob_delete_outcome"},
		{name: "retry later retries", result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "sandbox_release_retry_later"}, transition: "retry:qjob_delete_outcome:sandbox_release_retry_later"},
		{name: "transport failure retries", err: errors.New("release transport unavailable"), transition: "retry:qjob_delete_outcome:runtime_transport_error"},
		{name: "terminal failure dead letters", result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "sandbox_release_failed"}, transition: "dead:qjob_delete_outcome:sandbox_release_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{{
				Id: "qjob_delete_outcome", WorkspaceId: "default", Kind: "session_delete_cleanup", LeaseToken: "lease_delete_outcome",
				PayloadJson: `{"workspace_id":"default","session_id":"sesn_delete_outcome","delete_cleanup_id":"delcln_delete_outcome"}`,
			}}}
			deliverer := &recordingDeliverer{result: tc.result, err: tc.err}
			runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{tc.transition}) {
				t.Fatalf("queue transitions = %v; want %q", queueClient.transitions, tc.transition)
			}
		})
	}
}

func TestJobRunnerDeadLettersInvalidPayloadBeforeDelivery(t *testing.T) {
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{{
		Id:          "qjob_bad",
		WorkspaceId: "ws_bridge",
		Kind:        "runtime_input",
		LeaseToken:  "lease_bad",
		PayloadJson: `{"workspace_id":"ws_bridge","input_kind":"messages"}`,
	}}}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 0 {
		t.Fatalf("delivered invalid jobs = %d; want 0", len(deliverer.jobs))
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_bad:invalid_runtime_job_payload"}) {
		t.Fatalf("queue transitions = %v; want invalid payload dead letter", queueClient.transitions)
	}
}

func TestJobRunnerCancelsPendingMessagesBeforeInterruptDelivery(t *testing.T) {
	steps := []string{}
	queueClient := &recordingQueueClient{
		leased: []*queuev1.QueueJob{runtimeInterruptQueueJob()},
		steps:  &steps,
	}
	deliverer := &recordingDeliverer{
		result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted},
		steps:  &steps,
	}
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 1 || deliverer.jobs[0].InputKind != "interrupt_control" {
		t.Fatalf("delivered jobs = %+v; want interrupt after queue cancel", deliverer.jobs)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"cancel:sesn_1:thr_1:9", "ack:qjob_interrupt"}) {
		t.Fatalf("queue transitions = %v; want cancel before ack", queueClient.transitions)
	}
	if !reflect.DeepEqual(steps, []string{
		"cancel",
		"replay:qjob_interrupt",
		"deliver:qjob_interrupt",
		"ack:qjob_interrupt",
	}) {
		t.Fatalf("steps = %v; want cancel before replay, delivery, settlement, and ack", steps)
	}
}

func TestJobRunnerFencedPreparationFailureInterruptStillCancelsSiblingMessages(t *testing.T) {
	job := runtimeInterruptQueueJob()
	steps := []string{}
	queueClient := &recordingQueueClient{
		leased: []*queuev1.QueueJob{job},
		steps:  &steps,
	}
	deliverer := &recordingDeliverer{
		result:        RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted},
		sealedAttempt: "prep_failed",
		steps:         &steps,
	}
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 1 || deliverer.jobs[0].SealedPreparationAttemptID != "prep_failed" {
		t.Fatalf("delivered jobs = %+v; want fenced settlement interrupt", deliverer.jobs)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"cancel:sesn_1:thr_1:9", "ack:qjob_interrupt"}) {
		t.Fatalf("queue transitions = %v; want unconditional sibling-message cancellation before settle-only ack", queueClient.transitions)
	}
	if !reflect.DeepEqual(steps, []string{
		"cancel",
		"replay:qjob_interrupt",
		"deliver:qjob_interrupt",
		"ack:qjob_interrupt",
	}) {
		t.Fatalf("steps = %v; want cancel before replay, delivery, settlement, and ack", steps)
	}
}

func TestJobRunnerInterruptReplayStillCancelsSiblingMessagesFirst(t *testing.T) {
	steps := []string{}
	queueClient := &recordingQueueClient{
		leased: []*queuev1.QueueJob{runtimeInterruptQueueJob()},
		steps:  &steps,
	}
	deliverer := &recordingDeliverer{
		replayFound: true,
		replayResult: RuntimeDeliveryResult{
			Status: RuntimeDeliveryAccepted,
		},
		steps: &steps,
	}
	runner := &JobRunner{
		Queue:      queueClient,
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  deliverer,
	}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 0 {
		t.Fatalf("delivered jobs = %+v; want stored finalization replay", deliverer.jobs)
	}
	if !reflect.DeepEqual(steps, []string{
		"cancel",
		"replay:qjob_interrupt",
		"ack:qjob_interrupt",
	}) {
		t.Fatalf("steps = %v; want cancel before stored finalization replay", steps)
	}
}

func TestJobRunnerHandlesRuntimeConfigAndCleanupAsSeparateQueueKinds(t *testing.T) {
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{
		{ //nolint:gosec // Test lease token fixture, not a secret.
			Id:          "qjob_config",
			WorkspaceId: "ws_bridge",
			Kind:        "runtime_config_update",
			LeaseToken:  "lease_config",
			PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","config_generation":7}`,
		},
		{
			Id:          "qjob_cleanup",
			WorkspaceId: "ws_bridge",
			Kind:        "cleanup_session",
			LeaseToken:  "lease_cleanup",
			PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","cleanup_job_id":"cleanup_1"}`,
		},
		{
			Id: "qjob_delete_cleanup", WorkspaceId: "ws_bridge", Kind: "session_delete_cleanup", LeaseToken: "lease_delete_cleanup",
			PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","delete_cleanup_id":"delcln_1"}`,
		},
	}}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := len(deliverer.jobs); got != 3 {
		t.Fatalf("delivered jobs = %d; want 3", got)
	}
	if deliverer.jobs[0].Kind != "runtime_config_update" ||
		deliverer.jobs[0].CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH ||
		deliverer.jobs[0].RuntimeInputID != "runtime_config_update:sesn_1:7" ||
		deliverer.jobs[0].ConfigGeneration != "7" {
		t.Fatalf("runtime config job = %#v", deliverer.jobs[0])
	}
	if deliverer.jobs[1].Kind != "cleanup_session" ||
		deliverer.jobs[1].CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION ||
		deliverer.jobs[1].CleanupJobID != "cleanup_1" {
		t.Fatalf("cleanup job = %#v", deliverer.jobs[1])
	}
	if deliverer.jobs[2].Kind != "session_delete_cleanup" || deliverer.jobs[2].DeleteCleanupID != "delcln_1" || deliverer.jobs[2].RuntimeInputID != "session_delete_cleanup:delcln_1" {
		t.Fatalf("delete cleanup job = %#v", deliverer.jobs[2])
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"ack:qjob_config", "ack:qjob_cleanup", "ack:qjob_delete_cleanup"}) {
		t.Fatalf("queue transitions = %v; want duplicate responses to ack", queueClient.transitions)
	}
}

func TestJobRunnerDeadLettersStringRuntimeConfigGeneration(t *testing.T) {
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{
		{ //nolint:gosec // Test lease token fixture, not a secret.
			Id:          "qjob_config_string",
			WorkspaceId: "ws_bridge",
			Kind:        "runtime_config_update",
			LeaseToken:  "lease_config_string",
			PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","config_generation":"7"}`,
		},
	}}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}

	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.jobs) != 0 {
		t.Fatalf("delivered jobs = %+v; want invalid config payload dead-lettered before delivery", deliverer.jobs)
	}
	if !reflect.DeepEqual(queueClient.transitions, []string{"dead:qjob_config_string:invalid_runtime_job_payload"}) {
		t.Fatalf("queue transitions = %v; want dead-letter for invalid config payload", queueClient.transitions)
	}
}

func TestRuntimeConfigUpdateDecodesReferenceOnlyMCPManifestIntent(t *testing.T) {
	job := &queuev1.QueueJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Id:             "qjob_manifest_refs",
		WorkspaceId:    "ws_bridge",
		Kind:           "runtime_config_update",
		LeaseToken:     "lease_manifest_refs",
		PayloadVersion: 2,
		PayloadJson:    `{"workspace_id":"ws_bridge","session_id":"sesn_1","mcp_server_name":"github","manifest_generation":7}`,
		AttemptCount:   3,
		MaxAttempts:    5,
	}

	decoded, err := DecodeRuntimeJob(job)
	if err != nil {
		t.Fatalf("DecodeRuntimeJob refs-only MCP manifest: %v", err)
	}
	if decoded.Kind != "runtime_config_update" ||
		decoded.CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH ||
		decoded.RuntimeInputID != "runtime_config_update:mcp_manifest:sesn_1:github:7" ||
		decoded.ConfigGeneration != "" ||
		decoded.MCPServerName != "github" ||
		decoded.MCPManifestGeneration != "7" ||
		decoded.MCPManifestETag != "" ||
		decoded.AttemptCount != 3 || decoded.MaxAttempts != 5 {
		t.Fatalf("decoded refs-only MCP manifest job = %#v", decoded)
	}
}

func TestRuntimeConfigUpdateStillDecodesLegacyFatMCPManifestIntent(t *testing.T) {
	job := &queuev1.QueueJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Id:           "qjob_manifest",
		WorkspaceId:  "ws_bridge",
		Kind:         "runtime_config_update",
		LeaseToken:   "lease_manifest",
		PayloadJson:  `{"workspace_id":"ws_bridge","session_id":"sesn_1","config_generation":"legacy-ignored","mcp_manifest":{"mcp_server_name":"github","manifest_etag":"etag_1","manifest_generation":1,"tools":[{"name":"github_search","description":"Search GitHub","input_schema":{"type":"object"}}]}}`,
		AttemptCount: 3,
		MaxAttempts:  5,
	}

	decoded, err := DecodeRuntimeJob(job)
	if err != nil {
		t.Fatalf("DecodeRuntimeJob MCP manifest: %v", err)
	}
	if decoded.Kind != "runtime_config_update" ||
		decoded.CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH ||
		decoded.RuntimeInputID != "runtime_config_update:mcp_manifest:sesn_1:github:1" ||
		decoded.ConfigGeneration != "" ||
		decoded.MCPServerName != "github" ||
		decoded.MCPManifestETag != "etag_1" ||
		decoded.MCPManifestGeneration != "1" ||
		decoded.MCPManifestReadiness != "ready" ||
		decoded.AttemptCount != 3 || decoded.MaxAttempts != 5 ||
		decoded.PayloadJSON != job.GetPayloadJson() {
		t.Fatalf("decoded MCP manifest job = %#v", decoded)
	}
}

func TestJobRunnerFinalManifestAttemptFinalizesBeforeQueueDeadLetter(t *testing.T) {
	steps := []string{}
	job := &queuev1.QueueJob{ //nolint:gosec // Test lease token fixture, not a secret.
		Id: "qjob_manifest_final", WorkspaceId: "ws_bridge", Kind: "runtime_config_update", LeaseToken: "lease_manifest_final",
		AttemptCount: 5, MaxAttempts: 5,
		PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","mcp_manifest":{"mcp_server_name":"github","manifest_etag":"etag_1","manifest_generation":1,"readiness":"ready","diagnostic":null,"tools":[]}}`,
	}
	queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{job}, steps: &steps}
	deliverer := &recordingDeliverer{
		result:         RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "runtime_busy"},
		finalizeResult: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_delivery_exhausted"},
		steps:          &steps,
	}
	runner := &JobRunner{Queue: queueClient, Workspaces: staticWorkspaceLister{"ws_bridge"}, Deliverer: deliverer}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(deliverer.finalizations) != 1 {
		t.Fatalf("manifest finalizations = %#v; want one", deliverer.finalizations)
	}
	got := deliverer.finalizations[0].job
	if got.AttemptCount != 5 || got.MaxAttempts != 5 || got.MCPServerName != "github" || got.MCPManifestGeneration != "1" {
		t.Fatalf("finalized manifest job = %#v", got)
	}
	if !reflect.DeepEqual(steps, []string{"deliver:qjob_manifest_final", "finalize:qjob_manifest_final", "dead:qjob_manifest_final"}) {
		t.Fatalf("steps = %v; want manifest finalization before dead-letter", steps)
	}
}

func TestRuntimeInputKindRejectsConfigAndCleanupShapes(t *testing.T) {
	for _, inputKind := range []string{"runtime_config_patch", "cleanup_session"} {
		t.Run(inputKind, func(t *testing.T) {
			job := runtimeInputQueueJob()
			job.PayloadJson = fmt.Sprintf(`{"workspace_id":"ws_bridge","session_id":"sesn_1","session_thread_id":"thr_1","runtime_input_id":"rin_1","event_ids":["evt_1"],"sequence_from":1,"sequence_to":1,"input_kind":%q}`, inputKind)
			if _, err := DecodeRuntimeJob(job); err == nil {
				t.Fatalf("DecodeRuntimeJob accepted runtime_input input_kind %q", inputKind)
			}
		})
	}
}

func TestRuntimeInputTaskNotificationDoesNotRequirePublicEvents(t *testing.T) {
	job := runtimeInputQueueJob()
	job.AttemptCount = 3
	job.MaxAttempts = 5
	job.PayloadJson = `{"workspace_id":"ws_bridge","session_id":"sesn_1","session_thread_id":"thr_1","runtime_input_id":"rin_task","event_ids":[],"input_kind":"task_notification","preparation_attempt_id":"prep_1"}`
	decoded, err := DecodeRuntimeJob(job)
	if err != nil {
		t.Fatalf("DecodeRuntimeJob task_notification: %v", err)
	}
	if decoded.InputKind != "task_notification" ||
		decoded.CommandKind != agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION ||
		len(decoded.EventIDs) != 0 ||
		decoded.SequenceFrom != 0 ||
		decoded.SequenceTo != 0 ||
		decoded.AttemptCount != 3 ||
		decoded.MaxAttempts != 5 {
		t.Fatalf("decoded task notification = %#v; want no public event fence", decoded)
	}
}

func TestJobRunnerMapsRuntimeRejectedResponse(t *testing.T) {
	tests := []struct {
		name       string
		result     RuntimeDeliveryResult
		transition string
	}{
		{
			name: "retryable",
			result: RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "runtime_busy",
				ErrorMessage: "runtime busy",
			},
			transition: "retry:qjob_1:runtime_busy",
		},
		{
			name: "nonretryable",
			result: RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				ErrorKind:    "binding_generation_mismatch",
				ErrorMessage: "binding generation mismatch",
			},
			transition: "dead:qjob_1:binding_generation_mismatch",
		},
		{
			name:       "invalid-status",
			result:     RuntimeDeliveryResult{},
			transition: "dead:qjob_1:invalid_runtime_response",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queueClient := &recordingQueueClient{leased: []*queuev1.QueueJob{runtimeInputQueueJob()}}
			runner := &JobRunner{
				Queue:      queueClient,
				Workspaces: staticWorkspaceLister{"ws_bridge"},
				Deliverer:  &recordingDeliverer{result: test.result},
				Config:     JobRunnerConfig{},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if !reflect.DeepEqual(queueClient.transitions, []string{test.transition}) {
				t.Fatalf("queue transitions = %v; want %s", queueClient.transitions, test.transition)
			}
		})
	}
}

func TestRuntimeDeliveryResultFromResponse(t *testing.T) {
	tests := []struct {
		name     string
		response *agentruntimev1.RuntimeInputCommandResponse
		want     RuntimeDeliveryResult
	}{
		{
			name:     "accepted",
			response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED},
			want:     RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted},
		},
		{
			name:     "duplicate",
			response: &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_DUPLICATE},
			want:     RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate},
		},
		{
			name: "rejected",
			response: &agentruntimev1.RuntimeInputCommandResponse{
				Status:    agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
				Retryable: true,
				ErrorCode: agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BINDING_IDENTITY_MISMATCH,
			},
			want: RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "binding_identity_mismatch",
				ErrorMessage: "runtime rejected input",
			},
		},
		{
			name: "unspecified error code folds to generic",
			response: &agentruntimev1.RuntimeInputCommandResponse{
				Status:    agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
				Retryable: false,
				ErrorCode: agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_UNSPECIFIED,
			},
			want: RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    false,
				ErrorKind:    "runtime_rejected_input",
				ErrorMessage: "runtime rejected input",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := RuntimeDeliveryResultFromResponse(test.response); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("RuntimeDeliveryResultFromResponse = %#v; want %#v", got, test.want)
			}
		})
	}
}

func TestRuntimeDeliveryResultFromResponseForRequestRejectsIdentityMismatch(t *testing.T) {
	request := &agentruntimev1.RuntimeInputCommandRequest{
		SessionId:         "sesn_1",
		RuntimeInputId:    "rin_1",
		BindingId:         "bind_1",
		BindingGeneration: 7,
	}
	for _, response := range []*agentruntimev1.RuntimeInputCommandResponse{
		{
			Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED,
			SessionId:         "sesn_other",
			RuntimeInputId:    "rin_1",
			BindingId:         "bind_1",
			BindingGeneration: 7,
		},
		{
			Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_DUPLICATE,
			SessionId:         "sesn_1",
			RuntimeInputId:    "rin_other",
			BindingId:         "bind_1",
			BindingGeneration: 7,
		},
		{
			Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
			SessionId:         "sesn_1",
			RuntimeInputId:    "rin_1",
			BindingId:         "bind_other",
			BindingGeneration: 7,
		},
		{
			Status:            agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED,
			SessionId:         "sesn_1",
			RuntimeInputId:    "rin_1",
			BindingId:         "bind_1",
			BindingGeneration: 8,
		},
	} {
		result := RuntimeDeliveryResultFromResponseForRequest(response, request)
		if result.Status != RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "invalid_runtime_response_identity" {
			t.Fatalf("identity mismatch response result = %#v; want terminal invalid_runtime_response_identity", result)
		}
	}
}

func TestRunJobRunnerLoopLogsPollFailureWithSafeSharedFields(t *testing.T) {
	buffer := &lockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &JobRunner{
		Queue:      pollFailingQueueClient{err: errors.New("raw queue connection string should not appear")},
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  &recordingDeliverer{},
		Config: JobRunnerConfig{
			PollInterval: time.Millisecond,
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- RunJobRunnerLoop(ctx, runner, slog.New(slog.NewJSONHandler(buffer, nil)))
	}()
	deadline := time.After(time.Second)
	for !strings.Contains(buffer.String(), `"msg":"bridge.job_runner.poll_failed"`) {
		select {
		case <-deadline:
			t.Fatalf("job runner did not emit poll failure log: %s", buffer.String())
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunJobRunnerLoop returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunJobRunnerLoop did not stop after context cancellation")
	}
	logOutput := buffer.String()
	if strings.Contains(logOutput, "raw queue connection string") {
		t.Fatalf("job runner log leaked raw poll error: %s", logOutput)
	}
	for _, want := range []string{
		`"msg":"bridge.job_runner.poll_failed"`,
		`"operation":"bridge.job_runner.poll"`,
		`"event.kind":"poll_failed"`,
		`"component":"` + ServiceNameJobRunner + `"`,
		`"retryable":true`,
		`"terminal":false`,
		`"error.class":"bridge_job_runner_error"`,
		`"error.code":"poll_failed"`,
		`"error.message_safe":"job runner poll failed"`,
	} {
		if !strings.Contains(logOutput, want) {
			t.Fatalf("job runner log missing %s: %s", want, logOutput)
		}
	}
}

func TestRunJobRunnerLoopBacksOffAcrossConsecutiveEmptyPolls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &JobRunner{
		Queue:      &recordingQueueClient{},
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  &recordingDeliverer{},
		Config: JobRunnerConfig{
			PollInterval: time.Millisecond,
		},
	}
	var delays []time.Duration
	err := runJobRunnerLoop(ctx, runner, nil, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 4 {
			cancel()
			return context.Canceled
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runJobRunnerLoop: %v", err)
	}
	if want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond, 8 * time.Millisecond}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("empty-poll delays = %v; want %v", delays, want)
	}
}

func TestRunJobRunnerLoopResetsBackoffWhenRepairWasActiveBeforePollFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &JobRunner{
		Queue:      pollFailingQueueClient{err: errors.New("queue unavailable")},
		Workspaces: staticWorkspaceLister{"ws_bridge"},
		Deliverer:  &recordingDeliverer{repairCount: 1},
		Config: JobRunnerConfig{
			PollInterval: time.Millisecond,
		},
	}
	var delays []time.Duration
	err := runJobRunnerLoop(ctx, runner, nil, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		if len(delays) == 3 {
			cancel()
			return context.Canceled
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runJobRunnerLoop: %v", err)
	}
	if want := []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("poll delays = %v; want active repair to reset backoff to %v", delays, want)
	}
}

func runtimeInputQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:          "qjob_1",
		WorkspaceId: "ws_bridge",
		Kind:        "runtime_input",
		LeaseToken:  "lease_1",
		PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","session_thread_id":"thr_1","runtime_input_id":"rin_1","event_ids":["evt_1"],"sequence_from":1,"sequence_to":1,"input_kind":"messages","preparation_attempt_id":"prep_1"}`,
	}
}

func runtimeInterruptQueueJob() *queuev1.QueueJob {
	return &queuev1.QueueJob{
		Id:          "qjob_interrupt",
		WorkspaceId: "ws_bridge",
		Kind:        "runtime_input",
		LeaseToken:  "lease_interrupt",
		PayloadJson: `{"workspace_id":"ws_bridge","session_id":"sesn_1","session_thread_id":"thr_1","runtime_input_id":"rin_interrupt","event_ids":["evt_interrupt"],"sequence_from":9,"sequence_to":9,"input_kind":"interrupt_control","preparation_attempt_id":"prep_1"}`,
	}
}

type recordingQueueClient struct {
	mu                sync.Mutex
	leased            []*queuev1.QueueJob
	leaseKinds        []string
	leaseWorkspaceIDs []string
	leaseErrWorkspace string
	transitions       []string
	heartbeatLost     bool
	heartbeatErr      error
	heartbeats        int
	heartbeatNotify   chan struct{}
	steps             *[]string
}

type staticWorkspaceLister []workspace.ID

func (l staticWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return append([]workspace.ID(nil), l...), nil
}

func (c *recordingQueueClient) Lease(_ context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leaseKinds = append([]string(nil), request.GetKinds()...)
	c.leaseWorkspaceIDs = append(c.leaseWorkspaceIDs, request.GetWorkspaceId())
	if c.leaseErrWorkspace != "" && request.GetWorkspaceId() == c.leaseErrWorkspace {
		return nil, errors.New("queue unavailable for " + c.leaseErrWorkspace)
	}
	return &queuev1.LeaseResponse{Jobs: c.leased}, nil
}

func (c *recordingQueueClient) Heartbeat(_ context.Context, _ *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heartbeats++
	if c.heartbeatNotify != nil {
		select {
		case c.heartbeatNotify <- struct{}{}:
		default:
		}
	}
	if c.heartbeatErr != nil {
		return nil, c.heartbeatErr
	}
	return &queuev1.TransitionResponse{Updated: !c.heartbeatLost}, nil
}

func (c *recordingQueueClient) transitionSnapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.transitions...)
}

func (c *recordingQueueClient) Ack(_ context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	c.transitions = append(c.transitions, "ack:"+request.GetJobId())
	if c.steps != nil {
		*c.steps = append(*c.steps, "ack:"+request.GetJobId())
	}
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (c *recordingQueueClient) Retry(_ context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	c.transitions = append(c.transitions, "retry:"+request.GetJobId()+":"+request.GetErrorKind())
	if c.steps != nil {
		*c.steps = append(*c.steps, "retry:"+request.GetJobId())
	}
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (c *recordingQueueClient) DeadLetter(_ context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	c.transitions = append(c.transitions, "dead:"+request.GetJobId()+":"+request.GetErrorKind())
	if c.steps != nil {
		*c.steps = append(*c.steps, "dead:"+request.GetJobId())
	}
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (c *recordingQueueClient) Cancel(_ context.Context, request *queuev1.CancelRequest) (*queuev1.CancelResponse, error) {
	c.transitions = append(c.transitions, "cancel:"+request.GetSessionId()+":"+request.GetSessionThreadId()+":"+int64String(request.GetInterruptFenceSequence()))
	if c.steps != nil {
		*c.steps = append(*c.steps, "cancel")
	}
	return &queuev1.CancelResponse{CancelledCount: 2}, nil
}

type recordingDeliverer struct {
	result                 RuntimeDeliveryResult
	err                    error
	jobs                   []RuntimeJob
	repairErr              error
	repairCount            int
	repairCalls            int
	repairWorkspaceID      string
	repairWorkspaceIDs     []string
	repairLimit            int
	mailRepairErr          error
	mailRepairCount        int
	mailRepairCalls        int
	mailRepairWorkspaceID  string
	mailRepairWorkspaceIDs []string
	mailRepairLimit        int
	finalizeResult         RuntimeDeliveryResult
	finalizeErr            error
	finalizations          []recordedRuntimeFinalization
	steps                  *[]string
	replayResult           RuntimeDeliveryResult
	replayFound            bool
	replayErr              error
	replayJobs             []RuntimeJob
	sealedAttempt          string
	sealErr                error
}

type recordedRuntimeFinalization struct {
	job    RuntimeJob
	result RuntimeDeliveryResult
}

type blockingDeliverer struct {
	release   chan struct{}
	cancelled chan struct{}
}

func newBlockingDeliverer() *blockingDeliverer {
	return &blockingDeliverer{release: make(chan struct{}), cancelled: make(chan struct{})}
}

func (d *blockingDeliverer) DeliverRuntimeJob(ctx context.Context, _ RuntimeJob) (RuntimeDeliveryResult, error) {
	select {
	case <-d.release:
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
	case <-ctx.Done():
		close(d.cancelled)
		return RuntimeDeliveryResult{}, ctx.Err()
	}
}

func (d *blockingDeliverer) ReplayRuntimeDeliveryFinalization(context.Context, RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return RuntimeDeliveryResult{}, false, nil
}

func (d *blockingDeliverer) ResolveRuntimeInputSeal(context.Context, RuntimeJob) (string, error) {
	return "", nil
}

func (d *recordingDeliverer) ResolveRuntimeInputSeal(context.Context, RuntimeJob) (string, error) {
	return d.sealedAttempt, d.sealErr
}

func (d *recordingDeliverer) RepairRuntimeInbox(_ context.Context, workspaceID string, limit int) (int, error) {
	d.repairCalls++
	d.repairWorkspaceID = workspaceID
	d.repairWorkspaceIDs = append(d.repairWorkspaceIDs, workspaceID)
	d.repairLimit = limit
	return d.repairCount, d.repairErr
}

func (d *recordingDeliverer) RepairCompletionMail(_ context.Context, workspaceID string, limit int) (int, error) {
	d.mailRepairCalls++
	d.mailRepairWorkspaceID = workspaceID
	d.mailRepairWorkspaceIDs = append(d.mailRepairWorkspaceIDs, workspaceID)
	d.mailRepairLimit = limit
	return d.mailRepairCount, d.mailRepairErr
}

func (d *recordingDeliverer) DeliverRuntimeJob(_ context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	d.jobs = append(d.jobs, job)
	if d.steps != nil {
		*d.steps = append(*d.steps, "deliver:"+job.JobID)
	}
	return d.result, d.err
}

func (d *recordingDeliverer) FinalizeRuntimeDelivery(_ context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	d.finalizations = append(d.finalizations, recordedRuntimeFinalization{job: job, result: result})
	if d.steps != nil {
		*d.steps = append(*d.steps, "finalize:"+job.JobID)
	}
	if d.finalizeResult.Status == "" && d.finalizeErr == nil {
		return result, nil
	}
	return d.finalizeResult, d.finalizeErr
}

func (d *recordingDeliverer) ReplayRuntimeDeliveryFinalization(_ context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	d.replayJobs = append(d.replayJobs, job)
	if d.steps != nil {
		*d.steps = append(*d.steps, "replay:"+job.JobID)
	}
	return d.replayResult, d.replayFound, d.replayErr
}

type pollFailingQueueClient struct {
	err error
}

func (c pollFailingQueueClient) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	return nil, c.err
}

func (c pollFailingQueueClient) Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error) {
	return nil, errors.New("unexpected heartbeat")
}

func (c pollFailingQueueClient) Ack(context.Context, *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	return nil, errors.New("unexpected ack")
}

func (c pollFailingQueueClient) Retry(context.Context, *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	return nil, errors.New("unexpected retry")
}

func (c pollFailingQueueClient) DeadLetter(context.Context, *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	return nil, errors.New("unexpected dead letter")
}

func (c pollFailingQueueClient) Cancel(context.Context, *queuev1.CancelRequest) (*queuev1.CancelResponse, error) {
	return nil, errors.New("unexpected cancel")
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func int64String(value int64) string {
	return fmt.Sprintf("%d", value)
}
