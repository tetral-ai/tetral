package integration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestRuntimeRecoveryChildFinalExhaustionSkipsMailAfterParentCloseAdmission(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_child_recovery_parent_closing"
		mainThreadID   = "thr_child_recovery_parent_closing_main"
		parentThreadID = "thr_child_recovery_parent_closing"
		childThreadID  = "thr_child_recovery_parent_closing_target"
		recoverySource = "evt_child_recovery_parent_closing_source"
		closeSource    = "evt_child_recovery_parent_closing_close"
		bindingID      = "bind_child_recovery_parent_closing"
		podUID         = "pod_child_recovery_parent_closing"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, parentThreadID)
	sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, parentThreadID, childThreadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, "evt_child_recovery_parent_closing_created", 1, "session.thread_created",
		`{"type":"session.thread_created","parent_thread_id":"`+parentThreadID+`","source_tool_use_event_id":"evt_child_recovery_parent_closing_spawn"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childThreadID, recoverySource, 2, "session.thread_status_rescheduled", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainThreadID, closeSource, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"close_agent","input":{"task_name":"child"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, closeSource); err != nil {
		t.Fatalf("make close source public: %v", err)
	}
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, mainThreadID, closeSource)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	sessionfixture.SeedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET status='running' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("seed Session state: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='rescheduling'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childThreadID); err != nil {
		t.Fatalf("seed child recovery state: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	enqueue, err := queue.NewRuntimeRecoveryEnqueueRequest(workspace.DefaultID, sessionID, childThreadID, recoverySource, time.Now().UTC())
	if err != nil {
		t.Fatalf("build recovery Queue job: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue recovery Queue job: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET max_attempts=1
		WHERE workspace_id='default' AND id=$1`, queued.ID); err != nil {
		t.Fatalf("set recovery attempt ceiling: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeRecovery}, LeaseOwner: "closing-parent-recovery-finalizer",
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease recovery final attempt = %#v/%v", leased, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode recovery job: %v", err)
	}

	apiStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(client)
	admitted, err := apiStore.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: sessionfixture.BridgeAPIScope(sessionID, mainThreadID, bindingID, 1, podUID), SourceToolUseEventId: closeSource,
		TargetChildThreadId: parentThreadID, Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	})
	if err != nil || admitted.GetCommitted() == nil {
		t.Fatalf("admit parent close after recovery lease = %#v/%v", admitted, err)
	}

	deliveryStore := fixtureRuntimeDeliveryStore(client, admin, 9090)
	finalized, err := deliveryStore.FinalizeRuntimeDelivery(context.Background(), job, jobrunner.RuntimeDeliveryResult{
		Status: jobrunner.RuntimeDeliveryRejected, Retryable: true,
		ErrorKind: "runtime_transport_unavailable", ErrorMessage: "runtime recovery failed",
	})
	if err != nil || !finalized.QueueLeaseSettled || finalized.Retryable {
		t.Fatalf("finalize child recovery after parent close = %#v/%v", finalized, err)
	}

	var recoveryQueueStatus, childStatus string
	var failureEvents, closeoutEvents, completionEvents, completionInboxes, completionJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$2 AND id=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND session_thread_id=$3 AND type='session.error'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND session_thread_id=$3 AND type='session.thread_status_terminated'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$2 AND input_kind='agent_mail'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND partition_key='session:default:' || $2
		 AND kind='runtime_input' AND payload_json::jsonb->>'input_kind'='agent_mail')`,
		queued.ID, sessionID, childThreadID,
	).Scan(&recoveryQueueStatus, &childStatus, &failureEvents, &closeoutEvents, &completionEvents, &completionInboxes, &completionJobs); err != nil {
		t.Fatalf("read close-first recovery finalization: %v", err)
	}
	if recoveryQueueStatus != queue.StatusDeadLettered || childStatus != "failed" || failureEvents != 1 || closeoutEvents != 1 ||
		completionEvents != 0 || completionInboxes != 0 || completionJobs != 0 {
		t.Fatalf("close-first recovery = Queue %s child %s terminal %d/%d mail %d/%d/%d",
			recoveryQueueStatus, childStatus, failureEvents, closeoutEvents, completionEvents, completionInboxes, completionJobs)
	}
}

type racingInitialMCPManifestLister struct {
	initialStarted chan struct{}
	releaseInitial chan struct{}
	blockInitial   bool
	once           sync.Once
}

func (l *racingInitialMCPManifestLister) ListMCPTools(ctx context.Context, request mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
	if request.ManifestETag != "" {
		return mcpManifestResult(request.ManifestETag, "github_ready"), nil
	}
	if l.blockInitial {
		l.once.Do(func() { close(l.initialStarted) })
		select {
		case <-l.releaseInitial:
		case <-ctx.Done():
			return mcpmanifest.ListResult{}, ctx.Err()
		}
	}
	return mcpmanifest.ListResult{}, mcpmanifest.DiscoveryError{Diagnostic: mcpmanifest.DiagnosticDiscoveryUnavailable}
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPFailureSettlesSingleAttemptInput(t *testing.T) {
	connector := startInitialManifestFailureConnector(t)
	tests := []struct {
		name       string
		suffix     string
		lister     func(*testing.T) mcpmanifest.Lister
		diagnostic string
	}{
		{name: "credential unavailable", suffix: "credential", lister: func(*testing.T) mcpmanifest.Lister {
			return mcpmanifest.NewConnectorLister(connector.address, staticRuntimeCommandTokenSource{})
		}, diagnostic: mcpmanifest.DiagnosticCredentialUnavailable},
		{name: "server unavailable", suffix: "server", lister: func(*testing.T) mcpmanifest.Lister {
			return mcpmanifest.NewConnectorLister(connector.address, staticRuntimeCommandTokenSource{})
		}, diagnostic: mcpmanifest.DiagnosticDiscoveryUnavailable},
		{name: "discovery timeout", suffix: "timeout", lister: func(*testing.T) mcpmanifest.Lister {
			return mcpmanifest.NewConnectorLister(connector.address, staticRuntimeCommandTokenSource{})
		}, diagnostic: mcpmanifest.DiagnosticDiscoveryUnavailable},
		{name: "manifest invalid trailer", suffix: "invalid", lister: func(*testing.T) mcpmanifest.Lister {
			return mcpmanifest.NewConnectorLister(connector.address, staticRuntimeCommandTokenSource{})
		}, diagnostic: mcpmanifest.DiagnosticInvalid},
		{name: "untyped unavailable transport", suffix: "transport_unavailable", lister: func(t *testing.T) mcpmanifest.Lister { return newUntypedFailureMCPManifestLister(t, codes.Unavailable) }, diagnostic: mcpmanifest.DiagnosticDiscoveryUnavailable},
		{name: "untyped deadline transport", suffix: "transport_deadline", lister: func(t *testing.T) mcpmanifest.Lister {
			return newUntypedFailureMCPManifestLister(t, codes.DeadlineExceeded)
		}, diagnostic: mcpmanifest.DiagnosticDiscoveryUnavailable},
		{name: "manifest invalid locally", suffix: "local_invalid", lister: func(*testing.T) mcpmanifest.Lister {
			return &recordingMCPManifestLister{results: []mcpmanifest.ListResult{{ManifestETag: "etag_invalid", Tools: []mcpmanifest.Tool{{Name: "github_search", InputSchemaJSON: `not-json`}}}}}
		}, diagnostic: mcpmanifest.DiagnosticInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_initial_mcp_failure_" + test.suffix
			threadID := "thr_initial_mcp_failure_" + test.suffix
			seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_failure", 1, "pod_initial_mcp_failure")
			job := jobrunner.RuntimeJob{
				Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
				RuntimeInputID: "rin_initial_mcp_failure", EventIDs: []string{"evt_initial_mcp_failure"},
				SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"continue"}]}`)
			seedRuntimeInboxBirthForJob(t, admin, job)
			now := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			enqueueExhaustionJob(t, queueStore, job, now.Add(-time.Second))

			deliveryStore := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
			deliveryStore.Clock = func() time.Time { return now }
			deliveryStore.MCPManifestLister = test.lister(t)
			sender := &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
			runner := &jobrunner.JobRunner{
				Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{"default"},
				Deliverer: manifestCompositionDeliverer{direct: jobrunner.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}},
				Config:    jobrunner.JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
			}
			if err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("run single-attempt input: %v", err)
			}
			if len(sender.requests) != 0 {
				t.Fatalf("Runtime requests = %#v; want no model input", sender.requests)
			}
			var readiness, diagnostic, inboxStatus, inputQueueStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT readiness, diagnostic FROM session_mcp_manifests
				WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, sessionID).Scan(&readiness, &diagnostic); err != nil {
				t.Fatalf("read durable unready manifest: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
				WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
				t.Fatalf("read original Inbox custody: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
				WHERE workspace_id='default' AND kind='runtime_input' AND payload_json::jsonb ->> 'runtime_input_id'=$1`, job.RuntimeInputID).Scan(&inputQueueStatus); err != nil {
				t.Fatalf("read original Queue custody: %v", err)
			}
			if readiness != "unready" || diagnostic != test.diagnostic || inboxStatus != "dead_lettered" || inputQueueStatus != queue.StatusDeadLettered {
				t.Fatalf("manifest/Inbox/Queue = %s/%s %s/%s; want unready/%s dead_lettered/dead_lettered", readiness, diagnostic, inboxStatus, inputQueueStatus, test.diagnostic)
			}
			sessionfixture.AssertRuntimeMCPManifestQueueJob(t, admin, "default", sessionID, "github", 1)
			var manifestJobs, sessionErrors int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
				WHERE workspace_id='default' AND kind='runtime_config_update'
				AND payload_json::jsonb ->> 'session_id'=$1 AND payload_json::jsonb ->> 'mcp_server_name'='github'`, sessionID).Scan(&manifestJobs); err != nil {
				t.Fatalf("count manifest Queue custody: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
				WHERE workspace_id='default' AND session_id=$1 AND type='session.error'`, sessionID).Scan(&sessionErrors); err != nil {
				t.Fatalf("count Session errors: %v", err)
			}
			if manifestJobs != 1 || sessionErrors != 1 {
				t.Fatalf("manifest jobs/session errors = %d/%d; want 1/1", manifestJobs, sessionErrors)
			}
		})
	}
	stats := connector.finish(t)
	if stats.ListCalls != 12 || !stats.LogsRedacted {
		t.Fatalf("production Connector failure composition = %+v; want twelve typed calls with redacted logs", stats)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreSettlesUnclassifiedMCPFailureAfterBoundedRetries(t *testing.T) {
	tests := []struct {
		name   string
		code   codes.Code
		values []string
	}{
		{name: "invalid request", code: codes.InvalidArgument},
		{name: "unauthenticated", code: codes.Unauthenticated},
		{name: "permission denied", code: codes.PermissionDenied},
		{name: "internal", code: codes.Internal},
		{name: "failed precondition without token", code: codes.FailedPrecondition},
		{name: "duplicate token", code: codes.Unavailable, values: []string{"server_unavailable", "server_unavailable"}},
		{name: "wrong status pair", code: codes.FailedPrecondition, values: []string{"server_unavailable"}},
		{name: "unknown token", code: codes.Unavailable, values: []string{"unknown"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			const (
				sessionID = "sesn_initial_mcp_fail_closed"
				threadID  = "thr_initial_mcp_fail_closed"
			)
			seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_fail_closed", 1, "pod_initial_mcp_fail_closed")
			job := jobrunner.RuntimeJob{
				JobID: "qjob_initial_mcp_fail_closed", LeaseToken: "lease_initial_mcp_fail_closed", Kind: queue.KindRuntimeInput,
				WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
				RuntimeInputID: "rin_initial_mcp_fail_closed", EventIDs: []string{"evt_initial_mcp_fail_closed"},
				SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"retain custody"}]}`)
			seedRuntimeInboxBirthForJob(t, admin, job)
			store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
			store.MCPManifestLister = newExactFailureMCPManifestLister(t, test.code, test.values)
			sender := &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
			result, err := (jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
			if err != nil || result.Status == jobrunner.RuntimeDeliveryAccepted || len(sender.requests) != 0 {
				t.Fatalf("unclassified discovery delivery = %+v/%v requests:%d; want retained input without Runtime", result, err, len(sender.requests))
			}
			var inboxStatus string
			var manifests, manifestJobs, sessionErrors int
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
				WHERE workspace_id='default' AND runtime_input_id=$1`, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
				t.Fatalf("read retained Inbox: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT count(*) FROM session_mcp_manifests WHERE workspace_id='default' AND session_id=$1),
				(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_config_update'
				 AND payload_json::jsonb->>'session_id'=$1),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')`, sessionID).Scan(&manifests, &manifestJobs, &sessionErrors); err != nil {
				t.Fatalf("count fail-closed discovery facts: %v", err)
			}
			if inboxStatus != "dead_lettered" || manifests != 1 || manifestJobs != 1 || sessionErrors != 1 {
				t.Fatalf("fail-closed discovery facts = Inbox:%s manifests:%d jobs:%d errors:%d; want dead_lettered/1/1/1",
					inboxStatus, manifests, manifestJobs, sessionErrors)
			}
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreInitialMCPFailureRacesManifestNotification(t *testing.T) {
	setup := func(t *testing.T, lister mcpmanifest.Lister) (*sql.DB, *jobrunner.PostgreSQLRuntimeDeliveryStore, jobrunner.RuntimeJob, *recordingRuntimeCommandSender) {
		t.Helper()
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_initial_mcp_race"
			threadID  = "thr_initial_mcp_race"
		)
		seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_initial_mcp_race", 1, "pod_initial_mcp_race")
		job := jobrunner.RuntimeJob{
			JobID: "qjob_initial_mcp_race", LeaseToken: "lease_initial_mcp_race", Kind: queue.KindRuntimeInput,
			WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
			RuntimeInputID: "rin_initial_mcp_race", EventIDs: []string{"evt_initial_mcp_race"},
			SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
		}
		seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"race"}]}`)
		seedRuntimeInboxBirthForJob(t, admin, job)
		store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
		store.MCPManifestLister = lister
		sender := &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
		return admin, store, job, sender
	}
	readState := func(t *testing.T, admin *sql.DB, wantGeneration int64, wantReadiness string, wantJobs int) {
		t.Helper()
		var generation int64
		var readiness string
		if err := admin.QueryRowContext(context.Background(), `SELECT manifest_generation, readiness FROM session_mcp_manifests
			WHERE workspace_id='default' AND session_id='sesn_initial_mcp_race' AND mcp_server_name='github'`).Scan(&generation, &readiness); err != nil {
			t.Fatalf("read raced manifest: %v", err)
		}
		var jobs int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
			WHERE workspace_id='default' AND kind='runtime_config_update'
			AND payload_json::jsonb ->> 'session_id'='sesn_initial_mcp_race'
			AND payload_json::jsonb ->> 'mcp_server_name'='github'`).Scan(&jobs); err != nil {
			t.Fatalf("count raced manifest Queue jobs: %v", err)
		}
		if generation != wantGeneration || readiness != wantReadiness || jobs != wantJobs {
			t.Fatalf("raced manifest = generation %d readiness %s jobs %d; want %d/%s/%d", generation, readiness, jobs, wantGeneration, wantReadiness, wantJobs)
		}
	}

	t.Run("notification wins expected absence", func(t *testing.T) {
		lister := &racingInitialMCPManifestLister{initialStarted: make(chan struct{}), releaseInitial: make(chan struct{}), blockInitial: true}
		admin, store, job, sender := setup(t, lister)
		resultCh := make(chan jobrunner.RuntimeDeliveryResult, 1)
		errorCh := make(chan error, 1)
		go func() {
			result, err := (jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
			resultCh <- result
			errorCh <- err
		}()
		select {
		case <-lister.initialStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("initial discovery did not reach the staged list")
		}
		bridge := agentruntimebridge.NewPostgreSQLBridgeAPIStore(store.Client)
		bridge.MCPManifestLister = lister
		mustAcceptMCPManifestChange(t, bridge, job.SessionID, "etag_notification")
		close(lister.releaseInitial)
		if err := <-errorCh; err != nil {
			t.Fatalf("deliver after notification winner: %v", err)
		}
		if result := <-resultCh; result.Status != jobrunner.RuntimeDeliveryAccepted || len(sender.requests) != 2 {
			t.Fatalf("delivery after notification winner = %#v requests %d; want accepted/2", result, len(sender.requests))
		}
		readState(t, admin, 1, "ready", 1)
	})

	t.Run("initial failure wins before notification", func(t *testing.T) {
		lister := &racingInitialMCPManifestLister{}
		admin, store, job, sender := setup(t, lister)
		result, err := (jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
		if err != nil || result.Status != jobrunner.RuntimeDeliveryRejected || result.Retryable || len(sender.requests) != 0 {
			t.Fatalf("delivery after initial failure = %#v/%v requests %d; want terminal rejection/nil/0", result, err, len(sender.requests))
		}
		readState(t, admin, 1, "unready", 1)
		bridge := agentruntimebridge.NewPostgreSQLBridgeAPIStore(store.Client)
		bridge.MCPManifestLister = lister
		mustAcceptMCPManifestChange(t, bridge, job.SessionID, "etag_notification")
		readState(t, admin, 2, "ready", 2)
	})
}

func TestMCPManifestProductionCompositionRemovesWarmAndColdToolCatalogEntry(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_manifest_composition"
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, "thrd_"+sessionID)
	sessionfixture.SeedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions
		SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}'
		WHERE workspace_id = 'default' AND id = $1`, sessionID); err != nil {
		t.Fatalf("seed manifest composition tools: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_manifest_composition", 1, "pod_manifest_composition")

	exactTool := exactBoundMCPManifestTool(t)
	lister := &recordingMCPManifestLister{results: []mcpmanifest.ListResult{
		mcpManifestResult("etag_ready", "github_search"),
		{ManifestETag: "etag_over", Tools: []mcpmanifest.Tool{{
			Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON,
		}}},
		{ManifestETag: "etag_over", Tools: []mcpmanifest.Tool{{
			Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON,
		}}},
	}}
	bridge := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge.RuntimeBindingTokenHMACKey = []byte("manifest-composition-binding-token-key")
	bridge.MCPManifestLister = lister
	mustAcceptMCPManifestChange(t, bridge, sessionID, "etag_ready")
	readyPayload := deliverQueuedManifestPayload(t, runtime, admin, sessionID, 1)

	request := &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: sessionID, McpServerName: "github", ManifestEtag: "etag_over",
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := bridge.McpManifestChanged(context.Background(), request)
		if err != nil || (attempt == 0 && response.GetCommitted() == nil) || (attempt == 1 && response.GetDuplicate() == nil) {
			t.Fatalf("over-cap manifest attempt %d = %+v err %v; want committed then duplicate", attempt+1, response, err)
		}
	}
	var manifestRows, queueJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_mcp_manifests
		WHERE workspace_id = 'default' AND session_id = $1 AND manifest_generation = 2
		AND readiness = 'unready' AND diagnostic = 'manifest_too_large'`, sessionID).Scan(&manifestRows); err != nil {
		t.Fatalf("read unready manifest: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
		WHERE workspace_id = 'default' AND kind = 'runtime_config_update'
		AND payload_json::jsonb ->> 'session_id' = $1
		AND payload_json::jsonb ->> 'mcp_server_name' = 'github'`, sessionID).Scan(&queueJobs); err != nil {
		t.Fatalf("read manifest queue custody: %v", err)
	}
	if manifestRows != 1 || queueJobs != 2 {
		t.Fatalf("manifest rows/jobs = %d/%d; want one generation-2 row and generations 1+2 queue custody", manifestRows, queueJobs)
	}

	var runtimeConfigPayload string
	client := dbconnect.NewClientForTesting(runtime)
	var err error
	runtimeConfigPayload, err = prepareFixtureConfigPayload(context.Background(), client, admin, jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID, ConfigGeneration: "1", RuntimeInputID: "runtime_config_update:" + sessionID + ":1"})
	if err != nil {
		t.Fatalf("rebuild runtime config: %v", err)
	}

	cold, err := bridge.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: sessionfixture.BridgeAPIScope(sessionID, "thrd_"+sessionID, "bind_manifest_composition", 1, "pod_manifest_composition"),
	})
	if err != nil {
		t.Fatalf("load replacement Runtime context: %v", err)
	}
	sender := &bunRuntimeManifestCompositionSender{
		InputPath:                t.TempDir() + "/manifest-composition.json",
		RuntimeConfigPayloadJSON: runtimeConfigPayload,
		ReadyManifestPayloadJSON: readyPayload,
		ColdContextJSON:          cold.GetContextJson(),
		ColdRuntimeBindingToken:  cold.GetRuntimeBindingToken(),
		ReadyGeneration:          1,
		UnreadyGeneration:        2,
		ToolName:                 "github_search",
	}
	delivery := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	deliveryResult, err := (jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: sender}).DeliverRuntimeJob(
		context.Background(), queuedManifestJob(t, admin, sessionID, 2),
	)
	if err != nil {
		t.Fatalf("deliver unready manifest through Runtime command host: %v", err)
	}
	if deliveryResult.Status != jobrunner.RuntimeDeliveryAccepted {
		t.Fatalf("unready manifest delivery = %+v; want accepted Runtime command", deliveryResult)
	}
	if sender.Result.WarmCurrentGeneration != 2 || sender.Result.ColdCurrentGeneration != 2 ||
		sender.Result.WarmMCPConnectorCalls != 0 || sender.Result.WarmNextProviderRequests < 1 ||
		sender.Result.ColdMCPConnectorCalls != 0 || sender.Result.ColdNextProviderRequests < 1 {
		t.Fatalf("Runtime manifest composition = %+v; want warm/cold generation 2, completed next provider requests, and zero connector calls", sender.Result)
	}
}

func TestPostgreSQLMCPManifestExhaustionDefersAndRedrivesCurrentGenerationBeforePartitionFollower(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_manifest_exhaustion_composition"
		threadID  = "thrd_manifest_exhaustion_composition"
	)
	seedMCPFamilySession(t, admin, sessionID, threadID, "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_manifest_exhaustion", 1, "pod_manifest_exhaustion")
	bridge := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_exhaustion", "github_search")}
	mustAcceptMCPManifestChange(t, bridge, sessionID, "etag_exhaustion")

	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), queue.RetryPolicy{
		BaseDelay: time.Millisecond,
		MaxDelay:  time.Millisecond,
		RandomInt64: func(int64) int64 {
			return 0
		},
	})
	follower := jobrunner.RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_manifest_partition_follower", EventIDs: []string{"evt_manifest_partition_follower"},
		SequenceFrom: 1, SequenceTo: 1, InputKind: "messages",
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, follower.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"after manifest"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, follower)
	followerPayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": follower.RuntimeInputID, "event_ids": follower.EventIDs,
		"sequence_from": 1, "sequence_to": 1, "input_kind": "messages",
	})
	if err != nil {
		t.Fatalf("marshal partition follower: %v", err)
	}
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, follower.RuntimeInputID),
		PayloadVersion: 1, PayloadJSON: followerPayload, MaxAttempts: queue.DefaultMaxAttempts,
		Now: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("enqueue partition follower: %v", err)
	}

	var manifestJobID string
	var manifestMaxAttempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT id, max_attempts FROM queue_jobs
		WHERE workspace_id='default' AND kind='runtime_config_update'
		AND payload_json::jsonb ->> 'session_id'=$1
		AND payload_json::jsonb ->> 'mcp_server_name'='github'`, sessionID).Scan(&manifestJobID, &manifestMaxAttempts); err != nil {
		t.Fatalf("read manifest Queue job: %v", err)
	}
	rejected := jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryRejected, Retryable: true, ErrorKind: "binding_mismatch"}
	responses := make([]jobrunner.RuntimeDeliveryResult, 0, manifestMaxAttempts+1)
	for range manifestMaxAttempts {
		responses = append(responses, rejected)
	}
	responses = append(responses, jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted})
	sender := &recordingRuntimeCommandSender{results: responses}
	queueServer := tetralqueue.NewServer(queueStore, nil)
	deliveryStore := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	composedDeliverer := manifestCompositionDeliverer{direct: jobrunner.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}}
	runner := &jobrunner.JobRunner{
		Queue: queueServer, Workspaces: staticWorkspaceLister{"default"},
		Deliverer: composedDeliverer,
		Config:    jobrunner.JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	for attempt := 1; attempt <= manifestMaxAttempts; attempt++ {
		if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, manifestJobID); err != nil {
			t.Fatalf("make manifest attempt %d available: %v", attempt, err)
		}
		if err := runner.RunOnce(context.Background()); err != nil {
			t.Fatalf("run manifest attempt %d: %v", attempt, err)
		}
	}

	var generation int64
	var readiness, diagnostic, queueStatus string
	var attemptCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT manifest_generation, readiness, diagnostic FROM session_mcp_manifests
		WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, sessionID).Scan(&generation, &readiness, &diagnostic); err != nil {
		t.Fatalf("read exhausted manifest: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status, attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1`, manifestJobID).Scan(&queueStatus, &attemptCount); err != nil {
		t.Fatalf("read deferred manifest custody: %v", err)
	}
	if generation != 2 || readiness != "unready" || diagnostic != "delivery_exhausted" || queueStatus != queue.StatusPending || attemptCount != manifestMaxAttempts-1 {
		t.Fatalf("exhausted manifest/Queue = generation %d %s/%s, %s attempt %d; want generation 2 unready/delivery_exhausted, pending attempt %d", generation, readiness, diagnostic, queueStatus, attemptCount, manifestMaxAttempts-1)
	}
	blocked, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "partition-proof",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || len(blocked) != 0 {
		t.Fatalf("partition follower lease before manifest redrive = %d/%v; want blocked", len(blocked), err)
	}

	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second' WHERE workspace_id='default' AND id=$1`, manifestJobID); err != nil {
		t.Fatalf("make deferred manifest available: %v", err)
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("redrive deferred manifest: %v", err)
	}
	if len(sender.requests) != manifestMaxAttempts+1 {
		t.Fatalf("Runtime manifest requests = %d; want %d", len(sender.requests), manifestMaxAttempts+1)
	}
	redrive := sender.requests[len(sender.requests)-1].(*agentruntimev1.ApplyRuntimeConfigRequest)
	var rebuilt struct {
		MCPManifest struct {
			Generation int64  `json:"manifest_generation"`
			Readiness  string `json:"readiness"`
			Diagnostic string `json:"diagnostic"`
		} `json:"mcp_manifest"`
	}
	if err := json.Unmarshal([]byte(redrive.GetMcpManifest().GetContentJson()), &rebuilt); err != nil {
		t.Fatalf("decode redriven manifest command: %v", err)
	}
	if redrive.GetMcpManifest().GetGeneration() != 2 || rebuilt.MCPManifest.Generation != 2 || rebuilt.MCPManifest.Readiness != "unready" || rebuilt.MCPManifest.Diagnostic != "delivery_exhausted" {
		t.Fatalf("redriven config = generation %d payload %+v; want durable generation 2 unready payload", redrive.GetMcpManifest().GetGeneration(), rebuilt.MCPManifest)
	}

	followers, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "partition-proof",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if err != nil || len(followers) != 1 || followers[0].DedupeKey != queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, follower.RuntimeInputID) {
		t.Fatalf("partition follower after manifest ACK = %#v/%v; want one exact follower", followers, err)
	}
}

type manifestCompositionDeliverer struct {
	direct jobrunner.RuntimePodDirectDeliverer
}

func (d manifestCompositionDeliverer) DeliverRuntimeJob(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, error) {
	return d.direct.DeliverRuntimeJob(ctx, job)
}

func (d manifestCompositionDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job jobrunner.RuntimeJob, result jobrunner.RuntimeDeliveryResult) (jobrunner.RuntimeDeliveryResult, error) {
	return d.direct.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d manifestCompositionDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, bool, error) {
	return d.direct.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d manifestCompositionDeliverer) FinalizeMalformedRuntimeInputCustody(ctx context.Context, lease jobrunner.MalformedRuntimeInputLease) (jobrunner.MalformedRuntimeInputCustodyResult, error) {
	return d.direct.FinalizeMalformedRuntimeInputCustody(ctx, lease)
}

func deliverQueuedManifestPayload(t *testing.T, runtime *sql.DB, admin *sql.DB, sessionID string, generation int64) string {
	t.Helper()
	job := queuedManifestJob(t, admin, sessionID, generation)
	sender := &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
	delivery := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	result, err := (jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != jobrunner.RuntimeDeliveryAccepted || len(sender.requests) != 1 {
		t.Fatalf("deliver manifest generation %d = %+v, %v, requests %d", generation, result, err, len(sender.requests))
	}
	return sender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest).GetMcpManifest().GetContentJson()
}

func queuedManifestJob(t *testing.T, admin *sql.DB, sessionID string, generation int64) jobrunner.RuntimeJob {
	t.Helper()
	var queuedJobID, queuedPayload string
	if err := admin.QueryRowContext(context.Background(), `SELECT id, payload_json FROM queue_jobs
		WHERE workspace_id = 'default' AND kind = 'runtime_config_update'
		AND payload_json::jsonb ->> 'session_id' = $1
		AND payload_json::jsonb ->> 'mcp_server_name' = 'github'
		AND payload_json::jsonb ->> 'manifest_generation' = $2
		ORDER BY created_at LIMIT 1`, sessionID, fmt.Sprint(generation)).Scan(&queuedJobID, &queuedPayload); err != nil {
		t.Fatalf("read queued manifest generation %d: %v", generation, err)
	}
	job, err := jobrunner.DecodeRuntimeJob(&queuev1.QueueJob{
		Id: queuedJobID, WorkspaceId: "default", Kind: queue.KindRuntimeConfigUpdate,
		LeaseToken: "lease_manifest_composition", PayloadJson: queuedPayload,
	})
	if err != nil {
		t.Fatalf("decode queued manifest generation %d: %v", generation, err)
	}
	return job
}

type runtimeManifestCompositionResult struct {
	CommandResponse struct {
		Applied *struct{} `json:"applied"`
	} `json:"commandResponse"`
	WarmCurrentGeneration    int `json:"warmCurrentGeneration"`
	ColdCurrentGeneration    int `json:"coldCurrentGeneration"`
	WarmMCPConnectorCalls    int `json:"warmMcpConnectorCalls"`
	WarmNextProviderRequests int `json:"warmNextProviderRequests"`
	ColdMCPConnectorCalls    int `json:"coldMcpConnectorCalls"`
	ColdNextProviderRequests int `json:"coldNextProviderRequests"`
}

type bunRuntimeManifestCompositionSender struct {
	Recovery bool
	recordingRuntimeCommandSender
	InputPath                string
	RuntimeConfigPayloadJSON string
	ReadyManifestPayloadJSON string
	ColdContextJSON          string
	ColdRuntimeBindingToken  string
	ReadyGeneration          int
	UnreadyGeneration        int
	ToolName                 string
	Result                   runtimeManifestCompositionResult
}

func (s *bunRuntimeManifestCompositionSender) ApplyRuntimeConfig(ctx context.Context, _ jobrunner.RuntimePodTarget, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
	inputJSON, err := json.Marshal(map[string]any{
		"recovery":                 s.Recovery,
		"workspaceId":              request.GetWorkspaceId(),
		"sessionId":                request.GetSessionId(),
		"runtimeConfigPayloadJson": s.RuntimeConfigPayloadJSON,
		"readyManifestPayloadJson": s.ReadyManifestPayloadJSON,
		"runtimeCommandRequest": map[string]any{
			"workspaceId":       request.GetWorkspaceId(),
			"sessionId":         request.GetSessionId(),
			"bindingId":         request.GetBindingId(),
			"bindingGeneration": request.GetBindingGeneration(),
			"targetPodUid":      request.GetTargetPodUid(), "runtimeProcessId": request.GetRuntimeProcessId(),
			"mcpManifest": map[string]any{
				"mcpServerName": request.GetMcpManifest().GetMcpServerName(),
				"generation":    request.GetMcpManifest().GetGeneration(),
				"contentJson":   request.GetMcpManifest().GetContentJson(),
			},
		},
		"coldContextJson":         s.ColdContextJSON,
		"coldRuntimeBindingToken": s.ColdRuntimeBindingToken,
		"readyGeneration":         s.ReadyGeneration,
		"unreadyGeneration":       s.UnreadyGeneration,
		"toolName":                s.ToolName,
	})
	if err != nil {
		return nil, fmt.Errorf("encode Runtime composition input: %w", err)
	}
	if err := os.WriteFile(s.InputPath, inputJSON, 0o600); err != nil {
		return nil, fmt.Errorf("write Runtime composition input: %w", err)
	}
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/mcp-manifest-composition.ts", s.InputPath) //nolint:gosec // Fixed repository script and test-owned input.
	command.Dir = "../services/agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("run Runtime manifest composition: %w: %s", err, output)
	}
	if err := json.Unmarshal(output, &s.Result); err != nil {
		return nil, fmt.Errorf("decode Runtime manifest composition: %w: %s", err, output)
	}
	if s.Result.CommandResponse.Applied == nil {
		return nil, errors.New("Runtime manifest composition did not apply the config")
	}
	return &agentruntimev1.ApplyRuntimeConfigResponse{Outcome: &agentruntimev1.ApplyRuntimeConfigResponse_Applied{Applied: &agentruntimev1.ApplyRuntimeConfigApplied{}}}, nil
}

func TestPostgreSQLInitialMCPRefreshReachesRuntimeWithReadyToolCatalog(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_oauth_initial_manifest"
		threadID  = "thr_oauth_initial_manifest"
		eventID   = "evt_oauth_initial_manifest"
		inputID   = "rin_oauth_initial_manifest"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_oauth_manifest", 1, "pod_oauth_manifest")
	sessionfixture.SeedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}' WHERE workspace_id='default' AND id=$1`, sessionID); err != nil {
		t.Fatalf("seed installed MCP toolset: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"search"}]}`)
	connector := startOAuthInitialManifestConnector(t, runtime, admin, sessionID)

	job := jobrunner.RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: inputID, EventIDs: []string{eventID}, SequenceFrom: 1, SequenceTo: 1,
		InputKind: "messages",
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	now := time.Now().UTC()
	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(runtime), queue.RetryPolicy{
		BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
	})
	enqueueExhaustionJob(t, queueStore, job, now)
	seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(admin), "tetral-agent-runtime", "pod_oauth_manifest")
	deliveryStore := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, jobrunner.KubernetesRuntimeTargetResolver{LoadClient: fixtureRuntimeLoadClient(t), Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_oauth_manifest", PodIP: "10.0.0.10",
		}})
	}})
	deliveryStore.MCPManifestLister = mcpmanifest.NewConnectorLister(connector.address, staticRuntimeCommandTokenSource{})
	var runtimeConfigPayload string
	client := dbconnect.NewClientForTesting(runtime)
	var payloadErr error
	runtimeConfigPayload, payloadErr = prepareFixtureConfigPayload(context.Background(), client, admin, jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: sessionID, ConfigGeneration: "1", RuntimeInputID: "runtime_config_update:" + sessionID + ":1"})
	if payloadErr != nil {
		t.Fatalf("build OAuth Runtime config: %v", payloadErr)
	}

	sender := &oauthReadyRuntimeSender{recordingRuntimeCommandSender: recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}, admin: admin, client: client, inputPath: t.TempDir() + "/oauth-ready-provider.json", runtimeConfigPayload: runtimeConfigPayload}
	runner := &jobrunner.JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:    jobrunner.JobRunnerConfig{LeaseOwner: "oauth-manifest-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("deliver OAuth-backed initial manifest = active:%t err:%v", active, err)
	}
	if !sender.result.ToolPresent || sender.result.NextProviderRequests != 1 {
		t.Fatalf("Runtime ready Tool Catalog composition = %+v", sender.result)
	}

	var readiness, inboxStatus, inputQueueStatus string
	var diagnostic sql.NullString
	var generation, manifestJobs, sessionErrors int
	if err := admin.QueryRowContext(context.Background(), `SELECT readiness, diagnostic, manifest_generation
		FROM session_mcp_manifests WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, sessionID).Scan(&readiness, &diagnostic, &generation); err != nil {
		t.Fatalf("read OAuth-backed manifest: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
		WHERE workspace_id='default' AND runtime_input_id=$1`, inputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read accepted OAuth input: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID)).Scan(&inputQueueStatus); err != nil {
		t.Fatalf("read OAuth input Queue status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind='runtime_config_update'
		 AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'mcp_server_name'='github'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error')`, sessionID).Scan(&manifestJobs, &sessionErrors); err != nil {
		t.Fatalf("count OAuth manifest custody: %v", err)
	}
	if readiness != "ready" || diagnostic.Valid || generation != 1 || inboxStatus != "accepted" ||
		inputQueueStatus != queue.StatusAcknowledged || manifestJobs != 1 || sessionErrors != 0 {
		t.Fatalf("OAuth durable progression = ready:%s diagnostic:%q generation:%d Inbox:%s Queue:%s jobs:%d errors:%d sender:%s",
			readiness, diagnostic.String, generation, inboxStatus, inputQueueStatus, manifestJobs, sessionErrors, sender.lastError)
	}
	stats := connector.finish(t)
	if stats.TokenEndpointAttempts != 1 || stats.ToolsListCalls != 1 || !stats.UsedRotatedToken ||
		!stats.DurableRotation || !stats.LogsRedacted || len(stats.RefreshOutcomes) != 1 ||
		stats.RefreshOutcomes[0].Outcome != "refreshed" || stats.RefreshOutcomes[0].DurableWrite != "committed" ||
		stats.RefreshOutcomes[0].HTTPStatusClass != "2xx" {
		t.Fatalf("OAuth Connector composition stats = %+v", stats)
	}
}

type oauthReadyRuntimeSender struct {
	recordingRuntimeCommandSender
	admin                *sql.DB
	client               *dbconnect.Client
	inputPath            string
	runtimeConfigPayload string
	result               struct {
		ToolPresent          bool `json:"toolPresent"`
		NextProviderRequests int  `json:"nextProviderRequests"`
	}
	lastError string
}

func (s *oauthReadyRuntimeSender) AcceptInput(ctx context.Context, _ jobrunner.RuntimePodTarget, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	var manifestJobID, manifestJobEnvelope string
	if err := s.admin.QueryRowContext(ctx, `SELECT id, payload_json FROM queue_jobs
		WHERE workspace_id=$1 AND kind='runtime_config_update'
		AND payload_json::jsonb->>'session_id'=$2 AND payload_json::jsonb->>'mcp_server_name'='github'
		ORDER BY created_at LIMIT 1`, request.GetWorkspaceId(), request.GetSessionId()).Scan(&manifestJobID, &manifestJobEnvelope); err != nil {
		s.lastError = fmt.Sprintf("read ready manifest carrier: %v", err)
		return nil, errors.New(s.lastError)
	}
	manifestJob, err := jobrunner.DecodeRuntimeJob(&queuev1.QueueJob{
		Id: manifestJobID, WorkspaceId: request.GetWorkspaceId(), Kind: queue.KindRuntimeConfigUpdate,
		LeaseToken: "lease_oauth_manifest_projection", PayloadJson: manifestJobEnvelope,
	})
	if err != nil {
		s.lastError = fmt.Sprintf("decode ready manifest carrier: %v", err)
		return nil, errors.New(s.lastError)
	}
	var manifestPayload string
	manifestPayload, err = prepareFixtureConfigPayload(ctx, s.client, s.admin, manifestJob)
	if err != nil {
		s.lastError = fmt.Sprintf("build ready manifest payload: %v", err)
		return nil, errors.New(s.lastError)
	}

	raw, err := json.Marshal(map[string]any{
		"workspaceId": request.GetWorkspaceId(), "sessionId": request.GetSessionId(),
		"sessionThreadId": request.GetSessionThreadId(), "readyManifestPayloadJson": manifestPayload,
		"runtimeConfigPayloadJson": s.runtimeConfigPayload, "toolName": "github_search",
	})
	if err != nil {
		s.lastError = fmt.Sprintf("encode ready provider input: %v", err)
		return nil, errors.New(s.lastError)
	}
	if err := os.WriteFile(s.inputPath, raw, 0o600); err != nil {
		s.lastError = fmt.Sprintf("write ready provider input: %v", err)
		return nil, errors.New(s.lastError)
	}
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/mcp-ready-provider-composition.ts", s.inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../services/agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		s.lastError = fmt.Sprintf("run ready MCP provider composition: %v: %s", err, output)
		return nil, errors.New(s.lastError)
	}
	if err := json.Unmarshal(output, &s.result); err != nil {
		s.lastError = fmt.Sprintf("decode ready MCP provider composition: %v", err)
		return nil, errors.New(s.lastError)
	}
	return &agentruntimev1.AcceptInputResponse{Outcome: &agentruntimev1.AcceptInputResponse_Accepted{Accepted: &agentruntimev1.AcceptInputAccepted{}}}, nil
}

type oauthInitialManifestConnector struct {
	address  string
	command  *exec.Cmd
	scanner  *bufio.Scanner
	stdin    io.WriteCloser
	stderr   bytes.Buffer
	finished bool
}

type oauthInitialManifestStats struct {
	TokenEndpointAttempts int  `json:"tokenEndpointAttempts"`
	ToolsListCalls        int  `json:"toolsListCalls"`
	UsedRotatedToken      bool `json:"usedRotatedToken"`
	DurableRotation       bool `json:"durableRotation"`
	LogsRedacted          bool `json:"logsRedacted"`
	RefreshOutcomes       []struct {
		Outcome         string `json:"outcome"`
		DurableWrite    string `json:"durableWrite"`
		HTTPStatusClass string `json:"httpStatusClass"`
	} `json:"refreshOutcomes"`
}

func startOAuthInitialManifestConnector(t *testing.T, runtime, admin *sql.DB, sessionID string) *oauthInitialManifestConnector {
	t.Helper()
	var schema string
	if err := admin.QueryRowContext(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read isolated schema: %v", err)
	}
	input, err := json.Marshal(map[string]string{"schema": schema, "workspaceId": "default", "sessionId": sessionID})
	if err != nil {
		t.Fatalf("encode OAuth Connector fixture input: %v", err)
	}
	inputPath := t.TempDir() + "/oauth-connector.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write OAuth Connector fixture input: %v", err)
	}
	fixture := &oauthInitialManifestConnector{}
	fixture.command = exec.Command("bun", "packages/mcp-connector/test/fixtures/oauth-initial-manifest-server.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	fixture.command.Dir = "../services/gateway"
	fixture.command.Env = append(os.Environ(),
		"TETRAL_TEST_DATABASE_URL="+storagetest.AdminDatabaseURL(t, admin),
		"TETRAL_TEST_RUNTIME_DATABASE_URL="+storagetest.RuntimeDatabaseURL(t, runtime),
	)
	stdout, err := fixture.command.StdoutPipe()
	if err != nil {
		t.Fatalf("open OAuth Connector stdout: %v", err)
	}
	fixture.stdin, err = fixture.command.StdinPipe()
	if err != nil {
		t.Fatalf("open OAuth Connector stdin: %v", err)
	}
	fixture.command.Stderr = &fixture.stderr
	fixture.scanner = bufio.NewScanner(stdout)
	if err := fixture.command.Start(); err != nil {
		t.Fatalf("start OAuth Connector fixture: %v", err)
	}
	t.Cleanup(func() {
		if fixture.finished || fixture.command.Process == nil {
			return
		}
		_ = fixture.command.Process.Kill()
		_ = fixture.command.Wait()
	})
	if !fixture.scanner.Scan() {
		t.Fatalf("OAuth Connector fixture did not start: %v: %s", fixture.scanner.Err(), fixture.stderr.String())
	}
	var started struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(fixture.scanner.Bytes(), &started); err != nil || started.Address == "" {
		t.Fatalf("decode OAuth Connector address: %v: %q", err, fixture.scanner.Text())
	}
	fixture.address = started.Address
	return fixture
}

func (f *oauthInitialManifestConnector) finish(t *testing.T) oauthInitialManifestStats {
	t.Helper()
	if _, err := f.stdin.Write([]byte("finish\n")); err != nil {
		t.Fatalf("finish OAuth Connector fixture: %v", err)
	}
	if !f.scanner.Scan() {
		t.Fatalf("OAuth Connector fixture omitted stats: %v: %s", f.scanner.Err(), f.stderr.String())
	}
	var stats oauthInitialManifestStats
	if err := json.Unmarshal(f.scanner.Bytes(), &stats); err != nil {
		t.Fatalf("decode OAuth Connector stats: %v: %q", err, f.scanner.Text())
	}
	if err := f.command.Wait(); err != nil {
		t.Fatalf("OAuth Connector fixture exit: %v: %s", err, f.stderr.String())
	}
	f.finished = true
	return stats
}

type failingMCPManifestTransportServer struct {
	providergatewayv1.UnimplementedMcpConnectorServiceServer
	code   codes.Code
	values []string
}

func (s failingMCPManifestTransportServer) ListMcpTools(ctx context.Context, _ *providergatewayv1.ListMcpToolsRequest) (*providergatewayv1.ListMcpToolsResponse, error) {
	if len(s.values) > 0 {
		_ = grpc.SetTrailer(ctx, metadata.MD{mcpmanifest.FailureKindMetadataKey: s.values})
	}
	return nil, status.Error(s.code, "safe test failure")
}

func newUntypedFailureMCPManifestLister(t *testing.T, code codes.Code) mcpmanifest.Lister {
	return newExactFailureMCPManifestLister(t, code, nil)
}

func newExactFailureMCPManifestLister(t *testing.T, code codes.Code, values []string) mcpmanifest.Lister {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for untyped MCP failure: %v", err)
	}
	server := grpc.NewServer()
	providergatewayv1.RegisterMcpConnectorServiceServer(server, failingMCPManifestTransportServer{code: code, values: values})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return mcpmanifest.NewConnectorLister(listener.Addr().String(), staticRuntimeCommandTokenSource{})
}

type initialManifestFailureConnector struct {
	address  string
	command  *exec.Cmd
	scanner  *bufio.Scanner
	stdin    io.WriteCloser
	stderr   bytes.Buffer
	finished bool
}

type initialManifestFailureStats struct {
	ListCalls    int  `json:"listCalls"`
	LogsRedacted bool `json:"logsRedacted"`
}

func startInitialManifestFailureConnector(t *testing.T) *initialManifestFailureConnector {
	t.Helper()
	fixture := &initialManifestFailureConnector{}
	fixture.command = exec.Command("bun", "packages/mcp-connector/test/fixtures/initial-manifest-failure-server.ts") //nolint:gosec // Fixed repository fixture.
	fixture.command.Dir = "../services/gateway"
	stdout, err := fixture.command.StdoutPipe()
	if err != nil {
		t.Fatalf("open failure Connector stdout: %v", err)
	}
	fixture.stdin, err = fixture.command.StdinPipe()
	if err != nil {
		t.Fatalf("open failure Connector stdin: %v", err)
	}
	fixture.command.Stderr = &fixture.stderr
	fixture.scanner = bufio.NewScanner(stdout)
	if err := fixture.command.Start(); err != nil {
		t.Fatalf("start failure Connector fixture: %v", err)
	}
	t.Cleanup(func() {
		if fixture.finished || fixture.command.Process == nil {
			return
		}
		_ = fixture.command.Process.Kill()
		_ = fixture.command.Wait()
	})
	if !fixture.scanner.Scan() {
		t.Fatalf("failure Connector fixture did not start: %v: %s", fixture.scanner.Err(), fixture.stderr.String())
	}
	var started struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(fixture.scanner.Bytes(), &started); err != nil || started.Address == "" {
		t.Fatalf("decode failure Connector address: %v: %q", err, fixture.scanner.Text())
	}
	fixture.address = started.Address
	return fixture
}

func (f *initialManifestFailureConnector) finish(t *testing.T) initialManifestFailureStats {
	t.Helper()
	if _, err := f.stdin.Write([]byte("finish\n")); err != nil {
		t.Fatalf("finish failure Connector fixture: %v", err)
	}
	if !f.scanner.Scan() {
		t.Fatalf("failure Connector fixture omitted stats: %v: %s", f.scanner.Err(), f.stderr.String())
	}
	var stats initialManifestFailureStats
	if err := json.Unmarshal(f.scanner.Bytes(), &stats); err != nil {
		t.Fatalf("decode failure Connector stats: %v: %q", err, f.scanner.Text())
	}
	if err := f.command.Wait(); err != nil {
		t.Fatalf("failure Connector fixture exit: %v: %s", err, f.stderr.String())
	}
	f.finished = true
	return stats
}

func TestPostgreSQLRuntimeDeliveryStoreBuildsTaskNotificationFromBackgroundTask(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_task_delivery", "thr_bridge_task_delivery")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_delivery", "bind_bridge_task_delivery", 1, "pod_uid_task_delivery")
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", "sesn_bridge_task_delivery", "thr_bridge_task_delivery", "bind_bridge_task_delivery", "task_bridge_delivery", "sevt_tool_delivery")
	storedResult := fmt.Sprintf(
		`{"status":"completed","stdout":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":5000},"stderr":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":6000},"provider_command_id":"must_not_escape"}`,
		strings.Repeat("out-", 12800),
		strings.Repeat("err-", 12800),
	)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status='completed', terminal_result_json=$1, terminal_result_digest='digest', terminal_at='2026-01-01T00:01:00Z'
		WHERE workspace_id='default' AND session_id='sesn_bridge_task_delivery' AND task_id='task_bridge_delivery'`, storedResult); err != nil {
		t.Fatalf("seed terminal background task: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json,
		status, created_at, updated_at
	) VALUES ('default','sesn_bridge_task_delivery','thr_bridge_task_delivery','task_notification:task_bridge_delivery',
		'task_notification','[]','queued','2026-01-01T00:01:00Z','2026-01-01T00:01:00Z')`); err != nil {
		t.Fatalf("seed queued task notification: %v", err)
	}
	resolver := &recordingRuntimeTargetResolver{binding: runtimecontrol.Binding{
		BindingID:         "bind_bridge_task_delivery",
		BindingGeneration: 1,
		Namespace:         "runtime-ns",
		PodName:           "runtime-pod",
		PodUID:            "pod_uid_task_delivery",
		RuntimeProcessID:  "process_pod_uid_task_delivery",
		PodIP:             "10.0.0.1",
	}}
	store := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090, resolver)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }

	job := jobrunner.RuntimeJob{
		JobID:           "qjob_bridge_task_delivery",
		LeaseToken:      "lease_bridge_task_delivery",
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     "default",
		SessionID:       "sesn_bridge_task_delivery",
		SessionThreadID: "thr_bridge_task_delivery",
		RuntimeInputID:  "task_notification:task_bridge_delivery",
		EventIDs:        []string{},
		InputKind:       "task_notification",
		PayloadJSON:     `{"workspace_id":"default","session_id":"sesn_bridge_task_delivery","session_thread_id":"thr_bridge_task_delivery","runtime_input_id":"task_notification:task_bridge_delivery","event_ids":[],"input_kind":"task_notification"}`,
	}
	plan, err := store.PrepareRuntimeCommand(context.Background(), job)
	if err != nil {
		t.Fatalf("PrepareRuntimeCommand task notification: %v", err)
	}
	if plan.TaskNotification == nil || plan.TaskNotification.TaskID != "task_bridge_delivery" {
		t.Fatalf("prepared task notification plan = %#v", plan)
	}
	if len(resolver.jobs) != 1 || resolver.jobs[0].RuntimeInputID != job.RuntimeInputID || resolver.jobs[0].SessionThreadID != "thr_bridge_task_delivery" {
		t.Fatalf("target resolver jobs = %+v; want task notification delivery resolved through runtime target", resolver.jobs)
	}
	if plan.AcceptTask == nil || strings.Contains(plan.AcceptTask.GetNotificationJson(), "provider_command_id") {
		t.Fatalf("runtime task notification leaked provider metadata: %#v", plan.AcceptTask)
	}
	var payload struct {
		TaskID               string `json:"task_id"`
		SourceToolUseEventID string `json:"source_tool_use_event_id"`
		Status               string `json:"status"`
		Stdout               struct {
			Truncated     bool  `json:"truncated"`
			OriginalBytes int64 `json:"original_bytes"`
			OriginalLines int64 `json:"original_lines"`
		} `json:"stdout"`
		Stderr struct {
			Truncated     bool  `json:"truncated"`
			OriginalBytes int64 `json:"original_bytes"`
			OriginalLines int64 `json:"original_lines"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(plan.AcceptTask.GetNotificationJson()), &payload); err != nil {
		t.Fatalf("decode runtime task notification payload: %v", err)
	}
	if payload.TaskID != "task_bridge_delivery" || payload.SourceToolUseEventID != "sevt_tool_delivery" || payload.Status != "completed" {
		t.Fatalf("runtime task notification payload = %#v", payload)
	}
	if len([]byte(plan.AcceptTask.GetNotificationJson())) > runtimecontrol.RuntimeTaskNotificationPayloadMaxBytes {
		t.Fatalf("runtime task notification payload bytes = %d; want <= %d", len([]byte(plan.AcceptTask.GetNotificationJson())), runtimecontrol.RuntimeTaskNotificationPayloadMaxBytes)
	}
	if !payload.Stdout.Truncated || !payload.Stderr.Truncated ||
		payload.Stdout.OriginalBytes != 51200 || payload.Stdout.OriginalLines != 5000 ||
		payload.Stderr.OriginalBytes != 51200 || payload.Stderr.OriginalLines != 6000 {
		t.Fatalf("runtime task notification stream bounds = stdout:%+v stderr:%+v", payload.Stdout, payload.Stderr)
	}
	assertNoTaskOutputPaths(t, plan.AcceptTask.GetNotificationJson())
	apiStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	apiStore.Clock = store.Clock
	committed, err := apiStore.CommitTaskNotificationResult(context.Background(), sessionfixture.BridgeTaskNotificationRequestForTest(
		t,
		observedAttemptScope(job, plan.AttemptedBinding),
		job.RuntimeInputID,
	))
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult delivery: %v", err)
	}
	if committed.GetCommitted() == nil {
		t.Fatalf("CommitTaskNotificationResult outcome = %#v; want committed", committed)
	}
	var taskStatus string
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_delivery' AND task_id = 'task_bridge_delivery'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read task delivery status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = 'task_notification:task_bridge_delivery'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read task delivery inbox status: %v", err)
	}
	if taskStatus != "completed" || inboxStatus != "committed" {
		t.Fatalf("task delivery status=%q inbox=%q; want completed/committed", taskStatus, inboxStatus)
	}
}

func TestPostgreSQLJobRunnerReplaysIdleInterruptReceiptBeforeAckAndFollowerDelivery(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_bridge_interrupt_replay_fence"
		threadID         = "thr_bridge_interrupt_replay_fence"
		interruptEventID = "sevt_bridge_interrupt_replay_fence"
		messageEventID   = "sevt_bridge_interrupt_replay_successor"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_interrupt_replay_fence", 1, "pod_uid_interrupt_replay_fence")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEventID, 2, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, messageEventID, 3, "user.message", `{"content":[{"type":"text","text":"new turn"}]}`)

	interruptJob := jobrunner.RuntimeJob{
		JobID: "qjob_int_replay", LeaseToken: "lease_bridge_interrupt_replay_fence", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_bridge_interrupt_replay_fence", EventIDs: []string{interruptEventID},
		SequenceFrom: 2, SequenceTo: 2, InputKind: "interrupt_control",
		PayloadJSON: `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_replay_fence","session_thread_id":"thr_bridge_interrupt_replay_fence","runtime_input_id":"rin_bridge_interrupt_replay_fence","event_ids":["sevt_bridge_interrupt_replay_fence"],"sequence_from":2,"sequence_to":2,"input_kind":"interrupt_control"}`,
	}
	messageJob := jobrunner.RuntimeJob{
		JobID: "qjob_msg_replay", LeaseToken: "lease_bridge_interrupt_replay_successor", Kind: queue.KindRuntimeInput,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_bridge_interrupt_replay_successor", EventIDs: []string{messageEventID},
		SequenceFrom: 3, SequenceTo: 3, InputKind: "messages",
		PayloadJSON: `{"workspace_id":"default","session_id":"sesn_bridge_interrupt_replay_fence","session_thread_id":"thr_bridge_interrupt_replay_fence","runtime_input_id":"rin_bridge_interrupt_replay_successor","event_ids":["sevt_bridge_interrupt_replay_successor"],"sequence_from":3,"sequence_to":3,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, interruptJob)
	seedRuntimeInboxBirthForJob(t, admin, messageJob)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
		SET status='delivering', binding_id=$2, binding_generation=1, target_pod_uid=$3
		WHERE workspace_id='default' AND runtime_input_id=$1`,
		messageJob.RuntimeInputID, "bind_bridge_interrupt_replay_fence", "pod_uid_interrupt_replay_fence"); err != nil {
		t.Fatalf("seed preplanned follower Runtime authority: %v", err)
	}

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue := func(job jobrunner.RuntimeJob, maxAttempts int) {
		t.Helper()
		if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
			ID: job.JobID, WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
			PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
			DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, job.RuntimeInputID),
			PayloadVersion: 1, PayloadJSON: []byte(job.PayloadJSON), MaxAttempts: maxAttempts,
			Priority: func() int {
				if job.InputKind == "interrupt_control" {
					return 100
				}
				return 0
			}(),
			Now: time.Now().UTC().Add(-time.Minute),
		}); err != nil {
			t.Fatalf("enqueue %s: %v", job.RuntimeInputID, err)
		}
	}
	// Three attempts keep the receipt-bearing retry below terminal exhaustion,
	// so this proof distinguishes retry-start replay from final-attempt recovery.
	enqueue(interruptJob, 3)
	enqueue(messageJob, queue.DefaultMaxAttempts)

	deliveryStore := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)
	deliveryStore.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC) }
	apiStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	sender := &receiptGatedInterruptSender{
		recordingRuntimeCommandSender: &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}},
		store:                         apiStore,
		scope:                         sessionfixture.BridgeAPIScope(sessionID, threadID, "bind_bridge_interrupt_replay_fence", 1, "pod_uid_interrupt_replay_fence"),
		inputID:                       interruptJob.RuntimeInputID,
	}
	candidate := enginekubernetes.BindingCandidate{
		Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0",
		PodUID: "pod_uid_interrupt_replay_fence", PodIP: "10.0.0.10",
	}
	newRunner := func() *jobrunner.JobRunner {
		freshStore := jobrunner.NewJobRunnerRuntimeDeliveryStore(
			dbconnect.NewClientForTesting(runtime), nil, jobrunner.JobRunnerConfig{AgentRuntimeGRPCPort: 9090},
			func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{candidate})
			},
		)
		installFixtureRuntimeLoad(t, freshStore)
		freshStore.Clock = deliveryStore.Clock
		return &jobrunner.JobRunner{
			Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
			Deliverer: jobrunner.RuntimePodDirectDeliverer{Store: freshStore, Sender: sender},
			Config:    jobrunner.JobRunnerConfig{LeaseOwner: "interrupt-receipt-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
		}
	}

	if err := newRunner().RunOnce(context.Background()); err != nil {
		t.Fatalf("run hot-accepted interrupt without receipt: %v", err)
	}
	var interruptQueueStatus, followerQueueStatus, interruptInboxStatus string
	var interruptAvailableAt, followerAvailableAt time.Time
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT available_at FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT available_at FROM queue_jobs WHERE workspace_id='default' AND id=$2)`,
		interruptJob.JobID, messageJob.JobID, interruptJob.RuntimeInputID,
	).Scan(&interruptQueueStatus, &followerQueueStatus, &interruptInboxStatus, &interruptAvailableAt, &followerAvailableAt); err != nil {
		t.Fatalf("read receipt-pending barrier: %v", err)
	}
	if interruptQueueStatus != queue.StatusPending || followerQueueStatus != queue.StatusPending ||
		interruptInboxStatus != "delivering" || sender.interruptCalls != 1 || len(sender.requests) != 1 {
		t.Fatalf("receipt-pending facts = Queue %s follower %s Inbox %s interrupt calls/requests %d/%d",
			interruptQueueStatus, followerQueueStatus, interruptInboxStatus, sender.interruptCalls, len(sender.requests))
	}
	// Probe before the persisted retry deadline, even if the randomized delay
	// already elapsed before this test reached Lease.
	// The follower must already be time-eligible.
	probeAt := interruptAvailableAt.Add(-time.Microsecond)
	if followerAvailableAt.After(probeAt) {
		t.Fatal("follower is not yet time-eligible for the barrier probe")
	}
	blockedFollowers, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "receipt-pending-follower-proof",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: probeAt,
	})
	if err != nil || len(blockedFollowers) != 0 {
		t.Fatalf("follower leases before interrupt receipt = %#v/%v; want none", blockedFollowers, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, interruptJob.JobID); err != nil {
		t.Fatalf("make interrupt retry available: %v", err)
	}
	receiptLease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "receipt-response-lost",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
	})
	if receiptLease.ID != interruptJob.JobID || receiptLease.AttemptCount != 2 {
		t.Fatalf("receipt lease = %s attempt %d; want interrupt %s attempt 2", receiptLease.ID, receiptLease.AttemptCount, interruptJob.JobID)
	}
	committed, err := apiStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: sender.scope, RuntimeInputId: interruptJob.RuntimeInputID, InterruptLeaseRef: sessionfixture.BridgeInterruptLeaseRef(receiptLease),
	})
	if err != nil || committed.GetCommitted() == nil {
		t.Fatalf("commit late closeout before retry = %#v/%v", committed, err)
	}
	preAckFollower, err := apiStore.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: sender.scope, RuntimeInputId: messageJob.RuntimeInputID,
	})
	if err != nil || preAckFollower.GetBarrierStale() == nil {
		t.Fatalf("pre-ACK follower CommitInputs = %#v/%v; want delivery-barrier stale", preAckFollower, err)
	}
	var preACKMessages int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages
		WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&preACKMessages); err != nil {
		t.Fatalf("count pre-ACK follower Messages: %v", err)
	}
	if preACKMessages != 0 {
		t.Fatalf("pre-ACK follower Messages = %d; want 0", preACKMessages)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1`, receiptLease.ID); err != nil {
		t.Fatalf("expire lost receipt response lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim lost receipt response lease = %d/%v", reclaimed, err)
	}

	if err := newRunner().RunOnce(context.Background()); err != nil {
		t.Fatalf("run retry-start receipt replay: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3)`,
		interruptJob.JobID, messageJob.JobID, interruptJob.RuntimeInputID,
	).Scan(&interruptQueueStatus, &followerQueueStatus, &interruptInboxStatus); err != nil {
		t.Fatalf("read receipt-gated ACK: %v", err)
	}
	if interruptQueueStatus != queue.StatusAcknowledged || followerQueueStatus != queue.StatusPending ||
		interruptInboxStatus != "committed" || sender.interruptCalls != 1 || len(sender.requests) != 1 {
		t.Fatalf("receipt replay facts = Queue %s follower %s Inbox %s interrupt calls/requests %d/%d",
			interruptQueueStatus, followerQueueStatus, interruptInboxStatus, sender.interruptCalls, len(sender.requests))
	}

	if err := newRunner().RunOnce(context.Background()); err != nil {
		t.Fatalf("deliver receipt-released follower: %v", err)
	}
	var followerInboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		messageJob.JobID, messageJob.RuntimeInputID,
	).Scan(&followerQueueStatus, &followerInboxStatus); err != nil {
		t.Fatalf("read delivered follower: %v", err)
	}
	if followerQueueStatus != queue.StatusAcknowledged || followerInboxStatus != "accepted" ||
		sender.interruptCalls != 1 || len(sender.requests) != 2 {
		t.Fatalf("follower facts = Queue %s Inbox %s interrupt calls %d total requests %d; want acknowledged/accepted/1/2",
			followerQueueStatus, followerInboxStatus, sender.interruptCalls, len(sender.requests))
	}
	if _, ok := sender.requests[1].(*agentruntimev1.AcceptInputRequest); !ok {
		t.Fatalf("second Runtime request = %T; want exact follower AcceptInput", sender.requests[1])
	}
}

type receiptGatedInterruptSender struct {
	*recordingRuntimeCommandSender
	store          *agentruntimebridge.PostgreSQLBridgeAPIStore
	scope          *bridgev1.RuntimeScope
	inputID        string
	interruptCalls int
}

func (s *receiptGatedInterruptSender) Interrupt(
	_ context.Context,
	target jobrunner.RuntimePodTarget,
	request *agentruntimev1.InterruptRequest,
) (*agentruntimev1.InterruptResponse, error) {
	s.targets = append(s.targets, target)
	s.requests = append(s.requests, request)
	s.interruptCalls++
	return &agentruntimev1.InterruptResponse{
		Outcome: &agentruntimev1.InterruptResponse_Accepted{Accepted: &agentruntimev1.InterruptAccepted{}},
	}, nil
}
