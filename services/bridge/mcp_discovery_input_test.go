package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type inputDiscoveryLister func(context.Context, MCPManifestListRequest) (MCPManifestListResult, error)

func (f inputDiscoveryLister) ListMCPTools(ctx context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	return f(ctx, request)
}

func newInputDiscoveryFixture(t *testing.T) (*PostgreSQLRuntimeDeliveryStore, *sql.DB, RuntimeJob) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_discovery", "thrd_sesn_discovery", "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_discovery", "bind_discovery", 1, "pod_discovery")
	job := RuntimeJob{Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: "sesn_discovery", SessionThreadID: "thrd_sesn_discovery",
		RuntimeInputID: "rin_discovery", InputKind: "messages", EventIDs: []string{"evt_discovery"}, SequenceFrom: 1, SequenceTo: 1}
	seedBridgeAPIEvent(t, admin, "default", job.SessionID, job.SessionThreadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"run"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)
	return NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090), admin, job
}

func TestMCPInputDiscoveryFailureReplayAndNextInputRecovery(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	failed := &recordingMCPManifestLister{err: errors.New("private cursor failure")}
	store.MCPManifestLister = failed
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	deliver := RuntimePodDirectDeliverer{Store: store, Sender: sender}
	result, err := deliver.DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || result.Retryable || len(sender.requests) != 0 || len(failed.requests) != 3 {
		t.Fatalf("failed input = %+v/%v sends=%d attempts=%d", result, err, len(sender.requests), len(failed.requests))
	}
	// A new process handles the same input: no discovery, no second error, no model.
	restarted := NewPostgreSQLRuntimeDeliveryStore(store.Client, 9090)
	restarted.MCPManifestLister = failed
	_, err = (RuntimePodDirectDeliverer{Store: restarted, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || len(failed.requests) != 3 || len(sender.requests) != 0 {
		t.Fatalf("failed-input replay performed work: %v requests=%d sends=%d", err, len(failed.requests), len(sender.requests))
	}
	var state, diagnostic, payload string
	var errorsCount, idleCount int
	if err := admin.QueryRow(`SELECT status FROM sessions WHERE id=$1`, job.SessionID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FILTER (WHERE type='session.error'), count(*) FILTER (WHERE type='session.status_idle') FROM session_events WHERE session_id=$1`, job.SessionID).Scan(&errorsCount, &idleCount); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT mcp_discovery_diagnostic FROM session_runtime_inbox WHERE runtime_input_id=$1`, job.RuntimeInputID).Scan(&diagnostic); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT payload_json FROM session_events WHERE session_id=$1 AND type='session.error'`, job.SessionID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if state != "idle" || errorsCount != 1 || idleCount != 1 || diagnostic != "internal" {
		t.Fatalf("settlement = %s/%d/%d/%s", state, errorsCount, idleCount, diagnostic)
	}
	if !strings.Contains(payload, `"mcp_server_name":"github"`) || strings.Contains(payload, "private") {
		t.Fatalf("unsafe/incomplete error: %s", payload)
	}

	// Only a distinct input receives a fresh budget. It must install generation 2 first.
	next := job
	next.RuntimeInputID, next.EventIDs, next.SequenceFrom, next.SequenceTo = "rin_recovery", []string{"evt_recovery"}, 10, 10
	seedBridgeAPIEvent(t, admin, "default", next.SessionID, next.SessionThreadID, next.EventIDs[0], 10, "user.message", `{"content":[{"type":"text","text":"retry"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, next)
	restarted.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_recovered", "github_search")}
	result, err = (RuntimePodDirectDeliverer{Store: restarted, Sender: sender}).DeliverRuntimeJob(context.Background(), next)
	if err != nil || result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 2 {
		t.Fatalf("recovery = %+v/%v sends=%#v", result, err, sender.requests)
	}
	config, ok := sender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest)
	if !ok || config.GetMcpManifest().GetGeneration() != 2 || !strings.Contains(config.GetMcpManifest().GetContentJson(), "github_search") {
		t.Fatalf("missing generation-2 installation: %#v", sender.requests[0])
	}
	input, ok := sender.requests[1].(*agentruntimev1.AcceptInputRequest)
	if !ok || input.GetRuntimeInputId() != next.RuntimeInputID {
		t.Fatalf("wrong input after installation: %#v", sender.requests[1])
	}
}

func TestMCPInputDiscoveryReservationSurvivesRestartAndDeadline(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprint("deadline_expired=", expire), func(t *testing.T) {
			store, admin, job := newInputDiscoveryFixture(t)
			for range 2 {
				if _, err := store.reserveMCPDiscoveryAttempt(context.Background(), job, time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			if expire {
				if _, err := admin.Exec(`UPDATE session_runtime_inbox SET mcp_discovery_deadline_at=clock_timestamp()-interval '1 second' WHERE runtime_input_id=$1`, job.RuntimeInputID); err != nil {
					t.Fatal(err)
				}
			}
			restarted := NewPostgreSQLRuntimeDeliveryStore(store.Client, 9090)
			lister := &recordingMCPManifestLister{err: errors.New("failed")}
			restarted.MCPManifestLister = lister
			_, err := restarted.PrepareRuntimeCommand(context.Background(), job)
			var exhausted runtimeDeliveryPrepareError
			want := 1
			if expire {
				want = 0
			}
			if !errors.As(err, &exhausted) || exhausted.retryable || len(lister.requests) != want {
				t.Fatalf("restart = %v attempts=%d want=%d", err, len(lister.requests), want)
			}
		})
	}
}

func TestMCPInputDiscoveryCancellationDoesNotSettleInput(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	store.MCPManifestLister = inputDiscoveryLister(func(ctx context.Context, _ MCPManifestListRequest) (MCPManifestListResult, error) {
		cancel()
		<-ctx.Done()
		return MCPManifestListResult{}, ctx.Err()
	})
	_, err := store.PrepareRuntimeCommand(ctx, job)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
	var state string
	var attempts, events int
	if err := admin.QueryRow(`SELECT status, mcp_discovery_attempts FROM session_runtime_inbox WHERE runtime_input_id=$1`, job.RuntimeInputID).Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error'`, job.SessionID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if state != "queued" || attempts != 1 || events != 0 {
		t.Fatalf("cancel mutated settlement: %s/%d/%d", state, attempts, events)
	}
}

func TestMCPInputDiscoveryDeadlineRejectsLateSuccess(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	calls := 0
	store.MCPManifestLister = inputDiscoveryLister(func(ctx context.Context, _ MCPManifestListRequest) (MCPManifestListResult, error) {
		calls++
		<-ctx.Done()
		return mcpManifestResult("etag_after_deadline", "github_search"), nil
	})
	err := store.captureInitialMCPManifestsWithListTimeout(context.Background(), job, []MCPManifestToolsetConfig{{MCPServerName: "github", BuiltinFamily: "claude"}}, time.Now(), 10*time.Millisecond)
	var exhausted runtimeDeliveryPrepareError
	if !errors.As(err, &exhausted) || exhausted.retryable || calls != 1 {
		t.Fatalf("late success: %v calls=%d", err, calls)
	}
	var readiness string
	if err := admin.QueryRow(`SELECT readiness FROM session_mcp_manifests WHERE session_id=$1`, job.SessionID).Scan(&readiness); err != nil {
		t.Fatal(err)
	}
	if readiness != "unready" {
		t.Fatal("late candidate was published")
	}
}

func TestMCPInputDiscoveryRejectsOversizedBridgeProjection(t *testing.T) {
	store, _, job := newInputDiscoveryFixture(t)
	calls := 0
	store.MCPManifestLister = inputDiscoveryLister(func(context.Context, MCPManifestListRequest) (MCPManifestListResult, error) {
		calls++
		return MCPManifestListResult{ManifestETag: "etag_oversized", Tools: []MCPManifestTool{{Name: "github_search", Description: strings.Repeat("x", MaxMcpManifestBytes), InputSchemaJSON: `{"type":"object"}`}}}, nil
	})
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || result.Retryable || calls != 3 || len(sender.requests) != 0 {
		t.Fatalf("oversized candidate: %+v/%v calls=%d sends=%d", result, err, calls, len(sender.requests))
	}
}

func TestMCPInputDiscoveryInstallationFailureDoesNotExecuteOrRelist(t *testing.T) {
	store, _, job := newInputDiscoveryFixture(t)
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{mcpManifestResult("etag_install", "github_search")}}
	store.MCPManifestLister = lister
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || !result.Retryable || len(sender.requests) != 1 {
		t.Fatalf("rejected install: %+v/%v sends=%#v", result, err, sender.requests)
	}
	if _, ok := sender.requests[0].(*agentruntimev1.ApplyRuntimeConfigRequest); !ok {
		t.Fatal("input executed before config ACK")
	}
	// The same input retries transport, with the accepted manifest already durable.
	sender.requests = nil
	sender.result = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	result, err = (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryAccepted || len(sender.requests) != 2 || len(lister.requests) != 1 {
		t.Fatalf("installation retry: %+v/%v sends=%d lists=%d", result, err, len(sender.requests), len(lister.requests))
	}
}

func TestMCPInputDiscoveryFencesLostCustodyBeforeAcceptingCandidate(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	store.MCPManifestLister = inputDiscoveryLister(func(context.Context, MCPManifestListRequest) (MCPManifestListResult, error) {
		if _, err := admin.Exec(`UPDATE session_runtime_inbox SET status='dead_lettered' WHERE runtime_input_id=$1`, job.RuntimeInputID); err != nil {
			t.Fatal(err)
		}
		return mcpManifestResult("etag_too_late", "github_search"), nil
	})
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryAuthorityLost || len(sender.requests) != 0 {
		t.Fatalf("late discovery: %+v/%v sends=%d", result, err, len(sender.requests))
	}
	var manifests int
	if err := admin.QueryRow(`SELECT count(*) FROM session_mcp_manifests WHERE session_id=$1`, job.SessionID).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if manifests != 0 {
		t.Fatal("late discovery published after input custody ended")
	}
}

func TestMCPInputDiscoveryLeaseReclaimKeepsSpentAttempts(t *testing.T) {
	store, admin, original := newInputDiscoveryFixture(t)
	q := queue.NewPostgreSQLStore(store.Client)
	leased := enqueueAndLeaseExhaustionJob(t, q, original, time.Now().UTC())
	job, err := DecodeRuntimeJob(queueJobProto(leased))
	if err != nil {
		t.Fatal(err)
	}
	store.MCPManifestLister = inputDiscoveryLister(func(context.Context, MCPManifestListRequest) (MCPManifestListResult, error) {
		if _, err := admin.Exec(`UPDATE queue_jobs SET max_attempts=10, leased_until=clock_timestamp()-interval '1 second' WHERE id=$1`, job.JobID); err != nil {
			t.Fatal(err)
		}
		return mcpManifestResult("etag_late_lease", "github_search"), nil
	})
	_, err = store.PrepareRuntimeCommand(context.Background(), job)
	var lost mcpDiscoveryAuthorityLostError
	if !errors.As(err, &lost) {
		t.Fatalf("expired lease accepted discovery: %v", err)
	}
	if count, err := q.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1}); err != nil || count != 1 {
		t.Fatalf("reclaim expired discovery lease: %d/%v", count, err)
	}
	leases, err := q.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "new-discovery-process", MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC()})
	if err != nil || len(leases) != 1 {
		t.Fatalf("reclaim: %d/%v", len(leases), err)
	}
	reclaimed, err := DecodeRuntimeJob(queueJobProto(leases[0]))
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.LeaseToken == job.LeaseToken {
		t.Fatal("lease token was not replaced")
	}
	restarted := NewPostgreSQLRuntimeDeliveryStore(store.Client, 9090)
	failures := &recordingMCPManifestLister{err: errors.New("still unavailable")}
	restarted.MCPManifestLister = failures
	_, err = restarted.PrepareRuntimeCommand(context.Background(), reclaimed)
	var exhausted runtimeDeliveryPrepareError
	if !errors.As(err, &exhausted) || exhausted.retryable || len(failures.requests) != 2 {
		t.Fatalf("reclaim replenished attempts: %v requests=%d", err, len(failures.requests))
	}
}

func TestMCPInputDiscoveryFailurePreservesOtherActiveThread(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	seedBridgeAPIChildThread(t, admin, "default", job.SessionID, job.SessionThreadID, "thr_other")
	if _, err := admin.Exec(`UPDATE session_threads SET status='running' WHERE id='thr_other'`); err != nil {
		t.Fatal(err)
	}
	store.MCPManifestLister = &recordingMCPManifestLister{err: errors.New("failed")}
	_, _ = store.PrepareRuntimeCommand(context.Background(), job)
	var state string
	var idle int
	if err := admin.QueryRow(`SELECT status FROM session_threads WHERE id='thr_other'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, job.SessionID).Scan(&idle); err != nil {
		t.Fatal(err)
	}
	if state != "running" || idle != 0 {
		t.Fatalf("sibling affected: %s idle=%d", state, idle)
	}
}

func TestMCPInputRecoveryInstallsThroughRealRuntimeAndReachesConnector(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	store.MCPManifestLister = &recordingMCPManifestLister{err: errors.New("unavailable")}
	if _, err := store.PrepareRuntimeCommand(context.Background(), job); err == nil {
		t.Fatal("expected failed first input")
	}
	var unreadyPayload string
	if err := store.Client.WithWorkspaceTx(context.Background(), "default", "test.unready_payload", func(tx *dbconnect.Tx) error {
		var err error
		unreadyPayload, _, err = runtimeMCPManifestCommandPayloadTx(context.Background(), tx, RuntimeJob{WorkspaceID: "default", SessionID: job.SessionID, MCPServerName: "github"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	next := job
	next.RuntimeInputID, next.EventIDs, next.SequenceFrom, next.SequenceTo = "rin_real_recovery", []string{"evt_real_recovery"}, 10, 10
	seedBridgeAPIEvent(t, admin, "default", next.SessionID, next.SessionThreadID, next.EventIDs[0], 10, "user.message", `{"content":[{"type":"text","text":"retry"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, next)
	store.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_real_recovery", "github_search")}
	plan, err := store.PrepareRuntimeCommand(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	var configPayload string
	if err := store.Client.WithWorkspaceTx(context.Background(), "default", "test.recovery_config", func(tx *dbconnect.Tx) error {
		var err error
		configPayload, _, err = runtimeCommandPayloadForJobTx(context.Background(), tx, RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: job.SessionID, ConfigGeneration: "1"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	bridge := NewPostgreSQLBridgeAPIStore(store.Client)
	bridge.RuntimeBindingTokenHMACKey = []byte("manifest-recovery-binding-token-key")
	cold, err := bridge.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: bridgeAPIScope(job.SessionID, job.SessionThreadID, "bind_discovery", 1, "pod_discovery")})
	if err != nil {
		t.Fatal(err)
	}
	sender := &bunRuntimeManifestCompositionSender{
		recordingRuntimeCommandSender: recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}},
		Recovery:                      true, InputPath: t.TempDir() + "/recovery.json", RuntimeConfigPayloadJSON: configPayload,
		ReadyManifestPayloadJSON: unreadyPayload, ReadyGeneration: 1, UnreadyGeneration: 2,
		ColdContextJSON: cold.GetContextJson(), ColdRuntimeBindingToken: cold.GetRuntimeBindingToken(), ToolName: "github_search",
	}
	result, err := plan.send(context.Background(), sender)
	if err != nil || result.Status != RuntimeDeliveryAccepted {
		t.Fatalf("real Runtime recovery: %+v/%v", result, err)
	}
	if sender.Result.WarmMCPConnectorCalls < 1 || sender.Result.ColdMCPConnectorCalls < 1 || sender.Result.WarmCurrentGeneration != 2 || sender.Result.ColdCurrentGeneration != 2 {
		t.Fatalf("recovered route proof: %+v", sender.Result)
	}
}
