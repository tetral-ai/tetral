package jobrunner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

type inputDiscoveryLister func(context.Context, mcpmanifest.ListRequest) (mcpmanifest.ListResult, error)

func (f inputDiscoveryLister) ListMCPTools(ctx context.Context, request mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
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
	return fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090), admin, job
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
			restarted := fixtureRuntimeDeliveryStore(store.Client, admin, 9090)
			lister := &recordingMCPManifestLister{err: errors.New("failed")}
			restarted.MCPManifestLister = lister
			_, err := restarted.PrepareRuntimeCommand(context.Background(), job)
			var exhausted runtimecontrol.PreparationError
			want := 1
			if expire {
				want = 0
			}
			if !errors.As(err, &exhausted) || exhausted.Retryable || len(lister.requests) != want {
				t.Fatalf("restart = %v attempts=%d want=%d", err, len(lister.requests), want)
			}
		})
	}
}

func TestMCPInputDiscoveryCancellationDoesNotSettleInput(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	store.MCPManifestLister = inputDiscoveryLister(func(ctx context.Context, _ mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
		cancel()
		<-ctx.Done()
		return mcpmanifest.ListResult{}, ctx.Err()
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
	store.MCPManifestLister = inputDiscoveryLister(func(ctx context.Context, _ mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
		calls++
		<-ctx.Done()
		return mcpManifestResult("etag_after_deadline", "github_search"), nil
	})
	err := store.captureInitialMCPManifestsWithListTimeout(context.Background(), job, []mcpmanifest.ToolsetConfig{{MCPServerName: "github", BuiltinFamily: "claude"}}, time.Now(), 10*time.Millisecond)
	var exhausted runtimecontrol.PreparationError
	if !errors.As(err, &exhausted) || exhausted.Retryable || calls != 1 {
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
	store.MCPManifestLister = inputDiscoveryLister(func(context.Context, mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
		calls++
		return mcpmanifest.ListResult{ManifestETag: "etag_oversized", Tools: []mcpmanifest.Tool{{Name: "github_search", Description: strings.Repeat("x", mcpmanifest.MaxBytes), InputSchemaJSON: `{"type":"object"}`}}}, nil
	})
	sender := &recordingRuntimeCommandSender{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	result, err := (RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != RuntimeDeliveryRejected || result.Retryable || calls != 3 || len(sender.requests) != 0 {
		t.Fatalf("oversized candidate: %+v/%v calls=%d sends=%d", result, err, calls, len(sender.requests))
	}
}

func TestMCPInputDiscoveryInstallationFailureDoesNotExecuteOrRelist(t *testing.T) {
	store, _, job := newInputDiscoveryFixture(t)
	lister := &recordingMCPManifestLister{results: []mcpmanifest.ListResult{mcpManifestResult("etag_install", "github_search")}}
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
	store.MCPManifestLister = inputDiscoveryLister(func(context.Context, mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
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
	store.MCPManifestLister = inputDiscoveryLister(func(context.Context, mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
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
	restarted := fixtureRuntimeDeliveryStore(store.Client, admin, 9090)
	failures := &recordingMCPManifestLister{err: errors.New("still unavailable")}
	restarted.MCPManifestLister = failures
	_, err = restarted.PrepareRuntimeCommand(context.Background(), reclaimed)
	var exhausted runtimecontrol.PreparationError
	if !errors.As(err, &exhausted) || exhausted.Retryable || len(failures.requests) != 2 {
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
