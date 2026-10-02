package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestPostgreSQLSeparatedOwnersManifestAcceptance(t *testing.T) {
	for _, variant := range []string{"runner", "bridge", "concurrent"} {
		t.Run(variant, func(t *testing.T) {
			f := newSeparatedOwners(t, "manifest_"+variant, true)
			gate := &separatedManifestGate{result: mcpManifestResult("etag_shared", "github_search"), entered: make(chan mcpmanifest.ListRequest, 2), release: make(chan struct{})}
			f.runner.MCPManifestLister = gate
			f.bridge.MCPManifestLister = gate
			// Register cancellation and release before either discovery can block.
			ctx, cancel := context.WithCancel(f.ctx)
			t.Cleanup(cancel)
			var releaseOnce bool
			t.Cleanup(func() {
				if !releaseOnce {
					close(gate.release)
				}
			})
			results := make(chan error, 2)
			participants := 0
			if variant != "bridge" {
				job, _ := f.input(t)
				participants++
				separatedStartRun(ctx, t, func(ownerCtx context.Context) error {
					_, err := f.runner.PrepareRuntimeCommand(ownerCtx, job)
					results <- err
					return nil
				})
			}
			if variant != "runner" {
				participants++
				separatedStartRun(ctx, t, func(ownerCtx context.Context) error {
					_, err := f.bridge.McpManifestChanged(ownerCtx, &bridgev1.McpManifestChangedRequest{WorkspaceId: "default", SessionId: f.sessionID, McpServerName: "github", ManifestEtag: "etag_shared"})
					results <- err
					return nil
				})
			}
			for i := 0; i < participants; i++ {
				request := separatedWait(ctx, t, gate.entered)
				if request.SessionID != f.sessionID || request.MCPServerName != "github" {
					t.Fatalf("external discovery identity=%v", request)
				}
			}
			// Both owners are parked outside acceptance. A third independent Session
			// transaction must commit while external I/O is still outstanding.
			probeCtx, probeCancel := context.WithTimeout(ctx, 2*time.Second)
			probe, err := f.admin.BeginTx(probeCtx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := probe.ExecContext(probeCtx, `SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, f.sessionID); err != nil {
				_ = probe.Rollback()
				probeCancel()
				t.Fatalf("discovery held Session transaction: %v", err)
			}
			if err := probe.Commit(); err != nil {
				t.Fatal(err)
			}
			probeCancel()
			close(gate.release)
			releaseOnce = true
			for i := 0; i < participants; i++ {
				if err := separatedWait(ctx, t, results); err != nil {
					t.Fatal(err)
				}
			}
			if rows := f.count(t, `SELECT count(*) FROM session_mcp_manifests WHERE workspace_id='default' AND session_id=$1`, f.sessionID); rows != 1 {
				t.Fatalf("accepted manifest rows=%d", rows)
			}
			assertQueuedMCPManifestGenerations(t, f.admin, f.sessionID, []int64{1})
			assertSeparatedManifest(t, f, 1, "ready", "etag_shared", "github_search")
			duplicate, err := f.bridge.McpManifestChanged(ctx, &bridgev1.McpManifestChangedRequest{WorkspaceId: "default", SessionId: f.sessionID, McpServerName: "github", ManifestEtag: "etag_shared"})
			if err != nil || duplicate.GetDuplicate() == nil {
				t.Fatalf("cross-owner duplicate=%v/%v", duplicate, err)
			}
			assertQueuedMCPManifestGenerations(t, f.admin, f.sessionID, []int64{1})
			t.Logf("variant=%s participants=%d; external discovery barrier observed; independent Session Tx committed before release; one generation=1 row/job; duplicate joined", variant, participants)
		})
	}
	t.Run("ready_unready_ready_and_collision", func(t *testing.T) {
		f := newSeparatedOwners(t, "manifest_flap", true)
		lister := &separatedManifestGate{result: mcpmanifest.ListResult{ManifestETag: "etag_A", Tools: []mcpmanifest.Tool{{Name: "Read", Description: "must collide with claude builtin", InputSchemaJSON: `{"type":"object"}`}, {Name: "github_search", Description: "github_search", InputSchemaJSON: `{"type":"object"}`}}}}
		f.bridge.MCPManifestLister = lister
		change := func(etag string) {
			t.Helper()
			response, err := f.bridge.McpManifestChanged(f.ctx, &bridgev1.McpManifestChangedRequest{WorkspaceId: "default", SessionId: f.sessionID, McpServerName: "github", ManifestEtag: etag})
			if err != nil || response.GetCommitted() == nil {
				t.Fatalf("manifest transition=%v/%v", response, err)
			}
		}
		change("etag_A")
		assertSeparatedManifest(t, f, 1, "ready", "etag_A", "github_search")
		var retained string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT tools_json FROM session_mcp_manifests WHERE session_id=$1`, f.sessionID).Scan(&retained); err != nil {
			t.Fatal(err)
		}
		lister.result = mcpmanifest.ListResult{ManifestETag: "etag_over", Tools: []mcpmanifest.Tool{{Name: "github_over", Description: strings.Repeat("x", 262145), InputSchemaJSON: `{"type":"object"}`}}}
		change("etag_over")
		assertSeparatedManifest(t, f, 2, "unready", "", " ")
		var stored string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT tools_json FROM session_mcp_manifests WHERE session_id=$1`, f.sessionID).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if stored != retained {
			t.Fatal("overage replaced accepted manifest bytes")
		}
		// Replaying the retained ETag restores readiness without rediscovery.
		change("etag_A")
		assertSeparatedManifest(t, f, 3, "ready", "etag_A", "github_search")
		lister.result = mcpManifestResult("etag_B", "github_next")
		change("etag_B")
		assertSeparatedManifest(t, f, 4, "ready", "etag_B", "github_next")
		lister.result = mcpManifestResult("etag_A", "github_search")
		change("etag_A")
		assertSeparatedManifest(t, f, 5, "ready", "etag_A", "github_search")
		assertQueuedMCPManifestGenerations(t, f.admin, f.sessionID, []int64{1, 2, 3, 4, 5})
		t.Log("ready/unready/ready plus A/B/A generations=1/2/3/4/5; overage preserved accepted bytes; builtin collision excluded; hot and cold readiness observed")
	})
}

func assertSeparatedManifest(t *testing.T, f *separatedOwners, generation int64, readiness, etag, tool string) {
	t.Helper()
	var actualGeneration int64
	var actualReadiness string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT manifest_generation,readiness FROM session_mcp_manifests WHERE workspace_id='default' AND session_id=$1 AND mcp_server_name='github'`, f.sessionID).Scan(&actualGeneration, &actualReadiness); err != nil {
		t.Fatal(err)
	}
	if actualGeneration != generation || actualReadiness != readiness {
		t.Fatalf("persisted manifest=%d/%s; want %d/%s", actualGeneration, actualReadiness, generation, readiness)
	}
	plan, err := f.runner.PrepareRuntimeCommand(f.ctx, jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: f.sessionID, SessionThreadID: f.threadID, RuntimeInputID: mcpmanifest.InputID(f.sessionID, "github", 1), MCPServerName: "github", MCPManifestGeneration: "1"})
	if err != nil || plan.RuntimeConfig.GetMcpManifest() == nil {
		t.Fatalf("actual manifest command=%#v/%v", plan, err)
	}
	var hot map[string]any
	if err := json.Unmarshal([]byte(plan.RuntimeConfig.GetMcpManifest().GetContentJson()), &hot); err != nil {
		t.Fatal(err)
	}
	hot = hot["mcp_manifest"].(map[string]any)
	if plan.RuntimeConfig.GetMcpManifest().GetGeneration() != generation || hot["readiness"] != readiness {
		t.Fatalf("hot manifest=%v wire generation=%d", hot, plan.RuntimeConfig.GetMcpManifest().GetGeneration())
	}
	cold := f.cold(t, observedAttemptScope(f.configJob("1"), plan.AttemptedBinding))["mcpManifests"].([]any)
	if len(cold) != 1 {
		t.Fatalf("cold manifest count=%d", len(cold))
	}
	manifest := cold[0].(map[string]any)
	if manifest["manifestGeneration"] != float64(generation) || manifest["readiness"] != readiness {
		t.Fatalf("cold manifest=%v", manifest)
	}
	if readiness == "ready" {
		separatedEqual(t, hot["tools"], separatedJSON(t, fmt.Sprintf(`[{"name":%q,"description":%q,"input_schema":{"type":"object"}}]`, tool, tool)))
		separatedEqual(t, manifest["tools"], separatedJSON(t, fmt.Sprintf(`[{"name":%q,"description":%q,"inputSchema":{"type":"object"}}]`, tool, tool)))
		if manifest["manifestETag"] != etag || hot["manifest_etag"] != etag {
			t.Fatalf("hot/cold etag=%v/%v", hot, manifest)
		}
	} else {
		if _, present := hot["tools"]; present {
			t.Fatal("unready hot manifest exposes accepted tools")
		}
		if _, present := hot["manifest_etag"]; present {
			t.Fatal("unready hot manifest exposes accepted etag")
		}
		separatedEqual(t, manifest["tools"], []any{})
	}
}

func TestPostgreSQLSeparatedOwnersTransactionRollback(t *testing.T) {
	t.Run("first_manifest_queue_insert", func(t *testing.T) {
		f := newSeparatedOwners(t, "rollback_manifest", true)
		f.runner.MCPManifestLister = &separatedManifestGate{result: mcpManifestResult("etag_rollback", "github_search")}
		job, _ := f.input(t)
		f.sql(t, `CREATE FUNCTION separated_fail_manifest_job() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='runtime_config_update' THEN RAISE EXCEPTION 'injected manifest Queue insertion failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER separated_fail_manifest_job BEFORE INSERT ON queue_jobs FOR EACH ROW EXECUTE FUNCTION separated_fail_manifest_job()`)
		t.Cleanup(func() {
			_, _ = f.admin.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS separated_fail_manifest_job ON queue_jobs; DROP FUNCTION IF EXISTS separated_fail_manifest_job()`)
		})
		sender := separatedSender()
		failed, err := (jobrunner.RuntimePodDirectDeliverer{Store: f.runner, Sender: sender}).DeliverRuntimeJob(f.ctx, job)
		if err == nil && failed.Status != jobrunner.RuntimeDeliveryRejected {
			t.Fatal("manifest+Queue insert failure was not returned")
		}
		if f.count(t, `SELECT count(*) FROM session_mcp_manifests WHERE session_id=$1`, f.sessionID) != 0 || f.count(t, `SELECT count(*) FROM queue_jobs WHERE kind='runtime_config_update' AND payload_json::jsonb->>'session_id'=$1`, f.sessionID) != 0 || len(sender.requests) != 0 {
			t.Fatal("manifest insertion failure partially committed or sent input")
		}
		var state string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT status FROM session_runtime_inbox WHERE runtime_input_id=$1`, job.RuntimeInputID).Scan(&state); err != nil || state != "queued" {
			t.Fatalf("rolled back input=%s/%v", state, err)
		}
		f.sql(t, `DROP TRIGGER separated_fail_manifest_job ON queue_jobs; DROP FUNCTION separated_fail_manifest_job()`)
		result, err := (jobrunner.RuntimePodDirectDeliverer{Store: f.runner, Sender: sender}).DeliverRuntimeJob(f.ctx, job)
		if err != nil || result.Status != jobrunner.RuntimeDeliveryAccepted || len(sender.requests) != 2 {
			t.Fatalf("rollback retry=%#v/%v sends=%d; want manifest+input", result, err, len(sender.requests))
		}
		assertQueuedMCPManifestGenerations(t, f.admin, f.sessionID, []int64{1})
		t.Log("first manifest acceptance rollback: rows=0 update_jobs=0 input=queued sends=0; retry generation=1 update_jobs=1 sends=2")
	})
	t.Run("cleanup_queue_and_marker", func(t *testing.T) {
		f := newSeparatedOwners(t, "rollback_cleanup", false)
		scope := f.declare(t)
		if _, err := finishIdleWithStagedCaptureForTest(t, f.admin, f.bridge, bridgeAPIFinishIdleRequest(t, f.admin, scope, "evt_cleanup_running", `{"type":"end_turn"}`)); err != nil {
			t.Fatalf("actual Bridge idle fence: %v", err)
		}
		// Seed a claimed idle cleanup marker, using the actual declared binding.
		f.sql(t, `UPDATE session_runtime_status SET status='idle',binding_id=$2,binding_generation=$3,cleanup_after=clock_timestamp()-interval '1 hour',cleanup_job_id='cleanup_separated',cleanup_enqueued_at=clock_timestamp() WHERE workspace_id='default' AND session_id=$1`, f.sessionID, scope.Binding.BindingId, scope.Binding.BindingGeneration)
		job := jobrunner.RuntimeJob{Kind: queue.KindCleanupSession, WorkspaceID: "default", SessionID: f.sessionID, CleanupJobID: "cleanup_separated", RuntimeInputID: "cleanup_session:cleanup_separated", PayloadJSON: `{"workspace_id":"default","session_id":"` + f.sessionID + `","cleanup_job_id":"cleanup_separated"}`}
		if _, err := f.queue.Enqueue(f.ctx, queue.EnqueueRequest{ID: "qjob_sep_cleanup", WorkspaceID: "default", Kind: job.Kind, PartitionKey: queue.FormatSessionPartitionKey("default", f.sessionID), DedupeKey: queue.FormatCleanupSessionDedupeKey("default", f.sessionID, job.CleanupJobID), PayloadVersion: 1, PayloadJSON: []byte(job.PayloadJSON), MaxAttempts: 1}); err != nil {
			t.Fatal(err)
		}
		leases, err := f.queue.Lease(f.ctx, queue.LeaseRequest{WorkspaceID: "default", Kinds: []string{queue.KindCleanupSession}, LeaseOwner: "separated-cleanup", MaxJobs: 1, LeaseDuration: time.Minute})
		if err != nil || len(leases) != 1 {
			t.Fatalf("actual cleanup lease=%v/%v", leases, err)
		}
		lease := leases[0]
		job.JobID = lease.ID
		job.LeaseToken = lease.LeaseToken
		job.PartitionKey = lease.PartitionKey
		job.DedupeKey = lease.DedupeKey
		job.AttemptCount = int32(lease.AttemptCount)
		job.MaxAttempts = int32(lease.MaxAttempts)
		if _, err := f.runner.PrepareRuntimeCommand(f.ctx, job); err != nil {
			t.Fatal(err)
		}
		before := separatedCleanupSnapshot(t, f, job)
		f.sql(t, `CREATE FUNCTION separated_fail_cleanup_marker() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected cleanup marker failure after Queue mutation'; END $$; CREATE TRIGGER separated_fail_cleanup_marker BEFORE UPDATE OF cleanup_job_id,cleanup_after ON session_runtime_status FOR EACH ROW EXECUTE FUNCTION separated_fail_cleanup_marker()`)
		t.Cleanup(func() {
			_, _ = f.admin.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS separated_fail_cleanup_marker ON session_runtime_status; DROP FUNCTION IF EXISTS separated_fail_cleanup_marker()`)
		})
		if _, err := f.runner.FinalizeRuntimeCleanupExhaustion(f.ctx, job, jobrunner.RuntimeDeliveryResult{}); err == nil {
			t.Fatal("cleanup marker fault did not abort finalization")
		}
		separatedEqual(t, separatedCleanupSnapshot(t, f, job), before)
		f.sql(t, `DROP TRIGGER separated_fail_cleanup_marker ON session_runtime_status; DROP FUNCTION separated_fail_cleanup_marker()`)
		result, err := f.runner.FinalizeRuntimeCleanupExhaustion(f.ctx, job, jobrunner.RuntimeDeliveryResult{})
		if err != nil || !result.QueueLeaseSettled {
			t.Fatalf("cleanup retry=%#v/%v", result, err)
		}
		if f.count(t, `SELECT count(*) FROM queue_jobs WHERE id=$1 AND status='dead_lettered'`, job.JobID) != 1 || f.count(t, `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, f.sessionID) != 1 {
			t.Fatal("cleanup retry did not converge atomically")
		}
		assertCleanupMarkersRearmed(t, f.admin, f.sessionID, true)
		t.Logf("cleanup rollback exact lease+attempt+claim unchanged=%v; retry dead-letter and marker rearm committed together", before)
	})
}

func separatedCleanupSnapshot(t *testing.T, f *separatedOwners, job jobrunner.RuntimeJob) []any {
	t.Helper()
	var state, token, marker string
	var attempts int
	var claimed sql.NullTime
	if err := f.admin.QueryRowContext(f.ctx, `SELECT q.status,q.lease_token,q.attempt_count,s.cleanup_job_id,s.cleanup_claimed_at FROM queue_jobs q JOIN session_runtime_status s ON s.workspace_id=q.workspace_id AND s.session_id=$2 WHERE q.id=$1`, job.JobID, f.sessionID).Scan(&state, &token, &attempts, &marker, &claimed); err != nil {
		t.Fatal(err)
	}
	return []any{state, token, attempts, marker, claimed}
}
