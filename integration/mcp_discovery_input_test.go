package integration

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func newInputDiscoveryFixture(t *testing.T) (*jobrunner.PostgreSQLRuntimeDeliveryStore, *sql.DB, jobrunner.RuntimeJob) {
	t.Helper()
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_discovery", "thrd_sesn_discovery", "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_discovery", "bind_discovery", 1, "pod_discovery")
	job := jobrunner.RuntimeJob{Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: "sesn_discovery", SessionThreadID: "thrd_sesn_discovery",
		RuntimeInputID: "rin_discovery", InputKind: "messages", EventIDs: []string{"evt_discovery"}, SequenceFrom: 1, SequenceTo: 1}
	seedBridgeAPIEvent(t, admin, "default", job.SessionID, job.SessionThreadID, job.EventIDs[0], 1, "user.message", `{"content":[{"type":"text","text":"run"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, job)
	return fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090), admin, job
}

func TestMCPInputDiscoveryFailureReplayAndNextInputRecovery(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	failed := &recordingMCPManifestLister{err: errors.New("private cursor failure")}
	store.MCPManifestLister = failed
	sender := &recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}}
	deliver := jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}
	result, err := deliver.DeliverRuntimeJob(context.Background(), job)
	if err != nil || result.Status != jobrunner.RuntimeDeliveryRejected || result.Retryable || len(sender.requests) != 0 || len(failed.requests) != 3 {
		t.Fatalf("failed input = %+v/%v sends=%d attempts=%d", result, err, len(sender.requests), len(failed.requests))
	}
	// A new process handles the same input: no discovery, no second error, no model.
	restarted := fixtureRuntimeDeliveryStore(store.Client, admin, 9090)
	restarted.MCPManifestLister = failed
	_, err = (jobrunner.RuntimePodDirectDeliverer{Store: restarted, Sender: sender}).DeliverRuntimeJob(context.Background(), job)
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
	result, err = (jobrunner.RuntimePodDirectDeliverer{Store: restarted, Sender: sender}).DeliverRuntimeJob(context.Background(), next)
	if err != nil || result.Status != jobrunner.RuntimeDeliveryAccepted || len(sender.requests) != 2 {
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

func TestMCPInputRecoveryInstallsThroughRealRuntimeAndReachesConnector(t *testing.T) {
	store, admin, job := newInputDiscoveryFixture(t)
	store.MCPManifestLister = &recordingMCPManifestLister{err: errors.New("unavailable")}
	if _, err := store.PrepareRuntimeCommand(context.Background(), job); err == nil {
		t.Fatal("expected failed first input")
	}
	unreadyPlan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: job.SessionID, MCPServerName: "github", ConfigGeneration: "1", RuntimeInputID: mcpmanifest.InputID(job.SessionID, "github", 1)})
	if err != nil || unreadyPlan.RuntimeConfig == nil {
		t.Fatalf("prepare unready manifest command: %#v/%v", unreadyPlan, err)
	}
	unreadyPayload := unreadyPlan.RuntimeConfig.GetMcpManifest().GetContentJson()

	next := job
	next.RuntimeInputID, next.EventIDs, next.SequenceFrom, next.SequenceTo = "rin_real_recovery", []string{"evt_real_recovery"}, 10, 10
	seedBridgeAPIEvent(t, admin, "default", next.SessionID, next.SessionThreadID, next.EventIDs[0], 10, "user.message", `{"content":[{"type":"text","text":"retry"}]}`)
	seedRuntimeInboxBirthForJob(t, admin, next)
	store.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_real_recovery", "github_search")}
	if _, err := store.PrepareRuntimeCommand(context.Background(), next); err != nil {
		t.Fatalf("prepare recovery before cold context observation: %v", err)
	}
	configPlan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: "default", SessionID: job.SessionID, ConfigGeneration: "1", RuntimeInputID: "runtime_config_update:" + job.SessionID + ":1"})
	if err != nil || configPlan.RuntimeConfig == nil {
		t.Fatalf("prepare installed runtime config: %#v/%v", configPlan, err)
	}
	configPayload := configPlan.RuntimeConfig.GetSessionConfig().GetContentJson()

	bridge := agentruntimebridge.NewPostgreSQLBridgeAPIStore(store.Client)
	bridge.RuntimeBindingTokenHMACKey = []byte("manifest-recovery-binding-token-key")
	cold, err := bridge.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: sessionfixture.BridgeAPIScope(job.SessionID, job.SessionThreadID, "bind_discovery", 1, "pod_discovery")})
	if err != nil {
		t.Fatal(err)
	}
	sender := &bunRuntimeManifestCompositionSender{
		recordingRuntimeCommandSender: recordingRuntimeCommandSender{result: jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryAccepted}},
		Recovery:                      true, InputPath: t.TempDir() + "/recovery.json", RuntimeConfigPayloadJSON: configPayload,
		ReadyManifestPayloadJSON: unreadyPayload, ReadyGeneration: 1, UnreadyGeneration: 2,
		ColdContextJSON: cold.GetContextJson(), ColdRuntimeBindingToken: cold.GetRuntimeBindingToken(), ToolName: "github_search",
	}
	result, err := (jobrunner.RuntimePodDirectDeliverer{Store: store, Sender: sender}).DeliverRuntimeJob(context.Background(), next)
	if err != nil || result.Status != jobrunner.RuntimeDeliveryAccepted {
		t.Fatalf("real Runtime recovery: %+v/%v", result, err)
	}
	if sender.Result.WarmMCPConnectorCalls < 1 || sender.Result.ColdMCPConnectorCalls < 1 || sender.Result.WarmCurrentGeneration != 2 || sender.Result.ColdCurrentGeneration != 2 {
		t.Fatalf("recovered route proof: %+v", sender.Result)
	}
}
