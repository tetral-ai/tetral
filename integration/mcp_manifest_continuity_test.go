package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func TestPostgreSQLRuntimeDeliveryStoreFinalManifestAttemptTransitionsUnreadyWithoutReplacementJob(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_exhaust", "thr_mcp_exhaust", "claude")
	bridge := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_exhaust", "github_exhaust")}
	mustAcceptMCPManifestChange(t, bridge, "sesn_mcp_exhaust", "etag_exhaust")
	delivery := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 0)
	logs := &lockedBuffer{}
	delivery.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	job := jobrunner.RuntimeJob{
		Kind: "runtime_config_update", WorkspaceID: "default", SessionID: "sesn_mcp_exhaust",
		RuntimeInputID: mcpmanifest.InputID("sesn_mcp_exhaust", "github", 1), MCPServerName: "github",
		MCPManifestGeneration: "1", AttemptCount: 5, MaxAttempts: 5,
	}
	result, err := delivery.FinalizeRuntimeDelivery(context.Background(), jobrunner.RuntimeJob{
		Kind: job.Kind, WorkspaceID: job.WorkspaceID, SessionID: job.SessionID,
		RuntimeInputID: job.RuntimeInputID, MCPServerName: job.MCPServerName,
		MCPManifestGeneration: job.MCPManifestGeneration, AttemptCount: job.AttemptCount, MaxAttempts: job.MaxAttempts,
	}, jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryRejected, Retryable: true})
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery final MCP attempt: %v", err)
	}
	if result.Status != jobrunner.RuntimeDeliveryRejected || result.Retryable || result.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("finalized result = %#v; want typed same-job defer disposition", result)
	}
	var generation int64
	var readiness, diagnostic string
	if err := admin.QueryRow(`SELECT manifest_generation, readiness, diagnostic FROM session_mcp_manifests
		WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_exhaust' AND mcp_server_name = 'github'`).Scan(&generation, &readiness, &diagnostic); err != nil {
		t.Fatalf("read exhausted manifest: %v", err)
	}
	if generation != 2 || readiness != "unready" || diagnostic != "delivery_exhausted" {
		t.Fatalf("exhausted manifest = generation %d readiness %q diagnostic %q", generation, readiness, diagnostic)
	}
	assertQueuedMCPManifestGenerations(t, admin, "sesn_mcp_exhaust", []int64{1})
	var maxAttempts []int
	rows, err := admin.Query(`SELECT max_attempts FROM queue_jobs WHERE workspace_id = 'default' AND payload_json::jsonb ->> 'session_id' = 'sesn_mcp_exhaust' ORDER BY created_at`)
	if err != nil {
		t.Fatalf("query MCP max attempts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var attempts int
		if err := rows.Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		maxAttempts = append(maxAttempts, attempts)
	}
	if stringSliceJSON(maxAttempts) != stringSliceJSON([]int{5}) {
		t.Fatalf("MCP max attempts = %v; want the original queue job only", maxAttempts)
	}
	if _, err := delivery.FinalizeRuntimeDelivery(context.Background(), job, jobrunner.RuntimeDeliveryResult{Status: jobrunner.RuntimeDeliveryRejected, Retryable: true}); err != nil {
		t.Fatalf("replay final MCP attempt: %v", err)
	}
	if strings.Count(logs.String(), `"event.kind":"mcp_manifest_transition_committed"`) != 1 ||
		!strings.Contains(logs.String(), `"mcp.manifest.previous_generation":1`) ||
		!strings.Contains(logs.String(), `"mcp.manifest.generation":2`) ||
		!strings.Contains(logs.String(), `"mcp.manifest.diagnostic":"delivery_exhausted"`) ||
		!strings.Contains(logs.String(), `"queue.custody":"retained"`) {
		t.Fatalf("delivery-exhausted transition log = %s; want one retained-custody commit", logs.String())
	}
}

type constantMCPManifestLister struct {
	mu     sync.Mutex
	result mcpmanifest.ListResult
	calls  int
}

func (l *constantMCPManifestLister) ListMCPTools(_ context.Context, _ mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return l.result, nil
}

func mcpManifestResult(etag string, toolName string) mcpmanifest.ListResult {
	return mcpmanifest.ListResult{ManifestETag: etag, Tools: []mcpmanifest.Tool{{
		Name: toolName, Description: toolName, InputSchemaJSON: `{"type":"object"}`,
	}}}
}

func mustAcceptMCPManifestChange(t *testing.T, store *agentruntimebridge.PostgreSQLBridgeAPIStore, sessionID string, etag string) *bridgev1.McpManifestChangedResponse {
	t.Helper()
	response, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: sessionID, McpServerName: "github", ManifestEtag: etag,
	})
	if err != nil {
		t.Fatalf("McpManifestChanged(%s): %v", etag, err)
	}
	return response
}

func exactBoundMCPManifestTool(t *testing.T) mcpmanifest.Tool {
	t.Helper()
	tool := mcpmanifest.Tool{Name: "github_exact", InputSchemaJSON: `{"type":"object"}`}
	base, err := mcpmanifest.CanonicalToolsJSON([]mcpmanifest.Tool{tool})
	if err != nil {
		t.Fatalf("marshal base manifest: %v", err)
	}
	tool.Description = strings.Repeat("x", mcpmanifest.MaxBytes-len(base))
	exact, err := mcpmanifest.CanonicalToolsJSON([]mcpmanifest.Tool{tool})
	if err != nil {
		t.Fatalf("marshal exact manifest: %v", err)
	}
	if len(exact) != mcpmanifest.MaxBytes {
		t.Fatalf("exact manifest construction bytes = %d; want %d", len(exact), mcpmanifest.MaxBytes)
	}
	return tool
}

func assertQueuedMCPManifestGenerations(t *testing.T, db *sql.DB, sessionID string, want []int64) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT (payload_json::jsonb ->> 'manifest_generation')::bigint
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND payload_json::jsonb ->> 'session_id' = $1
		    AND payload_json::jsonb ->> 'mcp_server_name' = 'github'
		    AND kind = 'runtime_config_update'
		  ORDER BY (payload_json::jsonb ->> 'manifest_generation')::bigint`, sessionID)
	if err != nil {
		t.Fatalf("query queued manifest generations: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []int64
	for rows.Next() {
		var generation int64
		if err := rows.Scan(&generation); err != nil {
			t.Fatalf("scan queued manifest generation: %v", err)
		}
		got = append(got, generation)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate queued manifest generations: %v", err)
	}
	if stringSliceJSON(got) != stringSliceJSON(want) {
		t.Fatalf("queued manifest generations = %v; want %v", got, want)
	}
}

func stringSliceJSON(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}
