package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPostgreSQLBridgeAPIStoreManifestAcceptanceUsesMonotonicGenerationAcrossETagFlap(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_generation_flap", "thr_mcp_generation_flap", "claude")
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{
		mcpManifestResult("etag_a", "github_a"),
		mcpManifestResult("etag_b", "github_b"),
		mcpManifestResult("etag_a", "github_a_again"),
	}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister

	first := mustAcceptMCPManifestChange(t, store, "sesn_mcp_generation_flap", "etag_a")
	duplicate := mustAcceptMCPManifestChange(t, store, "sesn_mcp_generation_flap", "etag_a")
	second := mustAcceptMCPManifestChange(t, store, "sesn_mcp_generation_flap", "etag_b")
	third := mustAcceptMCPManifestChange(t, store, "sesn_mcp_generation_flap", "etag_a")

	if len(lister.requests) != 3 {
		t.Fatalf("connector list calls = %d; want 3 with current-etag duplicate skipped", len(lister.requests))
	}
	if first.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		duplicate.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		second.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		third.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("A/A/B/A ACK statuses = %s/%s/%s/%s; want committed/duplicate/committed/committed",
			first.GetAck().GetStatus(), duplicate.GetAck().GetStatus(), second.GetAck().GetStatus(), third.GetAck().GetStatus())
	}
	wantRuntimeInputIDs := []string{
		"runtime_config_update:mcp_manifest:sesn_mcp_generation_flap:github:1",
		"runtime_config_update:mcp_manifest:sesn_mcp_generation_flap:github:1",
		"runtime_config_update:mcp_manifest:sesn_mcp_generation_flap:github:2",
		"runtime_config_update:mcp_manifest:sesn_mcp_generation_flap:github:3",
	}
	gotRuntimeInputIDs := []string{
		first.GetAck().GetRuntimeInputId(), duplicate.GetAck().GetRuntimeInputId(),
		second.GetAck().GetRuntimeInputId(), third.GetAck().GetRuntimeInputId(),
	}
	if stringSliceJSON(gotRuntimeInputIDs) != stringSliceJSON(wantRuntimeInputIDs) {
		t.Fatalf("runtime input ids = %v; want %v", gotRuntimeInputIDs, wantRuntimeInputIDs)
	}

	var etag string
	var generation int64
	var toolsJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT manifest_etag, manifest_generation, tools_json
		   FROM session_mcp_manifests
		  WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_generation_flap' AND mcp_server_name = 'github'`).Scan(&etag, &generation, &toolsJSON); err != nil {
		t.Fatalf("read accepted manifest: %v", err)
	}
	if etag != "etag_a" || generation != 3 || !strings.Contains(toolsJSON, "github_a_again") {
		t.Fatalf("accepted manifest = etag %q generation %d tools %s; want etag_a generation 3 final A", etag, generation, toolsJSON)
	}
	assertQueuedMCPManifestGenerations(t, admin, "sesn_mcp_generation_flap", []int64{1, 2, 3})
}

func TestPostgreSQLBridgeAPIStoreConcurrentFirstManifestInsertAllocatesOneGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_concurrent_first", "thr_mcp_concurrent_first", "claude")
	lister := newStagedMCPManifestLister(mcpManifestResult("etag_first", "github_first"))
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister
	request := &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_concurrent_first", McpServerName: "github", ManifestEtag: "etag_first",
	}

	responses := make(chan *bridgev1.McpManifestChangedResponse, 2)
	errors := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := store.McpManifestChanged(context.Background(), request)
			responses <- response
			errors <- err
		}()
	}
	select {
	case <-lister.staged:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent requests did not both reach the connector/list pre-stage")
	}
	if calls := lister.callCount(); calls != 2 {
		t.Fatalf("connector list calls before acceptance = %d; want 2", calls)
	}
	close(lister.release)
	wait.Wait()
	close(responses)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent McpManifestChanged: %v", err)
		}
	}
	statuses := map[bridgev1.BridgeWriteStatus]int{}
	for response := range responses {
		statuses[response.GetAck().GetStatus()]++
	}
	if statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED] != 1 ||
		statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE] != 1 {
		t.Fatalf("concurrent ACK statuses = %v; want one committed and one duplicate", statuses)
	}
	var rowCount int
	var generation int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), COALESCE(max(manifest_generation), 0) FROM session_mcp_manifests
		  WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_concurrent_first' AND mcp_server_name = 'github'`).Scan(&rowCount, &generation); err != nil {
		t.Fatalf("read concurrent manifest: %v", err)
	}
	if rowCount != 1 || generation != 1 {
		t.Fatalf("concurrent first manifest rows/generation = %d/%d; want 1/1", rowCount, generation)
	}
	assertQueuedMCPManifestGenerations(t, admin, "sesn_mcp_concurrent_first", []int64{1})
}

func TestPostgreSQLBridgeAPIStoreManifestByteBoundAcceptsExactAndPreservesAcceptedRowOnOverage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_bytes", "thr_mcp_bytes", "claude")
	exactTool := exactBoundMCPManifestTool(t)
	lister := &recordingMCPManifestLister{results: []MCPManifestListResult{
		{ManifestETag: "etag_exact", Tools: []MCPManifestTool{exactTool}},
		{ManifestETag: "etag_over", Tools: []MCPManifestTool{{Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON}}},
		{ManifestETag: "etag_over", Tools: []MCPManifestTool{{Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON}}},
	}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MCPManifestLister = lister
	logOutput := &lockedBuffer{}
	store.Logger = slog.New(slog.NewJSONHandler(logOutput, nil))

	mustAcceptMCPManifestChange(t, store, "sesn_mcp_bytes", "etag_exact")
	var beforeTools string
	var beforeETag string
	var beforeGeneration int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT tools_json, manifest_etag, manifest_generation FROM session_mcp_manifests
		  WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_bytes' AND mcp_server_name = 'github'`).Scan(
		&beforeTools, &beforeETag, &beforeGeneration,
	); err != nil {
		t.Fatalf("read exact-bound manifest: %v", err)
	}
	if len([]byte(beforeTools)) != MaxMcpManifestBytes {
		t.Fatalf("exact accepted tools bytes = %d; want %d", len([]byte(beforeTools)), MaxMcpManifestBytes)
	}
	_, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_bytes", McpServerName: "github", ManifestEtag: "etag_over",
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("one-byte-over error = %v; want ResourceExhausted", err)
	}
	_, err = store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_bytes", McpServerName: "github", ManifestEtag: "etag_over",
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("repeated one-byte-over error = %v; want ResourceExhausted", err)
	}
	var afterTools string
	var afterETag string
	var afterGeneration int64
	var readiness string
	var diagnostic sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT tools_json, manifest_etag, manifest_generation, readiness, diagnostic FROM session_mcp_manifests
		  WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_bytes' AND mcp_server_name = 'github'`).Scan(
		&afterTools, &afterETag, &afterGeneration, &readiness, &diagnostic,
	); err != nil {
		t.Fatalf("read preserved manifest: %v", err)
	}
	if afterTools != beforeTools || afterETag != beforeETag || afterGeneration != beforeGeneration+1 || readiness != "unready" || diagnostic.String != "manifest_too_large" {
		t.Fatalf("over-bound state = (%q,%d,%s,%q,%d bytes); want preserved content, generation %d, unready/manifest_too_large",
			afterETag, afterGeneration, readiness, diagnostic.String, len(afterTools), beforeGeneration+1)
	}
	assertQueuedMCPManifestGenerations(t, admin, "sesn_mcp_bytes", []int64{1, 2})
	if !strings.Contains(logOutput.String(), `"event.kind":"mcp_manifest.readiness_changed"`) || !strings.Contains(logOutput.String(), `"mcp.manifest.diagnostic":"manifest_too_large"`) {
		t.Fatalf("over-cap readiness log = %s; want structured manifest_too_large transition", logOutput.String())
	}
}

func TestPostgreSQLBridgeAPIStoreFirstOverCapManifestCommitsReadinessOnlyAndColdLoadSurvives(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_first_over", "thr_mcp_first_over", "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_mcp_first_over", "bind_mcp_first_over", 1, "pod_mcp_first_over")
	exactTool := exactBoundMCPManifestTool(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("mcp-first-over-binding-token-key")
	lister := &constantMCPManifestLister{result: MCPManifestListResult{ManifestETag: "etag_over", Tools: []MCPManifestTool{{
		Name: exactTool.Name, Description: exactTool.Description + "x", InputSchemaJSON: exactTool.InputSchemaJSON,
	}}}}
	store.MCPManifestLister = lister

	_, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_first_over", McpServerName: "github", ManifestEtag: "etag_over",
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("first over-cap error = %v; want ResourceExhausted", err)
	}
	_, err = store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: "sesn_mcp_first_over", McpServerName: "github", ManifestEtag: "etag_over",
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("readiness-only replay error = %v; want ResourceExhausted", err)
	}
	if lister.calls != 2 {
		t.Fatalf("readiness-only connector reads = %d; want 2", lister.calls)
	}
	var toolsJSON, etag sql.NullString
	var generation int64
	var readiness string
	var diagnostic sql.NullString
	if err := admin.QueryRow(`SELECT tools_json, manifest_etag, manifest_generation, readiness, diagnostic FROM session_mcp_manifests
		WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_first_over' AND mcp_server_name = 'github'`).Scan(
		&toolsJSON, &etag, &generation, &readiness, &diagnostic,
	); err != nil {
		t.Fatalf("read readiness-only manifest: %v", err)
	}
	if toolsJSON.Valid || etag.Valid || generation != 1 || readiness != "unready" || diagnostic.String != "manifest_too_large" {
		t.Fatalf("readiness-only row = tools=%v etag=%v generation=%d readiness=%q diagnostic=%q", toolsJSON, etag, generation, readiness, diagnostic.String)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope("sesn_mcp_first_over", "thr_mcp_first_over", "bind_mcp_first_over", 1, "pod_mcp_first_over"), RuntimeInputId: "rin_mcp_first_over",
	})
	if err != nil {
		t.Fatalf("LoadContext readiness-only row: %v", err)
	}
	var contextPayload struct {
		MCPManifests []struct {
			ManifestETag string            `json:"manifestETag"`
			Readiness    string            `json:"readiness"`
			Diagnostic   string            `json:"diagnostic"`
			Tools        []json.RawMessage `json:"tools"`
		} `json:"mcpManifests"`
	}
	if err := json.Unmarshal([]byte(loaded.GetContextJson()), &contextPayload); err != nil {
		t.Fatalf("decode cold readiness-only context: %v", err)
	}
	if len(contextPayload.MCPManifests) != 1 || contextPayload.MCPManifests[0].Readiness != "unready" || contextPayload.MCPManifests[0].Diagnostic != "manifest_too_large" || contextPayload.MCPManifests[0].ManifestETag != "" || len(contextPayload.MCPManifests[0].Tools) != 0 {
		t.Fatalf("cold readiness-only manifest = %#v", contextPayload.MCPManifests)
	}
}

func TestPostgreSQLBridgeAPIStoreStoredETagDuplicatePathsRestoreReadyAndEnqueue(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_restore", "thr_mcp_restore", "claude")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	lister := &constantMCPManifestLister{result: mcpManifestResult("etag_restore", "github_restore")}
	store.MCPManifestLister = lister
	mustAcceptMCPManifestChange(t, store, "sesn_mcp_restore", "etag_restore")
	if _, err := admin.Exec(`UPDATE session_mcp_manifests SET readiness = 'unready', diagnostic = 'delivery_exhausted', manifest_generation = 2
		WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_restore' AND mcp_server_name = 'github'`); err != nil {
		t.Fatalf("mark stored etag unready: %v", err)
	}
	restored := mustAcceptMCPManifestChange(t, store, "sesn_mcp_restore", "etag_restore")
	if restored.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || lister.calls != 1 {
		t.Fatalf("pre-list restore ACK/list calls = %s/%d; want committed/1", restored.GetAck().GetStatus(), lister.calls)
	}
	if _, err := admin.Exec(`UPDATE session_mcp_manifests SET readiness = 'unready', diagnostic = 'delivery_exhausted', manifest_generation = 4
		WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_restore' AND mcp_server_name = 'github'`); err != nil {
		t.Fatalf("mark acceptance-path etag unready: %v", err)
	}
	var acceptance mcpManifestAcceptance
	if err := dbconnect.NewClientForTesting(runtime).WithWorkspaceTx(context.Background(), "default", "test.mcp_restore_acceptance", func(tx *dbconnect.Tx) error {
		var err error
		acceptance, err = captureMCPManifestAcceptanceTx(context.Background(), tx, "default", "sesn_mcp_restore", "github", "etag_restore", mcpManifestResult("etag_restore", "github_restore").Tools, time.Now().UTC())
		return err
	}); err != nil {
		t.Fatalf("acceptance-path restore: %v", err)
	}
	if acceptance.Duplicate || acceptance.Generation != 5 {
		t.Fatalf("acceptance-path restore = duplicate %v generation %d; want committed generation 5", acceptance.Duplicate, acceptance.Generation)
	}
	var generation int64
	var readiness string
	var diagnostic sql.NullString
	if err := admin.QueryRow(`SELECT manifest_generation, readiness, diagnostic FROM session_mcp_manifests
		WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_restore' AND mcp_server_name = 'github'`).Scan(&generation, &readiness, &diagnostic); err != nil {
		t.Fatalf("read restored row: %v", err)
	}
	if generation != 5 || readiness != "ready" || diagnostic.Valid {
		t.Fatalf("restored row = generation %d readiness %q diagnostic %v", generation, readiness, diagnostic)
	}
	assertQueuedMCPManifestGenerations(t, admin, "sesn_mcp_restore", []int64{1, 3, 5})
}

func TestPostgreSQLRuntimeDeliveryStoreFinalManifestAttemptTransitionsUnreadyBeforeDeadLetter(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_exhaust", "thr_mcp_exhaust", "claude")
	bridge := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridge.MCPManifestLister = &constantMCPManifestLister{result: mcpManifestResult("etag_exhaust", "github_exhaust")}
	mustAcceptMCPManifestChange(t, bridge, "sesn_mcp_exhaust", "etag_exhaust")
	delivery := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 0)
	result, err := delivery.FinalizeRuntimeDelivery(context.Background(), RuntimeJob{
		Kind: "runtime_config_update", WorkspaceID: "default", SessionID: "sesn_mcp_exhaust",
		RuntimeInputID: runtimeMCPManifestInputID("sesn_mcp_exhaust", "github", 1), MCPServerName: "github",
		MCPManifestGeneration: "1", AttemptCount: 5, MaxAttempts: 5,
	}, RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true})
	if err != nil {
		t.Fatalf("FinalizeRuntimeDelivery final MCP attempt: %v", err)
	}
	if result.Status != RuntimeDeliveryRejected || result.Retryable {
		t.Fatalf("finalized result = %#v; want terminal rejected", result)
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
	assertQueuedMCPManifestGenerations(t, admin, "sesn_mcp_exhaust", []int64{1, 2})
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
	if stringSliceJSON(maxAttempts) != stringSliceJSON([]int{5, 5}) {
		t.Fatalf("MCP max attempts = %v; want [5 5]", maxAttempts)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextReplaysLatestManifestForReplacementBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedMCPFamilySession(t, admin, "sesn_mcp_cold", "thr_mcp_cold", "claude")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_mcp_cold", "bind_mcp_cold_1", 1, "pod_mcp_cold_1")
	insertAcceptedMCPManifest(t, admin, "sesn_mcp_cold", "etag_1", 1, "github_search")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("mcp-cold-replay-binding-token-key")
	lister := &constantMCPManifestLister{result: mcpManifestResult("etag_connector_must_not_be_read", "connector_tool")}
	store.MCPManifestLister = lister

	first, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope("sesn_mcp_cold", "thr_mcp_cold", "bind_mcp_cold_1", 1, "pod_mcp_cold_1"), RuntimeInputId: "rin_mcp_cold",
	})
	if err != nil {
		t.Fatalf("LoadContext first binding: %v", err)
	}
	assertLoadContextMCPManifest(t, first.GetContextJson(), "etag_1", 1, "github_search")

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_mcp_manifests SET tools_json = $1, manifest_etag = 'etag_2', manifest_generation = 2, updated_at = '2026-01-01T00:00:02Z'
		  WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_cold' AND mcp_server_name = 'github'`,
		`[{"name":"github_issue","description":"github_issue","input_schema":{"type":"object"}}]`); err != nil {
		t.Fatalf("advance accepted manifest: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_mcp_cold_2', binding_generation = 2, agent_runtime_pod_uid = 'pod_mcp_cold_2', updated_at = '2026-01-01T00:00:02Z'
		  WHERE workspace_id = 'default' AND session_id = 'sesn_mcp_cold'`); err != nil {
		t.Fatalf("replace runtime binding: %v", err)
	}
	second, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope("sesn_mcp_cold", "thr_mcp_cold", "bind_mcp_cold_2", 2, "pod_mcp_cold_2"), RuntimeInputId: "rin_mcp_cold",
	})
	if err != nil {
		t.Fatalf("LoadContext replacement binding: %v", err)
	}
	if second.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("replacement binding ACK = %s; want committed fresh load", second.GetAck().GetStatus())
	}
	assertLoadContextMCPManifest(t, second.GetContextJson(), "etag_2", 2, "github_issue")
	if lister.calls != 0 {
		t.Fatalf("connector list calls during cold replay = %d; want 0", lister.calls)
	}
}

type constantMCPManifestLister struct {
	mu     sync.Mutex
	result MCPManifestListResult
	calls  int
}

type stagedMCPManifestLister struct {
	mu      sync.Mutex
	result  MCPManifestListResult
	calls   int
	staged  chan struct{}
	release chan struct{}
}

func newStagedMCPManifestLister(result MCPManifestListResult) *stagedMCPManifestLister {
	return &stagedMCPManifestLister{
		result:  result,
		staged:  make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (l *stagedMCPManifestLister) ListMCPTools(ctx context.Context, _ MCPManifestListRequest) (MCPManifestListResult, error) {
	l.mu.Lock()
	l.calls++
	if l.calls == 2 {
		close(l.staged)
	}
	l.mu.Unlock()
	select {
	case <-ctx.Done():
		return MCPManifestListResult{}, ctx.Err()
	case <-l.release:
		return l.result, nil
	}
}

func (l *stagedMCPManifestLister) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (l *constantMCPManifestLister) ListMCPTools(_ context.Context, _ MCPManifestListRequest) (MCPManifestListResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return l.result, nil
}

func mcpManifestResult(etag string, toolName string) MCPManifestListResult {
	return MCPManifestListResult{ManifestETag: etag, Tools: []MCPManifestTool{{
		Name: toolName, Description: toolName, InputSchemaJSON: `{"type":"object"}`,
	}}}
}

func mustAcceptMCPManifestChange(t *testing.T, store *PostgreSQLBridgeAPIStore, sessionID string, etag string) *bridgev1.McpManifestChangedResponse {
	t.Helper()
	response, err := store.McpManifestChanged(context.Background(), &bridgev1.McpManifestChangedRequest{
		WorkspaceId: "default", SessionId: sessionID, McpServerName: "github", ManifestEtag: etag,
	})
	if err != nil {
		t.Fatalf("McpManifestChanged(%s): %v", etag, err)
	}
	return response
}

func exactBoundMCPManifestTool(t *testing.T) MCPManifestTool {
	t.Helper()
	tool := MCPManifestTool{Name: "github_exact", InputSchemaJSON: `{"type":"object"}`}
	base, err := canonicalMCPManifestToolsJSON([]MCPManifestTool{tool})
	if err != nil {
		t.Fatalf("marshal base manifest: %v", err)
	}
	tool.Description = strings.Repeat("x", MaxMcpManifestBytes-len(base))
	exact, err := canonicalMCPManifestToolsJSON([]MCPManifestTool{tool})
	if err != nil {
		t.Fatalf("marshal exact manifest: %v", err)
	}
	if len(exact) != MaxMcpManifestBytes {
		t.Fatalf("exact manifest construction bytes = %d; want %d", len(exact), MaxMcpManifestBytes)
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

func insertAcceptedMCPManifest(t *testing.T, db *sql.DB, sessionID string, etag string, generation int64, toolName string) {
	t.Helper()
	tools, err := json.Marshal([]map[string]any{{
		"name": toolName, "description": toolName, "input_schema": map[string]any{"type": "object"},
	}})
	if err != nil {
		t.Fatalf("marshal accepted manifest: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_mcp_manifests (
			workspace_id, session_id, mcp_server_name, tools_json, manifest_etag, manifest_generation, created_at, updated_at
		 ) VALUES ('default', $1, 'github', $2, $3, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, string(tools), etag, generation); err != nil {
		t.Fatalf("insert accepted manifest: %v", err)
	}
}

func assertLoadContextMCPManifest(t *testing.T, contextJSON string, wantETag string, wantGeneration int64, wantToolName string) {
	t.Helper()
	var payload struct {
		MCPManifests []struct {
			MCPServerName      string `json:"mcpServerName"`
			ManifestETag       string `json:"manifestETag"`
			ManifestGeneration int64  `json:"manifestGeneration"`
			Tools              []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"mcpManifests"`
	}
	if err := json.Unmarshal([]byte(contextJSON), &payload); err != nil {
		t.Fatalf("parse LoadContext MCP manifests: %v", err)
	}
	if len(payload.MCPManifests) != 1 {
		t.Fatalf("LoadContext mcpManifests = %s; want one", contextJSON)
	}
	manifest := payload.MCPManifests[0]
	if manifest.MCPServerName != "github" || manifest.ManifestETag != wantETag || manifest.ManifestGeneration != wantGeneration ||
		len(manifest.Tools) != 1 || manifest.Tools[0].Name != wantToolName {
		t.Fatalf("LoadContext manifest = %#v; want github/%s/%d/%s", manifest, wantETag, wantGeneration, wantToolName)
	}
}

func stringSliceJSON(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

var _ MCPManifestLister = (*constantMCPManifestLister)(nil)
var _ MCPManifestLister = (*stagedMCPManifestLister)(nil)
