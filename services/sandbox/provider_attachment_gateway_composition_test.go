package tetralsandbox

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"

	"google.golang.org/grpc"
)

func TestPostgreSQLSandboxTransientAttachmentResolvesThroughProductionGateway(t *testing.T) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("production Gateway composition requires bun: %v", err)
	}
	gatewayRoot := filepath.Clean(filepath.Join("..", "gateway"))
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	if _, err := adminDB.ExecContext(context.Background(), `INSERT INTO session_runtime_bindings (
		workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
		agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at
	) VALUES (
		'ws_execution_store', 'sesn_execution_store', 'bind_provider_attachment_composition', 1,
		'tetral-agent-runtime', 'runtime-pod-0', 'pod_uid_provider_attachment_composition',
		'10.0.0.10', '2026-07-31T16:00:00Z', '2026-07-31T16:00:00Z'
	)`); err != nil {
		t.Fatalf("seed provider attachment Runtime binding: %v", err)
	}

	now := time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)
	blobStore := blob.NewFakeBlobStore()
	materializer := NewPostgreSQLSandboxMediaMaterializer(dbconnect.NewClientForTesting(runtimeDB), blobStore)
	raw := `{"status":"success","result":{"mime":"image/png","data_base64":"` + base64.StdEncoding.EncodeToString([]byte("gateway-bytes")) + `"}}`
	materialized, err := materializer.MaterializeResult(sandboxTestQueueContext(t, runtimeDB), SandboxExecutionRef{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store",
		SessionThreadID: "thr_execution_store", ToolUseEventID: "evt_execution_a",
	}, "view_image", `{"path":"gateway_attachment.png"}`, raw, now)
	if err != nil {
		t.Fatalf("materialize production Sandbox attachment: %v", err)
	}
	var published struct {
		Result struct {
			AttachmentRef string `json:"attachment_ref"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(materialized), &published); err != nil || published.Result.AttachmentRef == "" {
		t.Fatalf("decode production Sandbox attachment: %v: %s", err, materialized)
	}
	if _, err := adminDB.ExecContext(context.Background(), `UPDATE session_transient_attachments
		SET status = 'active', expires_at = '2026-07-31T16:15:00Z', updated_at = '2026-07-31T16:00:01Z'
		WHERE workspace_id = 'ws_execution_store' AND attachment_ref = $1 AND status = 'staged'`, published.Result.AttachmentRef); err != nil {
		t.Fatalf("activate production Sandbox attachment: %v", err)
	}

	bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	bridgeStore.AttachmentBlobStore = blobStore
	bridgeStore.Clock = func() time.Time { return now }
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for provider attachment composition: %v", err)
	}
	grpcServer := grpc.NewServer()
	agentruntimebridge.RegisterBridgeAPI(grpcServer, bridgeStore)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	run := func() gatewayAttachmentCompositionResult {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, bunPath, "run",
			"packages/provider-gateway/test/fixtures/transient-attachment-postgresql-composition.ts",
			listener.Addr().String(), published.Result.AttachmentRef,
			"ws_execution_store", "sesn_execution_store", "thr_execution_store",
			"bind_provider_attachment_composition", "pod_uid_provider_attachment_composition",
			"gateway_attachment.png", "gateway_attachment.png",
		) //nolint:gosec // fixed repository fixture and test-owned arguments.
		command.Dir = gatewayRoot
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("run provider attachment composition: %v\n%s", err, output)
		}
		var result gatewayAttachmentCompositionResult
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("decode provider attachment composition: %v: %s", err, output)
		}
		return result
	}

	resolved := run()
	if !resolved.OK || resolved.Code != "" || len(resolved.Attachments) != 1 || resolved.Attachments[0].AttachmentRef != published.Result.AttachmentRef || string(resolved.Attachments[0].Bytes) != "gateway-bytes" || len(resolved.Rejections) != 0 {
		t.Fatalf("resolved provider attachment = %#v", resolved)
	}
	if _, err := adminDB.ExecContext(context.Background(), `UPDATE session_transient_attachments
		SET source_tool_use_event_id = 'evt_missing_provider_attachment_source'
		WHERE workspace_id = 'ws_execution_store' AND attachment_ref = $1`, published.Result.AttachmentRef); err != nil {
		t.Fatalf("detach durable provider attachment source: %v", err)
	}
	invalidSource := run()
	if invalidSource.OK || invalidSource.Code != "provider_request_invalid" {
		t.Fatalf("detached durable source result = %#v; want provider_request_invalid", invalidSource)
	}
	if _, err := adminDB.ExecContext(context.Background(), `UPDATE session_transient_attachments
		SET source_tool_use_event_id = 'evt_execution_a', expires_at = '2026-07-31T15:59:59Z'
		WHERE workspace_id = 'ws_execution_store' AND attachment_ref = $1`, published.Result.AttachmentRef); err != nil {
		t.Fatalf("expire provider attachment: %v", err)
	}
	expired := run()
	if !expired.OK || len(expired.Attachments) != 0 || len(expired.Rejections) != 1 || expired.Rejections[0].AttachmentRef != published.Result.AttachmentRef {
		t.Fatalf("expired provider attachment = %#v; want one in-band rejection", expired)
	}
}

type gatewayAttachmentCompositionResult struct {
	OK          bool   `json:"ok"`
	Code        string `json:"code"`
	Attachments []struct {
		AttachmentRef string `json:"attachmentRef"`
		Bytes         []byte `json:"bytes"`
	} `json:"attachments"`
	Rejections []struct {
		AttachmentRef string `json:"attachmentRef"`
		Reason        int32  `json:"reason"`
	} `json:"rejections"`
}
