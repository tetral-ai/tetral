package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/encryption"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	runtimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	queueservice "github.com/tetral-ai/tetral/services/queue"
	sandbox "github.com/tetral-ai/tetral/services/sandbox"
	"google.golang.org/protobuf/proto"
)

// This composition uses installed workload roles and real SDK, Bridge, Queue,
// JobRunner, RuntimeControl and Core owners. MCP/provider peers, verified
// identities, Kubernetes target visibility, and empty output-capture/blob
// adapters are controlled locally; every acceptance and Queue transition is real.
func TestPostgreSQLMCPManifestDeliveryUpdatesRuntimeCatalog(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		t.Run(adapter, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			t.Cleanup(cancel)
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			roles := storagetest.OpenWorkloadDB(t, admin, "bridge")
			gatewayDB := roles.OpenWorkload(t, "mcp_connector", nil)
			runnerDB := roles.OpenWorkload(t, "job_runner", nil)
			queueDB := roles.OpenWorkload(t, "queue", nil)
			sandboxDB := roles.OpenWorkload(t, "sandbox", nil)
			const session, thread, binding, pod = "sesn_mcp_durable", "thr_mcp_durable", "bind_mcp_durable", "pod_mcp_durable"
			seedBridgeAPISession(t, admin, "default", session, thread)
			seedBridgeAPIRuntimeBinding(t, admin, "default", session, binding, 1, pod)
			seedReadySandboxForSharedToolExecution(t, admin, "default", session)
			startMCPDeliveryCaptures(t, ctx, sandboxDB, queueDB)
			installed := `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"work-github"},{"type":"mcp_toolset","mcp_server_name":"work-slack"}],"mcp_servers":[{"type":"url","name":"work-github","url":"https://api.githubcopilot.com/mcp/"},{"type":"url","name":"work-slack","url":"https://mcp.slack.com/mcp"}]}`
			mustMCPDeliveryExec(t, admin, `UPDATE sessions SET installed_tools_json=$1,vault_ids_json='["vlt_mcp_durable"]' WHERE id=$2`, installed, session)
			mustMCPDeliveryExec(t, admin, `INSERT INTO vaults(workspace_id,id,display_name,metadata_json,created_at,updated_at) VALUES('default','vlt_mcp_durable','MCP fixture','{}',now(),now())`)
			const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			enc, err := encryption.NewAES256GCMEncryptor(key)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"github", "slack"} {
				endpoint := "https://api.githubcopilot.com/mcp/"
				if name == "slack" {
					endpoint = "https://mcp.slack.com/mcp"
				}
				private, _ := json.Marshal(map[string]string{"type": "static_bearer", "mcp_server_url": endpoint, "token": "fixture-" + name + "-token"})
				cipher, err := enc.Encrypt(private)
				if err != nil {
					t.Fatal(err)
				}
				public, _ := json.Marshal(map[string]string{"type": "static_bearer", "mcp_server_url": endpoint})
				mustMCPDeliveryExec(t, admin, `INSERT INTO credentials(workspace_id,id,vault_id,display_name,metadata_json,auth_type,auth_public_json,mcp_server_url,encrypted_auth,created_at,updated_at)VALUES('default',$1,'vlt_mcp_durable',$1,'{}','static_bearer',$2,$3,$4,now(),now())`, "cred_mcp_"+name, string(public), endpoint, cipher)
			}
			store := bridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(roles.DB))
			store.RuntimeBindingTokenHMACKey = []byte("mcp-durable-binding-key-with-32-bytes")
			endpoint := serveReplicaBridgeIdentities(t, store, map[string]grpcauth.Identity{
				"mcp-production-gateway-token": {ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-system", Name: "mcp-connector"}, KubernetesPodUID: "mcp-local"},
				"mcp-production-runtime-token": {ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: pod},
			}, nil)
			dir := t.TempDir()
			paths := map[string]string{}
			for name, token := range map[string]string{"gateway": "mcp-production-gateway-token", "runtime": "mcp-production-runtime-token", "bridge": "mcp-durable-bridge-token", "runner": "mcp-delivery-job-runner-token"} {
				paths[name] = filepath.Join(dir, name+"-token")
				if err := os.WriteFile(paths[name], []byte(token+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			mcp := startMCPDeliveryChild(t, ctx, "../services/gateway", "packages/mcp-connector/test/fixtures/mcp-durable-composition.ts", map[string]any{"bridgeAddress": endpoint.Address, "gatewayTokenPath": paths["gateway"], "runtimeTokenPath": paths["runtime"], "bridgeTokenPath": paths["bridge"], "workspaceId": "default", "sessionId": session, "threadId": thread, "bindingId": binding, "podUid": pod, "masterKeyHex": key, "bindingKey": string(store.RuntimeBindingTokenHMACKey)}, []string{"TETRAL_TEST_GATEWAY_DATABASE_URL=" + storagetest.RuntimeDatabaseURL(t, gatewayDB)})
			lister := mcpmanifest.NewConnectorLister(mcp.startup.ConnectorAddress, grpcauth.FileTokenSource{Path: paths["bridge"]})
			defer lister.Close()
			store.MCPManifestLister = lister
			otherAdapter := map[string]string{"github": "slack", "slack": "github"}[adapter]
			mcp.action(map[string]any{"kind": "configure", "adapter": otherAdapter, "extraToolName": "other_control"})
			baseTools, baseETag := deliveryManifest(t, false)
			for _, name := range []string{"github", "slack"} {
				var discovery struct {
					OK       bool
					Response struct{ ManifestETag string }
				}
				json.Unmarshal(mcp.action(map[string]any{"kind": "discover", "adapter": name}), &discovery)
				tools, etag := baseTools, baseETag
				if name == otherAdapter {
					tools, etag = deliveryOtherManifest(t)
				}
				if !discovery.OK || discovery.Response.ManifestETag != etag {
					t.Fatal("actual SDK baseline differs from literal V1")
				}
				mustMCPDeliveryExec(t, admin, `INSERT INTO session_mcp_manifests(workspace_id,session_id,mcp_server_name,tools_json,manifest_etag,manifest_generation,readiness,created_at,updated_at)VALUES('default',$1,$2,$3,$4,7,'ready',now(),now())`, session, "work-"+name, tools, etag)
			}
			runtimeInput := map[string]any{"workspaceId": "default", "sessionId": session, "durableMode": map[string]any{"bridgeAddress": endpoint.Address, "tokenPath": paths["runtime"], "threadId": thread, "bindingId": binding, "podUid": pod, "runtimeProcessId": "process_" + pod, "serverName": "work-" + adapter}}
			runtime := startMCPDeliveryChild(t, ctx, "../services/agent-runtime", "packages/runtime-pod/test/fixtures/mcp-manifest-composition.ts", runtimeInput, nil)
			mustMCPDeliveryExec(t, admin, `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1' WHERE session_id=$1`, session)
			cold := runtime.observe()
			assertDeliveryGeneration(t, cold, 7)
			busy := adapter == "slack"
			seedProvider := func(number int, hold bool, expected int) {
				id := fmt.Sprintf("rin_manifest_delivery_%d", number)
				var sequence int64
				if err := admin.QueryRow(`SELECT COALESCE(MAX(sequence),0)+1 FROM session_events WHERE session_id=$1`, session).Scan(&sequence); err != nil {
					t.Fatal(err)
				}
				seedBridgeAPIEvent(t, admin, "default", session, thread, "evt_"+id, sequence, "user.message", `{"content":[{"type":"text","text":"observe catalog"}]}`)
				seedBridgeAPIRuntimeInbox(t, admin, "default", session, thread, id, "messages", fmt.Sprintf(`[%q]`, "evt_"+id), "delivering", binding, pod, sequence, sequence)
				runtime.action(map[string]any{"kind": "start-provider", "runtimeInputId": id, "inputOrder": sequence, "hold": hold})
				runtime.wait(func(observation deliveryRuntimeObservation) bool {
					return len(observation.ProviderRequests) >= expected
				}, "actual provider request")
			}
			if busy {
				seedProvider(1, true, 1)
				first := runtime.observe()
				assertDeliveryProvider(t, first.ProviderRequests[0], false, 7)
			}
			mcp.action(map[string]any{"kind": "reset"})
			mcp.action(map[string]any{"kind": "configure", "adapter": adapter, "version": "v2"})
			mcp.action(map[string]any{"kind": "notify", "adapter": adapter})
			waitMCPDelivery(t, ctx, "actual notification commits generation8", func() bool {
				var generation int64
				_ = admin.QueryRow(`SELECT manifest_generation FROM session_mcp_manifests WHERE session_id=$1 AND mcp_server_name=$2`, session, "work-"+adapter).Scan(&generation)
				return generation == 8
			})
			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueDB))
			delivery := jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runnerDB), runtime.startup.RuntimePort, jobrunner.KubernetesRuntimeTargetResolver{GetPod: func(_ context.Context, namespace, name string) (*enginekubernetes.PodObservation, error) {
				return &enginekubernetes.PodObservation{Namespace: namespace, Name: name, UID: pod, IP: "127.0.0.1", Running: true, Ready: true}, nil
			}, Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: pod, PodIP: "127.0.0.1"}})
			}})
			sender := jobrunner.NewRuntimePodCommandClient(grpcauth.FileTokenSource{Path: paths["runner"]})
			defer sender.Close()
			observed := &observedRuntimeDeliverer{RuntimePodDirectDeliverer: jobrunner.RuntimePodDirectDeliverer{Store: delivery, Sender: sender}}
			runner := &jobrunner.JobRunner{Queue: queueservice.NewServer(queueStore, nil), Deliverer: observed}
			lease := func(now time.Time) *queue.Job {
				jobs, err := queueStore.Lease(ctx, queue.LeaseRequest{WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeConfigUpdate}, LeaseOwner: "mcp-delivery-runner", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
				if err != nil || len(jobs) != 1 {
					t.Fatalf("actual Queue lease=%v err=%v", jobs, err)
				}
				return jobs[0]
			}
			leased := lease(time.Now())
			job, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := delivery.PrepareRuntimeCommand(ctx, job)
			if err != nil || plan.RuntimeConfig == nil {
				t.Fatalf("actual delivery plan=%+v err=%v", plan, err)
			}
			deliver := func(leased *queue.Job) {
				if err := runIssuedLeaseThroughRunner(ctx, runner, queueJobProto(leased), jobrunner.JobRunnerConfig{LeaseOwner: "mcp-delivery-runner", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}); err != nil {
					t.Fatal(err)
				}
			}
			deliver(leased)
			if busy {
				if observed.result.Status != jobrunner.RuntimeDeliveryRejected || !observed.result.Retryable {
					t.Fatalf("busy delivery=%+v", observed.result)
				}
				unchanged := runtime.observe()
				assertDeliveryGeneration(t, unchanged, 7)
				assertDeliveryProvider(t, unchanged.ProviderRequests[0], false, 7)
				runtime.action(map[string]any{"kind": "release-provider"})
				runtime.action(map[string]any{"kind": "wait-idle"})
				var available time.Time
				if err := admin.QueryRow(`SELECT available_at FROM queue_jobs WHERE id=$1`, leased.ID).Scan(&available); err != nil {
					t.Fatal(err)
				}
				retry := lease(available.Add(time.Millisecond))
				if retry.ID != leased.ID {
					t.Fatal("busy delivery replaced Queue custody")
				}
				deliver(retry)
			}
			if observed.result.Status != jobrunner.RuntimeDeliveryAccepted {
				t.Fatalf("actual Runtime application=%+v", observed.result)
			}
			applied := runtime.observe()
			assertDeliveryGeneration(t, applied, 8)
			duplicate, err := sender.ApplyRuntimeConfig(ctx, plan.Target, plan.RuntimeConfig)
			if err != nil || duplicate.GetDuplicate() == nil {
				t.Fatalf("actual duplicate=%v err=%v", duplicate, err)
			}
			stale := proto.Clone(plan.RuntimeConfig).(*runtimev1.ApplyRuntimeConfigRequest)
			payload, err := mcpmanifest.CommandPayload("default", session, "work-"+adapter, mcpmanifest.Row{ToolsJSON: sql.NullString{String: baseTools, Valid: true}, ManifestETag: sql.NullString{String: baseETag, Valid: true}, Generation: 7, Readiness: "ready"})
			if err != nil {
				t.Fatal(err)
			}
			stale.GetMcpManifest().Generation = 7
			stale.GetMcpManifest().ContentJson = payload
			staleResponse, err := sender.ApplyRuntimeConfig(ctx, plan.Target, stale)
			if err != nil || staleResponse.GetDuplicate() == nil {
				t.Fatalf("actual stale=%v err=%v", staleResponse, err)
			}
			assertDeliveryGeneration(t, runtime.observe(), 8)
			next := 1
			if busy {
				next = 2
			}
			seedProvider(next, false, next)
			runtime.action(map[string]any{"kind": "wait-idle"})
			warm := runtime.observe()
			assertDeliveryProvider(t, warm.ProviderRequests[next-1], true, 8)
			endpointObservation := mcp.action(map[string]any{"kind": "observe", "adapter": adapter})
			assertDeliveryEndpointCounts(t, endpointObservation)
			assertDeliveryEvents(t, warm, adapter)
			mcp.close()
			runtime.close()
			replacement := startMCPDeliveryChild(t, ctx, "../services/agent-runtime", "packages/runtime-pod/test/fixtures/mcp-manifest-composition.ts", runtimeInput, nil)
			fresh := replacement.observe()
			assertDeliveryGeneration(t, fresh, 8)
			runtime = replacement
			seedProvider(next+1, false, 1)
			replacement.action(map[string]any{"kind": "wait-idle"})
			fresh = replacement.observe()
			assertDeliveryProvider(t, fresh.ProviderRequests[0], true, 8)
			assertDeliveryOtherRoute(t, warm, "work-"+otherAdapter)
			assertDeliveryOtherRoute(t, fresh, "work-"+otherAdapter)
			wantTools, wantETag := deliveryManifest(t, true)
			var storedTools, storedETag, readiness, payloadSession, payloadServer, payloadGeneration string
			if err := admin.QueryRow(`SELECT tools_json,manifest_etag,readiness FROM session_mcp_manifests WHERE session_id=$1 AND mcp_server_name=$2`, session, "work-"+adapter).Scan(&storedTools, &storedETag, &readiness); err != nil {
				t.Fatal(err)
			}
			if storedTools != wantTools || storedETag != wantETag || readiness != "ready" {
				t.Fatal("actual stored V2 differs from literal complete ready manifest")
			}
			if err := admin.QueryRow(`SELECT payload_json::jsonb->>'session_id',payload_json::jsonb->>'mcp_server_name',payload_json::jsonb->>'manifest_generation' FROM queue_jobs WHERE id=$1`, leased.ID).Scan(&payloadSession, &payloadServer, &payloadGeneration); err != nil {
				t.Fatal(err)
			}
			if payloadSession != session || payloadServer != "work-"+adapter || payloadGeneration != "8" {
				t.Fatal("original Queue carrier identity differs from accepted manifest")
			}
			var generation, otherGeneration, jobs int
			var status string
			if err := admin.QueryRow(`SELECT manifest_generation,(SELECT manifest_generation FROM session_mcp_manifests WHERE session_id=$1 AND mcp_server_name=$3),(SELECT count(*) FROM queue_jobs WHERE causal_session_id=$1 AND kind='runtime_config_update'),(SELECT status FROM queue_jobs WHERE id=$4) FROM session_mcp_manifests WHERE session_id=$1 AND mcp_server_name=$2`, session, "work-"+adapter, "work-"+map[string]string{"github": "slack", "slack": "github"}[adapter], leased.ID).Scan(&generation, &otherGeneration, &jobs, &status); err != nil {
				t.Fatal(err)
			}
			if generation != 8 || otherGeneration != 7 || jobs != 1 || status != queue.StatusAcknowledged {
				t.Fatalf("durable custody/catalog=%d/%d jobs%d status%s", generation, otherGeneration, jobs, status)
			}
			evidence, _ := json.Marshal(map[string]any{"case_id": "runtime-manifest-delivery", "variant": adapter, "versions": mcp.startup.Versions, "substitutions": []string{"local MCP HTTP peer", "controlled provider stream", "verified service-account identity", "controlled Kubernetes ready target snapshot", "empty successful Sandbox capture provider", "fixture Blob store"}, "barriers": []string{"actual SDK notification before Bridge acceptance", "actual Queue issued lease before Runner delivery", "actual Runtime apply response before Queue ACK", "provider snapshot before and after update", "both Connector owners joined before replacement Bridge cold load", "entire old Runtime joined before replacement Bridge cold load"}, "endpoint": json.RawMessage(endpointObservation), "warm": warm, "replacement": fresh, "sql": map[string]any{"generation": generation, "other_server_generation": otherGeneration, "jobs": jobs, "queue_status": status}, "busy": busy})
			t.Logf("case_evidence=%s", evidence)
		})
	}
}

type deliveryRuntimeObservation struct {
	Cold   json.RawMessage
	Events []struct {
		Source, Disposition string
		McpServerName       string
		ReceivedGeneration  int
		CurrentGeneration   int
	}
	CurrentGeneration int
	ProviderRequests  []deliveryProviderRequest
	ActiveProviders   int
	CatalogNames      [][]string
	CatalogRoutes     [][]struct {
		Name  string
		Route struct{ Kind, Operation, McpServerName string }
	}
}
type deliveryProviderRequest struct {
	ToolNames      []string
	ModelRequestID string
	Generation     int
}

func assertDeliveryGeneration(t *testing.T, o deliveryRuntimeObservation, want int) {
	t.Helper()
	if o.CurrentGeneration != want {
		t.Fatalf("actual Runtime generation=%d want%d observation=%+v", o.CurrentGeneration, want, o)
	}
}
func assertDeliveryProvider(t *testing.T, p deliveryProviderRequest, extra bool, generation int) {
	t.Helper()
	hasEcho, hasExtra, hasOther := false, false, false
	for _, name := range p.ToolNames {
		hasOther = hasOther || name == "other_control"
		hasEcho = hasEcho || name == "read_echo"
		hasExtra = hasExtra || name == "read_extra"
	}
	if !hasEcho || !hasOther || hasExtra != extra || p.Generation != generation || p.ModelRequestID == "" {
		t.Fatalf("actual provider catalog=%+v expected extra%v generation%d", p, extra, generation)
	}
}
func mustMCPDeliveryExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatal(err)
	}
}
func waitMCPDelivery(t *testing.T, ctx context.Context, label string, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal(label)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
func deliveryManifest(t *testing.T, extra bool) (string, string) {
	t.Helper()
	schema := `{"additionalProperties":false,"properties":{"nonce":{"type":"string"}},"required":["nonce"],"type":"object"}`
	tools := []mcpmanifest.Tool{{Name: "read_echo", Description: "Echo the supplied nonce.", InputSchemaJSON: schema}}
	if extra {
		tools = append(tools, mcpmanifest.Tool{Name: "read_extra", Description: "Read an extra fixture value.", InputSchemaJSON: schema})
	}
	durable, err := mcpmanifest.CanonicalToolsJSON(tools)
	if err != nil {
		t.Fatal(err)
	}
	logical := []map[string]any{}
	for _, tool := range tools {
		var input any
		json.Unmarshal([]byte(schema), &input)
		logical = append(logical, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": input})
	}
	encoded, _ := json.Marshal(logical)
	digest := sha256.Sum256(encoded)
	return durable, hex.EncodeToString(digest[:])
}

type mcpDeliveryChild struct {
	t       *testing.T
	ctx     context.Context
	startup struct {
		ControlAddress, ConnectorAddress string
		RuntimePort                      int
		Versions                         map[string]string
	}
	command *exec.Cmd
	stderr  lockedBuffer
	once    sync.Once
}

func startMCPDeliveryChild(t *testing.T, ctx context.Context, dir, fixture string, input any, environment []string) *mcpDeliveryChild {
	t.Helper()
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "input.json")
	raw, _ := json.Marshal(input)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	child := &mcpDeliveryChild{t: t, ctx: ctx}
	child.command = exec.CommandContext(ctx, bun, "run", fixture, path)
	child.command.Dir = dir
	child.command.Env = append(os.Environ(), environment...)
	stdout, err := child.command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.command.Stderr = &child.stderr
	if err := child.command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(child.close)
	if err := json.NewDecoder(stdout).Decode(&child.startup); err != nil {
		t.Fatalf("start %s: %v stderr=%s", fixture, err, child.stderr.String())
	}
	return child
}
func (c *mcpDeliveryChild) tryAction(action map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(action)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.startup.ControlAddress, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	response, err := (&http.Client{Timeout: 8 * time.Second}).Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var result json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return result, fmt.Errorf("owned child action status %d", response.StatusCode)
	}
	return result, nil
}
func (c *mcpDeliveryChild) action(action map[string]any) json.RawMessage {
	c.t.Helper()
	result, err := c.tryAction(action)
	if err != nil {
		c.t.Fatalf("owned child action kind%v err%v result%s stderr%s", action["kind"], err, result, c.stderr.String())
	}
	return result
}
func (c *mcpDeliveryChild) observe() deliveryRuntimeObservation {
	c.t.Helper()
	var o deliveryRuntimeObservation
	if err := json.Unmarshal(c.action(map[string]any{"kind": "observe"}), &o); err != nil {
		c.t.Fatal(err)
	}
	return o
}
func (c *mcpDeliveryChild) wait(predicate func(deliveryRuntimeObservation) bool, label string) {
	waitMCPDelivery(c.t, c.ctx, label, func() bool { return predicate(c.observe()) })
}
func (c *mcpDeliveryChild) close() {
	c.once.Do(func() {
		if c.startup.ControlAddress != "" && c.ctx.Err() == nil {
			if _, err := c.tryAction(map[string]any{"kind": "shutdown"}); err != nil {
				c.t.Errorf("owned child shutdown request: %v", err)
			}
		}
		done := make(chan error, 1)
		go func() { done <- c.command.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				c.t.Errorf("owned child exit=%v stderr%s", err, c.stderr.String())
			}
		case <-time.After(8 * time.Second):
			_ = c.command.Process.Kill()
			<-done
			c.t.Errorf("owned child cleanup deadline stderr%s", c.stderr.String())
		}
		c.t.Log("case_cleanup=owned-child-joined")
	})
}

func deliveryOtherManifest(t *testing.T) (string, string) {
	t.Helper()
	schema := `{"additionalProperties":false,"properties":{"nonce":{"type":"string"}},"required":["nonce"],"type":"object"}`
	tools := []mcpmanifest.Tool{{Name: "read_echo", Description: "Echo the supplied nonce.", InputSchemaJSON: schema}, {Name: "other_control", Description: "Retained other-server control.", InputSchemaJSON: schema}}
	durable, err := mcpmanifest.CanonicalToolsJSON(tools)
	if err != nil {
		t.Fatal(err)
	}
	logical := []map[string]any{}
	for _, tool := range tools {
		var input any
		json.Unmarshal([]byte(schema), &input)
		logical = append(logical, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": input})
	}
	encoded, _ := json.Marshal(logical)
	digest := sha256.Sum256(encoded)
	return durable, hex.EncodeToString(digest[:])
}

func assertDeliveryOtherRoute(t *testing.T, o deliveryRuntimeObservation, server string) {
	t.Helper()
	if len(o.CatalogRoutes) == 0 {
		t.Fatal("actual catalog routes missing")
	}
	for _, entry := range o.CatalogRoutes[len(o.CatalogRoutes)-1] {
		if entry.Name == "other_control" && entry.Route.Kind == "gateway" && entry.Route.Operation == "RunMcpTool" && entry.Route.McpServerName == server {
			return
		}
	}
	t.Fatalf("other-server route not retained: %+v", o.CatalogRoutes)
}

func startMCPDeliveryCaptures(t *testing.T, ctx context.Context, sandboxDB, queueDB *sql.DB) {
	t.Helper()
	registry, err := sandbox.NewProviderRegistry(map[string]sandbox.ProviderAdapter{"daytona": handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}})
	if err != nil {
		t.Fatal(err)
	}
	worker := &sandbox.SandboxOutputCaptureJobRunner{Queue: queueservice.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueDB)), nil), Store: sandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(sandboxDB)), Providers: registry, BlobStore: blob.NewFakeBlobStore(), Config: sandbox.SandboxOutputCaptureRunnerConfig{WorkspaceID: "default", LeaseOwner: "mcp-manifest-capture", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Second}}
	owned, cancel := context.WithCancel(ctx)
	joined := make(chan error, 1)
	go func() {
		for {
			_, err := worker.RunOnceWithActivity(owned)
			if err != nil || owned.Err() != nil {
				if owned.Err() != nil {
					err = nil
				}
				joined <- err
				return
			}
			select {
			case <-owned.Done():
				joined <- nil
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-joined:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("actual output-capture worker failed join")
		}
	})
}

func assertDeliveryEndpointCounts(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var o struct {
		Counts      struct{ Initialize, List, Call, Effects, Notifications int }
		IssuerCalls int
		Requests    []struct{ Method, Origin string }
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	notice, verification := 0, 0
	for _, request := range o.Requests {
		if request.Method != "tools/list" {
			t.Fatalf("unexpected notification traffic=%+v", request)
		}
		switch request.Origin {
		case "sdk-notification":
			notice++
		case "bridge-verification":
			verification++
		default:
			t.Fatalf("unidentified notification list=%+v", request)
		}
	}
	if o.Counts.Initialize != 0 || o.Counts.List != 2 || o.Counts.Call != 0 || o.Counts.Effects != 0 || o.Counts.Notifications != 1 || o.IssuerCalls != 0 || notice != 1 || verification != 1 {
		t.Fatalf("literal notification/verification counters=%+v", o)
	}
}
func assertDeliveryEvents(t *testing.T, o deliveryRuntimeObservation, adapter string) {
	t.Helper()
	applied, stale := 0, 0
	for _, event := range o.Events {
		if event.McpServerName != "work-"+adapter || event.Source != "runtime_config_update" {
			continue
		}
		if event.Disposition == "applied" && event.CurrentGeneration == 8 && event.ReceivedGeneration == 8 {
			applied++
		}
		if event.Disposition == "stale" && event.CurrentGeneration == 8 && event.ReceivedGeneration == 7 {
			stale++
		}
	}
	if applied != 1 || stale != 1 {
		t.Fatalf("actual Runtime application event counts applied%d stale%d events%+v", applied, stale, o.Events)
	}
}
