package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	webconnector "github.com/tetral-ai/tetral/services/web-connector"

	"google.golang.org/grpc"
)

func TestPostgreSQLRuntimeWebFirstEffectAuthority(t *testing.T) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Fatalf("Web production composition requires bun: %v", err)
	}
	runtimeRoot := filepath.Clean(filepath.Join("..", "agent-runtime"))
	if _, err := os.Stat(filepath.Join(runtimeRoot, "node_modules", "@grpc", "grpc-js")); err != nil {
		t.Fatalf("Web production composition requires installed Runtime dependencies: %v", err)
	}

	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_web_effect_authority"
		threadID    = "thr_web_effect_authority"
		bindingID   = "bind_web_effect_authority"
		podUID      = "pod_web_effect_authority"
		requestID   = "mreq_web_effect_authority"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("web-effect-authority-signing-key")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_web_effect_start", requestID, requestKindAgentProviderRequest, 0)

	writeWebTool := func(writeID, callID, query string) string {
		t.Helper()
		response, writeErr := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: writeID, ModelRequestId: requestID,
			ToolDeclaration: bridgeToolDeclarationForTest(callID, "web", `{"search_query":[{"q":"`+query+`"}]}`, "allow", "web_execute"),
		})
		if writeErr != nil || response.GetCommitted() == nil {
			t.Fatalf("write Web Tool %s = %#v/%v", callID, response, writeErr)
		}
		return response.GetCommitted().GetEventId()
	}
	staleToolUseID := writeWebTool("rwrite_web_effect_stale", "call_web_effect_stale", "interrupt-first")
	concurrentToolUseID := writeWebTool("rwrite_web_effect_concurrent", "call_web_effect_concurrent", "concurrent")
	dispatchToolUseID := writeWebTool("rwrite_web_effect_dispatch", "call_web_effect_dispatch", "dispatch-first")

	settleCancelled := func(toolUseEventID string) {
		t.Helper()
		response, settleErr := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(scope, &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUseEventID,
			Outcome:        &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{}},
		}))
		if settleErr != nil || response.GetCommitted() == nil {
			t.Fatalf("cancel Web route %s = %#v/%v", toolUseEventID, response, settleErr)
		}
	}
	settleCancelled(staleToolUseID)

	_, bridgeAddress := startSandboxProductionBoundaryBridgeClient(t, BridgeAPIServer{store: store}, podUID)
	backend := newWebEffectAuthorityBackend()
	webAddress := startWebEffectAuthorityServer(t, backend, store.RuntimeBindingTokenHMACKey, podUID)
	runtimeBindingToken, err := store.runtimeBindingToken(scope)
	if err != nil {
		t.Fatalf("mint Web composition binding token: %v", err)
	}
	tokenPath := filepath.Join(t.TempDir(), "runtime-token")
	if err := os.WriteFile(tokenPath, []byte("sandbox-production-runtime-token\n"), 0o600); err != nil {
		t.Fatalf("write Web composition Runtime token: %v", err)
	}

	run := func(mode, callID, toolUseEventID, query string, beforeWait func(), afterStart func()) []webEffectRuntimeResult {
		t.Helper()
		inputPath := filepath.Join(t.TempDir(), "web-effect.json")
		raw, marshalErr := json.Marshal(map[string]any{
			"mode": mode, "bridgeAddress": bridgeAddress, "webAddress": webAddress,
			"tokenPath": tokenPath, "workspaceId": workspaceID, "sessionId": sessionID,
			"sessionThreadId": threadID, "bindingId": bindingID, "bindingGeneration": 1,
			"targetPodUid": podUID, "runtimeBindingToken": runtimeBindingToken,
			"modelRequestId": requestID, "modelToolCallId": callID,
			"toolUseEventId": toolUseEventID, "query": query,
		})
		if marshalErr != nil {
			t.Fatalf("marshal Web composition input: %v", marshalErr)
		}
		if err := os.WriteFile(inputPath, raw, 0o600); err != nil {
			t.Fatalf("write Web composition input: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, bunPath, "run", "packages/runtime-pod/test/fixtures/web-effect-authority-composition.ts", inputPath) //nolint:gosec // fixed repository fixture and test-owned path.
		command.Dir = runtimeRoot
		var stdout, stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		if beforeWait == nil {
			if err := command.Run(); err != nil {
				t.Fatalf("run Web composition: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
			}
		} else {
			finished := make(chan error, 1)
			if err := command.Start(); err != nil {
				t.Fatalf("start Web composition: %v", err)
			}
			go func() { finished <- command.Wait() }()
			beforeWait()
			if afterStart != nil {
				afterStart()
			}
			if err := <-finished; err != nil {
				t.Fatalf("run Web composition: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
			}
		}
		var result struct {
			Results []webEffectRuntimeResult `json:"results"`
		}
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("decode Web composition: %v\nstdout=%s", err, stdout.String())
		}
		return result.Results
	}

	stale := run("single", "call_web_effect_stale", staleToolUseID, "interrupt-first", nil, nil)
	if len(stale) != 1 || stale[0].Type != "stale_custody" || backend.callsFor("interrupt-first") != 0 {
		t.Fatalf("interrupt-first Web result = %+v calls=%d; want stale and zero backend calls", stale, backend.callsFor("interrupt-first"))
	}

	concurrent := run("concurrent", "call_web_effect_concurrent", concurrentToolUseID, "concurrent", func() {
		backend.waitStarted(t, "concurrent")
	}, func() {
		backend.release("concurrent")
	})
	if len(concurrent) != 2 || concurrent[0].Type != "completed" || concurrent[1].Type != "completed" ||
		concurrent[0].Output.Text != concurrent[1].Output.Text || backend.callsFor("concurrent") != 1 {
		t.Fatalf("concurrent Web results = %+v calls=%d; want one identical winner", concurrent, backend.callsFor("concurrent"))
	}
	replayed := run("single", "call_web_effect_concurrent", concurrentToolUseID, "concurrent", nil, nil)
	if len(replayed) != 1 || replayed[0] != concurrent[0] || backend.callsFor("concurrent") != 1 {
		t.Fatalf("Web replay = %+v calls=%d; want durable winner without execution", replayed, backend.callsFor("concurrent"))
	}

	dispatched := run("single", "call_web_effect_dispatch", dispatchToolUseID, "dispatch-first", func() {
		backend.waitStarted(t, "dispatch-first")
	}, func() {
		settleCancelled(dispatchToolUseID)
		backend.release("dispatch-first")
	})
	if len(dispatched) != 1 || dispatched[0].Type != "completed" || backend.callsFor("dispatch-first") != 1 {
		t.Fatalf("dispatch-first Web result = %+v calls=%d; want the already-dispatched call to finish", dispatched, backend.callsFor("dispatch-first"))
	}
}

type webEffectRuntimeResult struct {
	Type   string `json:"type"`
	Output struct {
		Text string `json:"text"`
	} `json:"output"`
}

type webEffectAuthorityBackend struct {
	mu       sync.Mutex
	calls    map[string]int
	started  map[string]chan struct{}
	releases map[string]chan struct{}
	once     map[string]*sync.Once
	total    atomic.Int32
}

func newWebEffectAuthorityBackend() *webEffectAuthorityBackend {
	return &webEffectAuthorityBackend{
		calls: map[string]int{}, started: map[string]chan struct{}{}, releases: map[string]chan struct{}{}, once: map[string]*sync.Once{},
	}
}

func (b *webEffectAuthorityBackend) Search(ctx context.Context, query string, _ []string) ([]webconnector.SearchHit, webconnector.BackendOutcome) {
	b.mu.Lock()
	b.calls[query]++
	b.total.Add(1)
	started := b.started[query]
	if started == nil {
		started = make(chan struct{})
		b.started[query] = started
	}
	release := b.releases[query]
	if release == nil {
		release = make(chan struct{})
		b.releases[query] = release
	}
	once := b.once[query]
	if once == nil {
		once = &sync.Once{}
		b.once[query] = once
	}
	once.Do(func() { close(started) })
	b.mu.Unlock()
	select {
	case <-release:
		return []webconnector.SearchHit{{URL: "https://example.com/" + query, Title: query}}, webconnector.BackendOutcome{Kind: webconnector.BackendSuccess, Requests: 1}
	case <-ctx.Done():
		return nil, webconnector.BackendOutcome{Kind: webconnector.BackendRuntimeError}
	}
}

func (*webEffectAuthorityBackend) Fetch(context.Context, string) (webconnector.Page, webconnector.BackendOutcome) {
	panic("unexpected Web fetch")
}

func (b *webEffectAuthorityBackend) waitStarted(t *testing.T, query string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.mu.Lock()
		started := b.started[query]
		b.mu.Unlock()
		if started != nil {
			select {
			case <-started:
				return
			case <-time.After(time.Until(deadline)):
				t.Fatalf("Web backend %q did not start", query)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Web backend %q was never invoked", query)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *webEffectAuthorityBackend) release(query string) {
	b.mu.Lock()
	release := b.releases[query]
	b.mu.Unlock()
	if release != nil {
		close(release)
	}
}

func (b *webEffectAuthorityBackend) callsFor(query string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[query]
}

func startWebEffectAuthorityServer(t *testing.T, backend webconnector.Backend, bindingKey []byte, podUID string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Web composition: %v", err)
	}
	service := webconnector.NewService(blob.NewFakeBlobStore(), backend, webconnector.NewBindingVerifier(bindingKey, time.Now), webconnector.NewMetrics(), time.Now, nil)
	server, err := internalgrpc.NewServer(internalgrpc.Config{
		ServiceName: "web-effect-authority-composition", Listener: listener,
		Authenticator:    sandboxCompositionAuthenticator{runtimePodUID: podUID},
		MethodAuthorizer: webconnector.MethodAuthorizer,
		Register: func(registrar *grpc.Server) {
			providergatewayv1.RegisterProviderGatewayServiceServer(registrar, service)
		},
	})
	if err != nil {
		t.Fatalf("construct Web composition server: %v", err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}
