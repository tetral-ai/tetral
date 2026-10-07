package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	gatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	web "github.com/tetral-ai/tetral/services/web-connector"
)

const replicaWebKey = "replica-web-binding-key-at-least-32-bytes"

type replicaWebIdentity struct{}

func (replicaWebIdentity) Authenticate(context.Context, string) (grpcauth.Identity, error) {
	return grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "pod_replica_web"}, nil
}

func TestReplicaWebSharedStore(t *testing.T) {
	t.Run("drain-completes", func(t *testing.T) { testReplicaWebCommandDrain(t, false) })
	t.Run("drain-cancels", func(t *testing.T) { testReplicaWebCommandDrain(t, true) })
	t.Run("three_instances", func(t *testing.T) {
		stores, objects, receipts := replicaWebObservedStores(t)
		var calls atomic.Int64
		var duplicateCalls atomic.Int64
		duplicateStarted, duplicateRelease := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseDuplicate := func() { releaseOnce.Do(func() { close(duplicateRelease) }) }
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			var input struct {
				URL string `json:"url"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, "invalid backend fixture request", http.StatusBadRequest)
				return
			}
			if input.URL == "https://duplicate.example/" {
				if duplicateCalls.Add(1) == 1 {
					close(duplicateStarted)
				}
				select {
				case <-duplicateRelease:
				case <-r.Context().Done():
					return
				}
			}
			if input.URL == "https://failure.example/" {
				http.Error(w, "controlled transient backend failure", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if strings.HasPrefix(r.URL.Path, "/search") {
				_, _ = io.WriteString(w, `{"code":200,"data":[{"title":"Example","url":"https://example.com/","description":"fixture"}],"meta":{"usage":{"tokens":1}}}`)
			} else {
				_, _ = io.WriteString(w, `{"code":200,"data":{"title":"Example","url":"https://example.com/","content":"Alpha\nbeta alpha","httpStatus":200,"usage":{"tokens":2}}}`)
			}
		}))
		defer func() { releaseDuplicate(); backend.Close() }()
		var clients []gatewayv1.ProviderGatewayServiceClient
		for i := 0; i < 3; i++ {
			blobs := stores[i]
			metrics := web.NewMetrics()
			jina := web.NewJinaBackend(backend.Client(), backend.URL+"/search", backend.URL+"/reader", []string{"fixture"}, time.Now)
			t.Cleanup(jina.Close)
			service := web.NewService(blobs, jina, web.NewBindingVerifier([]byte(replicaWebKey), time.Now), metrics, time.Now, nil)
			clients = append(clients, serveReplicaWeb(t, service, metrics))
		}
		ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer fixture"), 60*time.Second)
		defer cancel()
		run := func(index int, request *gatewayv1.RunWebRequest) *gatewayv1.RunWebResponse {
			t.Helper()
			response, err := clients[index].RunWeb(ctx, request)
			if err != nil || response.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
				t.Fatalf("replica%d response=%+v/%v", index, response, err)
			}
			return response
		}
		search := replicaWebRequest("event_search", &gatewayv1.WebToolInput{SearchQuery: []*gatewayv1.WebSearchQuery{{Q: "example"}}})
		found := run(0, search)
		if len(found.Refs) != 1 {
			t.Fatalf("references=%+v", found.Refs)
		}
		ref := found.Refs[0].RefId
		// Search creates a stub; materialize it on A before asserting cross-replica local reads.
		materialized := run(0, replicaWebRequest("event_materialize", &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{RefId: &ref}}}))
		if calls.Load() != 2 {
			t.Fatalf("search+materialization calls=%d", calls.Load())
		}
		opened := run(1, replicaWebRequest("event_open", &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{RefId: &ref}}}))
		matched := run(2, replicaWebRequest("event_find", &gatewayv1.WebToolInput{Find: []*gatewayv1.WebFindRequest{{RefId: ref, Pattern: "(?i)alpha"}}}))
		if opened.ResultText != materialized.ResultText || !strings.Contains(opened.ResultText, "lines 1-2 of 2\n\nAlpha\nbeta alpha") || !strings.Contains(matched.ResultText, "L2: beta alpha") || !strings.Contains(matched.ResultText, "Alpha") || opened.GetUsage().GetWebFetchRequests() != 0 || matched.GetUsage().GetWebFetchRequests() != 0 || calls.Load() != 2 {
			t.Fatalf("cross instance snapshot open=%s find=%s calls=%d", opened.ResultText, matched.ResultText, calls.Load())
		}
		replay := run(2, search)
		if !proto.Equal(found, replay) || calls.Load() != 2 {
			t.Fatal("committed receipt replay changed result or called backend")
		}
		searchDigest, err := replicaWebStoredReceipt(ctx, receipts, search, found)
		if err != nil {
			t.Fatal(err)
		}
		conflict := proto.Clone(search).(*gatewayv1.RunWebRequest)
		conflict.Input.SearchQuery[0].Q = "changed"
		rejected, err := clients[1].RunWeb(ctx, conflict)
		if err != nil || rejected.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || calls.Load() != 2 {
			t.Fatalf("conflict=%+v/%v", rejected, err)
		}
		replay = run(2, search)
		unchangedDigest, err := replicaWebStoredReceipt(ctx, receipts, search, replay)
		if err != nil || !proto.Equal(found, replay) || searchDigest != unchangedDigest || calls.Load() != 2 {
			t.Fatalf("conflict changed original receipt/result: %v calls=%d", err, calls.Load())
		}
		for _, foreign := range []struct{ name, workspace, session, thread string }{
			{"workspace", "other_workspace", "sesn_replica_web", "sthr_replica_web"},
			{"session", "default", "sesn_other", "sthr_replica_web"},
			{"thread", "default", "sesn_replica_web", "sthr_other"},
		} {
			isolated := replicaWebRequest("event_other_"+foreign.name, &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{RefId: &ref}}})
			isolated.WorkspaceId, isolated.SessionId, isolated.SessionThreadId = foreign.workspace, foreign.session, foreign.thread
			signReplicaWeb(isolated)
			other, err := clients[2].RunWeb(ctx, isolated)
			if err != nil || other.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_TOOL_ERROR || strings.Contains(other.GetResultText(), "Alpha") || strings.Contains(other.GetResultText(), "beta alpha") || len(other.GetRefs()) != 0 || calls.Load() != 2 {
				t.Fatalf("%s scope isolation=%+v/%v", foreign.name, other, err)
			}
		}
		_, snapshotsBefore := replicaWebPutCounts(objects, "")
		if snapshotsBefore != 2 {
			t.Fatalf("search stub/materialized page plus conflict/scope denials created %d snapshot writes; want 2", snapshotsBefore)
		}
		duplicateURL := "https://duplicate.example/"
		duplicate := replicaWebRequest("event_concurrent", &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{Url: &duplicateURL}}})
		type result struct {
			response *gatewayv1.RunWebResponse
			err      error
		}
		winner, waiter := make(chan result, 1), make(chan result, 1)
		go func() { response, err := clients[0].RunWeb(ctx, duplicate); winner <- result{response, err} }()
		select {
		case <-duplicateStarted:
		case <-ctx.Done():
			t.Fatal("concurrent winner backend barrier absent")
		}
		go func() {
			response, err := clients[1].RunWeb(ctx, proto.Clone(duplicate).(*gatewayv1.RunWebRequest))
			waiter <- result{response, err}
		}()
		// Do not release the backend until B has read the actual in-flight
		// receipt from MinIO. This rejects a merely sequential replay fixture.
		select {
		case <-objects[1].inFlightPolled:
		case <-ctx.Done():
			t.Fatal("duplicate B never observed A's actual in-flight MinIO claim")
		}
		releaseDuplicate()
		var original *gatewayv1.RunWebResponse
		for i, channel := range []<-chan result{winner, waiter} {
			select {
			case result := <-channel:
				if result.err != nil || result.response.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
					t.Fatalf("concurrent replica%d=%+v/%v", i, result.response, result.err)
				}
				if i == 0 {
					original = result.response
				} else if !proto.Equal(original, result.response) {
					t.Fatal("concurrent duplicate did not return the exact winner outcome")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		replay = run(2, duplicate)
		if !proto.Equal(original, replay) || duplicateCalls.Load() != 1 || calls.Load() != 3 {
			t.Fatalf("concurrent/C replay backend=%d total=%d", duplicateCalls.Load(), calls.Load())
		}
		duplicateDigest, err := replicaWebStoredReceipt(ctx, receipts, duplicate, original)
		if err != nil {
			t.Fatal(err)
		}
		jobKey := "default/sesn_replica_web/sthr_replica_web/jobs/event_concurrent.job"
		jobWrites, snapshotsAfter := replicaWebPutCounts(objects, jobKey)
		if jobWrites != 2 || snapshotsAfter != snapshotsBefore+1 {
			t.Fatalf("concurrent MinIO effects claim+result=%d snapshots=%d→%d", jobWrites, snapshotsBefore, snapshotsAfter)
		}
		lostURL := "https://lost.example/"
		lost := replicaWebRequest("event_response_lost", &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{Url: &lostURL}}})
		captured := replicaWebLoseCommittedResponse(ctx, t, clients[0], receipts, lost)
		replay = run(1, lost)
		lostDigest, err := replicaWebStoredReceipt(ctx, receipts, lost, replay)
		if err != nil || !proto.Equal(captured.response, replay) || captured.digest != lostDigest || calls.Load() != 4 {
			t.Fatalf("lost committed response replay changed receipt/outcome: calls=%d/%v", calls.Load(), err)
		}
		jobWrites, snapshotsAfter = replicaWebPutCounts(objects, "default/sesn_replica_web/sthr_replica_web/jobs/event_response_lost.job")
		if jobWrites != 2 || snapshotsAfter != snapshotsBefore+2 {
			t.Fatalf("lost response repeated MinIO effects=%d snapshots=%d", jobWrites, snapshotsAfter)
		}
		failureURL := "https://failure.example/"
		failed := replicaWebRequest("event_runtime_error", &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{Url: &failureURL}}})
		runtimeError, err := clients[0].RunWeb(ctx, failed)
		if err != nil || runtimeError.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_RUNTIME_ERROR || calls.Load() != 5 {
			t.Fatalf("runtime failure=%+v/%v calls=%d", runtimeError, err, calls.Load())
		}
		failureDigest, err := replicaWebStoredReceipt(ctx, receipts, failed, runtimeError)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < 3; i++ {
			replay, err := clients[i].RunWeb(ctx, failed)
			digest, receiptErr := replicaWebStoredReceipt(ctx, receipts, failed, replay)
			if err != nil || receiptErr != nil || !proto.Equal(runtimeError, replay) || digest != failureDigest || calls.Load() != 5 {
				t.Fatalf("persisted runtime error replay%d=%+v/%v/%v calls=%d", i, replay, err, receiptErr, calls.Load())
			}
		}
		jobWrites, snapshotsAfter = replicaWebPutCounts(objects, "default/sesn_replica_web/sthr_replica_web/jobs/event_runtime_error.job")
		if jobWrites != 2 || snapshotsAfter != snapshotsBefore+2 {
			t.Fatalf("runtime error repeated MinIO effects=%d snapshots=%d", jobWrites, snapshotsAfter)
		}
		t.Logf("real MinIO/backend counters: backend_total=%d concurrent_backend=%d concurrent_snapshot_delta=1 lost_snapshot_delta=1 runtime_error_snapshot_delta=0; each claim+completed receipt has 2 successful job PUTs; final snapshot writes=%d", calls.Load(), duplicateCalls.Load(), snapshotsAfter)
		t.Logf("three real Web services independently pooled MinIO; A materialization/B open/C find no backend; concurrent identity backend1 with observed B in-flight receipt and one snapshot; dropped committed RPC response replay exact; persisted runtime-error B/C replay no backend/effect; Workspace/Session/Thread and payload conflict denial; receipts duplicate=%s lost=%s runtime_error=%s", duplicateDigest, lostDigest, failureDigest)
	})
}
func replicaWebRequest(event string, input *gatewayv1.WebToolInput) *gatewayv1.RunWebRequest {
	r := &gatewayv1.RunWebRequest{WorkspaceId: "default", SessionId: "sesn_replica_web", SessionThreadId: "sthr_replica_web", ToolUseEventId: event, BindingId: "bind_replica_web", BindingGeneration: 1, RuntimeProcessId: "process_replica_web", Input: input}
	signReplicaWeb(r)
	return r
}
func signReplicaWeb(r *gatewayv1.RunWebRequest) {
	payload, _ := json.Marshal(map[string]any{"v": 1, "workspace_id": r.WorkspaceId, "session_id": r.SessionId, "session_thread_id": r.SessionThreadId, "binding_id": r.BindingId, "binding_generation": r.BindingGeneration, "runtime_pod_uid": "pod_replica_web", "runtime_process_id": r.RuntimeProcessId, "exp": time.Now().Add(time.Hour).Unix()})
	part := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(replicaWebKey))
	_, _ = mac.Write([]byte(part))
	r.RuntimeBindingToken = "rtbt_v1." + part + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func serveReplicaWeb(t *testing.T, service *web.Service, metrics *web.Metrics) gatewayv1.ProviderGatewayServiceClient {
	t.Helper()
	listeners := make([]net.Listener, 2)
	for i := range listeners {
		var err error
		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
	}
	index := 0
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- web.Run(ctx, web.Config{GRPCAddress: listeners[0].Addr().String(), MetricsAddress: listeners[1].Addr().String(), DrainTimeout: 200 * time.Millisecond}, service, metrics, web.RuntimeConfig{Authenticator: replicaWebIdentity{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Listen: func(string, string) (net.Listener, error) { l := listeners[index]; index++; return l, nil }})
	}()
	conn, err := grpc.NewClient(listeners[0].Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Web process failed to join")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get("http://" + listeners[1].Addr().String() + "/ready")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal(fmt.Errorf("Web readiness timeout"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	return gatewayv1.NewProviderGatewayServiceClient(conn)
}

func testReplicaWebCommandDrain(t *testing.T, force bool) {
	_, blobCfg := replicaMinIOStoresWithConfig(t)
	// Compilation/setup uses the enclosing test deadline; the real child owns
	// the separate non-Runtime process profile: 2s drain, 3s join, 15s watchdog.
	prepareCtx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		var cancelPrepare context.CancelFunc
		prepareCtx, cancelPrepare = context.WithDeadline(prepareCtx, deadline)
		defer cancelPrepare()
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "web-connector")
	build := exec.CommandContext(prepareCtx, "go", "build", "-o", binary, "./services/web-connector/cmd/web-connector")
	build.Dir = ".."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Web command: %v %s", err, output)
	}
	started, backendJoined, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseBackend := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseBackend()
	var calls atomic.Int64
	var cancelled atomic.Bool
	external := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/apis/") {
			_, _ = io.WriteString(w, `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"audiences":["tetral-internal-grpc"],"user":{"username":"system:serviceaccount:tetral-agent-runtime:agent-runtime","extra":{"authentication.kubernetes.io/pod-uid":["pod_replica_web"]}}}}`)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) != 1 {
			http.Error(w, "unexpected backend repetition", http.StatusConflict)
			return
		}
		close(started)
		defer close(backendJoined)
		select {
		case <-release:
			_, _ = io.WriteString(w, `{"code":200,"data":{"title":"Example","url":"https://example.com/","content":"Alpha\nbeta alpha","httpStatus":200,"usage":{"tokens":2}}}`)
		case <-r.Context().Done():
			cancelled.Store(true)
		}
	}))
	defer func() { releaseBackend(); external.Close() }()
	target, err := url.Parse(blobCfg.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	objectProxy := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(target))
	defer objectProxy.Close()
	caPath := filepath.Join(dir, "fixture-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: external.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "reviewer-token")
	if err := os.WriteFile(tokenPath, []byte("fixture-reviewer"), 0600); err != nil {
		t.Fatal(err)
	}
	addresses, releaseAddresses := publicReserveAddresses(t, 2)
	logPath := filepath.Join(dir, "command.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logFile.Close() }()
	commandEnv := append(os.Environ(),
		"TETRAL_DEPLOYMENT_ENVIRONMENT=test", "TETRAL_SERVICE_VERSION=replica-fixture", "TETRAL_DRAIN_TIMEOUT_MS=2000", "TETRAL_CANCEL_JOIN_TIMEOUT_MS=3000",
		"TETRAL_WEB_SEARCH_ENDPOINT="+external.URL+"/search", "TETRAL_WEB_READER_ENDPOINT="+external.URL+"/reader", "TETRAL_WEB_API_KEYS=[\"fixture\"]",
		"TETRAL_WEB_CONNECTOR_GRPC_ADDR="+addresses[0], "TETRAL_WEB_CONNECTOR_METRICS_ADDR="+addresses[1], "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY="+replicaWebKey,
		"TETRAL_INTERNAL_GRPC_AUDIENCE=tetral-internal-grpc", "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS=tetral-agent-runtime/agent-runtime",
		"KUBERNETES_API_SERVER_URL="+external.URL, "KUBERNETES_API_CA_CERT_PATH="+caPath, "KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH="+tokenPath, "SSL_CERT_FILE="+caPath,
		"TETRAL_BLOB_ENDPOINT="+objectProxy.URL, "TETRAL_BLOB_REGION="+blobCfg.Region, "TETRAL_BLOB_BUCKET="+blobCfg.Bucket, "TETRAL_BLOB_ACCESS_KEY="+blobCfg.AccessKey, "TETRAL_BLOB_SECRET_KEY="+blobCfg.SecretKey,
		"TETRAL_BLOB_LOCAL_TEST_MODE=false", "TETRAL_BLOB_ALLOW_INSECURE=false", "TETRAL_BLOB_TLS_CA_PATH="+caPath, "TETRAL_BLOB_TLS_SERVER_NAME=example.com")
	// Start the absolute runtime watchdog only after all child preparation.
	ctx, cancel := context.WithTimeout(prepareCtx, 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Stdout = logFile
	command.Stderr = logFile
	command.Env = commandEnv
	releaseAddresses()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait(); close(done) }()
	defer func() {
		cancel()
		releaseBackend()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Web child did not join")
		}
	}()
	get := func(ctx context.Context, path string) (*http.Response, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addresses[1]+path, nil)
		if err != nil {
			return nil, err
		}
		return http.DefaultClient.Do(request)
	}
	waitReady := func(want int) {
		t.Helper()
		readyCtx, cancelReady := context.WithTimeout(ctx, 5*time.Second)
		defer cancelReady()
		for readyCtx.Err() == nil {
			response, err := get(readyCtx, "/ready")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == want {
					return
				}
			}
			select {
			case err := <-done:
				data, _ := os.ReadFile(logPath)
				t.Fatalf("Web exited before readiness %d: %v; %s", want, err, publicStartupDiagnostic(string(data)))
			case <-readyCtx.Done():
				t.Fatalf("Web readiness never became %d: %v", want, readyCtx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		t.Fatalf("Web readiness never became %d: %v", want, readyCtx.Err())
	}
	waitReady(200)
	connection, err := grpc.NewClient(addresses[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	client := gatewayv1.NewProviderGatewayServiceClient(connection)
	result := make(chan error, 1)
	go func() {
		targetURL := "https://example.com/"
		response, err := client.RunWeb(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer fixture-runtime"), replicaWebRequest("event_command_drain", &gatewayv1.WebToolInput{Open: []*gatewayv1.WebOpenRequest{{Url: &targetURL}}}))
		if err == nil && response.GetStatus() != gatewayv1.RunWebStatus_RUN_WEB_STATUS_COMPLETED {
			err = fmt.Errorf("Web result status=%v", response.GetStatus())
		}
		result <- err
	}()
	select {
	case <-started:
	case err := <-result:
		data, _ := os.ReadFile(logPath)
		t.Fatalf("Web failed before backend: %v %s", err, data)
	case <-ctx.Done():
		t.Fatalf("Web backend request barrier absent: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("Web backend request barrier absent")
	}
	signaledAt := time.Now()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitReady(503)
	metricsCtx, cancelMetrics := context.WithTimeout(ctx, 5*time.Second)
	defer cancelMetrics()
	response, err := get(metricsCtx, "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(metrics), "web_draining 1") || !strings.Contains(string(metrics), "web_requests_active 1") {
		t.Fatalf("drain metrics=%s", metrics)
	}
	select {
	case err := <-done:
		t.Fatalf("Web closed before backend joined: %v", err)
	default:
	}
	if !force {
		releaseBackend()
	}
	select {
	case <-backendJoined:
	case <-ctx.Done():
		t.Fatalf("backend handler did not cancel/join: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("backend handler did not cancel/join")
	}
	var rpcErr error
	select {
	case rpcErr = <-result:
	case <-ctx.Done():
		t.Fatalf("actual RPC did not terminate: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("actual RPC did not terminate")
	}
	select {
	case err := <-done:
		if err != nil {
			data, _ := os.ReadFile(logPath)
			t.Fatalf("Web child failed: %v %s", err, data)
		}
	case <-ctx.Done():
		t.Fatalf("Web command did not exit after joined request: %v", ctx.Err())
	case <-time.After(5 * time.Second):
		t.Fatal("Web command did not exit after joined request")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("Web runtime watchdog expired before joined proof: %v", err)
	}
	data, _ := os.ReadFile(logPath)
	if !force && rpcErr != nil {
		t.Fatalf("Web RPC failed after backend/RPC/child joined: %v %s", rpcErr, data)
	}
	if cancelled.Load() != force || calls.Load() != 1 {
		t.Fatalf("backend cancellation=%t force=%t calls=%d", cancelled.Load(), force, calls.Load())
	}
	if !strings.Contains(string(data), "web.drain.joined") || !strings.Contains(string(data), `"closeout.active_count":0`) {
		t.Fatalf("joined lifecycle log missing: %s", data)
	}
	t.Logf("real Web child SIGTERM: force=%t drain=2s cancel_join=3s runtime_watchdog=15s; readiness/admission closed, protected MinIO alive through join, HTTP context exit, RPC completion, process joined after %s", force, time.Since(signaledAt))
}
