package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	internalevents "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/testinfra"
	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
	api "github.com/tetral-ai/tetral/services/api"
	authservice "github.com/tetral-ai/tetral/services/auth"
	gitproxy "github.com/tetral-ai/tetral/services/git-proxy"
)

// The existing public-streaming fixture owns legal Bridge transactions and the
// pinned SDK parser. This composition replaces only its edge and transport,
// retaining real role-separated services and durable stores behind Envoy.
type translatedPublicEdge struct {
	ctx                                  context.Context
	lifecycle                            *edgeTLSLifecycle
	client                               *http.Client
	baseURL, key, dataDirectory          string
	objects                              blob.BlobStore
	fixture                              *envoyGatewayTranslation
	gitAuthorization                     atomic.Pointer[string]
	gitClock                             atomic.Pointer[time.Time]
	gitGate                              atomic.Pointer[edgeGitGate]
	apiRequests, eventRequests, gitCalls atomic.Int64
	authExchanges                        atomic.Int64
	// Production diagnostic sinks of the Check adapter and API router; the
	// logging correlation assertion reads their structured records.
	checkRecords, apiRecords syncBuffer
}

func (edge *translatedPublicEdge) factory(profile string) func(*testing.T, *storagetest.WorkloadDB, blob.BlobStore, func(*internalevents.PostgreSQLReader, *auth.InternalPrincipalVerifier, string) http.Handler) (string, string, string) {
	return func(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore, eventFactory func(*internalevents.PostgreSQLReader, *auth.InternalPrincipalVerifier, string) http.Handler) (string, string, string) {
		t.Helper()
		// testing.T.Context is canceled before registered cleanup runs. Keep
		// this bounded fixture owner alive until its later-registered SDK,
		// listeners and credentials have explicitly joined, then cancel last.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)
		edge.ctx = ctx
		ports, releasePorts := reserveEnvoyGatewayFixturePorts(t)
		// api.localhost is an explicit concrete API hostname. The SDK
		// uses normal DNS and full CA/hostname verification without URL rewriting.
		fixture := translateProductionEnvoyGatewayHosts(t, profile, ports, "api.localhost", "git.localhost")
		fixture.ReleasePorts = releasePorts
		edge.fixture = fixture
		if edge.lifecycle != nil {
			edge.lifecycle.bind(ctx, fixture, profile)
		}
		private := transporttest.Must(auth.GenerateEd25519PrivateKeyBase64())
		signer := transporttest.Must(auth.NewInternalPrincipalSignerFromBase64(private))
		verifier := transporttest.Must(auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64()))
		bootstrapKey := strings.Repeat("e", auth.MinBootstrapKeyBytes)
		authDB := pools.OpenWorkload(t, "auth", nil)
		issuerVerifier := auth.NewAssertionVerifier(ctx)
		t.Cleanup(issuerVerifier.Close)
		authHandler := transporttest.Must(authservice.BuildRouter(ctx, authservice.RouterBuildConfig{RawDatabase: authDB, AssertionVerifier: issuerVerifier, Config: authservice.Config{BootstrapAPIKey: bootstrapKey, BootstrapWorkspaceID: workspace.DefaultID, InternalPrincipalPrivateKeyB64: private, InternalPrincipalTTL: time.Minute}}))
		checkDiagnostics := workload.DefaultDiagnosticConfig()
		checkDiagnostics.Level = slog.LevelDebug
		checkLog := workload.NewProcessLogger(&edge.checkRecords, "auth", "test", "edge", checkDiagnostics)
		t.Cleanup(checkLog.CloseWithBudget)
		// The Check adapter samples API-key usage like the Auth application.
		usage := auth.NewAPIKeyUsageRecorder(authDB, checkLog.Logger)
		usage.Start(ctx)
		t.Cleanup(usage.Close)
		adapter := transporttest.Must(authservice.NewExternalAuthorization(authservice.ExternalAuthorizationConfig{Authenticator: &auth.RequestAuthenticator{Resolver: auth.NewAuthorityResolver(authDB, workspace.DefaultID, usage)}, Signer: signer, PrincipalTTL: time.Minute, Logger: checkLog.Logger}))
		edge.startCheck(ctx, t, fixture, profile, adapter)
		edge.startHTTP(ctx, t, fixture, profile, "auth", "127.0.0.2:8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" && r.URL.Path == "/v1/oauth/token" {
				edge.authExchanges.Add(1)
			}
			authHandler.ServeHTTP(w, r)
		}))
		apiDB := pools.OpenWorkload(t, "api", nil)
		data := t.TempDir()
		edge.dataDirectory = data
		if err := os.Chmod(data, 0700); err != nil {
			t.Fatal(err)
		}
		if objects == nil {
			stores, _ := replicaMinIOStoresWithConfig(t)
			objects = stores()
		}
		edge.objects = objects
		apiLog := workload.NewProcessLogger(&edge.apiRecords, "api", "test", "edge", workload.DefaultDiagnosticConfig())
		t.Cleanup(apiLog.CloseWithBudget)
		apiHandler := transporttest.Must(api.BuildRouter(ctx, api.RouterConfig{RuntimeClient: dbconnect.NewClientForTesting(apiDB), RawDatabase: apiDB, VaultKey: sdkIntegrationVaultKey, DataDir: data, Logger: apiLog.Logger, Env: sdkIntegrationEnv{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_edge_public"}, BlobStore: objects, PrincipalVerifier: verifier}))
		if owner, ok := apiHandler.(io.Closer); ok {
			t.Cleanup(func() {
				if err := owner.Close(); err != nil {
					t.Error("actual edge API owner failed to close")
				}
			})
		}
		guard := func(handler http.Handler, count *atomic.Int64) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" || r.Header.Get("X-Api-Key") != "" || len(r.Header.Values("X-Tetral-Internal-Principal")) != 1 {
					http.Error(w, "edge credential mutation failure", 500)
					return
				}
				count.Add(1)
				handler.ServeHTTP(w, r)
			})
		}
		edge.startHTTP(ctx, t, fixture, profile, "api", "127.0.0.3:8080", guard(apiHandler, &edge.apiRequests))
		reader := internalevents.NewPostgreSQLReader(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "event_stream", nil)), internalevents.WithPageTokenSecret([]byte(sdkIntegrationVaultKey)))
		edge.startHTTP(ctx, t, fixture, profile, "event-stream", "127.0.0.4:8080", guard(eventFactory(reader, verifier, signer.PublicKeyBase64()), &edge.eventRequests))
		gitDB := dbconnect.NewClientForTesting(pools.OpenWorkload(t, "git_proxy", nil))
		publicGit := transporttest.Must(url.Parse(fmt.Sprintf("https://git.localhost:%d", ports.HTTPS)))
		gitHandler := gitproxy.NewHTTPHandler(gitproxy.TicketValidator{Store: gitticket.NewPostgreSQLStore(gitDB), Now: func() time.Time {
			if current := edge.gitClock.Load(); current != nil {
				return *current
			}
			return time.Now()
		}}, gitproxy.NewRepositoryPolicyAuthorizer(gitproxy.NewPostgreSQLRepositoryTokenResolver(gitDB, transporttest.Must(vault.NewEncryptor(sdkIntegrationVaultKey)))), gitproxy.HandlerOptions{PublicBaseURL: publicGit, Transport: edgeGitTransport{calls: &edge.gitCalls, expectedAuthorization: &edge.gitAuthorization, gate: &edge.gitGate}})
		edge.startHTTP(ctx, t, fixture, profile, "git-proxy", "127.0.0.5:8080", gitHandler)
		startEnvoyGatewayProxy(ctx, t, fixture)
		edgeAwaitReady(ctx, t, ports.Ready)
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(fixture.Authority.PEM)
		edge.client = &http.Client{Transport: &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS13}, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, fmt.Sprintf("127.0.0.1:%d", ports.HTTPS))
		}}, Timeout: 60 * time.Second}
		t.Cleanup(edge.client.CloseIdleConnections)
		edge.baseURL = fmt.Sprintf("https://api.localhost:%d", ports.HTTPS)
		request := transporttest.Must(http.NewRequestWithContext(ctx, "POST", edge.baseURL+"/v1/api_keys", bytes.NewBufferString(`{"name":"actual Envoy SDK"}`)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Api-Key", bootstrapKey)
		response, err := edge.client.Do(request)
		if err != nil {
			t.Fatal("actual Auth key creation through translated edge failed")
		}
		defer func() { _ = response.Body.Close() }()
		var created auth.CreateAPIKeyResult
		if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&created) != nil || created.APIKey == "" || created.KeyKind != auth.KindStandard {
			t.Fatal("actual Auth key creation omitted a standard scoped key")
		}
		edge.key = created.APIKey
		caPath := filepath.Join(fixture.Directory, "public-ca.crt")
		if err := os.WriteFile(caPath, fixture.Authority.PEM, 0600); err != nil {
			t.Fatal(err)
		}
		return edge.baseURL, edge.key, caPath
	}
}

type edgeGitTransport struct {
	calls                 *atomic.Int64
	expectedAuthorization *atomic.Pointer[string]
	gate                  *atomic.Pointer[edgeGitGate]
}

func (transport edgeGitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "https" || request.URL.Host != "github.com" || request.Header.Get("X-Tetral-Git-Ticket") != "" || request.Header.Get("X-Api-Key") != "" {
		return nil, fmt.Errorf("invalid controlled external Git transport boundary")
	}
	expected := ""
	if value := transport.expectedAuthorization.Load(); value != nil {
		expected = *value
	}
	if request.Header.Get("Authorization") != expected {
		return nil, fmt.Errorf("Git external credential injection boundary differs")
	}
	transport.calls.Add(1)
	if gate := transport.gate.Load(); gate != nil {
		if gate.prefix && request.Body != nil {
			if _, err := io.CopyN(io.Discard, request.Body, 64*1024); err != nil {
				return nil, err
			}
		}
		select {
		case gate.entered <- struct{}{}:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		select {
		case <-gate.release:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}
	if request.Body != nil {
		if _, err := io.Copy(io.Discard, request.Body); err != nil {
			return nil, err
		}
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-git-upload-pack-result"}}, Body: io.NopCloser(strings.NewReader("0008NAK\n")), Request: request}, nil
}

func TestPostgreSQLEnvoyGatewayPublicParity(t *testing.T) {
	isolatedTLSPostgreSQLRoot(t, "envoy_gateway_business_assertion=", envoyGatewayCompositionBudget, func(t *testing.T) {
		for _, profile := range []string{"standard-routed", "hardened"} {
			t.Run(profile, func(t *testing.T) {
				edge := &translatedPublicEdge{}
				f := newPublicProjectionFixture(t, publicProjectionOptions{publicEdge: edge.factory(profile)})
				f.open(t, "preview", []string{"agent.message"}, "")
				f.open(t, "formal", nil, "")
				f.open(t, "thread", nil, f.thread(t))
				publicWait(t, "actual edge immediate SSE heartbeat", func() bool { return f.snapshot(t, "preview").Heartbeats > 0 })
				request := f.start(t, "", "")
				message := f.text(t, request, "before EOF through actual Envoy", false)
				f.publish(t, f.frame(request, "request_open", "", "", 0, ""), f.frame(request, "event_start", message, "agent.message", 0, ""), f.frame(request, "event_delta", message, "agent.message", 1, "before EOF "))
				preview := f.waitEvent(t, "preview", "event_delta", 1)
				if preview.Ended || publicPreviewText(preview, message) != "before EOF " {
					t.Fatal("translated edge aggregated SDK preview until stream EOF")
				}
				historicalEnd := f.end(t, request)
				var originalMessage, originalEnd map[string]any
				for _, viewer := range []string{"preview", "formal", "thread"} {
					ended := f.waitEvent(t, viewer, "span.model_request_end", 1)
					f.assertFormal(t, ended, []string{"before EOF through actual Envoy"})
					if viewer == "formal" {
						for _, event := range ended.Events {
							switch publicEventID(event) {
							case message:
								originalMessage = event
							case historicalEnd:
								originalEnd = event
							}
						}
					}
				}
				if originalMessage == nil || originalEnd == nil {
					t.Fatal("original SDK stream omitted the committed message or exact End")
				}
				if edge.eventRequests.Load() != 3 || edge.apiRequests.Load() == 0 {
					t.Fatal("actual SDK streams and CRUD did not reach their owning handlers")
				}
				// A fresh stream starts at the current high-water. Recover durable
				// history through the SDK list API without replaying it into SSE.
				closed := decodePublicSnapshot(t, f.client.control(t, "close_viewer", map[string]any{"viewer": "preview"}))
				if !closed.Ended || closed.Error != nil {
					t.Fatal("SDK stream cancellation failed to join")
				}
				f.open(t, "reconnected", []string{"agent.message"}, "")
				foundMessage, foundEnd, complete := false, false, false
				page := ""
				for range 128 {
					history := f.list(t, "", "asc", page)
					for _, event := range history.Data {
						switch publicEventID(event) {
						case message:
							foundMessage = reflect.DeepEqual(event, originalMessage)
						case historicalEnd:
							foundEnd = reflect.DeepEqual(event, originalEnd)
						}
					}
					if history.Next == nil {
						complete = true
						break
					}
					if *history.Next == "" || *history.Next == page {
						t.Fatal("SDK durable-history cursor did not advance")
					}
					page = *history.Next
				}
				if !complete || !foundMessage || !foundEnd {
					t.Fatal("real SDK list did not recover the exact original message and End content")
				}
				edge.assertChildVisibility(t, f, message, historicalEnd)
				edge.assertStreamingUpload(t, f)
				edge.assertCancelledUpload(t, f)
				edge.assertGitBusiness(t, f)
				edge.assertCachedKeycloak(t, f)
				t.Logf("envoy_gateway_business_assertion=actual_sdk_services profile=%s passed=true", profile)
			})
		}
	})
}

// The producer holds after a partial body. API staging must advance before it
// releases the suffix; a body-buffering proxy cannot satisfy that observation.
func (edge *translatedPublicEdge) assertStreamingUpload(t *testing.T, f *publicProjectionFixture) {
	t.Helper()
	body := strings.Repeat("actual multipart bytes through production Envoy\n", 4096)
	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	ready, release, producerJoined := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	requestJoined := make(chan struct{})
	t.Cleanup(func() {
		unblock()
		_ = reader.Close()
		_ = writer.Close()
		select {
		case <-producerJoined:
		case <-time.After(5 * time.Second):
			t.Error("upload producer cleanup did not join")
		}
		select {
		case <-requestJoined:
		case <-time.After(5 * time.Second):
			t.Error("upload request cleanup did not join")
		}
	})
	go func() {
		part, err := multipartWriter.CreateFormFile("file", "edge.txt")
		if err == nil {
			_, err = io.WriteString(part, body[:64*1024])
		}
		close(ready)
		if err == nil {
			<-release
			_, err = io.WriteString(part, body[64*1024:])
		}
		if err == nil {
			err = multipartWriter.Close()
		}
		_ = writer.CloseWithError(err)
		producerJoined <- err
		close(producerJoined)
	}()
	request := transporttest.Must(http.NewRequestWithContext(edge.ctx, "POST", edge.baseURL+"/v1/files?beta=true", reader))
	request.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	request.Header.Set("X-Api-Key", edge.key)
	responses := make(chan *http.Response, 1)
	failures := make(chan error, 1)
	go func() {
		response, err := edge.client.Do(request)
		responses <- response
		failures <- err
		close(requestJoined)
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("streaming upload producer did not start")
	}
	publicWait(t, "actual API staging advances before upload suffix", func() bool {
		entries, _ := os.ReadDir(filepath.Join(edge.dataDirectory, "files-upload-stage"))
		for _, entry := range entries {
			info, _ := entry.Info()
			if info != nil && info.Size() > 0 {
				return true
			}
		}
		return false
	})
	// The API handler is already reading this body. Holding the suffix past
	// the API's slow-request threshold makes its default-verbosity boundary
	// record exist for the logging correlation assertion below.
	select {
	case <-time.After(httpapi.DefaultSlowRequestThreshold + 250*time.Millisecond):
	case <-edge.ctx.Done():
		t.Fatal("edge fixture ended while holding the upload suffix")
	}
	unblock()
	if err := <-producerJoined; err != nil {
		t.Fatal("upload producer did not join")
	}
	response := <-responses
	if err := <-failures; err != nil {
		t.Fatal("actual streamed upload failed")
	}
	defer func() { _ = response.Body.Close() }()
	var result struct {
		ID        string `json:"id"`
		SHA256    string `json:"sha256"`
		SizeBytes int64  `json:"size_bytes"`
	}
	digest := sha256.Sum256([]byte(body))
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&result) != nil || result.ID == "" {
		t.Fatal("actual upload omitted committed file")
	}
	var storedHash, objectKey, mimeType, filename string
	var storedBytes int64
	if err := f.db.QueryRow(`SELECT o.sha256,o.size_bytes,o.blob_key,f.mime_type,f.filename FROM files f JOIN file_objects o ON o.workspace_id=f.workspace_id AND o.object_id=f.object_id WHERE f.workspace_id=$1 AND f.file_id=$2`, string(workspace.DefaultID), result.ID).Scan(&storedHash, &storedBytes, &objectKey, &mimeType, &filename); err != nil || storedHash != hex.EncodeToString(digest[:]) || storedBytes != int64(len(body)) || mimeType != "application/octet-stream" || filename != "edge.txt" {
		t.Fatal("actual upload body hash/length changed")
	}
	object, err := edge.objects.Get(edge.ctx, objectKey)
	if err != nil {
		t.Fatal("actual committed upload object unavailable")
	}
	persisted, err := io.ReadAll(io.LimitReader(object, int64(len(body))+1))
	_ = object.Close()
	if err != nil || !bytes.Equal(persisted, []byte(body)) {
		t.Fatal("multipart boundary or body changed in actual object store")
	}
	edge.assertEdgeRequestCorrelation(t, response.Header.Get("request-id"))
	// Generate the real decimal limit plus one without allocating it. API owns
	// 413 and temporary-file cleanup, not a proxy request-buffer filter.
	edge.assertOverLimitUpload(t, files.MaxFileBytes+1)
}

// One edge request joins across services by structured fields: the API record
// keeps the API request ID returned to the client as request.id and carries the
// edge-generated ID as edge.request.id, which is the request.id of exactly one
// Auth Check admission record.
func (edge *translatedPublicEdge) assertEdgeRequestCorrelation(t *testing.T, apiRequestID string) {
	t.Helper()
	if !strings.HasPrefix(apiRequestID, "req_") {
		t.Fatal("API response omitted its own request ID")
	}
	var apiRecord map[string]any
	publicWait(t, "API boundary record for the held edge upload", func() bool {
		for _, record := range edgeDiagnosticRecords(t, edge.apiRecords.Bytes()) {
			if record["event"] == "http.request" && record["request.id"] == apiRequestID {
				apiRecord = record
				return true
			}
		}
		return false
	})
	edgeRequestID, _ := apiRecord["edge.request.id"].(string)
	if apiRecord["event.kind"] != "slow_request" || apiRecord["url.path"] != "/v1/files" || edgeRequestID == "" || edgeRequestID == apiRequestID {
		t.Fatalf("API boundary record lacks a distinct verified edge request ID: %v", apiRecord)
	}
	admissions := 0
	for _, record := range edgeDiagnosticRecords(t, edge.checkRecords.Bytes()) {
		if record["request.id"] == edgeRequestID {
			if record["event"] != "auth.admission.success" {
				t.Fatalf("edge request ID joined an unexpected Check record: %v", record)
			}
			admissions++
		}
	}
	if admissions != 1 {
		t.Fatalf("Check admission records for the edge request = %d; want 1", admissions)
	}
}

func edgeDiagnosticRecords(t *testing.T, output []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(output), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("diagnostic record is not structured JSON: %v", err)
		}
		records = append(records, record)
	}
	return records
}

func (edge *translatedPublicEdge) assertOverLimitUpload(t *testing.T, size int64) {
	t.Helper()
	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	joined := make(chan error, 1)
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("over-limit producer cleanup did not join")
		}
	})
	go func() {
		part, err := multipartWriter.CreateFormFile("file", "over-limit.bin")
		if err == nil {
			_, err = io.CopyN(part, edgeZeroReader{}, size)
		}
		if err == nil {
			err = multipartWriter.Close()
		}
		_ = writer.CloseWithError(err)
		joined <- err
		close(joined)
	}()
	request := transporttest.Must(http.NewRequestWithContext(edge.ctx, "POST", edge.baseURL+"/v1/files?beta=true", reader))
	request.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	request.Header.Set("X-Api-Key", edge.key)
	response, err := edge.client.Do(request)
	_ = reader.Close()
	if err != nil {
		t.Fatal("generated real-limit upload failed before application status")
	}
	_ = response.Body.Close()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("over-limit producer did not join")
	}
	if response.StatusCode != 413 {
		t.Fatalf("production file limit status=%d", response.StatusCode)
	}
	entries, err := os.ReadDir(filepath.Join(edge.dataDirectory, "files-upload-stage"))
	if err != nil || len(entries) != 0 {
		t.Fatal("rejected upload left API staging files")
	}
}

type edgeZeroReader struct{}

func (edgeZeroReader) Read(body []byte) (int, error) { clear(body); return len(body), nil }

func (edge *translatedPublicEdge) assertGitBusiness(t *testing.T, f *publicProjectionFixture) {
	t.Helper()
	token := transporttest.Must(gitticket.GenerateToken(rand.Reader))
	hash := transporttest.Must(gitticket.HashToken(token))
	ticketID := id.New("gitt_")
	store := gitticket.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.pools.OpenWorkload(t, "sandbox", nil)))
	if _, err := store.CreatePending(edge.ctx, workspace.DefaultID, f.session, ticketID, hash, time.Now()); err != nil {
		t.Fatal("actual Git ticket preparation failed")
	}
	if _, err := store.ActivatePending(edge.ctx, workspace.DefaultID, f.session, ticketID, time.Now()); err != nil {
		t.Fatal("actual Git ticket activation failed")
	}
	for _, test := range []struct{ method, path string }{{"GET", "/github.com/tetral-ai/public/info/refs?service=git-upload-pack"}, {"GET", "/github.com/tetral-ai/public/info/refs?service=git-receive-pack"}, {"POST", "/github.com/tetral-ai/public/git-upload-pack"}, {"POST", "/github.com/tetral-ai/public/git-receive-pack"}} {
		var input io.Reader
		if test.method == "POST" {
			input = strings.NewReader("0008NAK\n")
		}
		request := transporttest.Must(http.NewRequestWithContext(edge.ctx, test.method, fmt.Sprintf("https://git.localhost:%d%s", edge.fixture.Ports.HTTPS, test.path), input))
		request.Header.Set(gitticket.HeaderName, token)
		request.Header.Set("Authorization", "Bearer forbidden-public-credential")
		request.Header.Set("X-Api-Key", "forbidden-public-credential")
		before := edge.gitCalls.Load()
		response, err := edge.client.Do(request)
		if err != nil {
			t.Fatal("actual Git smart HTTP request failed")
		}
		body := transporttest.Must(io.ReadAll(response.Body))
		_ = response.Body.Close()
		if response.StatusCode != 200 || edge.gitCalls.Load() != before+1 || string(body) != "0008NAK\n" {
			t.Fatal("Git smart HTTP shape did not relay exactly once through actual proxy")
		}
	}
	// The mounted repository uses the actual encrypted vault column and real
	// Git resolver. Its injected external credential is distinct from the public
	// caller's forged Authorization, which the edge must remove.
	credential := "fixture-mounted-git-credential"
	encrypted := transporttest.Must(transporttest.Must(vault.NewEncryptor(sdkIntegrationVaultKey)).Encrypt([]byte(credential)))
	resourceID := id.New("rsrc_")
	transaction := transporttest.Must(f.db.BeginTx(edge.ctx, nil))
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.ExecContext(edge.ctx, `INSERT INTO session_resources(workspace_id,session_id,resource_id,type,created_at,updated_at) VALUES($1,$2,$3,'github_repository',$4,$4)`, string(workspace.DefaultID), f.session, resourceID, time.Now()); err != nil {
		t.Fatal("actual repository resource setup failed")
	}
	if _, err := transaction.ExecContext(edge.ctx, `INSERT INTO session_github_repository_resources(workspace_id,session_id,resource_id,url,mount_path,authorization_token_encrypted) VALUES($1,$2,$3,$4,$5,$6)`, string(workspace.DefaultID), f.session, resourceID, "https://github.com/tetral-ai/public.git", "/workspace/public", encrypted); err != nil {
		t.Fatal("actual repository credential setup failed")
	}
	if transaction.Commit() != nil {
		t.Fatal("actual repository resource commit failed")
	}
	expected := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+credential))
	edge.gitAuthorization.Store(&expected)
	assertTicket := func(candidate string, want int) {
		request := transporttest.Must(http.NewRequestWithContext(edge.ctx, "GET", fmt.Sprintf("https://git.localhost:%d/github.com/tetral-ai/public/info/refs?service=git-upload-pack", edge.fixture.Ports.HTTPS), nil))
		request.Header.Set(gitticket.HeaderName, candidate)
		request.Header.Set("Authorization", "Bearer forged-public-credential")
		before := edge.gitCalls.Load()
		response, err := edge.client.Do(request)
		if err != nil {
			t.Fatal("actual rotated Git request failed")
		}
		_ = response.Body.Close()
		if response.StatusCode != want || (want == 200 && edge.gitCalls.Load() != before+1) || (want != 200 && edge.gitCalls.Load() != before) {
			t.Fatal("Git ticket grace/credential relay distinction failed")
		}
	}
	assertTicket(token, 200)
	rotate := func(now time.Time) string {
		next := transporttest.Must(gitticket.GenerateToken(rand.Reader))
		hash := transporttest.Must(gitticket.HashToken(next))
		ticket := id.New("gitt_")
		if _, err := store.CreatePending(edge.ctx, workspace.DefaultID, f.session, ticket, hash, now); err != nil {
			t.Fatal("actual replacement Git ticket preparation failed")
		}
		if _, err := store.ActivatePending(edge.ctx, workspace.DefaultID, f.session, ticket, now); err != nil {
			t.Fatal("actual replacement Git ticket activation failed")
		}
		return next
	}
	second := rotate(time.Now())
	assertTicket(token, 200)
	assertTicket(second, 200)
	// Advance only the validator's test clock beyond the unchanged production
	// grace. The actual activation chronology and durable rows stay valid.
	afterGrace := time.Now().Add(31 * time.Minute)
	edge.gitClock.Store(&afterGrace)
	assertTicket(token, 401)
	assertTicket(second, 200)
	edge.assertGitFlowControl(t, second)
	request := transporttest.Must(http.NewRequestWithContext(edge.ctx, "GET", fmt.Sprintf("https://git.localhost:%d/github.com/tetral-ai/public/info/refs?service=git-upload-pack", edge.fixture.Ports.HTTPS), nil))
	request.Header.Set(gitticket.HeaderName, "invalid")
	before := edge.gitCalls.Load()
	response, err := edge.client.Do(request)
	if err != nil {
		t.Fatal("invalid Git ticket request failed")
	}
	_ = response.Body.Close()
	if response.StatusCode != 401 || edge.gitCalls.Load() != before {
		t.Fatal("pre-relay Git ticket rejection reached external upstream")
	}
}

func (edge *translatedPublicEdge) assertChildVisibility(t *testing.T, f *publicProjectionFixture, historicalMessage, historicalEnd string) {
	t.Helper()
	setup := f.start(t, "", "")
	child := f.child(t, setup)
	f.end(t, setup)
	f.open(t, "child", nil, child)
	request := f.start(t, child, "")
	message := f.text(t, request, "child formal text through Envoy", false)
	// Deliberate ineligible preview input crosses the real broker; formal
	// child admission remains a positive control through the SDK Thread URL.
	f.publish(t, f.frame(request, "request_open", "", "", 0, ""), f.frame(request, "event_start", message, "agent.message", 0, ""), f.frame(request, "event_delta", message, "agent.message", 1, "forbidden child preview"))
	f.end(t, request)
	childResult := f.waitEvent(t, "child", "span.model_request_end", 1)
	f.assertFormal(t, childResult, []string{"child formal text through Envoy"})
	assertPublicPreviewShapes(t, childResult, nil, nil)
	// A subsequent legal primary End fences the ordinary Session viewer.
	fence := f.start(t, "", "")
	fenceEnd := f.end(t, fence)
	session := f.waitEvent(t, "reconnected", "span.model_request_end", 2)
	fenceSeen := false
	for _, event := range session.Events {
		identity := publicEventID(event)
		if identity == fenceEnd && publicEventType(event) == "span.model_request_end" {
			fenceSeen = true
		}
		if identity == message || identity == historicalMessage || identity == historicalEnd || publicEventType(event) == "event_start" || publicEventType(event) == "event_delta" {
			t.Fatal("Session SDK replayed old formal/preview data or leaked child generated text")
		}
	}
	if !fenceSeen {
		t.Fatal("reconnected SDK stream omitted the exact new primary fence End")
	}
	ids := publicProjectionAllPages(t, f, "", "asc", false)
	if publicProjectionContains(ids, message) {
		t.Fatal("Session list leaked child generated text")
	}
	if edge.eventRequests.Load() != 5 {
		t.Fatal("reconnect or actual child Thread stream failed to reach owning service")
	}
}

func (edge *translatedPublicEdge) assertCachedKeycloak(t *testing.T, f *publicProjectionFixture) {
	t.Helper()
	fixture, err := testinfra.LoadKeycloakFixture()
	if err != nil {
		t.Fatal(err)
	}
	realm, err := fixture.ProvisionRealm(edge.ctx)
	if err != nil {
		t.Fatal("real Keycloak realm provisioning failed")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if realm.Close(cleanup) != nil {
			t.Error("real Keycloak realm cleanup failed")
		}
	})
	ca := transporttest.Must(os.ReadFile(realm.CAPath))
	issuer := transporttest.Must(url.Parse(realm.Issuer))
	rule := auth.FederationRule{ID: "fdrl_envoy_sdk", OrganizationID: "org_envoy_sdk", Issuer: realm.Issuer, Audience: realm.Audience, JWKSURL: realm.JWKSURL, AllowedOrigins: []string{issuer.Scheme + "://" + issuer.Host}, AllowedCIDRs: realm.AllowedCIDRs, TrustedCAPEM: string(ca), Algorithm: "RS256", Enabled: true}
	for _, actor := range []struct {
		name, subject, kind, selector string
		assertion                     func(context.Context) (string, error)
	}{
		{"human", realm.HumanSubject, auth.IdentityHuman, "", realm.HumanAssertion},
		{"service", realm.ServiceSubject, auth.IdentityService, realm.ServiceAccountID, realm.ServiceAssertion},
	} {
		identity := "identity_envoy_" + actor.name
		document := auth.PolicyDocument{FederationRules: []auth.FederationRule{rule}, Identities: []auth.IdentityBinding{{ID: identity, OrganizationID: rule.OrganizationID, Issuer: rule.Issuer, Subject: actor.subject, Kind: actor.kind, ServiceAccountID: actor.selector, Enabled: true}}, WorkspaceGrants: []auth.WorkspaceGrant{{ID: "grant_envoy_" + actor.name, IdentityID: identity, WorkspaceID: workspace.DefaultID, Role: auth.WorkspaceFullAccess, Enabled: true}}}
		if _, err := auth.NewPolicyStore(f.db).Apply(edge.ctx, document); err != nil {
			t.Fatal("actual issuer policy import failed")
		}
		assertion := transporttest.Must(actor.assertion(edge.ctx))
		path := filepath.Join(t.TempDir(), "identity.jwt")
		if os.WriteFile(path, []byte(assertion), 0600) != nil {
			t.Fatal("write private issuer assertion")
		}
		before := edge.authExchanges.Load()
		child := startOIDCSDKChild(edge.ctx, t, map[string]string{"identityID": identity, "baseURL": edge.baseURL, "caPath": filepath.Join(edge.fixture.Directory, "public-ca.crt"), "assertionPath": path, "ruleID": rule.ID, "organizationID": rule.OrganizationID, "workspaceID": "default", "serviceAccountID": actor.selector, "fixtureName": "oidc-sdk-" + actor.name})
		store := oidcDecodeSDKResult(t, child.control(t, "store", nil))
		created := oidcDecodeSDKResult(t, child.control(t, "create", map[string]any{"storeID": store.StoreID, "path": "/envoy.md", "content": "actual cached bearer"}))
		oidcAssertMemoryActor(edge.ctx, t, f.db, created.VersionID, actor.kind, identity, false)
		oidcAssertWireActor(t, created.Version.CreatedBy, actor.kind, identity)
		if edge.authExchanges.Load() != before+1 {
			t.Fatal("real SDK issuer login did not perform exactly one actual exchange")
		}
		// Removing the source assertion makes another exchange impossible. The
		// same persistent SDK process must admit cached bearer reads instead.
		if os.Remove(path) != nil {
			t.Fatal("remove private assertion after exchange")
		}
		cached := edge.authExchanges.Load()
		child.control(t, "read", map[string]any{"storeID": store.StoreID, "memoryID": created.MemoryID})
		if edge.authExchanges.Load() != cached {
			t.Fatal("cached SDK bearer attempted another exchange")
		}
	}
}

func (edge *translatedPublicEdge) assertCancelledUpload(t *testing.T, f *publicProjectionFixture) {
	t.Helper()
	var before int
	if f.db.QueryRow(`SELECT count(*) FROM files`).Scan(&before) != nil {
		t.Fatal("upload cancellation baseline unavailable")
	}
	ctx, cancel := context.WithTimeout(edge.ctx, 10*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	producer := make(chan struct{})
	result := make(chan error, 1)
	requestDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		_ = reader.Close()
		_ = writer.Close()
		select {
		case <-producer:
		case <-time.After(5 * time.Second):
			t.Error("cancelled upload producer did not join")
		}
		select {
		case <-requestDone:
		case <-time.After(5 * time.Second):
			t.Error("cancelled upload request cleanup did not join")
		}
	})
	go func() {
		defer close(producer)
		part, err := multipartWriter.CreateFormFile("file", "cancelled.bin")
		if err == nil {
			_, err = io.CopyN(part, edgeZeroReader{}, 64*1024)
		}
		if err == nil {
			<-ctx.Done()
			err = ctx.Err()
		}
		_ = writer.CloseWithError(err)
	}()
	request := transporttest.Must(http.NewRequestWithContext(ctx, "POST", edge.baseURL+"/v1/files?beta=true", reader))
	request.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	request.Header.Set("X-Api-Key", edge.key)
	go func() {
		response, err := edge.client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		result <- err
		close(requestDone)
	}()
	publicWait(t, "cancelled upload entered real API staging", func() bool {
		entries, _ := os.ReadDir(filepath.Join(edge.dataDirectory, "files-upload-stage"))
		for _, entry := range entries {
			info, _ := entry.Info()
			if info != nil && info.Size() > 0 {
				return true
			}
		}
		return false
	})
	cancel()
	_ = reader.Close()
	_ = writer.Close()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("shorter caller cancellation did not own upload termination")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled upload request did not join")
	}
	select {
	case <-producer:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled upload producer did not join")
	}
	publicWait(t, "cancelled upload releases all API staging", func() bool {
		entries, err := os.ReadDir(filepath.Join(edge.dataDirectory, "files-upload-stage"))
		return err == nil && len(entries) == 0
	})
	var after int
	if f.db.QueryRow(`SELECT count(*) FROM files`).Scan(&after) != nil || after != before {
		t.Fatal("cancelled multipart upload committed a file identity")
	}
}

type edgeGitGate struct {
	entered chan struct{}
	release chan struct{}
	prefix  bool
}

func (edge *translatedPublicEdge) assertGitFlowControl(t *testing.T, ticket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(edge.ctx, 20*time.Second)
	defer cancel()
	gate := &edgeGitGate{entered: make(chan struct{}, gitproxy.MaxConnsPerTicket), release: make(chan struct{})}
	edge.gitGate.Store(gate)
	var release sync.Once
	unblock := func() { release.Do(func() { close(gate.release) }) }
	var workers sync.WaitGroup
	joined := make(chan struct{})
	t.Cleanup(func() {
		unblock()
		cancel()
		edge.gitGate.Store(nil)
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("Git held requests did not join")
		}
	})
	results := make(chan int, gitproxy.MaxConnsPerTicket)
	for range gitproxy.MaxConnsPerTicket {
		workers.Add(1)
		go func() {
			defer workers.Done()
			request, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://git.localhost:%d/github.com/tetral-ai/public/info/refs?service=git-upload-pack", edge.fixture.Ports.HTTPS), nil)
			request.Header.Set(gitticket.HeaderName, ticket)
			response, err := edge.client.Do(request)
			status := 0
			if err == nil {
				status = response.StatusCode
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
			results <- status
		}()
	}
	go func() { workers.Wait(); close(joined) }()
	for range gitproxy.MaxConnsPerTicket {
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal("production Git in-flight limit did not reach controlled upstream")
		}
	}
	before := edge.gitCalls.Load()
	request := transporttest.Must(http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://git.localhost:%d/github.com/tetral-ai/public/info/refs?service=git-upload-pack", edge.fixture.Ports.HTTPS), nil))
	request.Header.Set(gitticket.HeaderName, ticket)
	response, err := edge.client.Do(request)
	if err != nil {
		t.Fatal("Git saturation control failed")
	}
	_ = response.Body.Close()
	if response.StatusCode != 429 || edge.gitCalls.Load() != before {
		t.Fatal("production Git per-ticket limit admitted another upstream call")
	}
	unblock()
	for range gitproxy.MaxConnsPerTicket {
		select {
		case status := <-results:
			if status != 200 {
				t.Fatal("admitted Git request lost relay after release")
			}
		case <-ctx.Done():
			t.Fatal("admitted Git requests did not finish")
		}
	}
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("Git relay workers did not join")
	}
	edge.gitGate.Store(nil)
	edge.assertGitStreamingBody(t, ticket)
}

func (edge *translatedPublicEdge) assertGitStreamingBody(t *testing.T, ticket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(edge.ctx, 20*time.Second)
	defer cancel()
	gate := &edgeGitGate{entered: make(chan struct{}, 1), release: make(chan struct{}), prefix: true}
	edge.gitGate.Store(gate)
	var once sync.Once
	unblock := func() { once.Do(func() { close(gate.release) }) }
	reader, writer := io.Pipe()
	producer := make(chan error, 1)
	requestJoined := make(chan struct{})
	status := make(chan int, 1)
	t.Cleanup(func() {
		unblock()
		cancel()
		_ = reader.Close()
		_ = writer.Close()
		edge.gitGate.Store(nil)
		select {
		case <-producer:
		case <-time.After(5 * time.Second):
			t.Error("Git streaming producer did not join")
		}
		select {
		case <-requestJoined:
		case <-time.After(5 * time.Second):
			t.Error("Git streaming request did not join")
		}
	})
	go func() {
		_, err := io.CopyN(writer, edgeZeroReader{}, 64*1024*1024)
		_ = writer.CloseWithError(err)
		producer <- err
		close(producer)
	}()
	request := transporttest.Must(http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("https://git.localhost:%d/github.com/tetral-ai/public/git-upload-pack", edge.fixture.Ports.HTTPS), reader))
	request.Header.Set(gitticket.HeaderName, ticket)
	request.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	go func() {
		response, err := edge.client.Do(request)
		code := 0
		if err == nil {
			code = response.StatusCode
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		status <- code
		close(requestJoined)
	}()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("Git prefix never crossed real proxy before request EOF")
	}
	select {
	case <-producer:
		t.Fatal("Git streamed body was fully consumed while upstream prefix was held")
	default:
	}
	unblock()
	select {
	case err := <-producer:
		if err != nil {
			t.Fatal("Git generated body producer failed")
		}
	case <-ctx.Done():
		t.Fatal("Git producer did not finish after release")
	}
	select {
	case code := <-status:
		if code != 200 {
			t.Fatal("actual Git streaming relay failed")
		}
	case <-ctx.Done():
		t.Fatal("Git streaming request did not finish")
	}
	select {
	case <-requestJoined:
	case <-ctx.Done():
		t.Fatal("Git streaming client did not join")
	}
	edge.gitGate.Store(nil)
}
