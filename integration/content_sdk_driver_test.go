package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	internaleventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralapi "github.com/tetral-ai/tetral/services/api"
	tetralauth "github.com/tetral-ai/tetral/services/auth"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

// Each real service factory authenticates as its installer-configured role.
// The edge is the existing local SDK topology: Auth signs the principal, then
// API/Event Stream validate it. Object storage is the actual isolated MinIO.
func startContentSDKPublicEdge(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore) (string, string) {
	return startContentSDKPublicEdgeWithEvents(t, pools, objects, func(reader eventstream.Reader, verifier *auth.InternalPrincipalVerifier, _ string) http.Handler {
		return eventstream.NewRouter(reader, verifier, eventstream.WithStreamPollInterval(time.Millisecond), eventstream.WithStreamMaxEmptyPolls(1))
	})
}

func startContentSDKPublicEdgeWithEvents(t *testing.T, pools *storagetest.WorkloadDB, objects blob.BlobStore, eventsFactory func(eventstream.Reader, *auth.InternalPrincipalVerifier, string) http.Handler) (string, string) {
	t.Helper()
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatal(err)
	}
	bootstrapKey := strings.Repeat("b", auth.MinBootstrapKeyBytes)
	ctx := context.Background()
	authPool := pools.OpenWorkload(t, "auth", nil)
	authRouter, err := tetralauth.BuildRouter(ctx, tetralauth.RouterBuildConfig{RawDatabase: authPool, Config: tetralauth.Config{
		BootstrapAPIKey: bootstrapKey, BootstrapWorkspaceID: workspace.DefaultID, InternalPrincipalPrivateKeyB64: privateKey, InternalPrincipalTTL: time.Minute,
	}})
	if err != nil {
		t.Fatal(err)
	}
	authServer := httptest.NewServer(authRouter)
	t.Cleanup(authServer.Close)
	apiPool := pools.OpenWorkload(t, "api", nil)
	dataDirectory := t.TempDir()
	if err := os.Chmod(dataDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	apiRouter, err := tetralapi.BuildRouter(ctx, tetralapi.RouterConfig{RuntimeClient: dbconnect.NewClientForTesting(apiPool), RawDatabase: apiPool, VaultKey: sdkIntegrationVaultKey, DataDir: dataDirectory, Env: sdkIntegrationEnv{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_content_sdk"}, BlobStore: objects, PrincipalVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	apiServer := httptest.NewServer(apiRouter)
	t.Cleanup(apiServer.Close)
	reader := internaleventstream.NewPostgreSQLReader(dbconnect.NewClientForTesting(pools.OpenWorkload(t, "event_stream", nil)), internaleventstream.WithPageTokenSecret([]byte(sdkIntegrationVaultKey)))
	events := httptest.NewServer(eventsFactory(reader, verifier, signer.PublicKeyBase64()))
	t.Cleanup(events.Close)
	edge, err := newSDKIntegrationEdge(authServer.URL, apiServer.URL, events.URL, startSDKAuthorization(t, authPool, signer, time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(edge)
	t.Cleanup(server.Close)
	return server.URL, mintSDKIntegrationAPIKey(t, server.URL, bootstrapKey)
}

type contentSDKChild struct {
	controlMu sync.Mutex
	command   *exec.Cmd
	input     io.WriteCloser
	lines     chan []byte
	joined    chan error
	output    lockedBuffer
	next      int
	stopped   bool
}

func startContentSDKChildContext(ctx context.Context, t *testing.T, baseURL, key string, caPaths ...string) *contentSDKChild {
	t.Helper()
	if os.Getenv("TETRAL_ENGINE_SDK_ROOT") == "" {
		t.Fatal("content E2E requires the declared pinned SDK checkout")
	}
	directory := t.TempDir()
	config := map[string]string{"baseURL": baseURL, "apiKey": key}
	if len(caPaths) != 0 && caPaths[0] != "" {
		config["caPath"] = caPaths[0]
	}
	bootstrap, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "sdk-bootstrap.json")
	if err := os.WriteFile(path, bootstrap, 0600); err != nil {
		t.Fatal(err)
	}
	child := &contentSDKChild{lines: make(chan []byte, 8), joined: make(chan error, 1)}
	child.command = exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/content-lifecycle-sdk.ts", path) //nolint:gosec // Fixed fixture; private credential file.
	child.command.Dir = "../services/agent-runtime"
	child.command.Stdout = nil
	child.command.Stderr = &child.output
	child.input, err = child.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := child.command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		for scanner.Scan() {
			child.lines <- append([]byte(nil), scanner.Bytes()...)
		}
		close(child.lines)
	}()
	go func() { child.joined <- child.command.Wait() }()
	t.Cleanup(func() { child.stop(t) })
	return child
}

func (c *contentSDKChild) control(t *testing.T, operation string, arguments map[string]any) json.RawMessage {
	t.Helper()
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.next++
	id := fmt.Sprintf("sdk-%d", c.next)
	request := map[string]any{"id": id, "operation": operation}
	for key, value := range arguments {
		request[key] = value
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.input.Write(append(encoded, '\n')); err != nil {
		t.Fatal(err)
	}
	select {
	case line, ok := <-c.lines:
		if !ok {
			t.Fatal("actual SDK child exited before operation result")
		}
		var response struct {
			ID     string          `json:"id"`
			OK     bool            `json:"ok"`
			Result json.RawMessage `json:"result"`
		}
		if json.Unmarshal(line, &response) != nil || response.ID != id || !response.OK {
			t.Fatalf("actual SDK operation %s failed with bounded fixture response", operation)
		}
		return response.Result
	case <-time.After(35 * time.Second):
		t.Fatalf("actual SDK operation %s exceeded deadline", operation)
	}
	return nil
}

func (c *contentSDKChild) stop(t *testing.T) {
	t.Helper()
	if c.stopped {
		return
	}
	c.stopped = true
	_ = c.input.Close()
	select {
	case err := <-c.joined:
		if err != nil {
			t.Error("actual SDK child failed to close cleanly")
		}
	case <-time.After(10 * time.Second):
		_ = c.command.Process.Kill()
		<-c.joined
		t.Error("actual SDK child failed to join within cleanup deadline")
	}
}
