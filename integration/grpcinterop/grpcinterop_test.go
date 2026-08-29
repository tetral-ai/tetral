package grpcinterop

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGoClientCallsBunRuntimePodAcceptInput(t *testing.T) {
	if os.Getenv("TETRAL_RUN_GO_BUN_GRPC_INTEROP") != "1" {
		t.Skip("set TETRAL_RUN_GO_BUN_GRPC_INTEROP=1 to run Go-to-Bun gRPC interop")
	}
	if _, err := exec.LookPath("bun"); err != nil {
		t.Skip("bun is not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	address := startBunRuntimePodHarness(ctx, t)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close grpc conn: %v", err)
		}
	})
	client := agentruntimev1.NewAgentRuntimePodServiceClient(conn)

	okResponse, err := client.AcceptInput(authContext(ctx), validAcceptInputRequest("runtime_input_go_ok"))
	if err != nil {
		t.Fatalf("AcceptInput OK: %v", err)
	}
	if okResponse.GetAccepted() == nil {
		t.Fatalf("AcceptInput response = %+v; want typed accepted outcome", okResponse)
	}

	_, err = client.AcceptInput(context.Background(), validAcceptInputRequest("runtime_input_go_no_auth"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("AcceptInput without auth error = %v; want Unauthenticated", err)
	}

	_, err = client.AcceptInput(authContext(ctx), &agentruntimev1.AcceptInputRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AcceptInput invalid request error = %v; want InvalidArgument", err)
	}
}

func TestRuntimePodCarriesMaximumContextProviderRequestsToGateway(t *testing.T) {
	if os.Getenv("TETRAL_RUN_GO_BUN_GRPC_INTEROP") != "1" {
		t.Skip("set TETRAL_RUN_GO_BUN_GRPC_INTEROP=1 to run Runtime-to-Gateway gRPC interop")
	}
	if _, err := exec.LookPath("bun"); err != nil {
		t.Skip("bun is not installed")
	}

	// This proves large-payload transport identity under Race; it is not a
	// latency benchmark and may share a small CI host with other Go packages.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	runtimeRoot := filepath.Join("..", "..", "services", "agent-runtime")
	command := exec.CommandContext(ctx, "bun", "run", "packages/runtime-pod/test/harness/gateway-transport-harness.ts")
	command.Dir = runtimeRoot
	var stderr syncBuffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("run Runtime-to-Gateway transport harness: %v stderr=%s", err, stderr.String())
	}
	var results []struct {
		Vector               string  `json:"vector"`
		EncodedRequestBytes  int     `json:"encodedRequestBytes"`
		ReceivedRequestBytes int     `json:"receivedRequestBytes"`
		ConfiguredFuseBytes  int     `json:"configuredFuseBytes"`
		HeadroomRatio        float64 `json:"headroomRatio"`
		LoadedContextBytes   int     `json:"loadedContextBytes"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output), &results); err != nil {
		t.Fatalf("decode transport harness output %q: %v stderr=%s", output, err, stderr.String())
	}
	if len(results) != 5 {
		t.Fatalf("transport vectors = %d; want 5", len(results))
	}
	for _, result := range results {
		if result.EncodedRequestBytes <= 4*1024*1024 || result.ReceivedRequestBytes != result.EncodedRequestBytes {
			t.Fatalf("%s provider request bytes sent/received = %d/%d; want identical payload above 4 MiB", result.Vector, result.EncodedRequestBytes, result.ReceivedRequestBytes)
		}
		if result.ConfiguredFuseBytes != 64*1024*1024 || result.HeadroomRatio < 0.20 {
			t.Fatalf("%s carrier/headroom = %d/%f; want 64 MiB and at least 20%%", result.Vector, result.ConfiguredFuseBytes, result.HeadroomRatio)
		}
		if result.Vector == "escape_dense_output_history" && result.LoadedContextBytes <= 20*1024*1024 {
			t.Fatalf("escape-dense loaded context bytes = %d; want above 20 MiB", result.LoadedContextBytes)
		}
	}
}

func startBunRuntimePodHarness(ctx context.Context, t *testing.T) string {
	t.Helper()
	runtimeRoot := filepath.Join("..", "..", "services", "agent-runtime")
	command := exec.CommandContext(ctx, "bun", "run", "packages/runtime-pod/test/harness/grpc-harness.ts")
	command.Dir = runtimeRoot
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start Bun harness: %v", err)
	}
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Signal(os.Interrupt)
			_ = command.Process.Kill()
		}
		_ = command.Wait()
	})

	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("Bun harness did not print address; stderr=%s err=%v", stderr.String(), scanner.Err())
	}
	var payload struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &payload); err != nil {
		t.Fatalf("decode Bun harness address %q: %v stderr=%s", scanner.Text(), err, stderr.String())
	}
	if payload.Address == "" {
		t.Fatalf("Bun harness returned empty address; stderr=%s", stderr.String())
	}
	return payload.Address
}

func authContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer caller-token")
}

func validAcceptInputRequest(runtimeInputID string) *agentruntimev1.AcceptInputRequest {
	return &agentruntimev1.AcceptInputRequest{
		WorkspaceId:       "wksp_go",
		SessionId:         "sesn_go",
		SessionThreadId:   "thrd_go",
		BindingId:         "bind_go",
		BindingGeneration: 42,
		TargetPodUid:      "uid-a",
		RuntimeInputId:    runtimeInputID,
		InputOrder:        1,
		Content:           &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: "{}"},
	}
}

// syncBuffer guards a child process's stderr. os/exec fills it from its own
// goroutine while this test reads it for failure messages, so the two need a
// lock between them.
type syncBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}
