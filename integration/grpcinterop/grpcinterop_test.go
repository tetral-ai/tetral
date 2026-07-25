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

	okResponse, err := client.AcceptInput(authContext(ctx), validRuntimeInputCommand("runtime_input_go_ok"))
	if err != nil {
		t.Fatalf("AcceptInput OK: %v", err)
	}
	if okResponse.GetStatus() != agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED ||
		okResponse.GetSessionId() != "sesn_go" ||
		okResponse.GetRuntimeInputId() != "runtime_input_go_ok" ||
		okResponse.GetBindingId() != "bind_go" ||
		okResponse.GetBindingGeneration() != 42 {
		t.Fatalf("AcceptInput response = %+v; want accepted exact identity", okResponse)
	}

	_, err = client.AcceptInput(context.Background(), validRuntimeInputCommand("runtime_input_go_no_auth"))
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("AcceptInput without auth error = %v; want Unauthenticated", err)
	}

	_, err = client.AcceptInput(authContext(ctx), &agentruntimev1.RuntimeInputCommandRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AcceptInput invalid request error = %v; want InvalidArgument", err)
	}
}

func TestRuntimePodCarriesMultiMegabyteProviderRequestToGateway(t *testing.T) {
	if os.Getenv("TETRAL_RUN_GO_BUN_GRPC_INTEROP") != "1" {
		t.Skip("set TETRAL_RUN_GO_BUN_GRPC_INTEROP=1 to run Runtime-to-Gateway gRPC interop")
	}
	if _, err := exec.LookPath("bun"); err != nil {
		t.Skip("bun is not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	var result struct {
		RequestBytes      int `json:"requestBytes"`
		ReceivedBytes     int `json:"receivedBytes"`
		ReceivedTextBytes int `json:"receivedTextBytes"`
		EventCount        int `json:"eventCount"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output), &result); err != nil {
		t.Fatalf("decode transport harness output %q: %v stderr=%s", output, err, stderr.String())
	}
	if result.RequestBytes <= 4*1024*1024 || result.ReceivedBytes != result.RequestBytes {
		t.Fatalf("provider request bytes sent/received = %d/%d; want identical payload above 4 MiB", result.RequestBytes, result.ReceivedBytes)
	}
	if result.ReceivedTextBytes != 5*1024*1024 || result.EventCount != 1 {
		t.Fatalf("provider text bytes/events = %d/%d; want 5 MiB and one finish event", result.ReceivedTextBytes, result.EventCount)
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

func validRuntimeInputCommand(runtimeInputID string) *agentruntimev1.RuntimeInputCommandRequest {
	return &agentruntimev1.RuntimeInputCommandRequest{
		RequestId:          "req_go",
		WorkspaceId:        "wksp_go",
		SessionId:          "sesn_go",
		SessionThreadId:    "thrd_go",
		BindingId:          "bind_go",
		BindingGeneration:  42,
		TargetPodNamespace: "engine",
		TargetPodName:      "runtime-pod-a",
		TargetPodUid:       "uid-a",
		TargetPodIp:        "10.0.0.1",
		RuntimeInputId:     runtimeInputID,
		EventIds:           []string{"evnt_go"},
		SequenceFrom:       1,
		SequenceTo:         1,
		CommandKind:        agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
		PayloadJson:        "{}",
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
