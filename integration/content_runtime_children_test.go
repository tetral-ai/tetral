package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// These children are the actual Gateway/SDK and RuntimePodApp factories. Their
// named controls only pause external provider input or an existing refresh
// boundary; no business ACK, declaration, settlement or delivery is synthesized.
type contentGatewayChild struct {
	controlMu sync.Mutex
	command   *exec.Cmd
	input     io.WriteCloser
	lines     chan []byte
	joined    chan error
	output    lockedBuffer
	address   string
	stopped   bool
}

func startContentGatewayChild(t *testing.T, options map[string]any) *contentGatewayChild {
	return startContentGatewayChildContext(context.Background(), t, options)
}

func startContentGatewayChildContext(ctx context.Context, t *testing.T, options map[string]any) *contentGatewayChild {
	t.Helper()
	command := exec.CommandContext(ctx, "bun", "packages/provider-gateway/test/fixtures/content-lifecycle-gateway.ts") //nolint:gosec // Repository-owned fixture.
	command.Dir = "../services/gateway"
	child := &contentGatewayChild{command: command, lines: make(chan []byte, 32), joined: make(chan error, 1)}
	var err error
	child.input, err = command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = &child.output
	if err := command.Start(); err != nil {
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
	go func() { child.joined <- command.Wait() }()
	t.Cleanup(func() {
		if !child.stopped {
			child.stop(t)
		}
	})
	ready := child.control(t, map[string]any{"kind": "start", "options": options}, "ready")
	if err := json.Unmarshal(ready["address"], &child.address); err != nil || child.address == "" {
		t.Fatal("Gateway ready has no RPC address")
	}
	return child
}

func (c *contentGatewayChild) control(t *testing.T, request map[string]any, expected string) map[string]json.RawMessage {
	t.Helper()
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.input.Write(append(encoded, '\n')); err != nil {
		t.Fatal(err)
	}
	select {
	case raw, ok := <-c.lines:
		if !ok {
			t.Fatalf("Gateway child exited before %s: %s", expected, c.output.String())
		}
		var response map[string]json.RawMessage
		if err := json.Unmarshal(raw, &response); err != nil {
			t.Fatal("Gateway child control is malformed")
		}
		var kind string
		if err := json.Unmarshal(response["kind"], &kind); err != nil || kind != expected {
			t.Fatalf("Gateway control kind = %q; want %s", kind, expected)
		}
		return response
	case <-time.After(20 * time.Second):
		t.Fatalf("Gateway child control %s did not finish: %s", expected, c.output.String())
	}
	return nil
}

func (c *contentGatewayChild) stop(t *testing.T) {
	t.Helper()
	if c.stopped {
		return
	}
	c.stopped = true
	deadline := time.Now().Add(10 * time.Second)
	// Preserve safe admission diagnostics even when the test exits before its
	// first ToolUse. This observation does not replace any lifecycle assertion.
	if _, err := c.input.Write([]byte("{\"kind\":\"observe\"}\n")); err == nil {
		select {
		case raw, ok := <-c.lines:
			if ok {
				var response map[string]json.RawMessage
				if json.Unmarshal(raw, &response) == nil {
					for _, field := range []string{"providerCalls", "admissionFailures", "activeProviderSources", "joinedSources"} {
						if value, exists := response[field]; exists {
							t.Logf("Gateway child evidence %s: %s", field, value)
						}
					}
				}
			}
		case <-time.After(2 * time.Second):
			t.Log("Gateway child admission observation unavailable before shutdown")
		}
	}
	// Closing the control input runs the fixture's real shutdown path even if
	// the test failed while an external provider gate remained held.
	_ = c.input.Close()
	select {
	case err := <-c.joined:
		if err != nil {
			t.Errorf("Gateway child shutdown: %v: %s", err, c.output.String())
		}
	case <-time.After(time.Until(deadline)):
		_ = c.command.Process.Kill()
		<-c.joined
		t.Error("Gateway child failed to join its owners")
	}
}

type contentRuntimeChild struct {
	command   *exec.Cmd
	directory string
	port      int
	joined    chan error
	output    lockedBuffer
	stopped   bool
}

func startContentRuntimeChildWithOptions(t *testing.T, bridgeAddress, gatewayAddress, podUID, processID, token string, options map[string]any) *contentRuntimeChild {
	return startContentRuntimeChildContext(context.Background(), t, bridgeAddress, gatewayAddress, podUID, processID, token, options)
}

func startContentRuntimeChildContext(ctx context.Context, t *testing.T, bridgeAddress, gatewayAddress, podUID, processID, token string, options map[string]any) *contentRuntimeChild {
	t.Helper()
	child := &contentRuntimeChild{directory: t.TempDir(), joined: make(chan error, 1)}
	input := map[string]any{"bridgeAddress": bridgeAddress, "gatewayAddress": gatewayAddress, "podUID": podUID, "processID": processID, "token": token, "directory": child.directory, "approvalMode": "full_access"}
	for key, value := range options {
		input[key] = value
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(child.directory, "input.json")
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	child.command = exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/content-lifecycle-runtime.ts", path) //nolint:gosec // Repository-owned fixture and private input.
	child.command.Dir = "../services/agent-runtime"
	child.command.Stdout = &child.output
	child.command.Stderr = &child.output
	if err := child.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { child.joined <- child.command.Wait() }()
	t.Cleanup(func() { child.stop(t) })
	ready := child.marker(t, "ready")
	if err := json.Unmarshal(ready["port"], &child.port); err != nil || child.port < 1 {
		t.Fatal("Runtime ready has no RPC port")
	}
	return child
}

func (c *contentRuntimeChild) marker(t *testing.T, name string) map[string]json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(filepath.Join(c.directory, name+".json")); err == nil {
			var value map[string]json.RawMessage
			if err := json.Unmarshal(raw, &value); err == nil {
				return value
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Runtime marker %s did not appear: %s", name, c.output.String())
	return nil
}

func (c *contentRuntimeChild) release(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(c.directory, name), nil, 0600); err != nil {
		t.Fatal(err)
	}
}

func (c *contentRuntimeChild) stop(t *testing.T) {
	t.Helper()
	if c.stopped {
		return
	}
	c.stopped = true
	defer func() {
		if file, err := os.Open(filepath.Join(c.directory, "runtime-observations.jsonl")); err == nil {
			defer func() { _ = file.Close() }()
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 16384), 1024*1024)
			for scanner.Scan() {
				if len(scanner.Bytes()) == 0 {
					continue
				}
				var record struct {
					Event string `json:"event"`
				}
				if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
					t.Errorf("invalid Runtime observation: %v", err)
					continue
				}
				if record.Event == "runtime_content_commit" || record.Event == "runtime_operation_completed" || record.Event == "runtime_operation_timing_unavailable" {
					t.Logf("Runtime stage sample: %s", scanner.Text())
				}
			}
			if err := scanner.Err(); err != nil {
				t.Errorf("Runtime observation read: %v", err)
			}
		}
		for _, name := range []string{"ready.json", "tool-declared.json", "refresh-held.json", "request-end-ack.json", "request-end-projected.json", "queued-tool-ownership.json", "projection-observation-failed.json", "closed.json", "trace.jsonl"} {
			if raw, err := os.ReadFile(filepath.Join(c.directory, name)); err == nil {
				if len(raw) > 16384 {
					raw = raw[:16384]
				}
				t.Logf("Runtime child evidence %s: %s", name, raw)
			}
		}
	}()
	if err := os.WriteFile(filepath.Join(c.directory, "stop"), nil, 0600); err != nil {
		t.Error(err)
	}
	select {
	case err := <-c.joined:
		if err != nil {
			t.Errorf("Runtime child shutdown: %v: %s", err, c.output.String())
			return
		}
		if _, err := os.Stat(filepath.Join(c.directory, "closed.json")); err != nil {
			t.Error("Runtime child exited without joining its owned resources")
		}
	case <-time.After(10 * time.Second):
		_ = c.command.Process.Kill()
		<-c.joined
		t.Error("Runtime child failed to join its owners")
	}
}
