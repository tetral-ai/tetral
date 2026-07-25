package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"
	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// TestProviderNativeBashRealDetachedLifecycle is the bounded D2.12 proof. It
// sends provider-native Bash through the real Runtime TypeScript runner and a
// live Bridge gRPC server, then uses the production driver translation and a
// real root-to-runtime-uid helper supervisor. The terminal half intentionally
// uses a fresh Bridge store to prove cold recovery from PostgreSQL.
func TestProviderNativeBashRealDetachedLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("real Bash lifecycle requires the Linux/root integration lane")
	}
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_real_bash", "thr_real_bash")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_real_bash", "bind_real_bash", 1, "pod_real_bash")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_real_bash", "prep_real_bash")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_real_bash", "2026-01-01T00:00:00Z")

	harness := newRealHelperSandboxHarness(t)
	driverExecutor := sandboxdriver.NewDaytonaHelperExecutorForSandboxServices(func(context.Context, string) (sandboxdriver.DaytonaSandboxServices, error) {
		return sandboxdriver.DaytonaSandboxServices{FileSystem: harness, Process: harness}, nil
	})
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = NewSandboxDriverToolExecutor(driverExecutor)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen Bridge: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	command := exec.Command("bun", "run", "testdata/run-real-background-e2e.ts")
	command.Dir = filepath.Join(repoRootFromBridgeTest(t), "services/agent-runtime/packages/runtime-pod")
	command.Env = append(os.Environ(), "TETRAL_E2E_BRIDGE_ADDRESS="+listener.Addr().String())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Runtime provider-native Bash: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"taskId":"sevt_real_bash"`) || !strings.Contains(string(output), `"status: running`) {
		t.Fatalf("Runtime Bash result = %s; want stable running background task", output)
	}

	var taskStatus, storedResult, storedInput string
	var resultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT t.status, r.result_json, r.input_json, count(*) OVER ()
		   FROM session_background_tasks t
		   JOIN session_runtime_tool_results r
		     ON r.workspace_id = t.workspace_id AND r.session_id = t.session_id AND r.tool_use_event_id = t.source_tool_use_event_id
		  WHERE t.workspace_id = 'default' AND t.session_id = 'sesn_real_bash' AND t.task_id = 'sevt_real_bash'`,
	).Scan(&taskStatus, &storedResult, &storedInput, &resultCount); err != nil {
		t.Fatalf("read durable running task/result: %v", err)
	}
	if taskStatus != "running" || resultCount != 1 || !strings.Contains(storedResult, `"status":"running"`) || storedInput != `{"command":"sleep 60","run_in_background":true,"timeout":120000}` {
		t.Fatalf("durable running state = %q/%d/%s/%s", taskStatus, resultCount, storedResult, storedInput)
	}

	fresh := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	fresh.Clock = store.Clock
	fresh.SandboxToolExecutor = NewSandboxDriverToolExecutor(driverExecutor)
	scope := bridgeAPIScope("sesn_real_bash", "thr_real_bash", "bind_real_bash", 1, "pod_real_bash")
	poll, err := fresh.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope: scope, TaskId: "sevt_real_bash", ToolUseEventId: "sevt_real_bash_poll",
	})
	if err != nil || !strings.Contains(poll.GetResultJson(), `"status":"running"`) {
		t.Fatalf("cold Bridge poll = %s err=%v; want same live helper task", poll.GetResultJson(), err)
	}
	cancelScope := bridgeAPIScope("sesn_real_bash", "thr_real_bash", "bind_real_bash", 1, "pod_real_bash")
	cancelScope.RequestId = "req_real_bash_cancel"
	cancel, err := fresh.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{
		Scope: cancelScope, TaskId: "sevt_real_bash", ToolUseEventId: "sevt_real_bash_cancel", Reason: "integration_complete",
	})
	if err != nil || !strings.Contains(cancel.GetResultJson(), `"status":"success"`) {
		t.Fatalf("cold Bridge cancel = %s err=%v", cancel.GetResultJson(), err)
	}
	var terminalEvent sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id FROM session_background_tasks
		  WHERE workspace_id = 'default' AND session_id = 'sesn_real_bash' AND task_id = 'sevt_real_bash'`,
	).Scan(&taskStatus, &terminalEvent); err != nil {
		t.Fatalf("read terminal CAS state: %v", err)
	}
	if taskStatus != "cancelled" || !terminalEvent.Valid {
		t.Fatalf("terminal CAS state = %q/%v; want cancelled terminal winner", taskStatus, terminalEvent)
	}
}

type realHelperSandboxHarness struct {
	helper            string
	payloadRoot       string
	payloadMu         sync.Mutex
	driverPayloadRoot string
}

func newRealHelperSandboxHarness(t *testing.T) *realHelperSandboxHarness {
	t.Helper()
	repo := repoRootFromBridgeTest(t)
	runtimeRoot := filepath.Join("/tmp", "tetral-runtime-bash-e2e-"+time.Now().Format("150405.000000000"))
	helperDir, err := os.MkdirTemp("/tmp", "tetral-real-bash-helper-*")
	if err != nil {
		t.Fatalf("mkdir helper directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(helperDir) })
	if err := os.Chmod(helperDir, 0o755); err != nil {
		t.Fatalf("chmod helper directory: %v", err)
	}
	payloadRoot, err := os.MkdirTemp("/tmp", "tetral-real-bash-payloads-*")
	if err != nil {
		t.Fatalf("mkdir unique payload root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(payloadRoot) })
	helper := filepath.Join(helperDir, "sandbox")
	ldflags := "-X github.com/tetral-ai/tetral/internal/sandbox/helper/internal/task.runtimeRoot=" + runtimeRoot + " -X github.com/tetral-ai/tetral/internal/sandbox/helper/internal/cli.payloadRoot=" + payloadRoot
	build := exec.Command("go", "build", "-ldflags", ldflags, "-o", helper, "./cmd/sandbox")
	build.Dir = filepath.Join(repo, "internal/sandbox/helper")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real helper: %v\n%s", err, output)
	}
	for _, path := range []string{"/workspace", "/mnt/session/uploads", "/mnt/session/outputs", "/mnt/memory", "/skills"} {
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("real lifecycle requires disposable root lane; path already exists: %s", path)
		}
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(path) })
		if err := os.Chown(path, 65534, 65534); err != nil {
			t.Fatalf("chown %s: %v", path, err)
		}
	}
	if err := os.MkdirAll(filepath.Join(runtimeRoot, "tasks"), 0o700); err != nil {
		t.Fatalf("mkdir helper task root: %v", err)
	}
	if err := os.Chown(runtimeRoot, 65534, 65534); err != nil {
		t.Fatalf("chown helper runtime root: %v", err)
	}
	if err := os.Chmod(runtimeRoot, 0o700); err != nil {
		t.Fatalf("chmod helper runtime root: %v", err)
	}
	if err := os.Chown(filepath.Join(runtimeRoot, "tasks"), 65534, 65534); err != nil {
		t.Fatalf("chown helper task root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimeRoot) })
	return &realHelperSandboxHarness{helper: helper, payloadRoot: payloadRoot}
}

func repoRootFromBridgeTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "../.."))
}

func (h *realHelperSandboxHarness) CreateFolder(_ context.Context, path string, _ ...func(*options.CreateFolder)) error {
	return os.MkdirAll(h.captureAndMapPayloadPath(path), 0o700)
}

func (h *realHelperSandboxHarness) UploadFileStream(_ context.Context, content io.Reader, path string, _ ...daytona.UploadStreamOption) error {
	path = h.localPayloadPath(path)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, content)
	closeErr := file.Close()
	return errors.Join(copyErr, closeErr)
}

func (h *realHelperSandboxHarness) DeleteFile(_ context.Context, path string, _ bool) error {
	return os.RemoveAll(h.localPayloadPath(path))
}

func (*realHelperSandboxHarness) DownloadFileStream(context.Context, string, ...daytona.DownloadStreamOption) (io.ReadCloser, error) {
	return nil, errors.New("not used by real Bash lifecycle")
}

func (h *realHelperSandboxHarness) ExecuteCommand(ctx context.Context, command string, _ ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	if strings.Contains(command, "/usr/local/bin/sandbox") && strings.Contains(command, " health") {
		// The generic golang root image intentionally lacks the production
		// sandbox image's rg/rclone/FUSE health dependencies. Keep the health
		// gate affirmative here; exec/poll/cancel below still run the real helper.
		return &types.ExecuteResponse{ExitCode: 0, Result: `{"schema_version":1,"tool":"health","status":"success","truncated":false,"error":null,"result":{"status":"ok","version":"e2e","checks":[]}}`}, nil
	}
	command = strings.ReplaceAll(command, "'/usr/local/bin/sandbox'", "'"+h.helper+"'")
	if driverPayloadRoot := h.recordedDriverPayloadRoot(); driverPayloadRoot != "" {
		command = strings.ReplaceAll(command, driverPayloadRoot, h.payloadRoot)
	}
	command = strings.ReplaceAll(command, "sudo -n -u 'root' ", "")
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	output, err := cmd.CombinedOutput()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return nil, err
		}
		exitCode = exitErr.ExitCode()
	}
	return &types.ExecuteResponse{ExitCode: exitCode, Result: string(output)}, nil
}

func (h *realHelperSandboxHarness) localPayloadPath(path string) string {
	driverPayloadRoot := h.recordedDriverPayloadRoot()
	if path == driverPayloadRoot {
		return h.payloadRoot
	}
	if driverPayloadRoot != "" && strings.HasPrefix(path, driverPayloadRoot+"/") {
		return filepath.Join(h.payloadRoot, strings.TrimPrefix(path, driverPayloadRoot+"/"))
	}
	return path
}

func (h *realHelperSandboxHarness) captureAndMapPayloadPath(payloadDir string) string {
	h.payloadMu.Lock()
	if h.driverPayloadRoot == "" {
		h.driverPayloadRoot = filepath.Dir(payloadDir)
	}
	h.payloadMu.Unlock()
	return h.localPayloadPath(payloadDir)
}

func (h *realHelperSandboxHarness) recordedDriverPayloadRoot() string {
	h.payloadMu.Lock()
	defer h.payloadMu.Unlock()
	return h.driverPayloadRoot
}
