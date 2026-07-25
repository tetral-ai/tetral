package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	daytonasdk "github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"
	toolbox "github.com/daytonaio/daytona/libs/toolbox-api-client-go"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

func TestPreparationCommandsSerializeExactServerSideTimeoutOnDaytonaWire(t *testing.T) {
	var timeouts []int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/process/execute" {
			t.Errorf("request path = %q; want /process/execute", request.URL.Path)
		}
		var body struct {
			Timeout int `json:"timeout"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode Daytona request: %v", err)
		}
		timeouts = append(timeouts, body.Timeout)
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"exitCode":0,"result":""}`))
	}))
	defer server.Close()
	toolboxConfig := toolbox.NewConfiguration()
	toolboxConfig.Servers = toolbox.ServerConfigurations{{URL: server.URL}}
	process := daytonasdk.NewProcessService(toolbox.NewAPIClient(toolboxConfig), nil, "")
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(staticPreparationGetter{handle: daytonaSandboxHandle{Process: process, FileSystem: &recordingMemoryProjectionFileSystem{}}}, 37*time.Second)

	if err := executor.RunPreparationCommand(context.Background(), PreparationCommandTarget{ProviderSandboxID: "provider"}, "true", nil, 37*time.Second); err != nil {
		t.Fatalf("file/resource preparation command: %v", err)
	}
	if err := executor.RemoveMemoryStore(context.Background(), "provider", sandbox.MemoryStoreMount{MountPath: "/mnt/memory/store"}); err != nil {
		t.Fatalf("Memory preparation command: %v", err)
	}
	if err := executor.RemoveGitHubRepository(context.Background(), "provider", sandbox.GitHubRepositoryMount{MountPath: "/workspace/repo"}); err != nil {
		t.Fatalf("GitHub preparation command: %v", err)
	}
	if fmt.Sprint(timeouts) != "[37 37 37]" {
		t.Fatalf("Daytona wire timeouts = %v; want exact integer seconds for file, Memory, GitHub", timeouts)
	}
}

func TestRunPreparationCommandPassesSecretEnvAndTimeout(t *testing.T) {
	process := &recordingPreparationProcess{}
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(recordingPreparationGetter{process: process}, 45*time.Second)
	env := map[string]string{
		"RCLONE_CONFIG_R2_ACCESS_KEY_ID":     "access-key",
		"RCLONE_CONFIG_R2_SECRET_ACCESS_KEY": "secret-key",
		"RCLONE_CONFIG_R2_SESSION_TOKEN":     "session-token",
	}

	err := executor.RunPreparationCommand(context.Background(), PreparationCommandTarget{ProviderSandboxID: "provider_sandbox"}, "setsid sudo rclone mount </dev/null >/dev/null 2>&1", env, 45*time.Second)
	if err != nil {
		t.Fatalf("RunPreparationCommand: %v", err)
	}
	env["RCLONE_CONFIG_R2_SECRET_ACCESS_KEY"] = "mutated-secret"

	if process.command != "setsid sudo rclone mount </dev/null >/dev/null 2>&1" {
		t.Fatalf("command = %q; want preparation command", process.command)
	}
	if process.opts.Timeout == nil || *process.opts.Timeout != 45*time.Second {
		t.Fatalf("timeout = %v; want 45s", process.opts.Timeout)
	}
	if got := process.opts.Env["RCLONE_CONFIG_R2_SECRET_ACCESS_KEY"]; got != "secret-key" {
		t.Fatalf("secret env = %q; want cloned original value", got)
	}
	if got := process.opts.Env["RCLONE_CONFIG_R2_SESSION_TOKEN"]; got != "session-token" {
		t.Fatalf("session token env = %q; want configured value", got)
	}
}

func TestRunPreparationCommandFailureDoesNotLeakCommandEnvOrOutput(t *testing.T) {
	const (
		commandSecret = "command-secret-sentinel"
		envSecret     = "env-secret-sentinel"
		outputSecret  = "output-secret-sentinel"
	)
	process := &recordingPreparationProcess{
		response: &types.ExecuteResponse{ExitCode: 23, Result: "rclone failed " + outputSecret},
	}
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(recordingPreparationGetter{process: process}, 45*time.Second)

	err := executor.RunPreparationCommand(context.Background(), PreparationCommandTarget{ProviderSandboxID: "provider_sandbox"}, "echo "+commandSecret, map[string]string{"SECRET": envSecret}, 45*time.Second)
	if err == nil {
		t.Fatal("RunPreparationCommand succeeded; want failure")
	}
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v; want ProviderError", err, err)
	}
	if providerErr.Stage != sandbox.StageMountResources || providerErr.Kind != sandbox.ProviderErrorUnknown || !providerErr.Retryable {
		t.Fatalf("ProviderError = %+v; want retryable mount_resources unknown", providerErr)
	}
	rendered := strings.Join([]string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)}, "\n")
	for _, secret := range []string{commandSecret, envSecret, outputSecret} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("preparation command error leaked %q:\n%s", secret, rendered)
		}
	}
}

func TestRunPreparationCommandExecuteErrorMapsThroughDaytonaBoundary(t *testing.T) {
	process := &recordingPreparationProcess{err: context.DeadlineExceeded}
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(recordingPreparationGetter{process: process}, 45*time.Second)

	err := executor.RunPreparationCommand(context.Background(), PreparationCommandTarget{ProviderSandboxID: "provider_sandbox"}, "mount", nil, 45*time.Second)
	var providerErr *sandbox.ProviderError
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v; want ProviderError", err, err)
	}
	if providerErr.Stage != sandbox.StageMountResources || providerErr.Kind != sandbox.ProviderErrorUnknown || !providerErr.Retryable {
		t.Fatalf("ProviderError = %+v; want mapped retryable mount_resources provider error", providerErr)
	}
}

func TestStagePreparationFileUploadsUnderTetralRuntime(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)

	err := executor.StagePreparationFile(context.Background(), PreparationCommandTarget{ProviderSandboxID: "provider_sandbox"}, "/tmp/tetral-runtime/resource-projection/session/resource/file", strings.NewReader("canonical"))
	if err != nil {
		t.Fatalf("StagePreparationFile: %v", err)
	}
	if len(client.fileSystem.created) != 1 ||
		client.fileSystem.created[0].path != "/tmp/tetral-runtime/resource-projection/session/resource" ||
		client.fileSystem.created[0].mode != "0700" {
		t.Fatalf("created folders = %+v; want 0700 staging parent", client.fileSystem.created)
	}
	if len(client.fileSystem.uploads) != 1 ||
		client.fileSystem.uploads[0].path != "/tmp/tetral-runtime/resource-projection/session/resource/file" ||
		client.fileSystem.uploads[0].body != "canonical" {
		t.Fatalf("uploads = %+v; want staged canonical bytes", client.fileSystem.uploads)
	}
	if len(client.process.commands) != 0 {
		t.Fatalf("process commands = %+v; want file staging through filesystem only", client.process.commands)
	}
}

func TestStagePreparationFileRejectsPathOutsideTetralRuntime(t *testing.T) {
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(newRecordingMemoryProjectionClient(), 45*time.Second)

	err := executor.StagePreparationFile(context.Background(), PreparationCommandTarget{ProviderSandboxID: "provider_sandbox"}, "/workspace/leak", strings.NewReader("canonical"))
	if err == nil || !strings.Contains(err.Error(), "under /tmp/tetral-runtime") {
		t.Fatalf("StagePreparationFile error = %v; want path boundary rejection", err)
	}
}

type recordingPreparationGetter struct {
	process *recordingPreparationProcess
}

type staticPreparationGetter struct {
	handle daytonaSandboxHandle
}

func (g staticPreparationGetter) Get(context.Context, string) (daytonaSandboxHandle, error) {
	return g.handle, nil
}

func (g recordingPreparationGetter) Get(context.Context, string) (daytonaSandboxHandle, error) {
	return daytonaSandboxHandle{Process: g.process}, nil
}

type recordingPreparationProcess struct {
	command  string
	opts     options.ExecuteCommand
	response *types.ExecuteResponse
	err      error
}

func (p *recordingPreparationProcess) ExecuteCommand(_ context.Context, command string, opts ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	p.command = command
	for _, opt := range opts {
		opt(&p.opts)
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.response != nil {
		return p.response, nil
	}
	return &types.ExecuteResponse{ExitCode: 0}, nil
}
