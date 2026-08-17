package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

// TestDaytonaProductionAdaptersUseFinalImageIdentity is compiled by the
// Sandbox image proof and executed as root inside the real final image. The
// local services below stand in only for Daytona's remote Process and
// FileSystem transports: lifecycle lowering, payload staging, helper command
// construction, raw Daytona commands, and Git commands all remain production
// driver paths and execute under the lifecycle-selected default user.
func TestDaytonaProductionAdaptersUseFinalImageIdentity(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DAYTONA_IMAGE_CONTRACT") != "1" {
		t.Skip("final Sandbox image identity proof only")
	}
	imageID := os.Getenv("TETRAL_TEST_EXPECTED_IMAGE_ID")
	if !strings.HasPrefix(imageID, "sha256:") {
		t.Fatalf("final image identity = %q; want Docker content identity", imageID)
	}
	account := observedRuntimeAccount(t)

	lifecycleClient := &imageContractLifecycleClient{}
	lifecycle := NewDaytonaLifecycleProviderForClient(lifecycleClient, 30*time.Second)
	handle, err := lifecycle.CreateSandbox(context.Background(), sandbox.CreateSandboxRequest{Setup: sandbox.SandboxSetup{
		SandboxID:           "sandbox_image_contract",
		ProviderArtifactRef: imageID,
	}})
	if err != nil {
		t.Fatalf("DaytonaLifecycleProvider.CreateSandbox: %v", err)
	}
	params, ok := lifecycleClient.createParams.(types.SnapshotParams)
	if !ok {
		t.Fatalf("Daytona lifecycle params = %T; want SnapshotParams", lifecycleClient.createParams)
	}
	if params.User != RuntimeUser || params.Snapshot != imageID {
		t.Fatalf("Daytona lifecycle identity = user %q snapshot %q; want %q and %q", params.User, params.Snapshot, RuntimeUser, imageID)
	}

	process := &imageContractProcess{user: params.User}
	fileSystem := &imageContractFileSystem{uid: account.uid, gid: account.gid}
	executor := NewDaytonaHelperExecutorForClientWithCommandTimeout(
		imageContractSandboxGetter{handle: daytonaSandboxHandle{Process: process, FileSystem: fileSystem}},
		30*time.Second,
	)
	target := ToolTarget{ProviderSandboxID: handle.SandboxID}

	if err := executor.InstallGitHubRepositoryConfiguration(context.Background(), sandbox.GitHubRepositoryConfiguration{
		SessionID:         "session_image_contract",
		ProviderSandboxID: handle.SandboxID,
		GitProxyHost:      "git-proxy.invalid",
		Ticket:            "image-contract-ticket",
	}); err != nil {
		t.Fatalf("production Git adapter: %v", err)
	}
	identityCommand := `test "$(id -un)" = daytona && test "$HOME" = /home/daytona && test "$(getent passwd "$(id -u)" | cut -d: -f7)" = /bin/bash`
	if err := executor.RunDaytonaCommand(context.Background(), DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, identityCommand, nil, 30*time.Second); err != nil {
		t.Fatalf("RunDaytonaCommand identity: %v", err)
	}

	foreground := runImageContractTool(t, executor, ToolInvocation{
		Target: target, ToolUseEventID: "evt_image_foreground", ToolName: "Bash",
		InputJSON: `{"command":"printf 'identity=%s:%s:%s git=%s\\n' \"$(id -un)\" \"$HOME\" \"$(getent passwd \"$(id -u)\" | cut -d: -f7)\" \"$(git config --global user.name)\"","timeout":30000}`,
	})
	if !strings.Contains(foreground.ResultJSON, "identity=daytona:/home/daytona:/bin/bash git=Tetral Agent") {
		t.Fatalf("foreground adapter identity = %s", foreground.ResultJSON)
	}

	const detachedProofPath = "/workspace/daytona-adapter-detached.txt"
	t.Cleanup(func() { _ = os.Remove(detachedProofPath) })
	detached := runImageContractTool(t, executor, ToolInvocation{
		Target: target, ToolUseEventID: "evt_image_detached", ToolName: "Bash",
		InputJSON: `{"command":"sleep 1; printf 'detached=%s:%s\\n' \"$(id -un)\" \"$HOME\" > /workspace/daytona-adapter-detached.txt","timeout":30000,"run_in_background":true}`,
	})
	if detached.BackgroundTask == nil {
		t.Fatalf("detached adapter result = %+v; want background task", detached)
	}
	var detachedResult CommandResult
	for attempt := 0; attempt < 10; attempt++ {
		detachedResult, err = executor.ReadCommandResult(context.Background(), CommandReference{
			Target: target,
			Task:   *detached.BackgroundTask,
		})
		if err != nil {
			t.Fatalf("ReadCommandResult: %v", err)
		}
		if detachedResult.TerminalStatus != "" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if detachedResult.TerminalStatus != "completed" {
		t.Fatalf("detached/Read adapter identity = %+v", detachedResult)
	}
	detachedRead := runImageContractTool(t, executor, ToolInvocation{
		Target: target, ToolUseEventID: "evt_image_detached_read", ToolName: "Read",
		InputJSON: `{"file_path":"` + detachedProofPath + `"}`,
	})
	if !strings.Contains(detachedRead.ResultJSON, "detached=daytona:/home/daytona") {
		t.Fatalf("detached adapter identity = %s", detachedRead.ResultJSON)
	}

	const proofPath = "/workspace/daytona-adapter-identity.txt"
	t.Cleanup(func() { _ = os.Remove(proofPath) })
	write := runImageContractTool(t, executor, ToolInvocation{
		Target: target, ToolUseEventID: "evt_image_write", ToolName: "Write",
		InputJSON: `{"file_path":"` + proofPath + `","content":"daytona adapter file identity\\n"}`,
	})
	if !strings.Contains(write.ResultJSON, `"status":"success"`) {
		t.Fatalf("file adapter write = %s", write.ResultJSON)
	}
	if _, err := os.Stat(proofPath); err != nil {
		t.Fatalf("stat adapter file: %v", err)
	}
	ownerCommand := fmt.Sprintf(`test "$(stat -c %%u %s)" = %d && test "$(stat -c %%g %s)" = %d`, shellQuote(proofPath), account.uid, shellQuote(proofPath), account.gid)
	if err := executor.RunDaytonaCommand(context.Background(), DaytonaCommandTarget{ProviderSandboxID: handle.SandboxID}, ownerCommand, nil, 30*time.Second); err != nil {
		t.Fatalf("file adapter owner: %v", err)
	}
	read := runImageContractTool(t, executor, ToolInvocation{
		Target: target, ToolUseEventID: "evt_image_read", ToolName: "Read",
		InputJSON: `{"file_path":"` + proofPath + `"}`,
	})
	if !strings.Contains(read.ResultJSON, "daytona adapter file identity") {
		t.Fatalf("Read adapter result = %s", read.ResultJSON)
	}

	commands := strings.Join(process.commands, "\n")
	for _, required := range []string{
		"sudo -n sh -c",
		"sudo -n -u 'root' '/usr/local/bin/sandbox'",
		"sudo -H -n -u 'daytona' sh -lc",
		identityCommand,
	} {
		if !strings.Contains(commands, required) {
			t.Fatalf("production Daytona command trace lacks %q:\n%s", required, commands)
		}
	}
	t.Logf("daytona-adapter-image: id=%s user=%s uid=%d gid=%d home=%s shell=%s", imageID, params.User, account.uid, account.gid, account.home, account.shell)
}

type imageContractAccount struct {
	uid   int
	gid   int
	home  string
	shell string
}

func observedRuntimeAccount(t *testing.T) imageContractAccount {
	t.Helper()
	entry, err := user.Lookup(RuntimeUser)
	if err != nil {
		t.Fatalf("lookup runtime account: %v", err)
	}
	uid, err := strconv.Atoi(entry.Uid)
	if err != nil {
		t.Fatalf("runtime uid: %v", err)
	}
	gid, err := strconv.Atoi(entry.Gid)
	if err != nil {
		t.Fatalf("runtime gid: %v", err)
	}
	passwd, err := os.ReadFile("/etc/passwd")
	if err != nil {
		t.Fatalf("read passwd: %v", err)
	}
	shell := ""
	for _, line := range strings.Split(string(passwd), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) == 7 && fields[0] == RuntimeUser {
			shell = fields[6]
			break
		}
	}
	if uid == 0 || gid == 0 || entry.HomeDir != RuntimeHome || shell != "/bin/bash" {
		t.Fatalf("runtime account = %d/%d %s %s; want observed non-root daytona contract", uid, gid, entry.HomeDir, shell)
	}
	return imageContractAccount{uid: uid, gid: gid, home: entry.HomeDir, shell: shell}
}

func runImageContractTool(t *testing.T, executor *DaytonaHelperExecutor, invocation ToolInvocation) ToolExecution {
	t.Helper()
	prepared, err := executor.PrepareTool(context.Background(), invocation)
	if err != nil {
		t.Fatalf("PrepareTool(%s): %v", invocation.ToolName, err)
	}
	result, err := executor.SubmitPreparedTool(context.Background(), prepared)
	if err != nil {
		t.Fatalf("SubmitPreparedTool(%s): %v", invocation.ToolName, err)
	}
	return result
}

type imageContractLifecycleClient struct{ createParams any }

func (c *imageContractLifecycleClient) Create(_ context.Context, params any, _ ...func(*options.CreateSandbox)) (*daytona.Sandbox, error) {
	c.createParams = params
	return &daytona.Sandbox{ID: "provider_image_contract"}, nil
}

func (c *imageContractLifecycleClient) Get(context.Context, string) (*daytona.Sandbox, error) {
	return &daytona.Sandbox{ID: "provider_image_contract"}, nil
}

type imageContractSandboxGetter struct{ handle daytonaSandboxHandle }

func (g imageContractSandboxGetter) Get(context.Context, string) (daytonaSandboxHandle, error) {
	return g.handle, nil
}

type imageContractProcess struct {
	user     string
	commands []string
}

func (p *imageContractProcess) ExecuteCommand(ctx context.Context, command string, opts ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	p.commands = append(p.commands, command)
	var applied options.ExecuteCommand
	for _, opt := range opts {
		opt(&applied)
	}
	keys := make([]string, 0, len(applied.Env))
	for key := range applied.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := []string{"-H", "-n", "-u", p.user, "env"}
	for _, key := range keys {
		args = append(args, key+"="+applied.Env[key])
	}
	args = append(args, "sh", "-lc", command)
	output, err := exec.CommandContext(ctx, "sudo", args...).CombinedOutput()
	if err == nil {
		return &types.ExecuteResponse{ExitCode: 0, Result: string(output)}, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return &types.ExecuteResponse{ExitCode: exitErr.ExitCode(), Result: string(output)}, nil
	}
	return nil, err
}

type imageContractFileSystem struct{ uid, gid int }

func (f *imageContractFileSystem) CreateFolder(_ context.Context, remotePath string, _ ...func(*options.CreateFolder)) error {
	if err := os.MkdirAll(remotePath, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(remotePath, 0o700); err != nil {
		return err
	}
	return os.Chown(remotePath, f.uid, f.gid)
}

func (f *imageContractFileSystem) UploadFileStream(_ context.Context, content io.Reader, remotePath string, _ ...daytona.UploadStreamOption) error {
	file, err := os.OpenFile(remotePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(file, content)
	closeErr := file.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Chown(remotePath, f.uid, f.gid)
}

func (f *imageContractFileSystem) DeleteFile(_ context.Context, remotePath string, recursive bool) error {
	if recursive {
		return os.RemoveAll(remotePath)
	}
	return os.Remove(remotePath)
}

func (f *imageContractFileSystem) DownloadFileStream(_ context.Context, remotePath string, _ ...daytona.DownloadStreamOption) (io.ReadCloser, error) {
	return os.Open(filepath.Clean(remotePath))
}
