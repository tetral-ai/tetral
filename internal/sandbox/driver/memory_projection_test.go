package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

// innerShellScript unwraps the `sudo sh -c '<script>'` privilege wrapper so
// assertions and local executions target the inner chain, whose semantics the
// wrapper must not change. Non-wrapped commands pass through unchanged.
func innerShellScript(t *testing.T, command string) string {
	t.Helper()
	const prefix = "sudo sh -c "
	if !strings.HasPrefix(command, prefix) {
		return command
	}
	quoted := strings.TrimPrefix(command, prefix)
	if len(quoted) < 2 || !strings.HasPrefix(quoted, "'") || !strings.HasSuffix(quoted, "'") {
		t.Fatalf("sudo sh -c payload is not single-quoted: %q", command)
	}
	return strings.ReplaceAll(quoted[1:len(quoted)-1], `'"'"'`, `'`)
}

func innerJoinedCommands(t *testing.T, commands []string) string {
	t.Helper()
	inner := make([]string, 0, len(commands))
	for _, command := range commands {
		inner = append(inner, innerShellScript(t, command))
	}
	return strings.Join(inner, "\n")
}

func TestDaytonaHelperExecutorProjectsMemoryUpsertWithStagedRename(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.RefreshMemoryProjection(context.Background(), MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_upsert"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops: []MemoryProjectionOp{{
			Kind:          "upsert",
			RelativePath:  "/notes/todo.md",
			Content:       "hello",
			ContentSHA256: testSHA256Hex("hello"),
		}},
	})
	if err != nil {
		t.Fatalf("RefreshMemoryProjection upsert: %v", err)
	}
	if len(client.fileSystem.created) != 1 || !strings.HasPrefix(client.fileSystem.created[0].path, "/mnt/memory/.staging/") || client.fileSystem.created[0].mode != "0700" {
		t.Fatalf("created folders = %+v; want one 0700 staging directory", client.fileSystem.created)
	}
	if len(client.fileSystem.uploads) != 1 || client.fileSystem.uploads[0].body != "hello" ||
		!strings.HasPrefix(client.fileSystem.uploads[0].path, client.fileSystem.created[0].path+"/f") {
		t.Fatalf("uploads = %+v; want staged content upload", client.fileSystem.uploads)
	}
	for _, command := range client.process.commands {
		if !strings.HasPrefix(command, "sudo ") {
			t.Fatalf("memory projection command runs unprivileged: %q", command)
		}
	}
	joined := innerJoinedCommands(t, client.process.commands)
	for _, required := range []string{
		"install -d -m 0700 -o 'daytona' -g 'daytona' '/mnt/memory/.staging'",
		"mkdir -p '/mnt/memory/project/notes'",
		"if [ -e '/mnt/memory/project/notes/todo.md' ] && [ ! -f '/mnt/memory/project/notes/todo.md' ]; then echo 'memory projection target is not a regular file' >&2; exit 1; fi",
		"chown root:root '" + client.fileSystem.uploads[0].path + "'",
		"chmod 0644 '" + client.fileSystem.uploads[0].path + "'",
		"mv -f '" + client.fileSystem.uploads[0].path + "' '/mnt/memory/project/notes/todo.md'",
		"rm -rf -- '" + client.fileSystem.created[0].path + "'",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("commands = %s; missing %s", joined, required)
		}
	}
}

func TestDaytonaHelperExecutorGuardsMemoryUpsertTargetKind(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.RefreshMemoryProjection(context.Background(), MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_kind_guard"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops: []MemoryProjectionOp{{
			Kind:         "upsert",
			RelativePath: "/a",
			Content:      "file",
		}},
	})
	if err != nil {
		t.Fatalf("RefreshMemoryProjection upsert: %v", err)
	}
	joined := innerJoinedCommands(t, client.process.commands)
	if !strings.Contains(joined, "if [ -e '/mnt/memory/project/a' ] && [ ! -f '/mnt/memory/project/a' ]; then echo 'memory projection target is not a regular file' >&2; exit 1; fi") {
		t.Fatalf("commands = %s; missing target kind guard", joined)
	}
}

func TestDaytonaHelperExecutorProjectsMemoryRemoveWithoutTouchingMountRoot(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.RefreshMemoryProjection(context.Background(), MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_remove"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops:        []MemoryProjectionOp{{Kind: "remove", RelativePath: "/a/b/c.txt"}},
	})
	if err != nil {
		t.Fatalf("RefreshMemoryProjection remove: %v", err)
	}
	if len(client.fileSystem.created) != 0 || len(client.fileSystem.uploads) != 0 {
		t.Fatalf("remove created/uploaded staging data: created=%+v uploads=%+v", client.fileSystem.created, client.fileSystem.uploads)
	}
	joined := innerJoinedCommands(t, client.process.commands)
	for _, required := range []string{
		"rm -f '/mnt/memory/project/a/b/c.txt'",
		"rmdir '/mnt/memory/project/a/b'",
		"rmdir '/mnt/memory/project/a'",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("commands = %s; missing %s", joined, required)
		}
	}
	if strings.Contains(joined, "rmdir '/mnt/memory/project'") || strings.Contains(joined, "rm -rf") {
		t.Fatalf("remove command touched mount root or recursive delete: %s", joined)
	}
}

func TestDaytonaHelperExecutorPassesConfiguredMemoryPreparationTimeoutOnWire(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	err := executor.RefreshMemoryProjection(ctx, MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_deadline"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops:        []MemoryProjectionOp{{Kind: "remove", RelativePath: "/notes/deadline.md"}},
	})
	if err != nil {
		t.Fatalf("RefreshMemoryProjection deadline: %v", err)
	}
	if len(client.process.opts) != 1 {
		t.Fatalf("ExecuteCommand option records = %d; want 1", len(client.process.opts))
	}
	timeout := client.process.opts[0].Timeout
	if timeout == nil || *timeout != 45*time.Second {
		t.Fatalf("ExecuteCommand timeout = %v; want exact configured 45s", timeout)
	}
}

func TestDaytonaHelperExecutorTreatsDirectoryRemoveAsNoOp(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.RefreshMemoryProjection(context.Background(), MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_remove_dir"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops:        []MemoryProjectionOp{{Kind: "remove", RelativePath: "/a/b"}},
	})
	if err != nil {
		t.Fatalf("RefreshMemoryProjection remove directory: %v", err)
	}
	joined := innerJoinedCommands(t, client.process.commands)
	for _, required := range []string{
		"if [ -f '/mnt/memory/project/a/b' ]; then rm -f '/mnt/memory/project/a/b'",
		"elif [ -d '/mnt/memory/project/a/b' ] && [ ! -L '/mnt/memory/project/a/b' ]; then :; elif",
		"rm -f '/mnt/memory/project/a/b'",
		"rmdir '/mnt/memory/project/a'",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("commands = %s; missing %s", joined, required)
		}
	}
	if strings.Contains(joined, "rmdir '/mnt/memory/project'") || strings.Contains(joined, "rm -rf") {
		t.Fatalf("directory no-op remove command touched mount root or recursive delete: %s", joined)
	}
}

func TestDaytonaHelperExecutorRemovesDirectorySymlinkWithoutTouchingTarget(t *testing.T) {
	mountPath := filepath.Join(t.TempDir(), "memory")
	targetDir := filepath.Join(t.TempDir(), "target")
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		t.Fatalf("mkdir mount: %v", err)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("mkdir symlink target: %v", err)
	}
	finalPath := filepath.Join(mountPath, "stale")
	if err := os.Symlink(targetDir, finalPath); err != nil {
		t.Fatalf("create directory symlink: %v", err)
	}

	if err := exec.Command("bash", "-c", innerShellScript(t, memoryProjectionRemoveCommand(mountPath, finalPath))).Run(); err != nil {
		t.Fatalf("remove directory symlink command: %v", err)
	}
	if _, err := os.Lstat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("directory symlink remains after remove: %v", err)
	}
	if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
		t.Fatalf("directory symlink target was touched: info=%v err=%v", info, err)
	}
}

func TestDaytonaHelperExecutorDeletesExistingMemoryFileRemove(t *testing.T) {
	mountPath := filepath.Join(t.TempDir(), "memory")
	filePath := filepath.Join(mountPath, "a", "b", "c.txt")
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("mkdir file parent: %v", err)
	}
	if err := os.WriteFile(filePath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale file: %v", err)
	}
	if err := exec.Command("bash", "-c", innerShellScript(t, memoryProjectionRemoveCommand(mountPath, filePath))).Run(); err != nil {
		t.Fatalf("remove existing file command: %v", err)
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale file stat err = %v; want removed", err)
	}
	if _, err := os.Stat(filepath.Join(mountPath, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty ancestor stat err = %v; want removed", err)
	}
}

func TestDaytonaHelperExecutorTreatsDescendantUnderFileRemoveAsNoOp(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.RefreshMemoryProjection(context.Background(), MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_remove_descendant"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops:        []MemoryProjectionOp{{Kind: "remove", RelativePath: "/a/b"}},
	})
	if err != nil {
		t.Fatalf("RefreshMemoryProjection remove descendant under file: %v", err)
	}
	joined := innerJoinedCommands(t, client.process.commands)
	for _, required := range []string{
		"ANCESTOR='/mnt/memory/project/a/b'",
		"while [ \"$ANCESTOR\" != '/mnt/memory/project' ] && [ ! -e \"$ANCESTOR\" ]; do ANCESTOR=$(dirname \"$ANCESTOR\"); done",
		"elif [ \"$ANCESTOR\" != '/mnt/memory/project' ] && [ -f \"$ANCESTOR\" ]; then :;",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("commands = %s; missing %s", joined, required)
		}
	}
	if strings.Contains(joined, "rm -rf") {
		t.Fatalf("descendant-under-file no-op remove command used recursive delete: %s", joined)
	}
}

func TestDaytonaHelperExecutorMaterializesMemoryStoreSnapshot(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.MaterializeMemoryStore(context.Background(), sandbox.MemoryStoreMaterialization{
		ProviderSandboxID: "provider_memory_materialize",
		MountPath:         "/mnt/memory/project",
		Files: []sandbox.MemorySnapshotFile{{
			Path:          "/notes/todo.md",
			Content:       "hello",
			ContentSHA256: testSHA256Hex("hello"),
		}},
	})
	if err != nil {
		t.Fatalf("MaterializeMemoryStore snapshot: %v", err)
	}
	if len(client.fileSystem.created) != 1 || client.fileSystem.created[0].mode != "0700" {
		t.Fatalf("created folders = %+v; want 0700 staging directory", client.fileSystem.created)
	}
	if len(client.fileSystem.uploads) != 2 {
		t.Fatalf("uploads = %+v; want tar and manifest uploads", client.fileSystem.uploads)
	}
	var manifest string
	for _, upload := range client.fileSystem.uploads {
		if strings.HasSuffix(upload.path, "/SHA256SUMS") {
			manifest = upload.body
		}
	}
	if !strings.Contains(manifest, testSHA256Hex("hello")+"  ./notes/todo.md") {
		t.Fatalf("manifest = %q; want durable content hash and projection path", manifest)
	}
	for _, command := range client.process.commands {
		if !strings.HasPrefix(command, "sudo ") {
			t.Fatalf("memory materialization command runs unprivileged: %q", command)
		}
	}
	joined := innerJoinedCommands(t, client.process.commands)
	for _, required := range []string{
		"install -d -m 0700 -o 'daytona' -g 'daytona' '/mnt/memory/.staging'",
		"sha256sum --check --quiet",
		"chown -R root:root",
		"find '" + client.fileSystem.created[0].path + "/extract' -type d -exec chmod 0755 {} +",
		"find '" + client.fileSystem.created[0].path + "/extract' -type f -exec chmod 0644 {} +",
		"rm -rf '/mnt/memory/project'",
		"mv -T '" + client.fileSystem.created[0].path + "/extract' '/mnt/memory/project'",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("commands = %s; missing %s", joined, required)
		}
	}

	emptyClient := newRecordingMemoryProjectionClient()
	executor = NewDaytonaHelperExecutorForClientWithPreparationTimeout(emptyClient, 45*time.Second)
	if err := executor.MaterializeMemoryStore(context.Background(), sandbox.MemoryStoreMaterialization{ProviderSandboxID: "provider_empty", MountPath: "/mnt/memory/empty"}); err != nil {
		t.Fatalf("MaterializeMemoryStore empty: %v", err)
	}
	if len(emptyClient.fileSystem.uploads) != 0 {
		t.Fatalf("empty store uploads = %+v; want none", emptyClient.fileSystem.uploads)
	}
	emptyCommand := innerJoinedCommands(t, emptyClient.process.commands)
	if !strings.Contains(emptyCommand, "rm -rf '/mnt/memory/empty'") ||
		!strings.Contains(emptyCommand, "mkdir -p '/mnt/memory/empty'") ||
		!strings.Contains(emptyCommand, "chmod 0755 '/mnt/memory/empty'") {
		t.Fatalf("empty store command = %s; want bare directory recreation", emptyCommand)
	}
}

func TestDaytonaHelperExecutorFailsMemoryProjectionOnCommandError(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.exitCode = 7
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	err := executor.RefreshMemoryProjection(context.Background(), MemoryProjectionRefresh{
		Target:     ToolTarget{ProviderSandboxID: "provider_memory_error"},
		MountPaths: []string{"/mnt/memory/project"},
		Ops:        []MemoryProjectionOp{{Kind: "remove", RelativePath: "/notes/todo.md"}},
	})
	if err == nil {
		t.Fatal("RefreshMemoryProjection command failure returned nil error")
	}
}

func TestDaytonaHelperExecutorRemovesDeletedMemoryStoreTree(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	if err := executor.RemoveMemoryStore(context.Background(), "provider_memory_delete", sandbox.MemoryStoreMount{MountPath: "/mnt/memory/project"}); err != nil {
		t.Fatalf("RemoveMemoryStore: %v", err)
	}
	if got := strings.Join(client.process.commands, "\n"); !strings.Contains(got, "sudo rm -rf -- '/mnt/memory/project'") {
		t.Fatalf("remove command = %q; want privileged whole-store rm -rf", got)
	}
}

func TestDaytonaHelperExecutorPropagatesDeletedMemoryStoreRemovalFailure(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.exitCode = 23
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)

	err := executor.RemoveMemoryStore(context.Background(), "provider_memory_delete", sandbox.MemoryStoreMount{MountPath: "/mnt/memory/project"})
	if err == nil {
		t.Fatal("RemoveMemoryStore succeeded; want command failure")
	}
	if len(client.process.commands) != 1 {
		t.Fatalf("remove commands = %d; want 1", len(client.process.commands))
	}
}

func TestDaytonaHelperExecutorRejectsMemoryRootMountPath(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	for _, mountPath := range []string{"/mnt/memory", "/mnt/memory/", "/mnt/memory/.staging", "/mnt/memory/.staging/store"} {
		t.Run(mountPath, func(t *testing.T) {
			err := executor.MaterializeMemoryStore(context.Background(), sandbox.MemoryStoreMaterialization{
				ProviderSandboxID: "provider_memory_root",
				MountPath:         mountPath,
			})
			if err == nil {
				t.Fatalf("MaterializeMemoryStore accepted mount path %q", mountPath)
			}
		})
	}

	if err := executor.MaterializeMemoryStore(context.Background(), sandbox.MemoryStoreMaterialization{
		ProviderSandboxID: "provider_memory_store",
		MountPath:         "/mnt/memory/project",
	}); err != nil {
		t.Fatalf("MaterializeMemoryStore rejected store mount: %v", err)
	}
}

type recordingMemoryProjectionClient struct {
	fileSystem *recordingMemoryProjectionFileSystem
	process    *recordingMemoryProjectionProcess
	err        error
}

func newRecordingMemoryProjectionClient() *recordingMemoryProjectionClient {
	return &recordingMemoryProjectionClient{
		fileSystem: &recordingMemoryProjectionFileSystem{},
		process:    &recordingMemoryProjectionProcess{},
	}
}

func (c *recordingMemoryProjectionClient) Get(context.Context, string) (daytonaSandboxHandle, error) {
	if c.err != nil {
		return daytonaSandboxHandle{}, c.err
	}
	return daytonaSandboxHandle{FileSystem: c.fileSystem, Process: c.process}, nil
}

type recordingMemoryProjectionFileSystem struct {
	created      []recordedMemoryProjectionCreate
	uploads      []recordedMemoryProjectionUpload
	uploadErrors []error
	deleted      []string
}

type recordedMemoryProjectionCreate struct {
	path string
	mode string
}

type recordedMemoryProjectionUpload struct {
	path string
	body string
}

func (f *recordingMemoryProjectionFileSystem) CreateFolder(_ context.Context, path string, opts ...func(*options.CreateFolder)) error {
	applied := options.Apply(opts...)
	mode := ""
	if applied.Mode != nil {
		mode = *applied.Mode
	}
	f.created = append(f.created, recordedMemoryProjectionCreate{path: path, mode: mode})
	return nil
}

func (f *recordingMemoryProjectionFileSystem) UploadFileStream(_ context.Context, source io.Reader, remotePath string, _ ...daytona.UploadStreamOption) error {
	body, err := io.ReadAll(source)
	if err != nil {
		return err
	}
	f.uploads = append(f.uploads, recordedMemoryProjectionUpload{path: remotePath, body: string(body)})
	index := len(f.uploads) - 1
	if index < len(f.uploadErrors) && f.uploadErrors[index] != nil {
		return f.uploadErrors[index]
	}
	return nil
}

func (f *recordingMemoryProjectionFileSystem) DeleteFile(_ context.Context, path string, _ bool) error {
	f.deleted = append(f.deleted, path)
	return nil
}

func (f *recordingMemoryProjectionFileSystem) DownloadFileStream(context.Context, string, ...daytona.DownloadStreamOption) (io.ReadCloser, error) {
	return nil, errors.New("DownloadFileStream is unused by memory projection tests")
}

type recordingMemoryProjectionProcess struct {
	commands     []string
	opts         []options.ExecuteCommand
	exitCode     int
	exitCodes    []int
	result       string
	results      []string
	errors       []error
	afterExecute func(index int, command string)
}

func (p *recordingMemoryProjectionProcess) ExecuteCommand(_ context.Context, command string, opts ...func(*options.ExecuteCommand)) (*types.ExecuteResponse, error) {
	p.commands = append(p.commands, command)
	var applied options.ExecuteCommand
	for _, opt := range opts {
		opt(&applied)
	}
	p.opts = append(p.opts, applied)
	index := len(p.commands) - 1
	exitCode := p.exitCode
	if index < len(p.exitCodes) {
		exitCode = p.exitCodes[index]
	}
	result := p.result
	if index < len(p.results) {
		result = p.results[index]
	}
	if p.afterExecute != nil {
		p.afterExecute(index, command)
	}
	if index < len(p.errors) && p.errors[index] != nil {
		return nil, p.errors[index]
	}
	return &types.ExecuteResponse{ExitCode: exitCode, Result: result}, nil
}

func testSHA256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
