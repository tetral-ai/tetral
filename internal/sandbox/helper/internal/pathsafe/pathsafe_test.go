package pathsafe

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

func TestToolErrorPreservesPathSafeFields(t *testing.T) {
	err := ToolError(&Error{Kind: KindPathEscape, Message: "outside root", Path: "/escape"})
	if err.Kind != KindPathEscape || err.Message != "outside root" || err.Path != "/escape" {
		t.Fatalf("unexpected tool error: %#v", err)
	}
}

func TestToolErrorMapsUnknownErrorsToInvalidInput(t *testing.T) {
	err := ToolError(errors.New("bad path"))
	if err.Kind != KindInvalidInput || err.Message != "bad path" || err.Path != "" {
		t.Fatalf("unexpected tool error: %#v", err)
	}
}

func TestResolveAllowsRelativeWorkspacePath(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	got, err := testResolver(workspace).Resolve("note.txt", false)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Path != filepath.Join(workspace, "note.txt") || got.DisplayPath != "note.txt" {
		t.Fatalf("resolved = %+v; want workspace note", got)
	}
}

func TestResolveRejectsSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(workspace, "link.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	_, err := testResolver(workspace).Resolve("link.txt", false)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindPathEscape {
		t.Fatalf("Resolve err = %T %v; want path_escape", err, err)
	}
}

func TestResolveAllowsReadOnlyAbsoluteRootAndRejectsWrite(t *testing.T) {
	workspace := t.TempDir()
	uploads := t.TempDir()
	filePath := filepath.Join(uploads, "data.csv")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write upload: %v", err)
	}
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: uploads, Mode: protocol.RootModeRead},
		},
	}
	got, err := resolver.Resolve(filePath, false)
	if err != nil {
		t.Fatalf("Resolve read: %v", err)
	}
	if got.DisplayPath != filePath {
		t.Fatalf("display path = %q; want absolute upload path", got.DisplayPath)
	}
	_, err = resolver.Resolve(filePath, true)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindPathEscape {
		t.Fatalf("Resolve write err = %T %v; want path_escape", err, err)
	}
}

func TestResolveRejectsWorkspaceBoundReadOnlyResourceWrite(t *testing.T) {
	workspace := t.TempDir()
	resource := filepath.Join(workspace, "data", "report.csv")
	if err := os.MkdirAll(filepath.Dir(resource), 0o755); err != nil {
		t.Fatalf("mkdir resource: %v", err)
	}
	if err := os.WriteFile(resource, []byte("x"), 0o600); err != nil {
		t.Fatalf("write resource: %v", err)
	}
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: filepath.Join(workspace, "data"), Mode: protocol.RootModeRead},
		},
	}
	got, err := resolver.Resolve(resource, false)
	if err != nil {
		t.Fatalf("Resolve read: %v", err)
	}
	if got.Mode != protocol.RootModeRead {
		t.Fatalf("resolved mode = %q; want most-specific read root", got.Mode)
	}
	_, err = resolver.Resolve(resource, true)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindPathEscape {
		t.Fatalf("Resolve write err = %T %v; want path_escape", err, err)
	}
}

func TestResolveRejectsWorkspaceBoundReadOnlyFileResourceWrite(t *testing.T) {
	workspace := t.TempDir()
	resource := filepath.Join(workspace, "data", "report.csv")
	if err := os.MkdirAll(filepath.Dir(resource), 0o755); err != nil {
		t.Fatalf("mkdir resource: %v", err)
	}
	if err := os.WriteFile(resource, []byte("x"), 0o600); err != nil {
		t.Fatalf("write resource: %v", err)
	}
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: resource, Mode: protocol.RootModeRead},
		},
	}
	_, err := resolver.Resolve(resource, true)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindPathEscape {
		t.Fatalf("Resolve workspace-bound file resource write err = %T %v; want path_escape", err, err)
	}
}

func TestResolveRejectsNestedReadRootWriteByMostSpecificRoot(t *testing.T) {
	workspace := t.TempDir()
	parent := t.TempDir()
	resource := filepath.Join(parent, "file_resource")
	if err := os.WriteFile(resource, []byte("x"), 0o600); err != nil {
		t.Fatalf("write resource: %v", err)
	}
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: parent, Mode: protocol.RootModeReadWrite},
			{Path: resource, Mode: protocol.RootModeRead},
		},
	}

	got, err := resolver.Resolve(resource, false)
	if err != nil {
		t.Fatalf("Resolve resource read: %v", err)
	}
	if got.Mode != protocol.RootModeRead {
		t.Fatalf("resource mode = %q; want most-specific read root", got.Mode)
	}
	_, err = resolver.Resolve(resource, true)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindPathEscape {
		t.Fatalf("Resolve resource write err = %T %v; want path_escape", err, err)
	}

	sibling := filepath.Join(parent, "scratch.txt")
	got, err = resolver.Resolve(sibling, true)
	if err != nil {
		t.Fatalf("Resolve sibling write: %v", err)
	}
	if got.Mode != protocol.RootModeReadWrite {
		t.Fatalf("sibling mode = %q; want parent read_write root", got.Mode)
	}
}

func TestResolveRejectsForbiddenRuntimePath(t *testing.T) {
	workspace := t.TempDir()
	for _, pathValue := range []string{
		"/tmp/tetral-runtime/tool-payloads/x/payload.json",
	} {
		_, err := testResolver(workspace).Resolve(pathValue, false)
		var pathErr *Error
		if !errors.As(err, &pathErr) || pathErr.Kind != KindForbiddenPath {
			t.Fatalf("Resolve(%q) err = %T %v; want forbidden_path", pathValue, err, err)
		}
	}
}

func TestResolveRejectsMemoryStagingByContainmentNotForbiddenRoot(t *testing.T) {
	workspace := t.TempDir()
	allowedMemory := t.TempDir()
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: allowedMemory, Mode: protocol.RootModeRead},
		},
	}
	_, err := resolver.Resolve("/mnt/memory/.staging/refresh-1/f0", false)
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindPathEscape {
		t.Fatalf("Resolve memory staging err = %T %v; want path_escape", err, err)
	}
}

func TestResolveContractFixtureMatrix(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	uploads := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "inside.txt"), []byte("inside"), 0o600); err != nil {
		t.Fatalf("write inside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.WriteFile(filepath.Join(uploads, "data.csv"), []byte("data"), 0o600); err != nil {
		t.Fatalf("write upload: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "escape-dir")); err != nil {
		t.Fatalf("symlink dir: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "missing.txt"), filepath.Join(workspace, "dangling.txt")); err != nil {
		t.Fatalf("dangling symlink: %v", err)
	}
	if err := os.Symlink(filepath.Join(uploads, "data.csv"), filepath.Join(workspace, "upload-link.csv")); err != nil {
		t.Fatalf("upload symlink: %v", err)
	}
	forbiddenDir := makeForbiddenFixture(t)
	if err := os.Symlink(filepath.Join(forbiddenDir, "secret.txt"), filepath.Join(workspace, "forbidden-link.txt")); err != nil {
		t.Fatalf("forbidden symlink: %v", err)
	}
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: uploads, Mode: protocol.RootModeRead},
		},
	}

	successes := []struct {
		name         string
		path         string
		requireWrite bool
		wantDisplay  string
		wantMode     string
	}{
		{name: "workspace root", path: ".", wantDisplay: ".", wantMode: protocol.RootModeReadWrite},
		{name: "creation suffix", path: "new/child.txt", requireWrite: true, wantDisplay: "new/child.txt", wantMode: protocol.RootModeReadWrite},
		{name: "absolute read root reports absolute", path: filepath.Join(uploads, "data.csv"), wantDisplay: filepath.Join(uploads, "data.csv"), wantMode: protocol.RootModeRead},
		{name: "symlink hops to allowed read root", path: "upload-link.csv", wantDisplay: filepath.Join(uploads, "data.csv"), wantMode: protocol.RootModeRead},
	}
	for _, tc := range successes {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolver.Resolve(tc.path, tc.requireWrite)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got.DisplayPath != tc.wantDisplay || got.Mode != tc.wantMode {
				t.Fatalf("resolved = %+v; want display %q mode %q", got, tc.wantDisplay, tc.wantMode)
			}
		})
	}

	failures := []struct {
		name         string
		path         string
		requireWrite bool
		want         string
	}{
		{name: "dotdot escape", path: "../" + filepath.Base(outside) + "/secret.txt", want: KindPathEscape},
		{name: "absolute outside", path: filepath.Join(outside, "secret.txt"), want: KindPathEscape},
		{name: "write under read root", path: filepath.Join(uploads, "data.csv"), requireWrite: true, want: KindPathEscape},
		{name: "symlink dir escape", path: "escape-dir/secret.txt", want: KindPathEscape},
		{name: "dangling symlink", path: "dangling.txt", want: KindPathEscape},
		{name: "dev shm forbidden lexical", path: "/dev/shm/tetral-runtime/tool-payloads/x/payload.json", want: KindForbiddenPath},
		{name: "resolved forbidden root", path: "forbidden-link.txt", want: KindForbiddenPath},
	}
	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolver.Resolve(tc.path, tc.requireWrite)
			var pathErr *Error
			if !errors.As(err, &pathErr) || pathErr.Kind != tc.want {
				t.Fatalf("Resolve err = %T %v; want %s", err, err, tc.want)
			}
		})
	}
}

func TestResolveIgnoresMissingUnrelatedRoots(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "note.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	resolver := Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
			{Path: filepath.Join(workspace, "missing-projection-root"), Mode: protocol.RootModeRead},
		},
	}
	if _, err := resolver.Resolve("note.txt", false); err != nil {
		t.Fatalf("Resolve with missing unrelated root: %v", err)
	}
}

func TestResolveExecCWDAllowsOutsideWorkspaceButRejectsForbiddenAndFiles(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	got, err := ResolveExecCWD(workspace, outside)
	if err != nil {
		t.Fatalf("ResolveExecCWD outside: %v", err)
	}
	if got.Path != outside || got.DisplayPath != outside {
		t.Fatalf("outside cwd = %+v; want absolute display", got)
	}
	if got, err := ResolveExecCWD(workspace, ""); err != nil || got.DisplayPath != "." {
		t.Fatalf("default cwd = %+v, %v; want workspace root", got, err)
	}
	filePath := filepath.Join(workspace, "file.txt")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	_, err = ResolveExecCWD(workspace, "file.txt")
	var pathErr *Error
	if !errors.As(err, &pathErr) || pathErr.Kind != KindNotADirectory {
		t.Fatalf("file cwd err = %T %v; want not_a_directory", err, err)
	}
	_, err = ResolveExecCWD(workspace, "/tmp/tetral-runtime/tasks/task_1")
	if !errors.As(err, &pathErr) || pathErr.Kind != KindForbiddenPath {
		t.Fatalf("forbidden cwd err = %T %v; want forbidden_path", err, err)
	}
	if pathErr.Path != "" {
		t.Fatalf("forbidden cwd path = %q; want redacted", pathErr.Path)
	}
	link := filepath.Join(workspace, "runtime-link")
	forbiddenTarget := makeForbiddenFixture(t)
	if err := os.Symlink(forbiddenTarget, link); err != nil {
		t.Fatalf("symlink forbidden cwd: %v", err)
	}
	_, err = ResolveExecCWD(workspace, "runtime-link")
	if !errors.As(err, &pathErr) || pathErr.Kind != KindForbiddenPath {
		t.Fatalf("forbidden symlink cwd err = %T %v; want forbidden_path", err, err)
	}
	if pathErr.Path != "" || strings.Contains(pathErr.Error(), forbiddenTarget) {
		t.Fatalf("forbidden symlink cwd leaked internal path: path=%q error=%q", pathErr.Path, pathErr.Error())
	}
}

func makeForbiddenFixture(t *testing.T) string {
	t.Helper()
	root := "/tmp/tetral-runtime"
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir forbidden root: %v", err)
	}
	dir, err := os.MkdirTemp(root, "pathsafe-test-*")
	if err != nil {
		t.Fatalf("mkdir forbidden fixture: %v", err)
	}
	t.Cleanup(func() {
		if strings.HasPrefix(dir, root+string(filepath.Separator)) {
			_ = os.RemoveAll(dir)
		}
	})
	if err := os.WriteFile(filepath.Join(dir, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("write forbidden fixture: %v", err)
	}
	return dir
}

func testResolver(workspace string) Resolver {
	return Resolver{
		WorkspaceRoot: workspace,
		Roots: []protocol.Root{
			{Path: workspace, Mode: protocol.RootModeReadWrite},
		},
	}
}
