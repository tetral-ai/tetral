package driver

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/sandbox"
)

// memoryProjectionStagingRoot is the staging directory for projection swaps.
// It is a sibling of the store mounts and never inside any mount_path (store
// slugs cannot begin with a dot). It bounds where staged trees are built. The
// value must share the projection's filesystem so that every mv into a mount
// is an atomic rename(2): /tmp is forbidden here because it may be a tmpfs,
// where mv degrades to copy+unlink and can expose partial bytes to a reader.
// The root is created via sudo but owned by the runtime user (0700): Daytona's
// filesystem transport operates as the runtime user, so uploads into the stage
// are only possible under runtime-user ownership. Staged trees are frozen to
// root ownership before the swap, and the in-sandbox sha256sum check against
// the engine-authored manifest gates the swap.
// UPDATE-WITH: memoryProjectionMountPath / validateMemoryStoreMountPath (which
// reject mounts overlapping this path); the staging setup in
// memoryProjectionStageRootCommand.
const memoryProjectionStagingRoot = "/mnt/memory/.staging"

// Memory projection driver — two command sequences, one DaytonaHelperExecutor
// surface, serving MaterializeMemoryStore (resource convergence) and
// RefreshMemoryProjection (live, post-mutation).
//
// SEQUENCE CHOICE (mid-read safety is the coupling): activation-time materialization
// pushes a WHOLE-STORE swap — build a staged tree, then rm -rf MOUNT and mv it into
// place. Live refresh instead pushes PER-FILE mv -f operations. Live refresh must
// never whole-swap, because a running model may be mid-read: per-file rename keeps
// every path the model observes either old-complete or new-complete, never absent.
//
// INVARIANTS:
//   - The materialize swap uses mv -T deliberately: if anything recreated MOUNT
//     inside the rm -rf-to-mv window, mv -T fails loudly instead of nesting the
//     staged tree inside the recreated directory.
//   - The in-sandbox `sha256sum --check` IS the materialization verification. The
//     SHA256SUMS manifest carries the durable memories.content_sha256 column verbatim
//     (never recomputed here), so a passing check proves the landed bytes match the
//     durable claim; a non-zero exit fails the step.
//   - Live-refresh upserts prefix the root command with `umask 022` so every
//     intermediate directory that mkdir -p creates gets 0755. `mkdir -p -m` alone
//     would set the mode only on the leaf and leave intermediates at the shell umask.
//   - Live-refresh remove type-tests the target first: a regular file is rm -f'd and
//     its now-empty ancestors rmdir'd deepest-first (never the mount root), while a
//     directory target is a no-op success. The directory no-op is correct only under
//     prefix-freeness (enforced in package sandbox): a directory exists only to hold
//     active descendant memories, so the projection already matches truth. Live
//     refresh never does recursive deletes.
//
// UPDATE-WITH: internal/sandbox/memory_projection.go (the materialization orchestrator
// and the prefix-freeness invariant the remove no-op depends on).

func (e *DaytonaHelperExecutor) MaterializeMemoryStore(ctx context.Context, materialization sandbox.MemoryStoreMaterialization) error {
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, materialization.ProviderSandboxID)
	if err != nil {
		return err
	}
	mountPath, err := memoryProjectionMountPath(materialization.MountPath)
	if err != nil {
		return err
	}
	files := append([]sandbox.MemorySnapshotFile(nil), materialization.Files...)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	if len(files) == 0 {
		command := sudoShellCommand(strings.Join([]string{
			"rm -rf " + shellQuote(mountPath),
			"mkdir -p " + shellQuote(mountPath),
			"chown root:root " + shellQuote(mountPath),
			"chmod 0755 " + shellQuote(mountPath),
		}, " && "))
		return e.executeMemoryProjectionCommand(ctx, sandboxHandle, command)
	}
	for _, file := range files {
		if err := validateProjectedMemoryPath(file.Path); err != nil {
			return err
		}
	}
	archive, manifest, err := buildMemoryStoreArchive(files)
	if err != nil {
		return err
	}
	stage := memoryProjectionStagePath("materialize")
	// The stage root must exist before the runtime-user filesystem transport
	// can create the stage inside it, so the sudo'd root preparation runs
	// first.
	if err := e.executeMemoryProjectionCommand(ctx, sandboxHandle, memoryProjectionStageRootCommand()); err != nil {
		return err
	}
	if err := sandboxHandle.FileSystem.CreateFolder(ctx, stage, options.WithMode("0700")); err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			// Cleanup goes through sudo rather than the filesystem transport:
			// after the freeze the staged tree is root-owned and the
			// runtime-user transport can no longer delete it.
			_ = e.executeMemoryProjectionCommand(context.WithoutCancel(ctx), sandboxHandle, "sudo rm -rf -- "+shellQuote(stage))
		}
	}()
	if err := sandboxHandle.FileSystem.UploadFileStream(ctx, bytes.NewReader(archive), stage+"/store.tar.gz"); err != nil {
		return err
	}
	if err := sandboxHandle.FileSystem.UploadFileStream(ctx, bytes.NewReader(manifest), stage+"/SHA256SUMS"); err != nil {
		return err
	}
	command := sudoShellCommand(strings.Join([]string{
		"mkdir -p " + shellQuote(stage+"/extract"),
		"tar -xzf " + shellQuote(stage+"/store.tar.gz") + " -C " + shellQuote(stage+"/extract"),
		"(cd " + shellQuote(stage+"/extract") + " && sha256sum --check --quiet " + shellQuote(stage+"/SHA256SUMS") + ")",
		"chown -R root:root " + shellQuote(stage+"/extract"),
		"find " + shellQuote(stage+"/extract") + " -type d -exec chmod 0755 {} +",
		"find " + shellQuote(stage+"/extract") + " -type f -exec chmod 0644 {} +",
		"mkdir -p " + shellQuote(path.Dir(mountPath)),
		"rm -rf " + shellQuote(mountPath),
		"mv -T " + shellQuote(stage+"/extract") + " " + shellQuote(mountPath),
		"rm -rf " + shellQuote(stage),
	}, " && "))
	if err := e.executeMemoryProjectionCommand(ctx, sandboxHandle, command); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (e *DaytonaHelperExecutor) RemoveMemoryStore(ctx context.Context, providerSandboxID string, mount sandbox.MemoryStoreMount) error {
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, providerSandboxID)
	if err != nil {
		return err
	}
	mountPath, err := memoryProjectionMountPath(mount.MountPath)
	if err != nil {
		return err
	}
	return e.executeMemoryProjectionCommand(ctx, sandboxHandle, "sudo rm -rf -- "+shellQuote(mountPath))
}

func (e *DaytonaHelperExecutor) RefreshMemoryProjection(ctx context.Context, refresh MemoryProjectionRefresh) error {
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, refresh.Target.ProviderSandboxID)
	if err != nil {
		return err
	}
	if len(refresh.MountPaths) == 0 || len(refresh.Ops) == 0 {
		return nil
	}
	stage := memoryProjectionStagePath("refresh")
	stageCreated := false
	defer func() {
		if stageCreated {
			_ = e.executeMemoryProjectionCommand(context.WithoutCancel(ctx), sandboxHandle, "sudo rm -rf -- "+shellQuote(stage))
		}
	}()
	uploadIndex := 0
	for _, mount := range refresh.MountPaths {
		mountPath, err := memoryProjectionMountPath(mount)
		if err != nil {
			return err
		}
		for _, op := range refresh.Ops {
			if err := validateProjectedMemoryPath(op.RelativePath); err != nil {
				return err
			}
			switch op.Kind {
			case "upsert":
				if !stageCreated {
					if err := e.executeMemoryProjectionCommand(ctx, sandboxHandle, memoryProjectionStageRootCommand()); err != nil {
						return err
					}
					if err := sandboxHandle.FileSystem.CreateFolder(ctx, stage, options.WithMode("0700")); err != nil {
						return err
					}
					stageCreated = true
				}
				stagedPath := fmt.Sprintf("%s/f%d", stage, uploadIndex)
				uploadIndex++
				if err := sandboxHandle.FileSystem.UploadFileStream(ctx, strings.NewReader(op.Content), stagedPath); err != nil {
					return err
				}
				finalPath := mountPath + op.RelativePath
				command := sudoShellCommand(strings.Join([]string{
					"umask 022",
					"mkdir -p " + shellQuote(path.Dir(finalPath)),
					"if [ -e " + shellQuote(finalPath) + " ] && [ ! -f " + shellQuote(finalPath) + " ]; then echo 'memory projection target is not a regular file' >&2; exit 1; fi",
					"chown root:root " + shellQuote(stagedPath),
					"chmod 0644 " + shellQuote(stagedPath),
					"mv -f " + shellQuote(stagedPath) + " " + shellQuote(finalPath),
				}, " && "))
				if err := e.executeMemoryProjectionCommand(ctx, sandboxHandle, command); err != nil {
					return err
				}
			case "remove":
				finalPath := mountPath + op.RelativePath
				command := memoryProjectionRemoveCommand(mountPath, finalPath)
				if err := e.executeMemoryProjectionCommand(ctx, sandboxHandle, command); err != nil {
					return err
				}
			default:
				return fmt.Errorf("memory projection op kind %q is unsupported", op.Kind)
			}
		}
	}
	if stageCreated {
		if err := e.executeMemoryProjectionCommand(ctx, sandboxHandle, "sudo rm -rf -- "+shellQuote(stage)); err != nil {
			return err
		}
		stageCreated = false
	}
	return nil
}

func memoryProjectionRemoveCommand(mountPath string, finalPath string) string {
	parts := []string{"rm -f " + shellQuote(finalPath)}
	for _, dir := range memoryProjectionEmptyAncestorDirs(mountPath, finalPath) {
		parts = append(parts, "(rmdir "+shellQuote(dir)+" || true)")
	}
	removeExactFile := strings.Join(parts, " && ")
	return sudoShellCommand(strings.Join([]string{
		"ANCESTOR=" + shellQuote(finalPath),
		"while [ \"$ANCESTOR\" != " + shellQuote(mountPath) + " ] && [ ! -e \"$ANCESTOR\" ]; do ANCESTOR=$(dirname \"$ANCESTOR\"); done",
		"if [ -f " + shellQuote(finalPath) + " ]; then " + removeExactFile + "; elif [ -d " + shellQuote(finalPath) + " ] && [ ! -L " + shellQuote(finalPath) + " ]; then :; elif [ \"$ANCESTOR\" != " + shellQuote(mountPath) + " ] && [ -f \"$ANCESTOR\" ]; then :; else " + removeExactFile + "; fi",
	}, " && "))
}

// memoryProjectionStageRootCommand prepares the staging root under the
// root-owned projection root. sudo creates it; the runtime user owns it so the
// Daytona filesystem transport (which operates as the runtime user) can create
// stages and upload payloads inside it.
func memoryProjectionStageRootCommand() string {
	return "sudo install -d -m 0700 -o " + shellQuote(RuntimeUser) + " -g " + shellQuote(RuntimeUser) + " " + shellQuote(memoryProjectionStagingRoot)
}

// sudoShellCommand runs a full command chain as root. Daytona executes
// commands as the runtime user, and these chains create, chown, and swap
// root-owned projection state; wrapping the whole chain preserves the original
// root-execution semantics (umask, ordering, atomic rename) unchanged.
func sudoShellCommand(script string) string {
	return "sudo sh -c " + shellQuote(script)
}

func (e *DaytonaHelperExecutor) memoryProjectionSandbox(ctx context.Context, providerSandboxID string) (daytonaSandboxHandle, error) {
	if e == nil || e.client == nil {
		return daytonaSandboxHandle{}, errors.New("daytona sandbox client is unavailable")
	}
	if providerSandboxID == "" {
		return daytonaSandboxHandle{}, errors.New("provider sandbox id is required")
	}
	sandboxHandle, err := e.client.Get(ctx, providerSandboxID)
	if err != nil {
		return daytonaSandboxHandle{}, MarkProviderOperationNotSubmitted(mapDaytonaError(sandbox.StageMountResources, err))
	}
	if sandboxHandle.Process == nil || sandboxHandle.FileSystem == nil {
		return daytonaSandboxHandle{}, errors.New("daytona sandbox is missing process or filesystem service")
	}
	return sandboxHandle, nil
}

func buildMemoryStoreArchive(files []sandbox.MemorySnapshotFile) ([]byte, []byte, error) {
	var archive bytes.Buffer
	gzipWriter := gzip.NewWriter(&archive)
	tarWriter := tar.NewWriter(gzipWriter)
	writtenDirs := map[string]bool{"./": true}
	var manifest strings.Builder
	for _, file := range files {
		entryName := "." + file.Path
		dirs := memoryProjectionTarDirs(entryName)
		for _, dir := range dirs {
			if writtenDirs[dir] {
				continue
			}
			if err := tarWriter.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0755}); err != nil {
				return nil, nil, err
			}
			writtenDirs[dir] = true
		}
		body := []byte(file.Content)
		if err := tarWriter.WriteHeader(&tar.Header{Name: entryName, Typeflag: tar.TypeReg, Mode: 0644, Size: int64(len(body))}); err != nil {
			return nil, nil, err
		}
		if _, err := tarWriter.Write(body); err != nil {
			return nil, nil, err
		}
		manifest.WriteString(file.ContentSHA256)
		manifest.WriteString("  ")
		manifest.WriteString(entryName)
		manifest.WriteByte('\n')
	}
	if err := tarWriter.Close(); err != nil {
		return nil, nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, nil, err
	}
	return archive.Bytes(), []byte(manifest.String()), nil
}

func memoryProjectionTarDirs(entryName string) []string {
	var dirs []string
	dir := path.Dir(entryName)
	for dir != "." && dir != "/" {
		dirs = append(dirs, dir+"/")
		dir = path.Dir(dir)
	}
	for left, right := 0, len(dirs)-1; left < right; left, right = left+1, right-1 {
		dirs[left], dirs[right] = dirs[right], dirs[left]
	}
	return dirs
}

func memoryProjectionMountPath(mountPath string) (string, error) {
	mountPath = strings.TrimRight(mountPath, "/")
	if mountPath == "" || strings.Contains(mountPath, "\x00") {
		return "", errors.New("memory mount path is required")
	}
	if clean := path.Clean(mountPath); clean != mountPath {
		return "", errors.New("memory mount path must be lexically clean")
	}
	if mountPath == "/mnt/memory" || !strings.HasPrefix(mountPath, "/mnt/memory/") {
		return "", errors.New("memory mount path must name a store below /mnt/memory")
	}
	if mountPath == memoryProjectionStagingRoot || strings.HasPrefix(mountPath, memoryProjectionStagingRoot+"/") {
		return "", errors.New("memory mount path must not overlap projection staging")
	}
	return mountPath, nil
}

func validateProjectedMemoryPath(memoryPath string) error {
	if !strings.HasPrefix(memoryPath, "/") || memoryPath == "/" {
		return errors.New("memory projection path must be absolute")
	}
	for _, segment := range strings.Split(strings.TrimPrefix(memoryPath, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("memory projection path has an invalid segment")
		}
	}
	return nil
}

func memoryProjectionStagePath(prefix string) string {
	return memoryProjectionStagingRoot + "/" + safePayloadID(prefix+"-"+id.New("memproj_"))
}

func memoryProjectionEmptyAncestorDirs(mountPath string, finalPath string) []string {
	var dirs []string
	for dir := path.Dir(finalPath); dir != "." && dir != "/" && dir != mountPath && strings.HasPrefix(dir, mountPath+"/"); dir = path.Dir(dir) {
		dirs = append(dirs, dir)
	}
	return dirs
}

func (e *DaytonaHelperExecutor) executeMemoryProjectionCommand(ctx context.Context, sandboxHandle daytonaSandboxHandle, command string) error {
	if e.commandTimeout <= 0 {
		return errors.New("daytona command timeout is required")
	}
	opts := []func(*options.ExecuteCommand){options.WithExecuteTimeout(e.commandTimeout)}
	response, err := sandboxHandle.Process.ExecuteCommand(ctx, command, opts...)
	if err != nil {
		return err
	}
	if response == nil {
		return errors.New("memory projection command returned no response")
	}
	if response.ExitCode != 0 {
		return fmt.Errorf("memory projection command exited with code %d", response.ExitCode)
	}
	return nil
}
