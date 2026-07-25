package filetool

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/pathsafe"
	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

type WriteResult struct {
	Created      bool `json:"created"`
	BytesWritten int  `json:"bytes_written"`
}

type writeInput struct {
	Path    string  `json:"path"`
	Content *string `json:"content"`
}

var renameForAtomicWrite = os.Rename

func Write(payload protocol.Payload) (WriteResult, *protocol.ToolError) {
	var input writeInput
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return WriteResult{}, invalidInput("input must be an object")
	}
	pathValue := input.Path
	if pathValue == "" {
		return WriteResult{}, invalidInput("path is required")
	}
	if input.Content == nil {
		return WriteResult{}, invalidInput("content is required")
	}
	resolved, err := (pathsafe.Resolver{WorkspaceRoot: payload.WorkspaceRoot, Roots: payload.Roots}).Resolve(pathValue, true)
	if err != nil {
		if isWritePathIOFailure(err) {
			return WriteResult{}, &protocol.ToolError{Kind: "write_failed", Message: "write failed"}
		}
		return WriteResult{}, pathsafe.ToolError(err)
	}
	info, statErr := os.Stat(resolved.Path)
	exists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return WriteResult{}, &protocol.ToolError{Kind: "write_failed", Message: "target could not be inspected", Path: resolved.DisplayPath}
	}
	if exists && info.IsDir() {
		return WriteResult{}, &protocol.ToolError{Kind: "is_directory", Message: "path is a directory", Path: resolved.DisplayPath}
	}
	mode := os.FileMode(0o644)
	if exists {
		mode = preservedFileMode(info.Mode())
	}
	content := []byte(*input.Content)
	if err := atomicWriteFile(resolved.Path, content, mode); err != nil {
		return WriteResult{}, &protocol.ToolError{Kind: "write_failed", Message: "write failed", Path: resolved.DisplayPath}
	}
	return WriteResult{Created: !exists, BytesWritten: len(content)}, nil
}

func preservedFileMode(mode os.FileMode) os.FileMode {
	return mode & (os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
}

func isWritePathIOFailure(err error) bool {
	var pathErr *pathsafe.Error
	if !errors.As(err, &pathErr) || pathErr.Kind != pathsafe.KindInvalidInput {
		return false
	}
	return strings.Contains(pathErr.Message, "not a directory")
}

func atomicWriteFile(target string, content []byte, mode os.FileMode) error {
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // Workspace-created dirs must remain traversable by the runtime user as well as the helper user.
		return err
	}
	tmp, err := createAtomicTemp(dir, filepath.Base(target))
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	n, err := tmp.Write(content)
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if n != len(content) {
		_ = tmp.Close()
		return io.ErrShortWrite
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}
	if err := renameForAtomicWrite(tmpPath, target); err != nil {
		return err
	}
	committed = true
	fsyncDirectoryBestEffort(dir)
	return nil
}

func createAtomicTemp(dir string, base string) (*os.File, error) {
	for attempt := 0; attempt < 16; attempt++ {
		name, err := atomicTempName(dir, base)
		if err != nil {
			return nil, err
		}
		file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // temp path is generated in the resolved target directory.
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("could not allocate temporary file")
}

func atomicTempName(dir string, base string) (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return filepath.Join(dir, "."+base+".tetral-tmp-"+hex.EncodeToString(raw[:])), nil
}

func fsyncDirectoryBestEffort(dir string) {
	handle, err := os.Open(dir) //nolint:gosec // dir is the resolved target directory.
	if err != nil {
		return
	}
	defer func() { _ = handle.Close() }()
	_ = handle.Sync()
}
