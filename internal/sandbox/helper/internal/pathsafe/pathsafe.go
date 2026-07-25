package pathsafe

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/tetral-ai/tetral/internal/sandbox/helper/protocol"
)

const (
	KindInvalidInput  = "invalid_input"
	KindPathEscape    = "path_escape"
	KindForbiddenPath = "forbidden_path"
	KindNotFound      = "not_found"
	KindNotADirectory = "not_a_directory"
)

var forbiddenRoots = []string{
	"/tmp/tetral-runtime",
	"/dev/shm/tetral-runtime",
}

const maxSymlinkResolveDepth = 40

type Error struct {
	Kind    string
	Message string
	Path    string
	Err     error
}

func ToolError(err error) *protocol.ToolError {
	var pathErr *Error
	if errors.As(err, &pathErr) {
		return &protocol.ToolError{Kind: pathErr.Kind, Message: pathErr.Message, Path: pathErr.Path}
	}
	return &protocol.ToolError{Kind: KindInvalidInput, Message: err.Error()}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type Resolver struct {
	WorkspaceRoot string
	Roots         []protocol.Root
}

type Resolved struct {
	Path        string
	DisplayPath string
	Mode        string
}

type resolvedRoot struct {
	path string
	mode string
}

func (r Resolver) Validate() error {
	_, _, err := r.validate()
	return err
}

func (r Resolver) Resolve(inputPath string, requireWrite bool) (Resolved, error) {
	if strings.TrimSpace(inputPath) == "" {
		return Resolved{}, &Error{Kind: KindInvalidInput, Message: "path is required"}
	}
	workspaceRoot, roots, err := r.validate()
	if err != nil {
		return Resolved{}, err
	}
	candidate := inputPath
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(workspaceRoot, candidate)
	}
	cleaned := filepath.Clean(candidate)
	if isForbidden(cleaned) {
		return Resolved{}, &Error{Kind: KindForbiddenPath, Message: "path is under a forbidden runtime directory"}
	}
	resolvedPath, err := resolvePhysical(cleaned)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Resolved{}, &Error{Kind: KindNotFound, Message: "path does not exist", Path: displayPath(cleaned, workspaceRoot), Err: err}
		}
		return Resolved{}, &Error{Kind: KindInvalidInput, Message: err.Error(), Err: err}
	}
	if isForbidden(resolvedPath) {
		return Resolved{}, &Error{Kind: KindForbiddenPath, Message: "path is under a forbidden runtime directory"}
	}
	var best resolvedRoot
	bestOK := false
	for i := range roots {
		root := roots[i]
		if !underOrEqual(resolvedPath, root.path) {
			continue
		}
		if !bestOK || len(root.path) > len(best.path) ||
			(len(root.path) == len(best.path) && best.mode == protocol.RootModeReadWrite && root.mode == protocol.RootModeRead) {
			best = root
			bestOK = true
		}
	}
	if !bestOK {
		return Resolved{}, &Error{Kind: KindPathEscape, Message: "path leaves the allowed roots"}
	}
	if requireWrite && best.mode != protocol.RootModeReadWrite {
		return Resolved{}, &Error{Kind: KindPathEscape, Message: "path is not writable from this tool"}
	}
	return Resolved{Path: resolvedPath, DisplayPath: displayPath(resolvedPath, workspaceRoot), Mode: best.mode}, nil
}

func ResolveExecCWD(workspaceRoot string, inputPath string) (Resolved, error) {
	if inputPath == "" {
		inputPath = workspaceRoot
	}
	if !filepath.IsAbs(workspaceRoot) {
		return Resolved{}, &Error{Kind: KindInvalidInput, Message: "workspace_root must be absolute"}
	}
	resolvedWorkspace, err := filepath.EvalSymlinks(filepath.Clean(workspaceRoot))
	if err != nil {
		return Resolved{}, &Error{Kind: KindInvalidInput, Message: "workspace_root must exist", Err: err}
	}
	candidate := inputPath
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(resolvedWorkspace, candidate)
	}
	cleaned := filepath.Clean(candidate)
	if isForbidden(cleaned) {
		return Resolved{}, &Error{Kind: KindForbiddenPath, Message: "cwd is under a forbidden runtime directory"}
	}
	resolvedPath, err := resolvePhysical(cleaned)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Resolved{}, &Error{Kind: KindNotFound, Message: "cwd does not exist", Path: displayPath(cleaned, resolvedWorkspace)}
		}
		return Resolved{}, &Error{Kind: KindInvalidInput, Message: "cwd must resolve", Err: err}
	}
	if isForbidden(resolvedPath) {
		return Resolved{}, &Error{Kind: KindForbiddenPath, Message: "cwd is under a forbidden runtime directory"}
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Resolved{}, &Error{Kind: KindNotFound, Message: "cwd does not exist", Path: displayPath(resolvedPath, resolvedWorkspace)}
		}
		return Resolved{}, &Error{Kind: KindInvalidInput, Message: "cwd could not be inspected", Path: displayPath(resolvedPath, resolvedWorkspace), Err: err}
	}
	if !info.IsDir() {
		return Resolved{}, &Error{Kind: KindNotADirectory, Message: "cwd is not a directory", Path: displayPath(resolvedPath, resolvedWorkspace)}
	}
	return Resolved{Path: resolvedPath, DisplayPath: displayPath(resolvedPath, resolvedWorkspace), Mode: protocol.RootModeReadWrite}, nil
}

func (r Resolver) validate() (string, []resolvedRoot, error) {
	if !filepath.IsAbs(r.WorkspaceRoot) {
		return "", nil, &Error{Kind: KindInvalidInput, Message: "workspace_root must be absolute"}
	}
	workspaceRoot, err := filepath.EvalSymlinks(filepath.Clean(r.WorkspaceRoot))
	if err != nil {
		return "", nil, &Error{Kind: KindInvalidInput, Message: "workspace_root must exist", Err: err}
	}
	info, err := os.Stat(workspaceRoot)
	if err != nil {
		return "", nil, &Error{Kind: KindInvalidInput, Message: "workspace_root must exist", Err: err}
	}
	if !info.IsDir() {
		return "", nil, &Error{Kind: KindInvalidInput, Message: "workspace_root must be a directory"}
	}
	roots := make([]resolvedRoot, 0, len(r.Roots))
	hasWorkspaceRoot := false
	for _, root := range r.Roots {
		cleaned := filepath.Clean(root.Path)
		if !filepath.IsAbs(cleaned) || cleaned == string(filepath.Separator) {
			return "", nil, &Error{Kind: KindInvalidInput, Message: "root path must be absolute and non-root"}
		}
		if root.Mode != protocol.RootModeRead && root.Mode != protocol.RootModeReadWrite {
			return "", nil, &Error{Kind: KindInvalidInput, Message: "root mode is invalid"}
		}
		resolvedPath, err := resolvePhysical(cleaned)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", nil, &Error{Kind: KindInvalidInput, Message: "root path must resolve", Err: err}
		}
		if resolvedPath == workspaceRoot && root.Mode == protocol.RootModeReadWrite {
			hasWorkspaceRoot = true
		}
		roots = append(roots, resolvedRoot{path: resolvedPath, mode: root.Mode})
	}
	if !hasWorkspaceRoot {
		return "", nil, &Error{Kind: KindInvalidInput, Message: "workspace_root must appear as read_write root"}
	}
	return workspaceRoot, roots, nil
}

func resolvePhysical(cleaned string) (string, error) {
	return resolvePhysicalDepth(filepath.Clean(cleaned), 0)
}

func resolvePhysicalDepth(cleaned string, depth int) (string, error) {
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	current := cleaned
	suffix := []string{}
	for {
		if current == "" || current == "." {
			return "", os.ErrNotExist
		}
		if info, err := os.Lstat(current); err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				if depth >= maxSymlinkResolveDepth {
					return "", errors.New("too many symlink evaluations")
				}
				target, err := os.Readlink(current)
				if err != nil {
					return "", err
				}
				if !filepath.IsAbs(target) {
					target = filepath.Join(filepath.Dir(current), target)
				}
				resolvedTarget, err := resolvePhysicalDepth(filepath.Clean(target), depth+1)
				if err != nil {
					return "", err
				}
				parts := append([]string{resolvedTarget}, reverseStrings(suffix)...)
				return filepath.Clean(filepath.Join(parts...)), nil
			}
			ancestor, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", err
			}
			parts := append([]string{ancestor}, reverseStrings(suffix)...)
			return filepath.Clean(filepath.Join(parts...)), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", os.ErrNotExist
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func reverseStrings(values []string) []string {
	out := make([]string, len(values))
	for i := range values {
		out[len(values)-1-i] = values[i]
	}
	return out
}

func underOrEqual(pathValue string, root string) bool {
	if pathValue == root {
		return true
	}
	return strings.HasPrefix(pathValue, root+string(filepath.Separator))
}

func isForbidden(pathValue string) bool {
	cleaned := filepath.Clean(pathValue)
	for _, root := range forbiddenRoots {
		if underOrEqual(cleaned, root) {
			return true
		}
	}
	return false
}

func displayPath(pathValue string, workspaceRoot string) string {
	if pathValue == workspaceRoot {
		return "."
	}
	if underOrEqual(pathValue, workspaceRoot) {
		if rel, err := filepath.Rel(workspaceRoot, pathValue); err == nil {
			return rel
		}
	}
	return pathValue
}
