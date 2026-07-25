// Package pathsafe owns workspace containment for every path-taking tool:
// read, write, edit, apply_patch (each patch path), the grep/glob search root,
// and view_image. exec cwd resolves through a separate, looser entrypoint.
//
// Containment is checked against the payload roots set, not against
// workspace_root alone. The sandbox layout places projected read-only file
// resources and the model-writable output target outside /workspace, and file
// tools must reach them. workspace_root keeps only two roles: the join base for
// relative input paths and the default cwd / search root.
//
// OWNS:
//   - Resolver.Resolve: path resolution + containment for path-taking tools.
//   - ResolveExecCWD: the exec-cwd resolver (existence only, not contained).
//   - Resolved / Error: the resolved-path and typed-error shapes.
//   - forbiddenRoots: the runtime-directory denylist.
//
// STATE MACHINE: none; each call resolves one path independently.
//
// INVARIANTS:
//   - The most-specific root decides mode: when several roots contain the
//     resolved path, the one with the deepest resolved path wins; equal-depth
//     ties resolve to read. A write-class target (write, edit, apply_patch
//     targets and move destinations) whose deciding root is read-mode fails
//     path_escape, even when a writable ancestor root also contains it.
//   - Forbidden runtime roots are checked independently of containment, so the
//     error kind stays forbidden_path even if a runtime root ever sits inside a
//     workspace root. The check is redundant with containment on purpose, to
//     keep that error kind stable.
//   - Kind checks (not_found / is_directory) run only after containment
//     succeeds, so probing an escaping path cannot tell an existing outside
//     file from a missing one; both surface as path_escape.
//   - Reporting shape is fixed by root: paths under workspace_root are reported
//     workspace-relative; paths under any other root are reported as resolved
//     absolute paths; paths outside every root never appear in an envelope.
//   - exec cwd is exempt from containment: ResolveExecCWD requires only an
//     existing directory not under a forbidden root, defaulting to
//     workspace_root. A shell can cd anywhere in the sandbox, so containing cwd
//     would add ceremony without a boundary; Resolve and ResolveExecCWD are
//     asymmetric for that reason.
//   - Projected read-only file resources are enforced twice. Each projected
//     file is its own most-specific read root, so a file-tool write to it fails
//     path_escape here. Preparation-time mount-level read-only is the backstop
//     for writers that bypass containment (exec/Bash) and for a write-class op
//     that passes containment because a projected file is absent from roots,
//     which then fails with the OS permission error as write_failed.
//   - TOCTOU between resolve and I/O is accepted: the sandbox is single-tenant,
//     so the only writer racing the helper is the model racing itself. The
//     helper takes no path locks and relies on whole-file temp+rename to bound
//     same-file races.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/pathsafe/pathsafe.go
//   - internal/sandbox/helper/internal/filetool/write.go
//   - internal/sandbox/helper/internal/filetool/edit.go
//   - internal/sandbox/helper/internal/filetool/read.go
//   - internal/sandbox/helper/internal/patch/apply.go
//   - internal/sandbox/helper/internal/search/search.go
//   - internal/sandbox/helper/internal/media/view_image.go
package pathsafe
