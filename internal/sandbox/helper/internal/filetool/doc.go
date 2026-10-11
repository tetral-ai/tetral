// Package filetool owns the Read, Write, and Edit tools and the atomic write
// sequence they share.
//
// OWNS:
//   - Read: bounded windowed reads, media/PDF detection, binary detection.
//   - Write: atomic file creation and replacement.
//   - Edit: exact substring replacement with a single quote-normalization pass.
//   - atomicWriteFile: the whole-file temp+rename mutation used by Write and
//     Edit (and duplicated in the patch package).
//
// STATE MACHINE: none; each call is a single operation.
//
// INVARIANTS:
//   - Every mutation goes through the atomic sequence: mkdir the parent, create
//     a sibling temp file O_EXCL 0600, write, fsync, close, chmod to the
//     preserved mode, rename over the target, then best-effort fsync the
//     directory. A crash leaves the target either the full old bytes or the
//     full new bytes, never a torn mix. This exact sequence is duplicated in
//     internal/patch/apply.go and the crash-safety guarantee must hold in both.
//   - Write, Edit, and apply_patch are at-most-once, not idempotent, at this
//     layer. The caller must consult its own durable RunTool record before
//     re-invoking.
//   - Read emits the raw window bytes with no line numbering. The runtime
//     formatter adds cat -n line-number prefixes before model delivery.
//   - Read pages by whole lines. A page is a run of whole lines from
//     start_line (a line cut at 2000 runes counts as whole); only a first line
//     that alone exceeds the byte budget or the 200,000-byte envelope cap is
//     shown cut, and that page holds only that line. next_offset is the first
//     line not shown in full, or the line after a cut first line, so no line
//     is skipped and every page advances by at least one line.
//     FitReadResultForEnvelope keeps the longest such run whose envelope fits
//     the cap with the widest metrics member the caller can add.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/filetool/read.go
//   - internal/sandbox/helper/internal/filetool/write.go
//   - internal/sandbox/helper/internal/filetool/edit.go
//   - internal/sandbox/helper/internal/patch/apply.go (duplicate atomic write)
package filetool
