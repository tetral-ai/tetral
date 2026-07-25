// Package patch is the apply_patch engine: it parses the freeform patch text,
// verifies every operation against the filesystem, and applies them in two
// phases.
//
// OWNS:
//   - Patch grammar parsing and preprocessing (parser.go).
//   - Placement/replacement matching via seek (apply.go).
//   - Two-phase verify (verifyPlan) and apply (applyPlan).
//   - patchReadMaxBytes and patchDiagnosticMaxBytes (apply.go).
//
// STATE MACHINE — the core guarantee is verify-then-apply:
//
//	Phase 1 (verifyPlan): parse, duplicate-path check, resolve and contain
//	  every source and destination, read targets, compute every resulting file
//	  in memory. Zero writes. Any failure here (patch_parse / patch_verify /
//	  path error) leaves the filesystem untouched.
//	Phase 2 (applyPlan): apply in patch order through the atomic write
//	  sequence (move writes the destination, then unlinks the source; delete
//	  unlinks).
//
// INVARIANTS:
//   - Semantic failures can only occur in phase 1, so a semantic error never
//     leaves partial state. The only way phase 2 stops midway is a raw I/O
//     failure (disk full, permission), which returns write_failed with
//     error.detail.applied — the paths already committed, in order — and
//     failed_at. Partial application is therefore always an exactly-reported
//     I/O failure, never silent.
//   - Two operations resolving to the same target path, including a move
//     destination colliding with any other operation's path, are rejected as
//     patch_verify duplicate-path. Under two-phase verify a sequential apply
//     would let the later operation silently discard the earlier one's result,
//     which is why duplicates are illegal.
//   - Add creates parents and overwrites an existing file. Add and Update
//     preserve Codex trailing-blank bytes, including a zero-byte empty Add.
//     A move+modify is a single UpdateFile operation (write destination,
//     remove source), not two ops.
//
// INTENTIONAL CODEX DIFFERENCES:
//   - Duplicate target paths are rejected because two-phase verification must
//     compute one unambiguous result for every target before any write.
//   - Consecutive @@ markers are accepted as a lenient grammar extension.
//   - Path containment is enforced in-process through pathsafe before I/O.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/patch/parser.go
//   - internal/sandbox/helper/internal/patch/apply.go
//   - internal/sandbox/helper/internal/filetool/write.go (the shared atomic
//     write sequence)
package patch
