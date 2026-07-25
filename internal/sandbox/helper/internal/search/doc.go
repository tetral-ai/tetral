// Package search owns the Grep and Glob tools, both implemented over rg.
//
// OWNS:
//   - Grep (files / content / count modes) and Glob.
//   - The rg subprocess: its process group, bounded stdout streaming, and kill.
//   - Grep/Glob cap and deadline constants (search.go).
//
// STATE MACHINE: none; each call runs one rg process to a stop condition.
//
// INVARIANTS:
//   - Both stream rg stdout with capture-point bounding and SIGKILL the rg
//     process group as soon as the caps are met, so rg never runs to completion
//     on an oversized result set. A 20 s deadline kills rg and returns timeout
//     with no partial results.
//   - rg exit codes map as: 0 and 1 are success (1 means zero matches — an
//     empty result, not an error), and 2 is invalid_input with a stderr
//     excerpt bounded to 4 KiB, because regex/glob errors are model-fixable.
//   - Grep and Glob diverge on rg flags on purpose and must stay divergent:
//     Grep respects .gitignore and sorts files newest-first; Glob passes
//     --no-ignore --hidden and emits rg's --sort=modified oldest-first order.
//     The asymmetry mirrors the Claude-family tools the models are tuned
//     against.
//   - Grep mode semantics: files collects up to 10000 candidates, then
//     mtime-sorts newest-first and slices by offset+head_limit; content streams
//     matching lines and stops at head_limit matches or 50 KiB; count uses
//     rg --count where head_limit bounds the number of output rows, NOT the
//     cumulative match_count, which still sums the reported per-row counts.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/search/search.go
package search
