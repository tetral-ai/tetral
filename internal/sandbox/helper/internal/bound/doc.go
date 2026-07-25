// Package bound is the single stream-capture primitive the helper uses
// everywhere bytes must be bounded as they arrive.
//
// CapBuffer is the one buffer type behind foreground exec output, the detached
// supervisor's per-stream rings, and rg stdout. Bounding happens at write
// time, never after the fact: Write keeps a fixed head prefix and a ring of
// the last tail bytes, and drops the middle. The byte and line counters keep
// running after storage stops, so total_bytes / total_lines stay truthful even
// though the stored text is capped.
//
// OWNS:
//   - CapBuffer: head+tail capture with running totals.
//   - StreamSnapshot: the bounded text plus totals handed to result envelopes.
//   - Snapshot / DrainSnapshot: the two ways to read a buffer out.
//   - The elision seam text "[... N bytes truncated ...]".
//
// STATE MACHINE: none; a CapBuffer only accumulates and is read out. Drain
// resets the stored head/tail and window while leaving the cumulative
// counters intact.
//
// INVARIANTS:
//   - The helper never buffers a whole stream and truncates later. Every
//     producer feeds CapBuffer from a bounded read loop, so nothing between the
//     source and the buffer grows without limit.
//   - Snapshot's elision seam "[... N bytes truncated ...]" is a wire format,
//     not only display text: task.splitTruncatedSnapshot re-parses that exact
//     line to reassemble a detached stream across successive polls. Changing
//     the seam string breaks that accumulator.
//   - DrainSnapshot hands out the stored ring bytes exactly once (poll
//     draining), but total_bytes / total_lines never reset, so cumulative
//     totals reported across successive polls stay correct.
//   - Capture shapes differ by producer and each shape is intentional: command
//     streams keep head AND tail (signal lives at both the start, e.g. startup
//     errors, and the end, e.g. final status); file reads keep head only, with
//     continuation carried by next_offset; grep/glob keep head only because
//     their output is already significance-sorted.
//   - Payload limits may only lower the visible caps for one call (mapping the
//     caller's max_output_tokens). Above-default values clamp down; the hard
//     capture caps (store size, per-line cut) are compile-time constants and
//     cannot be raised by a payload.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/bound/cap_buffer.go
//   - internal/sandbox/helper/internal/task/exec.go (splitTruncatedSnapshot
//     consumes the seam; the detached poll accumulator)
//   - internal/sandbox/helper/internal/task/supervisor.go (per-stream rings)
package bound
