// Package capture implements the idle-output capture mode reached through the
// hidden __capture entrypoint.
//
// This is not a model tool: it is absent from the subcommand table and from
// every tool catalog, and none of the tool payload/envelope rules apply. Its
// consumer is the caller projecting /mnt/session/outputs files into durable
// session Files, one path per invocation.
//
// OWNS:
//   - Run: classify one outputs path and return either a bounded directory
//     listing, a skip marker, or bytes.
//   - The capture envelope success/failure shaping.
//
// STATE MACHINE: none; each invocation classifies one path.
//
// INVARIANTS:
//   - Authorization is an identity gate, not catalog absence. The binary is on
//     PATH, so the model's runtime user could exec it; the mode therefore
//     requires euid == 0 (the caller's driver exec runs as root, the model's
//     runtime user cannot become root) and refuses otherwise. Even if reached,
//     the mode reads only files the model can already read and writes nothing,
//     so the gate plus read-only behavior grants nothing.
//   - Handle safety is atomic per path within one invocation: the outputs root
//     is opened as a directory, and the path is classified fd-relative with an
//     O_PATH openat2 no-follow descriptor. Non-regular, multi-link, and over-cap
//     entries return successful skip markers before read access is acquired.
//     Qualified files are reopened only through /proc/self/fd, with no pathname
//     fallback, and the read descriptor must identify the same device/inode and
//     remain regular and single-link before bytes and sha256 are read.
//     Directories are likewise reopened through /proc/self/fd, same-inode
//     verified, and enumerated through a count- and name-byte-bounded getdents
//     loop before their names are sorted. This is a
//     stronger property than the accepted resolve-then-I/O TOCTOU of the
//     model-facing path tools; that acceptance stands for the model tools and
//     is not overturned here.
//   - The outputs root is legitimately model-writable, so the payload-root
//     ownership and write-bit checks used elsewhere in the helper do not apply
//     here. Containment is the resolve flags plus the per-file fstat alone.
//     Bytes return base64 in-band on stdout, exempt from the envelope
//     self-cap, exactly like the Read media envelope.
//
// UPDATE-WITH:
//   - internal/sandbox/helper/internal/capture/capture.go
//   - internal/sandbox/helper/internal/cli/cli.go (the __capture dispatch)
package capture
