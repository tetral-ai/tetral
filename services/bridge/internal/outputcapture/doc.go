// Package outputcapture persists bounded sandbox output files during Runtime closeout.
//
// OWNS:
//   - session_output_captures — the durable per-path capture index, keyed
//     UNIQUE(workspace_id, session_id, source_path); each row records the latest
//     Files artifact (last_file_id), size, sha256, and capture time.
//
// STATE MODEL (per source_path row):
//   - absent — the path has never been captured.
//   - present — the row records the latest positively observed regular-file
//     identity and captured bytes. Later scans never infer that the path vanished.
//
// INVARIANTS:
//   - The helper is the sole enumeration and classification authority. It
//     classifies each path through a no-read O_PATH descriptor, reopens
//     directories through /proc/self/fd for bounded same-inode enumeration, and
//     reopens qualified files through /proc/self/fd before streaming bytes and
//     hash.
//   - Per-file skips and empty scans leave prior rows untouched. Truncation bounds
//     capture work but has no durability significance because scans make no
//     absence claims.
//   - The driver owns deterministic file-count and byte selection; Bridge verifies
//     those caps as protocol assertions before any blob upload.
//   - Capture scanning is best-effort by design: scanner and scan-normalization
//     failures return a typed pre-persistence outcome that FinishIdle records and
//     downgrades. That record is the alarm. Once the workspace lock or any
//     durable write begins, database, quota, and object-store failures remain
//     settlement failures; best-effort capture never means best-effort
//     persistence.
//
// UPDATE-WITH:
//   - services/bridge/internal/outputcapture/capturer.go
package outputcapture
