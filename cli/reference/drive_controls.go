package reference

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/tetral-ai/tetral/cli/reference/wire"
)

// The drive controls set what `protocol/README.md` leaves to Engine: a
// commit by another writer, a drive's own limits, the removal of a version,
// the ageing of change records and the workspace's upload budget.

// decodedBy gives the error of package wire's decoder for message, so that a
// control refuses a payload that the server could not send.
func decodedBy[M any, P interface {
	*M
	wire.Message
}](message M) error {
	encoded, err := wire.Encode(message)
	if err != nil {
		return err
	}
	_, err = wire.Decode[M, P](encoded)
	return err
}

// controlDriveCommit commits changes to a drive as another writer does
// through the developer API: without a base, the deletions first, under one
// seq, with the drive's notice after it. A change deletes its path, writes
// `text`, or writes content `generated` from a seed; one that leaves its path
// as it is makes no version. Each path must pass package wire's decoder and
// the drive path rules; a commit refused for a later change has applied the
// changes before it.
func (server *Server) controlDriveCommit(body json.RawMessage) (any, error) {
	var input struct {
		DriveID  string `json:"drive_id"`
		APIKeyID string `json:"api_key_id"`
		Changes  []struct {
			Path      string  `json:"path"`
			Delete    bool    `json:"delete"`
			Text      *string `json:"text"`
			Generated *struct {
				SizeBytes int64  `json:"size_bytes"`
				Seed      string `json:"seed"`
			} `json:"generated"`
			Executable bool `json:"executable"`
		} `json:"changes"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	if input.APIKeyID == "" {
		input.APIKeyID = "key_other_writer"
	}
	writer := wire.DriveWriter{Actor: &wire.APIActor{APIKeyID: wire.ID(input.APIKeyID)}}
	if err := decodedBy(wire.DrivePathParams{DriveID: wire.ID(input.DriveID)}); err != nil {
		return nil, err
	}
	// A rejection is the message that carries a writer.
	if err := decodedBy(wire.CommitResponse{Results: []wire.CommitResult{{Result: &wire.RejectedResult{
		Reason: "conflict", Current: wire.Some(wire.CurrentVersion{VersionID: "ver", Writer: writer}),
	}}}}); err != nil {
		return nil, err
	}
	type written struct {
		content driveContent
		sha256  string
	}
	contents := make([]written, len(input.Changes))
	for index, change := range input.Changes {
		switch {
		case change.Delete && (change.Text != nil || change.Generated != nil):
			return nil, fmt.Errorf("change %d both deletes and writes %q", index, change.Path)
		case change.Delete:
		case change.Text != nil && change.Generated == nil:
			digest := sha256.Sum256([]byte(*change.Text))
			contents[index] = written{driveContent{size: int64(len(*change.Text)), data: []byte(*change.Text)}, hex.EncodeToString(digest[:])}
		case change.Generated != nil && change.Text == nil:
			if change.Generated.SizeBytes < 0 || change.Generated.SizeBytes > wire.FileMaxBytes {
				return nil, fmt.Errorf("change %d: a file holds 0 to 500,000,000 bytes", index)
			}
			content, hash := generatedContent(change.Generated.SizeBytes, change.Generated.Seed)
			contents[index] = written{content, hash}
		default:
			return nil, fmt.Errorf("change %d needs one of delete, text and generated", index)
		}
		if err := decodedBy(wire.ListFilesResponse{Files: []wire.FileEntry{{
			Path: wire.DrivePath(change.Path), VersionID: "ver", ContentSha256: wire.Sha256(strings.Repeat("0", 64)),
		}}, StartSeq: wire.Some[int64](0)}); err != nil {
			return nil, fmt.Errorf("change %d: %w", index, err)
		}
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	target := server.driveOf(input.DriveID)
	commit := &pendingCommit{server: server, target: target, writer: writer}
	type fileState struct {
		Path          string `json:"path"`
		VersionID     string `json:"version_id"`
		ContentSha256 string `json:"content_sha256,omitempty"`
		SizeBytes     int64  `json:"size_bytes"`
	}
	states := make([]fileState, len(input.Changes))
	for _, deletions := range []bool{true, false} {
		for index, change := range input.Changes {
			if change.Delete != deletions {
				continue
			}
			current := target.current[change.Path]
			switch {
			case change.Delete && current.isFile():
				current = commit.put(change.Path, "", false)
			case change.Delete:
			case current.isFile() && current.sha256 == contents[index].sha256 && current.executable == change.Executable:
			default:
				if rule := target.pathRule(change.Path); rule != "" {
					commit.end()
					return nil, fmt.Errorf("change %d: %s", index, rule)
				}
				target.contents[contents[index].sha256] = contents[index].content
				current = commit.put(change.Path, contents[index].sha256, change.Executable)
			}
			states[index] = fileState{Path: change.Path}
			if current != nil {
				states[index] = fileState{Path: change.Path, VersionID: current.id, ContentSha256: current.sha256, SizeBytes: current.size}
			}
		}
	}
	seq := commit.end()
	return map[string]any{"seq": seq, "changes": states}, nil
}

// controlDriveLimits sets a drive's own limits; a limit left out is none.
func (server *Server) controlDriveLimits(body json.RawMessage) (any, error) {
	var input struct {
		DriveID  string `json:"drive_id"`
		MaxBytes *int64 `json:"max_bytes"`
		MaxFiles *int64 `json:"max_files"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	if err := decodedBy(wire.DrivePathParams{DriveID: wire.ID(input.DriveID)}); err != nil {
		return nil, err
	}
	if (input.MaxBytes != nil && *input.MaxBytes < 0) || (input.MaxFiles != nil && *input.MaxFiles < 0) {
		return nil, errors.New("a drive limit is not below 0")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	target := server.driveOf(input.DriveID)
	target.maxBytes, target.maxFiles = input.MaxBytes, input.MaxFiles
	return map[string]any{}, nil
}

// controlDriveRemoveVersion makes a drive keep one version no longer, as
// its retention does: its change record stays listed, and its content is
// found only while another kept version names it. A deletion that is its
// path's current version leaves the drive without a version of the path; a
// file's current version is never removed.
func (server *Server) controlDriveRemoveVersion(body json.RawMessage) (any, error) {
	var input struct {
		DriveID   string `json:"drive_id"`
		VersionID string `json:"version_id"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	target := server.drives[input.DriveID]
	if target == nil || target.versions[input.VersionID] == nil {
		return nil, fmt.Errorf("drive %q has no version %q", input.DriveID, input.VersionID)
	}
	version := target.versions[input.VersionID]
	if target.current[version.path] == version {
		if version.isFile() {
			return nil, fmt.Errorf("version %q is the current file at %q", version.id, version.path)
		}
		target.setCurrent(version.path, nil)
	}
	version.removed = true
	return map[string]any{}, nil
}

// controlDriveExpire removes the change records of a drive's commits before
// before_seq, as the records' 30 days do.
func (server *Server) controlDriveExpire(body json.RawMessage) (any, error) {
	var input struct {
		DriveID   string `json:"drive_id"`
		BeforeSeq int64  `json:"before_seq"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	target := server.drives[input.DriveID]
	if target == nil {
		return nil, fmt.Errorf("no drive %q", input.DriveID)
	}
	kept := slices.IndexFunc(target.records, func(record wire.ChangeRecord) bool { return record.Seq >= input.BeforeSeq })
	if kept < 0 {
		kept = len(target.records)
	}
	if kept == 0 {
		return nil, fmt.Errorf("drive %q lists no record before seq %d", input.DriveID, input.BeforeSeq)
	}
	expired := target.records[kept-1]
	target.expired, target.records = &expired, slices.Clone(target.records[kept:])
	return map[string]any{"removed": kept}, nil
}

// controlDriveBudget sets the workspace's upload budget to bytes_per_minute
// and fills it.
func (server *Server) controlDriveBudget(body json.RawMessage) (any, error) {
	var input struct {
		BytesPerMinute int64 `json:"bytes_per_minute"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	if input.BytesPerMinute <= 0 {
		return nil, errors.New("the upload budget needs bytes_per_minute above 0")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	server.budget = uploadBudget{perMinute: input.BytesPerMinute, level: float64(input.BytesPerMinute), at: server.clock.Now()}
	return map[string]any{}, nil
}

// DriveState is what the drive_state control reports of one drive.
type DriveState struct {
	DriveID string `json:"drive_id"`
	// Seq is the seq of the latest commit that made a version, 0 before the
	// first.
	Seq int64 `json:"seq"`
	// Files are the current files in path order.
	Files []wire.FileEntry `json:"files"`
	// Versions are every version the drive made, in the order made.
	Versions []DriveVersionState `json:"versions"`
	// Records are the change records still listed.
	Records []wire.ChangeRecord `json:"records"`
	// Contents are the hashes of the content the drive holds, sorted.
	Contents []string `json:"contents"`
}

type DriveVersionState struct {
	VersionID string `json:"version_id"`
	Path      string `json:"path"`
	// ContentSha256 is empty for a deletion.
	ContentSha256 string           `json:"content_sha256,omitempty"`
	SizeBytes     int64            `json:"size_bytes"`
	Executable    bool             `json:"executable"`
	Writer        wire.DriveWriter `json:"writer"`
	Removed       bool             `json:"removed"`
	made          int
}

// controlDriveState reports one drive. It is apart from the state control,
// which a test reads often and a large drive would make slow.
func (server *Server) controlDriveState(body json.RawMessage) (any, error) {
	var input struct {
		DriveID string `json:"drive_id"`
	}
	if err := decodeControl(body, &input); err != nil {
		return nil, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	target := server.drives[input.DriveID]
	if target == nil {
		return nil, fmt.Errorf("no drive %q", input.DriveID)
	}
	state := DriveState{
		DriveID: target.id, Seq: target.seq, Files: []wire.FileEntry{}, Versions: []DriveVersionState{},
		Records: slices.Clone(target.records), Contents: slices.Sorted(maps.Keys(target.contents)),
	}
	for _, file := range target.files() {
		state.Files = append(state.Files, wire.FileEntry{
			Path: wire.DrivePath(file.path), VersionID: wire.ID(file.id), ContentSha256: wire.Sha256(file.sha256),
			SizeBytes: file.size, Executable: file.executable,
		})
	}
	for _, version := range target.versions {
		state.Versions = append(state.Versions, DriveVersionState{
			VersionID: version.id, Path: version.path, ContentSha256: version.sha256, SizeBytes: version.size,
			Executable: version.executable, Writer: version.writer, Removed: version.removed, made: version.made,
		})
	}
	slices.SortFunc(state.Versions, func(a, b DriveVersionState) int { return a.made - b.made })
	return state, nil
}
