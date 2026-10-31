package drive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// NoticeChannel carries {workspace_id, drive_id, seq} after each commit that
// creates a version; pg_notify delivers it only once the commit is durable.
const NoticeChannel = "tetral_drive_changed"

// Mode is how a commit treats a rejected change.
type Mode int

const (
	// ModeMachine decides each change on its own.
	ModeMachine Mode = iota
	// ModeAllOrNothing applies every change or none: the first rejection, in
	// decision order, rejects the whole commit.
	ModeAllOrNothing
)

// BaseKind is what a change was based on.
type BaseKind int

const (
	// BaseHash is the content hash the change was based on.
	BaseHash BaseKind = iota
	// BaseMustNotExist requires that the path has no current file.
	BaseMustNotExist
	// BaseAny checks no base; only the API and restores pass it.
	BaseAny
)

type Base struct {
	Kind   BaseKind
	SHA256 string
}

// Change is a write of content the drive holds, or a deletion.
type Change struct {
	Path          string
	Delete        bool
	Base          Base
	ContentSHA256 string
	Executable    bool
}

// Reason is why a change was rejected.
type Reason string

const (
	ReasonPathRule Reason = "path_rule"
	ReasonConflict Reason = "conflict"
	ReasonLimit    Reason = "limit"
)

// CurrentVersion is a path's current kept version, a deletion included.
type CurrentVersion struct {
	VersionID     string
	ContentSHA256 *string
	SizeBytes     int64
	Executable    bool
	Writer        Writer
}

// ChangeResult is one change's outcome. An accepted change names the new
// version, or under rule b.1 the path's current version (nil when the drive
// keeps none). A rejected change names its reason and rule text, and Current
// unless the drive keeps no version of the path.
type ChangeResult struct {
	Accepted   bool
	NewVersion bool
	VersionID  *string
	Reason     Reason
	Rule       string
	Current    *CurrentVersion
}

// CommitResult holds one result per change in request order. Seq is nil when
// the commit created no version.
type CommitResult struct {
	Seq     *int64
	Results []ChangeResult
}

// Commit is the one writer of versions, current files, change records and
// usage, all in one transaction under the drive lock.
func (s *Service) Commit(ctx context.Context, ws workspace.ID, driveID string, writer Writer, mode Mode, changes []Change) (CommitResult, error) {
	if err := writer.validate(); err != nil {
		return CommitResult{}, err
	}
	var result CommitResult
	err := s.db.WithWorkspaceTx(ctx, string(ws), "drive.commit", func(tx *dbconnect.Tx) error {
		locked, err := lockDrive(ctx, tx, ws, driveID)
		if err != nil {
			return err
		}
		result, err = commitLocked(ctx, tx, ws, driveID, locked, writer, mode, changes)
		return err
	})
	return result, err
}

// commit is the state of one commit while its changes are decided; each
// accepted change is written at once, so later decisions see it.
type commit struct {
	ctx        context.Context
	tx         *dbconnect.Tx
	ws         workspace.ID
	driveID    string
	writer     Writer
	drive      lockedDrive
	time       time.Time
	seq        int64
	nextIndex  int
	sizes      map[string]int64
	workspace  *workspaceUsage
	deltaBytes int64
	deltaFiles int64
	touched    []string
}

type workspaceUsage struct{ bytes, files int64 }

type currentFile struct {
	versionID  string
	sha256     string
	size       int64
	executable bool
}

func commitLocked(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID string, locked lockedDrive, writer Writer, mode Mode, changes []Change) (CommitResult, error) {
	seen := make(map[string]struct{}, len(changes))
	var hashes []string
	for _, change := range changes {
		if _, dup := seen[change.Path]; dup {
			return CommitResult{}, fmt.Errorf("drive: path %q appears twice in one commit", change.Path)
		}
		seen[change.Path] = struct{}{}
		if !change.Delete {
			hashes = append(hashes, change.ContentSHA256)
		}
	}
	c := &commit{ctx: ctx, tx: tx, ws: ws, driveID: driveID, writer: writer, drive: locked, seq: locked.lastSeq + 1, sizes: map[string]int64{}}
	if err := c.checkContent(hashes); err != nil {
		return CommitResult{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT greatest(clock_timestamp(),last_commit_at) FROM drives WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID).Scan(&c.time); err != nil {
		return CommitResult{}, err
	}
	order := make([]int, 0, len(changes))
	for _, deletions := range []bool{true, false} {
		for i, change := range changes {
			if change.Delete == deletions {
				order = append(order, i)
			}
		}
	}
	results := make([]ChangeResult, len(changes))
	for _, i := range order {
		result, err := c.decide(changes[i])
		if err != nil {
			return CommitResult{}, err
		}
		if !result.Accepted && mode == ModeAllOrNothing {
			return CommitResult{}, &CommitRejectedError{Index: i, Path: changes[i].Path, Result: result}
		}
		results[i] = result
	}
	if err := c.finish(); err != nil {
		return CommitResult{}, err
	}
	out := CommitResult{Results: results}
	if c.nextIndex > 0 {
		out.Seq = &c.seq
	}
	return out, nil
}

// checkContent requires every written hash to be held by the drive.
func (c *commit) checkContent(hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	rows, err := c.tx.Query(c.ctx, `SELECT content_sha256,size_bytes FROM drive_contents WHERE workspace_id=$1 AND drive_id=$2 AND content_sha256=ANY($3)`,
		string(c.ws), c.driveID, hashes)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sha string
		var size int64
		if err := rows.Scan(&sha, &size); err != nil {
			return err
		}
		c.sizes[sha] = size
	}
	if err := rows.Err(); err != nil {
		return err
	}
	var missing []string
	reported := map[string]bool{}
	for _, sha := range hashes {
		if _, held := c.sizes[sha]; !held && !reported[sha] {
			missing, reported[sha] = append(missing, sha), true
		}
	}
	if len(missing) > 0 {
		return &ContentMissingError{MissingSHA256: missing}
	}
	return nil
}

// decide applies the first matching rule of b.1 to b.5 to one change.
func (c *commit) decide(change Change) (ChangeResult, error) {
	current, err := c.currentFile(change.Path)
	if err != nil {
		return ChangeResult{}, err
	}
	// b.1: the path already holds the change.
	if (change.Delete && current == nil) ||
		(!change.Delete && current != nil && current.sha256 == change.ContentSHA256 && current.executable == change.Executable) {
		version, err := c.currentVersion(change.Path)
		if err != nil || version == nil {
			return ChangeResult{Accepted: true}, err
		}
		return ChangeResult{Accepted: true, VersionID: &version.VersionID}, nil
	}
	// b.2: path rules.
	if rule, err := c.pathRule(change.Path); err != nil || rule != "" {
		return c.reject(change.Path, ReasonPathRule, rule, err)
	}
	// b.3: the base.
	switch change.Base.Kind {
	case BaseHash:
		if current == nil || current.sha256 != change.Base.SHA256 {
			return c.reject(change.Path, ReasonConflict, "", nil)
		}
	case BaseMustNotExist:
		if current != nil {
			return c.reject(change.Path, ReasonConflict, "", nil)
		}
	}
	// b.4: limits, for a change that raises bytes or files.
	var deltaBytes, deltaFiles int64
	switch {
	case change.Delete:
		deltaBytes, deltaFiles = -current.size, -1
	case current == nil:
		deltaBytes, deltaFiles = c.sizes[change.ContentSHA256], 1
	default:
		deltaBytes = c.sizes[change.ContentSHA256] - current.size
	}
	if deltaBytes > 0 || deltaFiles > 0 {
		if rule, err := c.limitRule(deltaBytes, deltaFiles); err != nil || rule != "" {
			return c.reject(change.Path, ReasonLimit, rule, err)
		}
	}
	// b.5: a new version.
	versionID, err := c.apply(change, current)
	if err != nil {
		return ChangeResult{}, err
	}
	c.deltaBytes += deltaBytes
	c.deltaFiles += deltaFiles
	c.drive.usageBytes += deltaBytes
	c.drive.usageFiles += deltaFiles
	if c.workspace != nil {
		c.workspace.bytes += deltaBytes
		c.workspace.files += deltaFiles
	}
	return ChangeResult{Accepted: true, NewVersion: true, VersionID: &versionID}, nil
}

func (c *commit) reject(path string, reason Reason, rule string, err error) (ChangeResult, error) {
	if err != nil {
		return ChangeResult{}, err
	}
	current, err := c.currentVersion(path)
	if err != nil {
		return ChangeResult{}, err
	}
	return ChangeResult{Reason: reason, Rule: rule, Current: current}, nil
}

func (c *commit) currentFile(path string) (*currentFile, error) {
	if !storable(path) {
		return nil, nil
	}
	var file currentFile
	err := c.tx.QueryRow(c.ctx, `SELECT version_id,content_sha256,size_bytes,executable FROM drive_files WHERE workspace_id=$1 AND drive_id=$2 AND path=$3`,
		string(c.ws), c.driveID, path).Scan(&file.versionID, &file.sha256, &file.size, &file.executable)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &file, nil
}

func (c *commit) currentVersion(path string) (*CurrentVersion, error) {
	if !storable(path) {
		return nil, nil
	}
	var v CurrentVersion
	var sha, computer sql.NullString
	err := c.tx.QueryRow(c.ctx, `SELECT version_id,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id FROM drive_versions v
		WHERE workspace_id=$1 AND drive_id=$2 AND path=$3 AND superseded_at IS NULL AND `+keptVersion("v"),
		string(c.ws), c.driveID, path).Scan(&v.VersionID, &sha, &v.SizeBytes, &v.Executable, &v.Writer.Kind, &v.Writer.ID, &computer)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if sha.Valid {
		v.ContentSHA256 = &sha.String
	}
	v.Writer.ComputerID = computer.String
	return &v, nil
}

// pathRule checks rules 1 to 9 against drive_files as this commit has changed
// it so far, so a deletion decided earlier in the commit frees its fold key.
func (c *commit) pathRule(path string) (string, error) {
	if rule := CheckPath(path); rule != "" {
		return rule, nil
	}
	key := FoldKey(path)
	checks := []struct {
		rule  string
		query string
		arg   any
	}{
		{ruleFoldCollision, `fold_key=$3 AND path<>$4`, key},
		{ruleFileAtFolder, `fold_key=ANY($3)`, folderFoldKeys(path)},
		{ruleFilesBelowPath, `fold_key LIKE $3`, likePrefix(key + "/")},
	}
	for _, check := range checks {
		args := []any{string(c.ws), c.driveID, check.arg}
		if check.rule == ruleFoldCollision {
			args = append(args, path)
		}
		var exists bool
		if err := c.tx.QueryRow(c.ctx, `SELECT EXISTS (SELECT 1 FROM drive_files WHERE workspace_id=$1 AND drive_id=$2 AND `+check.query+`)`, args...).Scan(&exists); err != nil {
			return "", err
		}
		if exists {
			return check.rule, nil
		}
	}
	return "", nil
}

// limitRule names the first limit a change raising bytes or files would pass,
// with usage counted after the commit's earlier accepted changes.
func (c *commit) limitRule(deltaBytes, deltaFiles int64) (string, error) {
	if c.workspace == nil {
		// The Workspace row lock keeps concurrent commits to drives of one
		// Workspace from passing its limit together.
		usage := &workspaceUsage{}
		if _, err := c.tx.ExecContext(c.ctx, `INSERT INTO drive_workspace_usage(workspace_id) VALUES ($1) ON CONFLICT (workspace_id) DO NOTHING`, string(c.ws)); err != nil {
			return "", err
		}
		if err := c.tx.QueryRow(c.ctx, `SELECT bytes,files FROM drive_workspace_usage WHERE workspace_id=$1 FOR UPDATE`, string(c.ws)).Scan(&usage.bytes, &usage.files); err != nil {
			return "", err
		}
		usage.bytes += c.deltaBytes
		usage.files += c.deltaFiles
		c.workspace = usage
	}
	switch {
	case deltaBytes > 0 && c.drive.maxBytes.Valid && c.drive.usageBytes+deltaBytes > c.drive.maxBytes.Int64:
		return fmt.Sprintf("the drive's max_bytes limit of %d bytes", c.drive.maxBytes.Int64), nil
	case deltaFiles > 0 && c.drive.maxFiles.Valid && c.drive.usageFiles+deltaFiles > c.drive.maxFiles.Int64:
		return fmt.Sprintf("the drive's max_files limit of %d files", c.drive.maxFiles.Int64), nil
	case deltaBytes > 0 && c.workspace.bytes+deltaBytes > WorkspaceMaxBytes:
		return fmt.Sprintf("the workspace limit of %d bytes", WorkspaceMaxBytes), nil
	case deltaFiles > 0 && c.workspace.files+deltaFiles > WorkspaceMaxFiles:
		return fmt.Sprintf("the workspace limit of %d files", WorkspaceMaxFiles), nil
	}
	return "", nil
}

// apply writes the new version, supersedes the path's previous current version,
// updates the current file and appends the change record.
func (c *commit) apply(change Change, current *currentFile) (string, error) {
	versionID := id.New("dver_")
	operation, sha, size, executable := OperationModified, any(change.ContentSHA256), c.sizes[change.ContentSHA256], change.Executable
	switch {
	case change.Delete:
		operation, sha, size, executable = OperationDeleted, nil, 0, false
	case current == nil:
		operation = OperationCreated
	}
	ws, drive := string(c.ws), c.driveID
	if _, err := c.tx.ExecContext(c.ctx, `UPDATE drive_versions SET superseded_at=$4 WHERE workspace_id=$1 AND drive_id=$2 AND path=$3 AND superseded_at IS NULL`,
		ws, drive, change.Path, c.time); err != nil {
		return "", err
	}
	row := append([]any{ws, drive, versionID, change.Path, string(operation), sha, size, executable}, writerArgs(c.writer)...)
	if _, err := c.tx.ExecContext(c.ctx, `INSERT INTO drive_versions(workspace_id,drive_id,version_id,path,operation,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id,created_at,seq)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, append(row, c.time, c.seq)...); err != nil {
		return "", err
	}
	var err error
	switch {
	case change.Delete:
		_, err = c.tx.ExecContext(c.ctx, `DELETE FROM drive_files WHERE workspace_id=$1 AND drive_id=$2 AND path=$3`, ws, drive, change.Path)
	case current == nil:
		_, err = c.tx.ExecContext(c.ctx, `INSERT INTO drive_files(workspace_id,drive_id,path,fold_key,version_id,content_sha256,size_bytes,executable) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			ws, drive, change.Path, FoldKey(change.Path), versionID, sha, size, executable)
	default:
		_, err = c.tx.ExecContext(c.ctx, `UPDATE drive_files SET version_id=$4,content_sha256=$5,size_bytes=$6,executable=$7 WHERE workspace_id=$1 AND drive_id=$2 AND path=$3`,
			ws, drive, change.Path, versionID, sha, size, executable)
	}
	if err != nil {
		return "", err
	}
	record := append([]any{ws, drive, c.seq, c.nextIndex, change.Path, string(operation), versionID, sha, size, executable}, writerArgs(c.writer)...)
	if _, err := c.tx.ExecContext(c.ctx, `INSERT INTO drive_changes(workspace_id,drive_id,seq,change_index,path,operation,version_id,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, append(record, c.time)...); err != nil {
		return "", err
	}
	c.nextIndex++
	c.touched = append(c.touched, change.Path)
	return versionID, nil
}

// finish keeps at most 10 old versions of each changed path, applies the usage
// deltas and, when a version was created, advances the drive and notifies.
func (c *commit) finish() error {
	ws, drive := string(c.ws), c.driveID
	for _, path := range c.touched {
		if _, err := c.tx.ExecContext(c.ctx, `DELETE FROM drive_versions WHERE workspace_id=$1 AND drive_id=$2 AND version_id IN (
			SELECT version_id FROM drive_versions v WHERE workspace_id=$1 AND drive_id=$2 AND path=$3 AND superseded_at IS NOT NULL AND `+keptVersion("v")+`
			ORDER BY created_at DESC,seq DESC OFFSET $4)`, ws, drive, path, keptOldVersions); err != nil {
			return err
		}
	}
	if c.deltaBytes != 0 || c.deltaFiles != 0 {
		if _, err := c.tx.ExecContext(c.ctx, `UPDATE drives SET usage_bytes=usage_bytes+$3,usage_files=usage_files+$4 WHERE workspace_id=$1 AND drive_id=$2`,
			ws, drive, c.deltaBytes, c.deltaFiles); err != nil {
			return err
		}
		// limitRule created the Workspace row before any change that raised a
		// count, so the row exists whenever there are files to count.
		if _, err := c.tx.ExecContext(c.ctx, `UPDATE drive_workspace_usage SET bytes=bytes+$2,files=files+$3 WHERE workspace_id=$1`,
			ws, c.deltaBytes, c.deltaFiles); err != nil {
			return err
		}
	}
	if c.nextIndex == 0 {
		return nil
	}
	if _, err := c.tx.ExecContext(c.ctx, `UPDATE drives SET last_seq=$3,last_commit_at=$4 WHERE workspace_id=$1 AND drive_id=$2`, ws, drive, c.seq, c.time); err != nil {
		return err
	}
	notice, err := json.Marshal(struct {
		WorkspaceID string `json:"workspace_id"`
		DriveID     string `json:"drive_id"`
		Seq         int64  `json:"seq"`
	}{ws, drive, c.seq})
	if err != nil {
		return err
	}
	_, err = c.tx.ExecContext(c.ctx, `SELECT pg_notify($1,$2)`, NoticeChannel, string(notice))
	return err
}
