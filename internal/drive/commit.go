package drive

import (
	"context"
	"database/sql"
	"encoding/json"
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
// usage, all in one transaction under the drive lock. Its statement count does
// not grow with its changes: it loads the rows its rules read once, decides
// every change in memory and writes the accepted ones in set statements.
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
// accepted change updates state at once, so later decisions see it, and joins
// applied, which write stores.
type commit struct {
	ctx        context.Context
	tx         *dbconnect.Tx
	ws         workspace.ID
	driveID    string
	writer     Writer
	drive      lockedDrive
	time       time.Time
	seq        int64
	sizes      map[string]int64
	workspace  *workspaceUsage
	deltaBytes int64
	deltaFiles int64
	state      driveState
	applied    []appliedChange
}

type workspaceUsage struct{ bytes, files int64 }

type currentFile struct {
	foldKey    string
	versionID  string
	sha256     string
	size       int64
	executable bool
}

// driveState holds the rows of drive_files and drive_versions that the
// commit's rules read, and how its accepted changes have changed drive_files.
type driveState struct {
	// files are the loaded current files by path: those at the changed paths
	// and those whose fold key the rules look up.
	files map[string]*currentFile
	// byFold names the loaded file of each fold key; the unique fold key index
	// allows one.
	byFold map[string]string
	// below lists, for the fold key k of each changed path, files whose fold
	// key starts with k + "/": all of them, or one more than the commit
	// deletes, so that one of those listed stays whatever is decided.
	below map[string][]string
	// versions are the changed paths' current kept versions.
	versions map[string]*CurrentVersion
	deleted  map[string]bool
	// created holds the fold key and path of each file the commit created,
	// createdFolders each prefix of such a fold key that a "/" follows.
	created        map[string]string
	createdFolders map[string]bool
}

// appliedChange is an accepted change that creates a version.
type appliedChange struct {
	path       string
	operation  Operation
	versionID  string
	sha256     string
	size       int64
	executable bool
	// created and deleted say how the change alters drive_files; neither is a
	// modification.
	created, deleted bool
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
	// The commit's statements are prepared once per connection. A cached
	// generic plan knows neither the drive nor its arrays, and tests a filter
	// such as path=ANY($3) by a linear search of the array for every row; a
	// plan for the given values hashes the array.
	if _, err := tx.ExecContext(ctx, `SET LOCAL plan_cache_mode = force_custom_plan`); err != nil {
		return CommitResult{}, err
	}
	if err := c.checkContent(hashes); err != nil {
		return CommitResult{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT greatest(clock_timestamp(),last_commit_at) FROM drives WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID).Scan(&c.time); err != nil {
		return CommitResult{}, err
	}
	if err := c.load(changes); err != nil {
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
	if err := c.write(); err != nil {
		return CommitResult{}, err
	}
	out := CommitResult{Results: results}
	if len(c.applied) > 0 {
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

// load reads, in three statements, the current files at the changed paths,
// the files at the fold keys rules 7 and 8 look up, enough files below each
// key rule 9 looks up, and the changed paths' current kept versions. Only
// paths that Postgres text can hold are looked up, and only paths that pass
// rules 1 to 6 reach rules 7 to 9.
func (c *commit) load(changes []Change) error {
	c.state = driveState{
		files: map[string]*currentFile{}, byFold: map[string]string{},
		below:    map[string][]string{},
		versions: map[string]*CurrentVersion{}, deleted: map[string]bool{},
		created: map[string]string{}, createdFolders: map[string]bool{},
	}
	var paths, keys, ruleKeys []string
	looked, belowLooked := map[string]bool{}, map[string]bool{}
	for _, change := range changes {
		if storable(change.Path) {
			paths = append(paths, change.Path)
		}
		if CheckPath(change.Path) != "" {
			continue
		}
		key := FoldKey(change.Path)
		if !belowLooked[key] {
			belowLooked[key] = true
			ruleKeys = append(ruleKeys, key)
		}
		for _, k := range append(folderFoldKeys(change.Path), key) {
			if !looked[k] {
				looked[k] = true
				keys = append(keys, k)
			}
		}
	}
	ws, drive := string(c.ws), c.driveID
	rows, err := c.tx.Query(c.ctx, `SELECT path,fold_key,version_id,content_sha256,size_bytes,executable FROM drive_files
		WHERE workspace_id=$1 AND drive_id=$2 AND (path=ANY($3) OR fold_key=ANY($4))`, ws, drive, paths, keys)
	if err != nil {
		return err
	}
	err = scanAll(rows, func(scan func(...any) error) error {
		var path string
		var file currentFile
		if err := scan(&path, &file.foldKey, &file.versionID, &file.sha256, &file.size, &file.executable); err != nil {
			return err
		}
		c.state.files[path], c.state.byFold[file.foldKey] = &file, path
		return nil
	})
	if err != nil {
		return err
	}
	if err := c.loadBelow(changes, ruleKeys); err != nil {
		return err
	}
	rows, err = c.tx.Query(c.ctx, `SELECT path,version_id,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id FROM drive_versions v
		WHERE workspace_id=$1 AND drive_id=$2 AND path=ANY($3) AND superseded_at IS NULL AND `+keptVersion("v"), ws, drive, paths)
	if err != nil {
		return err
	}
	return scanAll(rows, func(scan func(...any) error) error {
		var path string
		var v CurrentVersion
		var sha, computer sql.NullString
		if err := scan(&path, &v.VersionID, &sha, &v.SizeBytes, &v.Executable, &v.Writer.Kind, &v.Writer.ID, &computer); err != nil {
			return err
		}
		if sha.Valid {
			v.ContentSHA256 = &sha.String
		}
		v.Writer.ComputerID = computer.String
		c.state.versions[path] = &v
		return nil
	})
}

// loadBelow loads, for each key, the files whose fold key starts with key +
// "/", up to one more than the commit's deletions of such files. The range
// [key + "/", key + "0") holds exactly those fold keys in byte order, which the
// fold key index serves. Only that index gives the ORDER BY without a sort, so
// the planner takes it even when stale statistics put few rows in the drive;
// the primary key would otherwise scan the whole drive for every key.
func (c *commit) loadBelow(changes []Change, keys []string) error {
	deletable := map[string]int32{}
	for _, change := range changes {
		if file := c.state.files[change.Path]; change.Delete && file != nil {
			for i := range len(file.foldKey) {
				if file.foldKey[i] == '/' {
					deletable[file.foldKey[:i]]++
				}
			}
		}
	}
	limits := make([]int32, len(keys))
	for i, key := range keys {
		limits[i] = deletable[key] + 1
	}
	rows, err := c.tx.Query(c.ctx, `SELECT k.key,f.path FROM unnest($3::text[],$4::int[]) AS k(key,lim)
		CROSS JOIN LATERAL (SELECT path FROM drive_files WHERE workspace_id=$1 AND drive_id=$2
			AND fold_key ~>=~ (k.key||'/') AND fold_key ~<~ (k.key||'0') ORDER BY fold_key USING ~<~ LIMIT k.lim) f`,
		string(c.ws), c.driveID, keys, limits)
	if err != nil {
		return err
	}
	return scanAll(rows, func(scan func(...any) error) error {
		var key, path string
		if err := scan(&key, &path); err != nil {
			return err
		}
		c.state.below[key] = append(c.state.below[key], path)
		return nil
	})
}

func scanAll(rows *dbconnect.Rows, each func(scan func(...any) error) error) error {
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if err := each(rows.Scan); err != nil {
			return err
		}
	}
	return rows.Err()
}

// decide applies the first matching rule of b.1 to b.5 to one change.
func (c *commit) decide(change Change) (ChangeResult, error) {
	current := c.state.files[change.Path]
	// b.1: the path already holds the change.
	if (change.Delete && current == nil) ||
		(!change.Delete && current != nil && current.sha256 == change.ContentSHA256 && current.executable == change.Executable) {
		version := c.state.versions[change.Path]
		if version == nil {
			return ChangeResult{Accepted: true}, nil
		}
		return ChangeResult{Accepted: true, VersionID: &version.VersionID}, nil
	}
	// b.2: path rules.
	if rule := c.pathRule(change.Path); rule != "" {
		return c.reject(change.Path, ReasonPathRule, rule), nil
	}
	// b.3: the base.
	switch change.Base.Kind {
	case BaseHash:
		if current == nil || current.sha256 != change.Base.SHA256 {
			return c.reject(change.Path, ReasonConflict, ""), nil
		}
	case BaseMustNotExist:
		if current != nil {
			return c.reject(change.Path, ReasonConflict, ""), nil
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
			if err != nil {
				return ChangeResult{}, err
			}
			return c.reject(change.Path, ReasonLimit, rule), nil
		}
	}
	// b.5: a new version.
	versionID := c.apply(change, current)
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

func (c *commit) reject(path string, reason Reason, rule string) ChangeResult {
	return ChangeResult{Reason: reason, Rule: rule, Current: c.state.versions[path]}
}

// pathRule checks rules 1 to 9 against drive_files as this commit has changed
// it so far, so a deletion decided earlier in the commit frees its fold key.
func (c *commit) pathRule(path string) string {
	if rule := CheckPath(path); rule != "" {
		return rule
	}
	key := FoldKey(path)
	if other, ok := c.state.fileAt(key); ok && other != path {
		return ruleFoldCollision
	}
	for _, folder := range folderFoldKeys(path) {
		if _, ok := c.state.fileAt(folder); ok {
			return ruleFileAtFolder
		}
	}
	if c.state.filesBelow(key) {
		return ruleFilesBelowPath
	}
	return ""
}

// fileAt is the path of the current file whose fold key is key.
func (s *driveState) fileAt(key string) (string, bool) {
	if path, ok := s.byFold[key]; ok && !s.deleted[path] {
		return path, true
	}
	path, ok := s.created[key]
	return path, ok
}

// filesBelow is whether a current file's fold key starts with key + "/".
func (s *driveState) filesBelow(key string) bool {
	if s.createdFolders[key] {
		return true
	}
	for _, path := range s.below[key] {
		if !s.deleted[path] {
			return true
		}
	}
	return false
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

// apply records the change's new version and its effect on drive_files.
func (c *commit) apply(change Change, current *currentFile) string {
	applied := appliedChange{path: change.Path, operation: OperationModified, versionID: id.New("dver_"),
		sha256: change.ContentSHA256, size: c.sizes[change.ContentSHA256], executable: change.Executable}
	switch {
	case change.Delete:
		applied.operation, applied.sha256, applied.size, applied.executable, applied.deleted = OperationDeleted, "", 0, false, true
		c.state.deleted[change.Path] = true
	case current == nil:
		applied.operation, applied.created = OperationCreated, true
		key := FoldKey(change.Path)
		c.state.created[key] = change.Path
		for i := range len(key) {
			if key[i] == '/' {
				c.state.createdFolders[key[:i]] = true
			}
		}
	}
	c.applied = append(c.applied, applied)
	return applied.versionID
}

// write supersedes the changed paths' current versions, inserts the new
// versions, deletes, inserts and updates current files, inserts the change
// records in decision order, keeps at most 10 old versions of each changed
// path, applies the usage deltas and advances the drive and notifies.
func (c *commit) write() error {
	if len(c.applied) == 0 {
		return nil
	}
	ws, drive := string(c.ws), c.driveID
	var paths, versionIDs, operations, hashes []string
	var sizes []int64
	var executables []bool
	var indexes []int32
	var deleted, created, createdKeys, createdVersions, createdHashes, modified, modifiedVersions, modifiedHashes []string
	var createdSizes, modifiedSizes []int64
	var createdExecutables, modifiedExecutables []bool
	for i, a := range c.applied {
		paths, versionIDs, operations, hashes = append(paths, a.path), append(versionIDs, a.versionID), append(operations, string(a.operation)), append(hashes, a.sha256)
		sizes, executables, indexes = append(sizes, a.size), append(executables, a.executable), append(indexes, int32(i)) //nolint:gosec // at most the commit's change count
		switch {
		case a.deleted:
			deleted = append(deleted, a.path)
		case a.created:
			created, createdKeys, createdVersions, createdHashes = append(created, a.path), append(createdKeys, FoldKey(a.path)), append(createdVersions, a.versionID), append(createdHashes, a.sha256)
			createdSizes, createdExecutables = append(createdSizes, a.size), append(createdExecutables, a.executable)
		default:
			modified, modifiedVersions, modifiedHashes = append(modified, a.path), append(modifiedVersions, a.versionID), append(modifiedHashes, a.sha256)
			modifiedSizes, modifiedExecutables = append(modifiedSizes, a.size), append(modifiedExecutables, a.executable)
		}
	}
	writer := writerArgs(c.writer)
	statements := []struct {
		skip  bool
		query string
		args  []any
	}{
		// Recreating a path must not give an already expired deletion a new
		// retention window. Its original age still governs subsequent reads.
		{false, `UPDATE drive_versions SET superseded_at=CASE WHEN operation='deleted' AND created_at < $4::timestamptz - ` + retentionInterval + ` THEN created_at ELSE $4 END
			WHERE workspace_id=$1 AND drive_id=$2 AND path=ANY($3) AND superseded_at IS NULL`, []any{ws, drive, paths, c.time}},
		{false, `INSERT INTO drive_versions(workspace_id,drive_id,version_id,path,operation,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id,created_at,seq)
			SELECT $1,$2,u.version_id,u.path,u.operation,NULLIF(u.sha,''),u.size,u.executable,$9,$10,$11,$12,$13
			FROM unnest($3::text[],$4::text[],$5::text[],$6::text[],$7::bigint[],$8::boolean[]) AS u(version_id,path,operation,sha,size,executable)`,
			[]any{ws, drive, versionIDs, paths, operations, hashes, sizes, executables, writer[0], writer[1], writer[2], c.time, c.seq}},
		{len(deleted) == 0, `DELETE FROM drive_files WHERE workspace_id=$1 AND drive_id=$2 AND path=ANY($3)`, []any{ws, drive, deleted}},
		{len(created) == 0, `INSERT INTO drive_files(workspace_id,drive_id,path,fold_key,version_id,content_sha256,size_bytes,executable)
			SELECT $1,$2,u.path,u.fold_key,u.version_id,u.sha,u.size,u.executable
			FROM unnest($3::text[],$4::text[],$5::text[],$6::text[],$7::bigint[],$8::boolean[]) AS u(path,fold_key,version_id,sha,size,executable)`,
			[]any{ws, drive, created, createdKeys, createdVersions, createdHashes, createdSizes, createdExecutables}},
		{len(modified) == 0, `UPDATE drive_files f SET version_id=u.version_id,content_sha256=u.sha,size_bytes=u.size,executable=u.executable
			FROM unnest($3::text[],$4::text[],$5::text[],$6::bigint[],$7::boolean[]) AS u(path,version_id,sha,size,executable)
			WHERE f.workspace_id=$1 AND f.drive_id=$2 AND f.path=u.path`,
			[]any{ws, drive, modified, modifiedVersions, modifiedHashes, modifiedSizes, modifiedExecutables}},
		{false, `INSERT INTO drive_changes(workspace_id,drive_id,seq,change_index,path,operation,version_id,content_sha256,size_bytes,executable,writer_kind,writer_id,writer_computer_id,created_at)
			SELECT $1,$2,$3,u.change_index,u.path,u.operation,u.version_id,NULLIF(u.sha,''),u.size,u.executable,$11,$12,$13,$14
			FROM unnest($4::int[],$5::text[],$6::text[],$7::text[],$8::text[],$9::bigint[],$10::boolean[]) AS u(change_index,path,operation,version_id,sha,size,executable)`,
			[]any{ws, drive, c.seq, indexes, paths, operations, versionIDs, hashes, sizes, executables, writer[0], writer[1], writer[2], c.time}},
		{c.deltaBytes == 0 && c.deltaFiles == 0, `UPDATE drives SET usage_bytes=usage_bytes+$3,usage_files=usage_files+$4 WHERE workspace_id=$1 AND drive_id=$2`,
			[]any{ws, drive, c.deltaBytes, c.deltaFiles}},
		// limitRule created the Workspace row before any change that raised a
		// count, so the row exists whenever there are files to count.
		{c.deltaBytes == 0 && c.deltaFiles == 0, `UPDATE drive_workspace_usage SET bytes=bytes+$2,files=files+$3 WHERE workspace_id=$1`,
			[]any{ws, c.deltaBytes, c.deltaFiles}},
		{false, `UPDATE drives SET last_seq=$3,last_commit_at=$4 WHERE workspace_id=$1 AND drive_id=$2`, []any{ws, drive, c.seq, c.time}},
	}
	for _, statement := range statements {
		if statement.skip {
			continue
		}
		if _, err := c.tx.ExecContext(c.ctx, statement.query, statement.args...); err != nil {
			return err
		}
	}
	if err := c.pruneVersions(paths); err != nil {
		return err
	}
	notice, err := json.Marshal(Notice{WorkspaceID: ws, DriveID: drive, Seq: c.seq})
	if err != nil {
		return err
	}
	_, err = c.tx.ExecContext(c.ctx, `SELECT pg_notify($1,$2)`, NoticeChannel, string(notice))
	return err
}

// pruneVersions keeps at most keptOldVersions old versions of each of paths.
// It reads the versions past that and then deletes them by ID. One statement
// that joins the drive's versions to themselves is planned as a nested loop
// when stale statistics put few rows in the drive, and then repeats the
// window over every version of the changed paths for each of them.
func (c *commit) pruneVersions(paths []string) error {
	ws, drive := string(c.ws), c.driveID
	rows, err := c.tx.Query(c.ctx, `SELECT version_id FROM (SELECT version_id,row_number() OVER (PARTITION BY path ORDER BY created_at DESC,seq DESC) AS n
		FROM drive_versions v WHERE workspace_id=$1 AND drive_id=$2 AND path=ANY($3) AND superseded_at IS NOT NULL AND `+keptVersion("v")+`) old
		WHERE n > $4`, ws, drive, paths, keptOldVersions)
	if err != nil {
		return err
	}
	var pruned []string
	err = scanAll(rows, func(scan func(...any) error) error {
		var versionID string
		if err := scan(&versionID); err != nil {
			return err
		}
		pruned = append(pruned, versionID)
		return nil
	})
	if err != nil || len(pruned) == 0 {
		return err
	}
	_, err = c.tx.ExecContext(c.ctx, `DELETE FROM drive_versions WHERE workspace_id=$1 AND drive_id=$2 AND version_id=ANY($3)`, ws, drive, pruned)
	return err
}
