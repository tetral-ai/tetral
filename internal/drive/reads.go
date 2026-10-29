package drive

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"math"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// FileQuery lists current files whose path is after After and starts with the
// raw bytes of Prefix, in UTF-8 byte order of path.
type FileQuery struct {
	After  string
	Prefix string
	Limit  int
}

func (s *Service) ListFiles(ctx context.Context, ws workspace.ID, driveID string, query FileQuery) ([]File, error) {
	var files []File
	err := s.read(ctx, ws, driveID, "drive.files_list", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+fileColumns+` FROM drive_files WHERE workspace_id=$1 AND drive_id=$2 AND path>$3 AND path LIKE $4
			ORDER BY path LIMIT $5`, string(ws), driveID, query.After, likePrefix(query.Prefix), query.Limit)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			file, err := scanFile(rows)
			if err != nil {
				return err
			}
			files = append(files, file)
		}
		return rows.Err()
	})
	return files, err
}

// ListVersions lists a path's kept versions, oldest first.
func (s *Service) ListVersions(ctx context.Context, ws workspace.ID, driveID, path string) ([]Version, error) {
	var versions []Version
	err := s.read(ctx, ws, driveID, "drive.versions_list", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+versionColumns+` FROM drive_versions v WHERE workspace_id=$1 AND drive_id=$2 AND path=$3 AND `+keptVersion("v")+`
			ORDER BY created_at,seq`, string(ws), driveID, path)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			version, err := scanVersion(rows)
			if err != nil {
				return err
			}
			versions = append(versions, version)
		}
		return rows.Err()
	})
	return versions, err
}

// GetVersion reads one kept version.
func (s *Service) GetVersion(ctx context.Context, ws workspace.ID, driveID, versionID string) (Version, error) {
	var version Version
	err := s.read(ctx, ws, driveID, "drive.version_read", func(tx *dbconnect.Tx) error {
		var readErr error
		version, readErr = readKeptVersion(ctx, tx, ws, driveID, versionID)
		return readErr
	})
	return version, err
}

func readKeptVersion(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID, versionID string) (Version, error) {
	version, err := scanVersion(tx.QueryRow(ctx, `SELECT `+versionColumns+` FROM drive_versions v WHERE workspace_id=$1 AND drive_id=$2 AND version_id=$3 AND `+keptVersion("v"),
		string(ws), driveID, versionID))
	if errors.Is(err, sql.ErrNoRows) {
		return Version{}, ErrNotFound
	}
	return version, err
}

// OpenVersionContent opens a version's content. A deletion, a redacted version
// and one not kept have none.
func (s *Service) OpenVersionContent(ctx context.Context, ws workspace.ID, driveID, versionID string) (io.ReadCloser, Version, error) {
	var version Version
	var key string
	err := s.read(ctx, ws, driveID, "drive.version_content_read", func(tx *dbconnect.Tx) error {
		var readErr error
		if version, readErr = readKeptVersion(ctx, tx, ws, driveID, versionID); readErr != nil {
			return readErr
		}
		if version.ContentSHA256 == nil || version.RedactedAt != nil {
			return ErrNotFound
		}
		key, readErr = contentKey(ctx, tx, ws, driveID, *version.ContentSHA256)
		return readErr
	})
	if err != nil {
		return nil, Version{}, err
	}
	body, err := s.blobs.Get(ctx, key)
	return body, version, err
}

// OpenContent opens content by hash while a kept, unredacted version of the
// drive names it.
func (s *Service) OpenContent(ctx context.Context, ws workspace.ID, driveID, sha256 string) (io.ReadCloser, Content, error) {
	var key string
	var content Content
	err := s.read(ctx, ws, driveID, "drive.content_read", func(tx *dbconnect.Tx) error {
		err := tx.QueryRow(ctx, `SELECT c.blob_key,c.size_bytes FROM drive_contents c WHERE c.workspace_id=$1 AND c.drive_id=$2 AND c.content_sha256=$3
			AND EXISTS (SELECT 1 FROM drive_versions v WHERE v.workspace_id=c.workspace_id AND v.drive_id=c.drive_id AND v.content_sha256=c.content_sha256 AND `+namesContent("v")+`)`,
			string(ws), driveID, sha256).Scan(&key, &content.SizeBytes)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	})
	if err != nil {
		return nil, Content{}, err
	}
	content.SHA256 = sha256
	body, err := s.blobs.Get(ctx, key)
	return body, content, err
}

func contentKey(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID, sha256 string) (string, error) {
	var key string
	err := tx.QueryRow(ctx, `SELECT blob_key FROM drive_contents WHERE workspace_id=$1 AND drive_id=$2 AND content_sha256=$3`, string(ws), driveID, sha256).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return key, err
}

// ChangeQuery lists change records after a cursor, in (seq, index) order. With
// AfterIndex nil the cursor is AfterSeq in whole commits; with neither set the
// list starts at the oldest listed record. SessionID keeps the records that
// one Session's computers made.
type ChangeQuery struct {
	AfterSeq   *int64
	AfterIndex *int
	SessionID  string
	Limit      int
}

// ListChanges returns at most Limit records and whether more exist. A cursor
// below the newest removed record returns ErrCursorExpired.
func (s *Service) ListChanges(ctx context.Context, ws workspace.ID, driveID string, query ChangeQuery) ([]ChangeRecord, bool, error) {
	var records []ChangeRecord
	var more bool
	err := s.read(ctx, ws, driveID, "drive.changes_list", func(tx *dbconnect.Tx) error {
		after := changePosition{seq: 0, index: -1}
		if query.AfterSeq != nil {
			after = changePosition{seq: *query.AfterSeq, index: math.MaxInt32}
			if query.AfterIndex != nil {
				after.index = *query.AfterIndex
			}
			removed, found, err := newestRemovedChange(ctx, tx, ws, driveID)
			if err != nil {
				return err
			}
			expired := after.seq < removed.seq
			if query.AfterIndex != nil {
				expired = after.before(removed)
			}
			if found && expired {
				return ErrCursorExpired
			}
		}
		var session any
		if query.SessionID != "" {
			session = query.SessionID
		}
		rows, err := tx.Query(ctx, `SELECT `+changeColumns+` FROM drive_changes c WHERE workspace_id=$1 AND drive_id=$2 AND (seq,change_index)>($3,$4) AND `+listedChange("c")+`
			AND ($5::text IS NULL OR (writer_kind='session_actor' AND writer_id=$5)) ORDER BY seq,change_index LIMIT $6`,
			string(ws), driveID, after.seq, after.index, session, query.Limit+1)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			record, err := scanChange(rows)
			if err != nil {
				return err
			}
			records = append(records, record)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(records) > query.Limit {
			records, more = records[:query.Limit], true
		}
		return nil
	})
	return records, more, err
}

// LastSeq is the sequence number of the drive's latest commit, 0 before one.
func (s *Service) LastSeq(ctx context.Context, ws workspace.ID, driveID string) (int64, error) {
	var seq int64
	err := s.db.WithWorkspaceReadOnlyTx(ctx, string(ws), "drive.last_seq_read", func(tx *dbconnect.Tx) error {
		err := tx.QueryRow(ctx, `SELECT last_seq FROM drives WHERE workspace_id=$1 AND drive_id=$2 AND deleted_at IS NULL`, string(ws), driveID).Scan(&seq)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDriveNotFound
		}
		return err
	})
	return seq, err
}

// read runs fn in a read-only transaction once the drive is known to be active.
func (s *Service) read(ctx context.Context, ws workspace.ID, driveID, operation string, fn func(*dbconnect.Tx) error) error {
	return s.db.WithWorkspaceReadOnlyTx(ctx, string(ws), operation, func(tx *dbconnect.Tx) error {
		var active bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM drives WHERE workspace_id=$1 AND drive_id=$2 AND deleted_at IS NULL)`, string(ws), driveID).Scan(&active); err != nil {
			return err
		}
		if !active {
			return ErrDriveNotFound
		}
		return fn(tx)
	})
}
