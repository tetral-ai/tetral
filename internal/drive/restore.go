package drive

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// RestoreResult reports one restore. Restored counts the writes that created a
// version; both lists are in UTF-8 byte order; Seq is nil when the restore
// created no version.
type RestoreResult struct {
	Seq           *int64
	Restored      int
	RedactedPaths []string
	RemovedPaths  []string
}

// Restore brings every path under the raw byte prefix that changed after asOf
// back to its content at asOf, in one all_or_nothing commit under the drive
// lock. A kept, unredacted version names its content, so a restore uploads
// nothing.
func (s *Service) Restore(ctx context.Context, ws workspace.ID, driveID, prefix string, asOf time.Time, writer Writer) (RestoreResult, error) {
	if err := writer.validate(); err != nil {
		return RestoreResult{}, err
	}
	result := RestoreResult{RedactedPaths: []string{}, RemovedPaths: []string{}}
	err := s.db.WithWorkspaceTx(ctx, string(ws), "drive.restore", func(tx *dbconnect.Tx) error {
		locked, err := lockDrive(ctx, tx, ws, driveID)
		if err != nil {
			return err
		}
		histories, err := changedSince(ctx, tx, ws, driveID, prefix, asOf)
		if err != nil {
			return err
		}
		var changes []Change
		for _, history := range histories {
			at := history.versionAt(asOf)
			switch {
			case at == nil && history.oldest().Operation != OperationCreated:
				result.RemovedPaths = append(result.RemovedPaths, history.path)
			case at == nil || at.Operation == OperationDeleted:
			case at.RedactedAt != nil:
				result.RedactedPaths = append(result.RedactedPaths, history.path)
			default:
				changes = append(changes, Change{Path: history.path, Base: Base{Kind: BaseAny}, ContentSHA256: *at.ContentSHA256, Executable: at.Executable})
			}
		}
		if len(changes) == 0 {
			return nil
		}
		committed, err := commitLocked(ctx, tx, ws, driveID, locked, writer, ModeAllOrNothing, changes)
		if err != nil {
			return err
		}
		result.Seq = committed.Seq
		for _, change := range committed.Results {
			if change.NewVersion {
				result.Restored++
			}
		}
		return nil
	})
	return result, err
}

// pathHistory is a path's kept versions, oldest first.
type pathHistory struct {
	path     string
	versions []Version
}

func (h pathHistory) oldest() Version { return h.versions[0] }

// versionAt is the newest kept version created at or before asOf.
func (h pathHistory) versionAt(asOf time.Time) *Version {
	var at *Version
	for i := range h.versions {
		if !h.versions[i].CreatedAt.After(asOf) {
			at = &h.versions[i]
		}
	}
	return at
}

// changedSince lists the kept versions of every path under prefix whose
// current version was created after asOf.
func changedSince(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID, prefix string, asOf time.Time) ([]pathHistory, error) {
	rows, err := tx.Query(ctx, `SELECT `+versionColumns+` FROM drive_versions v WHERE workspace_id=$1 AND drive_id=$2 AND path LIKE $3 AND `+keptVersion("v")+`
		AND path IN (SELECT path FROM drive_versions c WHERE c.workspace_id=$1 AND c.drive_id=$2 AND c.superseded_at IS NULL AND c.created_at>$4 AND `+keptVersion("c")+`)
		ORDER BY path,created_at,seq`, string(ws), driveID, likePrefix(prefix), asOf)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var histories []pathHistory
	for rows.Next() {
		version, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		if n := len(histories); n == 0 || histories[n-1].path != version.Path {
			histories = append(histories, pathHistory{path: version.Path})
		}
		last := &histories[len(histories)-1]
		last.versions = append(last.versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return histories, nil
}
