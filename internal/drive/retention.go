package drive

import (
	"context"
	"database/sql"
	"errors"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Every read decides what is kept by age against the database clock, whether
// or not the daily cleanup has deleted the rows yet: an old version is kept
// for 30 days after it was superseded, a current deletion for 30 days after it
// was made, and a current file always.
func keptVersion(alias string) string {
	return `((` + alias + `.superseded_at IS NULL AND (` + alias + `.operation<>'deleted' OR ` + alias + `.created_at > now() - ` + retentionInterval + `))` +
		` OR ` + alias + `.superseded_at > now() - ` + retentionInterval + `)`
}

// namesContent holds for a kept version that is neither a deletion nor
// redacted; only such a version keeps its content readable.
func namesContent(alias string) string {
	return `(` + keptVersion(alias) + ` AND ` + alias + `.operation<>'deleted' AND ` + alias + `.redacted_at IS NULL)`
}

// listedChange holds for a change record younger than 30 days, whether or not
// its version is still kept.
func listedChange(alias string) string {
	return alias + `.created_at > now() - ` + retentionInterval
}

// changePosition is a change record's place in the change list.
type changePosition struct {
	seq   int64
	index int
}

func (p changePosition) before(other changePosition) bool {
	return p.seq < other.seq || (p.seq == other.seq && p.index < other.index)
}

// newestRemovedChange is the later of the newest record the cleanup deleted
// and the newest record that has aged out; ok is false when none is removed.
func newestRemovedChange(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID string) (changePosition, bool, error) {
	var removed changePosition
	var found bool
	var seq sql.NullInt64
	var index sql.NullInt32
	if err := tx.QueryRow(ctx, `SELECT removed_change_seq,removed_change_index FROM drives WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID).Scan(&seq, &index); err != nil {
		return changePosition{}, false, err
	}
	if seq.Valid {
		removed, found = changePosition{seq: seq.Int64, index: int(index.Int32)}, true
	}
	var aged changePosition
	err := tx.QueryRow(ctx, `SELECT seq,change_index FROM drive_changes c WHERE workspace_id=$1 AND drive_id=$2 AND NOT `+listedChange("c")+`
		ORDER BY seq DESC,change_index DESC LIMIT 1`, string(ws), driveID).Scan(&aged.seq, &aged.index)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return changePosition{}, false, err
	case !found || removed.before(aged):
		removed, found = aged, true
	}
	return removed, found, nil
}
