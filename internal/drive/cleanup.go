package drive

import (
	"context"
	"database/sql"
	"errors"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// blobDeletePage is how many due blob cleanup entries one read takes.
const blobDeletePage = 100

// Cleaner runs the daily drive cleanup: it deletes what retention has
// removed and records each drive's stored_bytes.
type Cleaner struct {
	db    *dbconnect.Client
	blobs blob.BlobStore
}

func NewCleaner(db *dbconnect.Client, blobs blob.BlobStore) *Cleaner {
	return &Cleaner{db: db, blobs: blobs}
}

// CleanDrive runs one drive's steps in one transaction under its drive lock.
// An active drive loses the versions and change records retention excludes
// and the content no version names that was uploaded more than 24 hours ago,
// then gets its stored_bytes. A deleted drive loses all its content, then its
// row. Removed content leaves its blob key in drive_blob_cleanup, due now.
func (c *Cleaner) CleanDrive(ctx context.Context, ws workspace.ID, driveID string) error {
	return c.db.WithWorkspaceTx(ctx, string(ws), "drive.cleanup", func(tx *dbconnect.Tx) error {
		var deleted bool
		err := tx.QueryRow(ctx, `SELECT deleted_at IS NOT NULL FROM drives WHERE workspace_id=$1 AND drive_id=$2 FOR UPDATE`, string(ws), driveID).Scan(&deleted)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if deleted {
			if err := releaseContents(ctx, tx, ws, driveID, `TRUE`); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `DELETE FROM drives WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID)
			return err
		}
		// The kept predicate is NULL, not false, for a current deletion.
		if _, err := tx.ExecContext(ctx, `DELETE FROM drive_versions v WHERE workspace_id=$1 AND drive_id=$2 AND (`+keptVersion("v")+`) IS NOT TRUE`, string(ws), driveID); err != nil {
			return err
		}
		var seq sql.NullInt64
		var index sql.NullInt32
		err = tx.QueryRow(ctx, `WITH gone AS (DELETE FROM drive_changes c WHERE workspace_id=$1 AND drive_id=$2 AND NOT `+listedChange("c")+` RETURNING seq,change_index)
			SELECT seq,change_index FROM gone ORDER BY seq DESC,change_index DESC LIMIT 1`, string(ws), driveID).Scan(&seq, &index)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return err
		default:
			// Records age in seq order, so the newest one deleted now is the
			// newest one ever deleted.
			if _, err := tx.ExecContext(ctx, `UPDATE drives SET removed_change_seq=$3,removed_change_index=$4 WHERE workspace_id=$1 AND drive_id=$2`,
				string(ws), driveID, seq.Int64, index.Int32); err != nil {
				return err
			}
		}
		unnamed := `uploaded_at < now() - ` + uploadGraceInterval + ` AND NOT EXISTS (SELECT 1 FROM drive_versions v
			WHERE v.workspace_id=c.workspace_id AND v.drive_id=c.drive_id AND v.content_sha256=c.content_sha256 AND ` + namesContent("v") + `)`
		if err := releaseContents(ctx, tx, ws, driveID, unnamed); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE drives SET stored_bytes=(SELECT coalesce(sum(size_bytes),0) FROM (
				SELECT DISTINCT v.content_sha256,v.size_bytes FROM drive_versions v WHERE v.workspace_id=$1 AND v.drive_id=$2 AND `+namesContent("v")+`) named)
			WHERE workspace_id=$1 AND drive_id=$2`, string(ws), driveID)
		return err
	})
}

// releaseContents deletes the drive's content rows matching predicate, over
// alias c, and makes each blob due for deletion now.
func releaseContents(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID, predicate string) error {
	_, err := tx.ExecContext(ctx, `WITH gone AS (DELETE FROM drive_contents c WHERE workspace_id=$1 AND drive_id=$2 AND `+predicate+` RETURNING blob_key)
		INSERT INTO drive_blob_cleanup(workspace_id,blob_key,delete_after) SELECT $1,blob_key,clock_timestamp() FROM gone
		ON CONFLICT (workspace_id,blob_key) DO UPDATE SET delete_after=excluded.delete_after`, string(ws), driveID)
	return err
}

// DeleteDueBlobs deletes the blob of every cleanup entry of the Workspace
// whose delete_after has passed, then the entry. A blob already missing
// counts as deleted; a failed delete keeps the entry for the next run.
func (c *Cleaner) DeleteDueBlobs(ctx context.Context, ws workspace.ID) error {
	var errs []error
	for after := ""; ; {
		var keys []string
		err := c.db.WithWorkspaceReadOnlyTx(ctx, string(ws), "drive.blob_cleanup_due", func(tx *dbconnect.Tx) error {
			rows, err := tx.Query(ctx, `SELECT blob_key FROM drive_blob_cleanup WHERE workspace_id=$1 AND delete_after<=clock_timestamp() AND blob_key>$2
				ORDER BY blob_key LIMIT $3`, string(ws), after, blobDeletePage)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err != nil {
					return err
				}
				keys = append(keys, key)
			}
			return rows.Err()
		})
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, key := range keys {
			if err := c.deleteBlob(ctx, ws, key); err != nil {
				errs = append(errs, err)
			}
		}
		if len(keys) < blobDeletePage {
			return errors.Join(errs...)
		}
		after = keys[len(keys)-1]
	}
}

func (c *Cleaner) deleteBlob(ctx context.Context, ws workspace.ID, key string) error {
	var missing *blob.NotFoundError
	if err := c.blobs.Delete(ctx, key); err != nil && !errors.As(err, &missing) {
		return err
	}
	return c.db.WithWorkspaceTx(ctx, string(ws), "drive.blob_cleanup_delete", func(tx *dbconnect.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM drive_blob_cleanup WHERE workspace_id=$1 AND blob_key=$2 AND delete_after<=clock_timestamp()`, string(ws), key)
		return err
	})
}
