package drive

import (
	"context"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Redact erases one past version's content under the drive lock: the record
// stays, marked with who redacted it, and names no content from then on. The
// content stays stored only while another version names the same hash.
func (s *Service) Redact(ctx context.Context, ws workspace.ID, driveID, versionID string, writer Writer) (Version, error) {
	if err := writer.validate(); err != nil {
		return Version{}, err
	}
	var redacted Version
	err := s.db.WithWorkspaceTx(ctx, string(ws), "drive.redact", func(tx *dbconnect.Tx) error {
		if _, err := lockDrive(ctx, tx, ws, driveID); err != nil {
			return err
		}
		version, err := readKeptVersion(ctx, tx, ws, driveID, versionID)
		if err != nil {
			return err
		}
		current, err := isCurrent(ctx, tx, ws, driveID, versionID)
		switch {
		case err != nil:
			return err
		case current:
			return ErrRedactCurrent
		case version.RedactedAt != nil:
			redacted = version
			return nil
		}
		args := append([]any{string(ws), driveID, versionID}, writerArgs(writer)...)
		redacted, err = scanVersion(tx.QueryRow(ctx, `UPDATE drive_versions SET redacted_at=clock_timestamp(),redacted_by_kind=$4,redacted_by_id=$5,redacted_by_computer_id=$6
			WHERE workspace_id=$1 AND drive_id=$2 AND version_id=$3 RETURNING `+versionColumns, args...))
		return err
	})
	return redacted, err
}

func isCurrent(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, driveID, versionID string) (bool, error) {
	var current bool
	err := tx.QueryRow(ctx, `SELECT superseded_at IS NULL FROM drive_versions WHERE workspace_id=$1 AND drive_id=$2 AND version_id=$3`, string(ws), driveID, versionID).Scan(&current)
	return current, err
}
