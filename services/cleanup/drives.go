package tetralcleanup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/drive"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// drivePageSize is how many drives one discovery page holds.
const drivePageSize = 100

type discoveredDrive struct {
	workspaceID string
	driveID     string
}

// RunDrives runs the daily drive cleanup once: every drive of every Workspace
// in (workspace_id, drive_id) order, then the due blobs of every Workspace
// that holds a due blob cleanup entry, whether or not it still has a drive. A
// failed drive is logged as drive.cleanup_failed and the run goes on; the run
// then fails after the blobs. Failed discovery or ctx ending stops the run at
// once.
func RunDrives(ctx context.Context, client *dbconnect.Client, blobs blob.BlobStore, logger *slog.Logger) error {
	cleaner := drive.NewCleaner(client, blobs)
	failed := 0
	var after *discoveredDrive
	for {
		page, err := discoverDrives(ctx, client, after)
		if err != nil {
			return err
		}
		for _, found := range page {
			if err := cleaner.CleanDrive(ctx, workspace.ID(found.workspaceID), found.driveID); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logDriveCleanupFailed(ctx, logger, found)
				failed++
			}
		}
		if len(page) < drivePageSize {
			break
		}
		after = &page[len(page)-1]
	}
	var errs []error
	var afterWorkspace *string
	for {
		page, err := discoverDueBlobWorkspaces(ctx, client, afterWorkspace)
		if err != nil {
			return err
		}
		for _, ws := range page {
			if err := cleaner.DeleteDueBlobs(ctx, workspace.ID(ws)); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				errs = append(errs, err)
			}
		}
		if len(page) < drivePageSize {
			break
		}
		afterWorkspace = &page[len(page)-1]
	}
	if failed > 0 {
		errs = append(errs, fmt.Errorf("%d drive cleanups failed", failed))
	}
	return errors.Join(errs...)
}

func discoverDrives(ctx context.Context, client *dbconnect.Client, after *discoveredDrive) ([]discoveredDrive, error) {
	var afterWorkspace, afterDrive *string
	if after != nil {
		afterWorkspace, afterDrive = &after.workspaceID, &after.driveID
	}
	var page []discoveredDrive
	err := client.WithTx(ctx, "tetralcleanup.discover_drives", &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx, `SELECT workspace_id, drive_id FROM public.tetral_cleanup_drive_page($1, $2, $3)`, afterWorkspace, afterDrive, drivePageSize)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var found discoveredDrive
			if err := rows.Scan(&found.workspaceID, &found.driveID); err != nil {
				return err
			}
			page = append(page, found)
		}
		return rows.Err()
	})
	return page, err
}

func discoverDueBlobWorkspaces(ctx context.Context, client *dbconnect.Client, after *string) ([]string, error) {
	var page []string
	err := client.WithTx(ctx, "tetralcleanup.discover_due_blobs", &sql.TxOptions{ReadOnly: true}, func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx, `SELECT workspace_id FROM public.tetral_cleanup_due_blob_workspaces($1, $2)`, after, drivePageSize)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var ws string
			if err := rows.Scan(&ws); err != nil {
				return err
			}
			page = append(page, ws)
		}
		return rows.Err()
	})
	return page, err
}

func logDriveCleanupFailed(ctx context.Context, logger *slog.Logger, failed discoveredDrive) {
	if logger == nil {
		return
	}
	logger.ErrorContext(ctx, "drive.cleanup_failed",
		slog.String("operation", "cleanup.drives"),
		slog.String("component", ServiceName),
		slog.String("workspace.id", failed.workspaceID),
		slog.String("drive.id", failed.driveID),
		slog.String("error.class", "dependency_unavailable"),
		slog.String("error.code", "drive_cleanup_failed"),
		slog.String("error.message_safe", "drive cleanup failed"),
	)
}
