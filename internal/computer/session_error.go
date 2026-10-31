package computer

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// SessionError is one session.error a stored machine report writes: its
// error type, message and the resource fields of that type.
type SessionError struct {
	Type    string
	Message string
	Fields  map[string]any
}

// repositoryErrorTypes maps a repository failure's cause to its error type.
var repositoryErrorTypes = map[string]string{
	"clone":          "repository_clone_error",
	"not_found":      "repository_not_found_error",
	"authentication": "repository_authentication_error",
	"forbidden":      "repository_forbidden_error",
	"checkout":       "repository_checkout_error",
}

func preparationError(computerID string, failure protocol.PreparationError) SessionError {
	return SessionError{Type: "computer_preparation_error", Message: failure.Message, Fields: map[string]any{"computer_id": computerID, "code": failure.Code}}
}

// DriveMountErrors are the drive_mount_error events of the mount failures a
// report on computerID lists, one per failure.
func DriveMountErrors(computerID string, failures []protocol.DriveMountFailure) []SessionError {
	errs := make([]SessionError, 0, len(failures))
	for _, failure := range failures {
		errs = append(errs, SessionError{Type: "drive_mount_error", Message: failure.Error, Fields: map[string]any{"drive_id": failure.DriveID, "computer_id": computerID}})
	}
	return errs
}

func repositoryErrors(failures []protocol.RepositoryFailure) []SessionError {
	errs := make([]SessionError, 0, len(failures))
	for _, failure := range failures {
		errs = append(errs, SessionError{Type: repositoryErrorTypes[failure.Cause], Message: failure.Message, Fields: map[string]any{"repository_url": failure.URL}})
	}
	return errs
}

// WriteSessionErrors writes errs, in order, as session.error events of
// sessionID, which belong to no thread, on its primary stream, each with
// retry_status retrying, under the Session's runtime mutation advisory lock,
// which every writer of a Session event takes before it numbers one.
func WriteSessionErrors(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, now time.Time, errs []SessionError) error {
	if len(errs) == 0 {
		return nil
	}
	if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(ws), sessionID); err != nil {
		return err
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(sequence),0) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id IS NULL`,
		string(ws), sessionID).Scan(&sequence); err != nil {
		return err
	}
	for _, sessionErr := range errs {
		body := map[string]any{"type": sessionErr.Type, "message": sessionErr.Message, "retry_status": map[string]any{"type": "retrying"}}
		for key, value := range sessionErr.Fields {
			body[key] = value
		}
		payload, err := runtimecontrol.MarshalJSON(map[string]any{"type": "session.error", "error": body})
		if err != nil {
			return err
		}
		sequence++
		if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
			WorkspaceID: string(ws), SessionID: sessionID, EventID: id.New("evt_"), Sequence: sequence, Type: "session.error",
			PayloadJSON: payload, ProjectionJSON: payload, Visibility: "public", SessionVisible: true, CreatedAt: now, ProcessedAt: &now,
		}); err != nil {
			return err
		}
	}
	return nil
}
