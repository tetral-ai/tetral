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
	Terminal bool // no automatic retry remains
	Type     string
	Message  string
	Fields   map[string]any
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
// sessionID (writeSessionEvents), each with retry_status retrying (terminal
// when automatic recovery has ended).
func WriteSessionErrors(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, now time.Time, errs []SessionError) error {
	events := make([]map[string]any, 0, len(errs))
	for _, sessionErr := range errs {
		retry := "retrying"
		if sessionErr.Terminal {
			retry = "terminal"
		}
		body := map[string]any{"type": sessionErr.Type, "message": sessionErr.Message, "retry_status": map[string]any{"type": retry}}
		for key, value := range sessionErr.Fields {
			body[key] = value
		}
		events = append(events, map[string]any{"type": "session.error", "error": body})
	}
	return writeSessionEvents(ctx, tx, ws, sessionID, now, events)
}

// writeSessionEvents writes events, in order, as public events of sessionID,
// which belong to no thread, on its primary stream, under the Session's
// runtime mutation advisory lock, which every writer of a Session event takes
// before it numbers one. Each event's "type" is its event type.
func writeSessionEvents(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, now time.Time, events []map[string]any) error {
	if len(events) == 0 {
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
	for _, event := range events {
		payload, err := runtimecontrol.MarshalJSON(event)
		if err != nil {
			return err
		}
		sequence++
		eventType, _ := event["type"].(string)
		if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
			WorkspaceID: string(ws), SessionID: sessionID, EventID: id.New("evt_"), Sequence: sequence, Type: eventType,
			PayloadJSON: payload, ProjectionJSON: payload, Visibility: "public", SessionVisible: true, CreatedAt: now, ProcessedAt: &now,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ToolReportErrors are the session.error events a stored tool report writes,
// in order: computer_sandbox_error, with the CLI's message, for the first
// sandbox_unavailable rejection on its connection, then one drive_mount_error
// per drive it lists as unmounted.
func ToolReportErrors(stored runtimecontrol.StoredReport) []SessionError {
	var errs []SessionError
	if stored.SandboxUnavailable.Set {
		errs = append(errs, SessionError{Type: "computer_sandbox_error", Message: stored.SandboxUnavailable.Value,
			Fields: map[string]any{"computer_id": stored.Dispatch.ComputerID}})
	}
	return append(errs, DriveMountErrors(stored.Dispatch.ComputerID, stored.Unmounted)...)
}
