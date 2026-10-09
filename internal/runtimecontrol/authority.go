package runtimecontrol

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage"
)

func LockSessionRuntimeArbitrationTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (string, error) {
	if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, workspaceID, sessionID); err != nil {
		return "", err
	}
	var lifecycleState string
	if err := tx.QueryRow(ctx,
		`SELECT lifecycle_state
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	).Scan(&lifecycleState); dbconnect.IsNoRows(err) {
		return "", ScopeSupersededError(status.Error(codes.NotFound, "session not found"))
	} else if err != nil {
		return "", err
	}
	return lifecycleState, nil
}

func LockRuntimeMutationSessionTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	lifecycleState, err := LockSessionRuntimeArbitrationTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return err
	}
	if lifecycleState == "deleted" {
		return ScopeSupersededError(status.Error(codes.FailedPrecondition, "session is deleted"))
	}
	return nil
}

func LockMainThreadIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (string, error) {
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND role = 'main'
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	)
	var sessionThreadID string
	if err := row.Scan(&sessionThreadID); dbconnect.IsNoRows(err) {
		return "", status.Error(codes.FailedPrecondition, "session main thread is unavailable")
	} else if err != nil {
		return "", err
	}
	return sessionThreadID, nil
}
