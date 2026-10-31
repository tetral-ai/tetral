package hubflow

import (
	"context"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// AcceptReady stores ready as connection's accepted ready, with its renewed
// lease, fulfils the requests the ready answers (computer.FulfilRequests),
// issues a session.prepare for every attached computer on the instance,
// those whose requests it fulfilled included, and notices the pod holding
// the connection's stream.
func AcceptReady(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, ready instance.ReadyAcceptance) error {
	if err := instance.AcceptReady(ctx, tx, ws, connection, ready); err != nil {
		return err
	}
	if err := computer.FulfilRequests(ctx, tx, ws, connection.InstanceID); err != nil {
		return err
	}
	if err := computer.IssuePreparationsOn(ctx, tx, ws, connection.InstanceID); err != nil {
		return err
	}
	return instance.NotifyConnection(ctx, tx, ws, connection.ID)
}
