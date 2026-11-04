package hubflow

import (
	"context"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// EndConnection ends connection with cause, one of instance.EndLeaseExpired,
// instance.EndRevoked and instance.EndDeregistered, and everything that rests
// on it: its instance reads offline, its active bindings become invalid and
// their attached computers waiting (computer.InvalidateConnection), and its
// calls and background commands end (runtimecontrol.EndConnectionCallsTx).
// The pod holding its stream then writes the disconnect of a revocation or
// deregistration and closes the stream. Ending the lease fences Engine's
// authority only; it is no evidence that the machine stopped, and no
// execution moves to another connection.
func EndConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, cause string) error {
	if err := instance.EndConnection(ctx, tx, ws, connection, cause); err != nil {
		return err
	}
	if err := runtimecontrol.LockConnectionTaskSessionsTx(ctx, tx, ws, connection.ID); err != nil {
		return err
	}
	if err := computer.InvalidateConnection(ctx, tx, ws, connection.ID); err != nil {
		return err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return err
	}
	return runtimecontrol.EndConnectionCallsTx(ctx, tx, ws, connection.ID, now)
}
