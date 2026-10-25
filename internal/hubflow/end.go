package hubflow

import (
	"context"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// EndConnection ends connection with cause, one of instance.EndLeaseExpired,
// instance.EndRevoked and instance.EndDeregistered; the pod holding its
// stream then writes the disconnect of a revocation or deregistration and
// closes the stream.
func EndConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, cause string) error {
	return instance.EndConnection(ctx, tx, ws, connection, cause)
}
