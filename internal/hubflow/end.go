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
// on it: its instance reads offline; its active and closing bindings become
// invalid, their attached computers waiting and their detached ones unknown
// (computer.InvalidateConnection), and the instance's unused is refreshed;
// and its calls and background commands end
// (runtimecontrol.EndConnectionCallsTx).
// The pod holding its stream then writes the disconnect of a revocation or
// deregistration and closes the stream. Ending the lease fences Engine's
// authority only; it is no evidence that the machine stopped, and no
// execution moves to another connection.
//
// The caller holds locks, taken before any row, and the instance's and the
// connection's rows. EndConnection reads the connection's Sessions again
// and answers LockSetChangedError when locks does not hold one of them.
func EndConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, cause string, locks SessionLocks) error {
	sessions, err := connectionSessions(ctx, tx, ws, connection.ID)
	if err != nil {
		return err
	}
	if err := locks.cover(sessions); err != nil {
		return err
	}
	if err := instance.EndConnection(ctx, tx, ws, connection, cause); err != nil {
		return err
	}
	if err := computer.InvalidateConnection(ctx, tx, ws, connection.ID); err != nil {
		return err
	}
	if err := computer.RefreshUnused(ctx, tx, ws, connection.InstanceID); err != nil {
		return err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return err
	}
	return runtimecontrol.EndConnectionCallsTx(ctx, tx, ws, connection.ID, now)
}

// EndExpiredConnection is the Cleanup computer phase's end of connectionID,
// found with its lease passed: it locks the Sessions the end changes, the
// instance's row FOR NO KEY UPDATE and the connection's row, and ends it
// with cause lease_expired. It reports false when the connection has ended
// or was renewed since it was found.
func EndExpiredConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) (bool, error) {
	found, err := instance.ReadConnection(ctx, tx, ws, connectionID)
	if err != nil {
		return false, err
	}
	sessions, err := connectionSessions(ctx, tx, ws, connectionID)
	if err != nil {
		return false, err
	}
	locks, err := LockSessions(ctx, tx, ws, sessions)
	if err != nil {
		return false, err
	}
	if _, err := instance.LockInstanceNoKeyUpdate(ctx, tx, ws, found.InstanceID); err != nil {
		return false, err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return false, err
	}
	connection, expired, err := instance.LockExpiredConnection(ctx, tx, ws, connectionID, now)
	if err != nil || !expired {
		return false, err
	}
	return true, EndConnection(ctx, tx, ws, connection, instance.EndLeaseExpired, locks)
}

// RevokeCredential revokes credentialID of instanceID
// (instance.RevokeCredential) and ends, with cause revoked, the instance's
// live connection under it, after the locks of the Sessions that end changes
// and the instance's row FOR NO KEY UPDATE. It reports whether it revoked.
func RevokeCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string) (bool, error) {
	_, sessions, _, err := unendedSessions(ctx, tx, ws, instanceID, credentialID)
	if err != nil {
		return false, err
	}
	locks, err := LockSessions(ctx, tx, ws, sessions)
	if err != nil {
		return false, err
	}
	if _, err := instance.LockInstanceNoKeyUpdate(ctx, tx, ws, instanceID); err != nil {
		return false, err
	}
	revoked, err := instance.RevokeCredential(ctx, tx, ws, instanceID, credentialID)
	if err != nil || !revoked {
		return revoked, err
	}
	return true, endLiveConnection(ctx, tx, ws, instanceID, credentialID, locks)
}

// endLiveConnection ends, with cause revoked, instanceID's live connection
// under credentialID, when there is one.
func endLiveConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string, locks SessionLocks) error {
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return err
	}
	connection, found, err := instance.LockLiveConnectionOf(ctx, tx, ws, instanceID, credentialID, now)
	if err != nil || !found {
		return err
	}
	return EndConnection(ctx, tx, ws, connection, instance.EndRevoked, locks)
}
