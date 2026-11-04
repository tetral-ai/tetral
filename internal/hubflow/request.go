package hubflow

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// ReturnRequestToPending returns a claimed request to pending
// (computer.ReturnToPending) and ends, with cause revoked, the live connection
// of the claim credential that revoked. It returns that credential, "" when
// none was revoked.
func ReturnRequestToPending(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) (string, error) {
	revoked, err := computer.ReturnToPending(ctx, tx, ws, requestID)
	if err != nil {
		return "", err
	}
	return revoked, endRevokedConnection(ctx, tx, ws, revoked)
}

// ReturnTimedOutClaim is ReturnRequestToPending for a claim that timed out
// (computer.ReturnTimedOutClaim): a request claimed again or fulfilled since
// it was found is left alone. It returns the revoked credential and whether
// the request returned to pending.
func ReturnTimedOutClaim(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, claimedBefore time.Time) (string, bool, error) {
	revoked, returned, err := computer.ReturnTimedOutClaim(ctx, tx, ws, requestID, claimedBefore)
	if err != nil {
		return "", false, err
	}
	return revoked, returned, endRevokedConnection(ctx, tx, ws, revoked)
}

// FailRequest ends a request as failed (computer.Fail, fromPending as there)
// and ends, with cause revoked, the live connection of the claim credential
// that revoked.
func FailRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, fromPending bool) (computer.Ended, error) {
	ended, err := computer.Fail(ctx, tx, ws, requestID, fromPending)
	if err != nil {
		return computer.Ended{}, err
	}
	return ended, endRevokedConnection(ctx, tx, ws, ended.Revoked)
}

// endRevokedConnection ends the live connection under credentialID, when
// there is one, as every revocation of a live connection's credential does.
func endRevokedConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, credentialID string) error {
	if credentialID == "" {
		return nil
	}
	credential, err := instance.ReadCredential(ctx, tx, ws, credentialID)
	if err != nil {
		return err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return err
	}
	connection, found, err := instance.LockLiveConnectionOf(ctx, tx, ws, credential.InstanceID, credentialID, now)
	if err != nil || !found {
		return err
	}
	return EndConnection(ctx, tx, ws, connection, instance.EndRevoked)
}
