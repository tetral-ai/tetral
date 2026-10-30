package hubflow

import (
	"context"

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

// FailRequest ends a request as failed (computer.Fail, fromPending as there)
// and ends, with cause revoked, the live connection of the claim credential
// that revoked. It returns that credential, "" when none was revoked.
func FailRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, fromPending bool) (string, error) {
	revoked, err := computer.Fail(ctx, tx, ws, requestID, fromPending)
	if err != nil {
		return "", err
	}
	return revoked, endRevokedConnection(ctx, tx, ws, revoked)
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
