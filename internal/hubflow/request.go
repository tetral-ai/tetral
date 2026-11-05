package hubflow

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// LockRequest takes, for a transaction that releases, fails or expires
// requestID, every lock it needs before the request's row: the runtime
// mutation locks of the request's Session and of the Sessions the end of its
// claim credential's connection changes, then its instance's row FOR NO KEY
// UPDATE. It then locks the request's row (computer.LockRequest) and
// returns it, with LockSetChangedError when its instance moved meanwhile.
func LockRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) (computer.Request, SessionLocks, error) {
	read, err := computer.ReadRequest(ctx, tx, ws, requestID)
	if err != nil {
		return computer.Request{}, SessionLocks{}, err
	}
	sessions := []string{read.SessionID}
	if read.InstanceID != nil && read.ClaimCredentialID != nil {
		_, claimed, _, err := unendedSessions(ctx, tx, ws, *read.InstanceID, *read.ClaimCredentialID)
		if err != nil {
			return computer.Request{}, SessionLocks{}, err
		}
		sessions = append(sessions, claimed...)
	}
	locks, err := LockSessions(ctx, tx, ws, sessions)
	if err != nil {
		return computer.Request{}, SessionLocks{}, err
	}
	if read.InstanceID != nil {
		if _, err := instance.LockInstanceNoKeyUpdate(ctx, tx, ws, *read.InstanceID); err != nil {
			return computer.Request{}, SessionLocks{}, err
		}
	}
	request, err := computer.LockRequest(ctx, tx, ws, requestID)
	if err != nil {
		return computer.Request{}, SessionLocks{}, err
	}
	if request.InstanceID != nil && (read.InstanceID == nil || *read.InstanceID != *request.InstanceID) {
		return computer.Request{}, SessionLocks{}, &LockSetChangedError{Message: "request " + requestID + " moved to another instance after its instance was locked"}
	}
	return request, locks, nil
}

// ReturnRequestToPending returns a claimed request to pending
// (computer.ReturnToPending) and ends, with cause revoked, the live connection
// of the claim credential that revoked. It returns that credential, "" when
// none was revoked. The caller holds the request's locks (LockRequest).
func ReturnRequestToPending(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, locks SessionLocks) (string, error) {
	revoked, err := computer.ReturnToPending(ctx, tx, ws, requestID)
	if err != nil {
		return "", err
	}
	return revoked, endRevokedConnection(ctx, tx, ws, revoked, locks)
}

// ReturnTimedOutClaim is ReturnRequestToPending for a claim that timed out
// (computer.ReturnTimedOutClaim): a request claimed again or fulfilled since
// it was found is left alone. It returns the revoked credential and whether
// the request returned to pending.
func ReturnTimedOutClaim(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, claimedBefore time.Time, locks SessionLocks) (string, bool, error) {
	revoked, returned, err := computer.ReturnTimedOutClaim(ctx, tx, ws, requestID, claimedBefore)
	if err != nil {
		return "", false, err
	}
	return revoked, returned, endRevokedConnection(ctx, tx, ws, revoked, locks)
}

// FailRequest ends a request as failed (computer.Fail, fromPending as there)
// and ends, with cause revoked, the live connection of the claim credential
// that revoked. The caller holds the request's locks (LockRequest), or a
// deregistration's.
func FailRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, fromPending bool, locks SessionLocks) (computer.Ended, error) {
	ended, err := computer.Fail(ctx, tx, ws, requestID, fromPending)
	if err != nil {
		return computer.Ended{}, err
	}
	return ended, endRevokedConnection(ctx, tx, ws, ended.Revoked, locks)
}

// endRevokedConnection ends the live connection under credentialID, when
// there is one, as every revocation of a live connection's credential does.
func endRevokedConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, credentialID string, locks SessionLocks) error {
	if credentialID == "" {
		return nil
	}
	credential, err := instance.ReadCredential(ctx, tx, ws, credentialID)
	if err != nil {
		return err
	}
	return endLiveConnection(ctx, tx, ws, credential.InstanceID, credentialID, locks)
}
