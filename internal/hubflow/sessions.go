package hubflow

import (
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// LockSetChangedError is a transaction that found, once it held its rows, a
// Session it changes whose runtime mutation lock it did not take first, or
// a request whose instance is no longer the one it locked. Its caller rolls
// it back and runs it again: a route through RetryLockSetChange, the
// Cleanup computer phase at its next run.
type LockSetChangedError struct{ Message string }

func (e *LockSetChangedError) Error() string { return e.Message }

// IsLockSetChanged reports whether err is a LockSetChangedError.
func IsLockSetChanged(err error) bool {
	var changed *LockSetChangedError
	return errors.As(err, &changed)
}

// lockSetRetries is how many times RetryLockSetChange runs a transaction
// again.
const lockSetRetries = 3

// RetryLockSetChange runs run, one whole transaction, and runs it again
// while it fails with LockSetChangedError, at most lockSetRetries times. It
// returns the last error, which the route answers 503.
func RetryLockSetChange(run func() error) error {
	err := run()
	for retry := 0; retry < lockSetRetries && IsLockSetChanged(err); retry++ {
		err = run()
	}
	return err
}

// SessionLocks are the Sessions whose runtime mutation locks a transaction
// took before any row lock (LockSessions).
type SessionLocks struct{ held map[string]bool }

// LockSessions takes the runtime mutation lock of every Session in sets,
// once each, in Session ID order.
func LockSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sets ...[]string) (SessionLocks, error) {
	locks := SessionLocks{held: map[string]bool{}}
	for _, set := range sets {
		for _, sessionID := range set {
			locks.held[sessionID] = true
		}
	}
	for _, sessionID := range slices.Sorted(maps.Keys(locks.held)) {
		if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(ws), sessionID); err != nil {
			return SessionLocks{}, err
		}
	}
	return locks, nil
}

// cover is the second read's check: LockSetChangedError when a Session of
// sets is not held.
func (l SessionLocks) cover(sets ...[]string) error {
	for _, set := range sets {
		for _, sessionID := range set {
			if !l.held[sessionID] {
				return &LockSetChangedError{Message: "Session " + sessionID + " joined the transaction after its Sessions were locked"}
			}
		}
	}
	return nil
}

// connectionSessions reads the Sessions the end of connectionID changes:
// those of its bindings (computer.ConnectionSessions) and of its calls and
// background commands (runtimecontrol.ConnectionCallSessionsTx).
func connectionSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) ([]string, error) {
	bindings, err := computer.ConnectionSessions(ctx, tx, ws, connectionID)
	if err != nil {
		return nil, err
	}
	calls, err := runtimecontrol.ConnectionCallSessionsTx(ctx, tx, ws, connectionID)
	return append(bindings, calls...), err
}

// unendedSessions reads instanceID's connection that has not ended and the
// Sessions its end changes; with credentialID set, only a connection under
// that credential counts. It returns the connection, found false when none
// counts.
func unendedSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string) (instance.Connection, []string, bool, error) {
	connection, found, err := instance.ReadUnendedConnection(ctx, tx, ws, instanceID)
	if err != nil || !found || (credentialID != "" && connection.CredentialID != credentialID) {
		return instance.Connection{}, nil, false, err
	}
	sessions, err := connectionSessions(ctx, tx, ws, connection.ID)
	return connection, sessions, err == nil, err
}

// readySessions reads the Sessions a ready of instanceID that carries
// lostChanges changes: those of the computers and requests on the instance
// (computer.InstanceSessions) and of its lost changes
// (computer.LostChangeSessions).
func readySessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, lostChanges []protocol.LostChange) ([]string, error) {
	onInstance, err := computer.InstanceSessions(ctx, tx, ws, instanceID)
	if err != nil {
		return nil, err
	}
	lost, err := computer.LostChangeSessions(ctx, tx, ws, instanceID, lostChanges)
	return append(onInstance, lost...), err
}

// LockReadySessions is the first step of a ready's transaction, before any
// row lock: it locks the Sessions the ready changes (readySessions), which
// AcceptReady reads again.
func LockReadySessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, lostChanges []protocol.LostChange) (SessionLocks, error) {
	sessions, err := readySessions(ctx, tx, ws, instanceID, lostChanges)
	if err != nil {
		return SessionLocks{}, err
	}
	return LockSessions(ctx, tx, ws, sessions)
}

// LockPredecessorSessions is the first step of an events request's
// transaction, before any row lock: when instanceID's connection that has
// not ended has an expired lease, which the request then ends, it locks the
// Sessions that end changes and reports expired, so the request locks the
// instance for the update the end makes.
func LockPredecessorSessions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (SessionLocks, bool, error) {
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return SessionLocks{}, false, err
	}
	previous, sessions, found, err := unendedSessions(ctx, tx, ws, instanceID, "")
	if err != nil {
		return SessionLocks{}, false, err
	}
	expired := found && !previous.Live(now)
	if !expired {
		sessions = nil
	}
	locks, err := LockSessions(ctx, tx, ws, sessions)
	return locks, expired, err
}
