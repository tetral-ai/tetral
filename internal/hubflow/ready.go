package hubflow

import (
	"context"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// ReadyTransitions are facts to record only after the ready transaction commits.
type ReadyTransitions struct {
	Fulfilled          []computer.Request
	Preparations       []computer.Binding
	SkippedLostChanges []computer.SkippedLostChange
}

// AcceptReady stores ready as connection's accepted ready, with its renewed
// lease; applies the lost_changes the ready carries
// (computer.RecordLostChanges); fulfils the requests the ready answers
// (computer.FulfilRequests); issues a session.prepare for every attached
// computer on the instance, those whose requests it fulfilled included, and
// a late close for every detached computer on it whose closure is unknown
// (computer.IssueLateCloses); and notices the pod holding the connection's
// stream. The caller holds locks (LockReadySessions), then the instance's,
// credential's and connection's rows; AcceptReady first reads the ready's
// Sessions again and answers LockSetChangedError when locks does not hold
// one of them.
func AcceptReady(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, ready instance.ReadyAcceptance, lostChanges []protocol.LostChange, locks SessionLocks) (ReadyTransitions, error) {
	sessions, err := readySessions(ctx, tx, ws, connection.InstanceID, lostChanges)
	if err != nil {
		return ReadyTransitions{}, err
	}
	if err := locks.cover(sessions); err != nil {
		return ReadyTransitions{}, err
	}
	if err := instance.AcceptReady(ctx, tx, ws, connection, ready); err != nil {
		return ReadyTransitions{}, err
	}
	skipped, err := computer.RecordLostChanges(ctx, tx, ws, connection.InstanceID, "", lostChanges, ready.AcceptedAt)
	if err != nil {
		return ReadyTransitions{}, err
	}
	requests, err := computer.FulfilRequests(ctx, tx, ws, connection)
	if err != nil {
		return ReadyTransitions{}, err
	}
	preparations, err := computer.IssuePreparationsOn(ctx, tx, ws, connection.InstanceID)
	if err != nil {
		return ReadyTransitions{}, err
	}
	if err := computer.IssueLateCloses(ctx, tx, ws, connection); err != nil {
		return ReadyTransitions{}, err
	}
	if err := instance.NotifyConnection(ctx, tx, ws, connection.ID); err != nil {
		return ReadyTransitions{}, err
	}
	return ReadyTransitions{Fulfilled: requests, Preparations: preparations, SkippedLostChanges: skipped}, nil
}
