package hubflow

import (
	"context"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// ReadyTransitions are facts to record only after the ready transaction commits.
type ReadyTransitions struct {
	Fulfilled    []computer.Request
	Preparations []computer.Binding
}

// AcceptReady stores ready as connection's accepted ready, with its renewed
// lease, fulfils the requests the ready answers (computer.FulfilRequests),
// issues a session.prepare for every attached computer on the instance,
// those whose requests it fulfilled included, and notices the pod holding
// the connection's stream.
func AcceptReady(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection instance.Connection, ready instance.ReadyAcceptance) (ReadyTransitions, error) {
	if err := instance.AcceptReady(ctx, tx, ws, connection, ready); err != nil {
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
	if err := instance.NotifyConnection(ctx, tx, ws, connection.ID); err != nil {
		return ReadyTransitions{}, err
	}
	return ReadyTransitions{Fulfilled: requests, Preparations: preparations}, nil
}
