package computer

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

// RecordPreparationStarted records a binding issued by a committed transaction.
func RecordPreparationStarted(ctx context.Context, binding Binding) {
	ctx = workload.WithFields(ctx, "session.id", binding.SessionID, "computer.id", binding.ComputerID,
		"instance.id", binding.InstanceID, "connection.id", binding.ConnectionID, "binding.id", binding.BindingID)
	workload.RecordStoredPhase(ctx, "session_computer", workload.StoredPhase{Name: "prepare", Start: binding.CreatedAt})
}

// RecordRequestPhase records a committed request transition, using its stored
// times: request runs from its creation to its first claim, instance from its
// first claim, which a later claim keeps. A zero end starts the phase. The
// records of one request carry its computer, which has at most one open
// request, and the instance of the attempt at that transition. Replayed
// mutations must not call it.
func RecordRequestPhase(ctx context.Context, request Request, phase string, end time.Time) {
	workload.RecordStoredPhase(requestContext(ctx, request), "session_computer", requestPhase(request, phase, end))
}

func requestContext(ctx context.Context, request Request) context.Context {
	ctx = workload.WithFields(ctx, "session.id", request.SessionID, "computer.id", request.ComputerID)
	if request.InstanceID != nil {
		ctx = workload.WithFields(ctx, "instance.id", *request.InstanceID)
	}
	return ctx
}

func requestPhase(request Request, phase string, end time.Time) workload.StoredPhase {
	start := request.CreatedAt
	if phase == "instance" && request.FirstClaimedAt != nil {
		start = *request.FirstClaimedAt
	}
	return workload.StoredPhase{Name: phase, Start: start, End: end}
}
