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
// times. A zero end starts the phase. Replayed mutations must not call it.
func RecordRequestPhase(ctx context.Context, request Request, phase string, end time.Time) {
	ctx = workload.WithFields(ctx, "session.id", request.SessionID, "computer.id", request.ComputerID)
	if request.InstanceID != nil {
		ctx = workload.WithFields(ctx, "instance.id", *request.InstanceID)
	}
	start := request.CreatedAt
	if phase == "instance" && request.ClaimedAt != nil {
		start = *request.ClaimedAt
	}
	workload.RecordStoredPhase(ctx, "session_computer", workload.StoredPhase{Name: phase, Start: start, End: end})
}
