package computer

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"
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

// RecordRequestEnded records a committed failed or expired request: its
// outstanding phase, request before a first claim and instance after it, ends
// at the transition, and the computer's cold start ends with the code the
// calls waiting on it end with, so no histogram observes it.
func RecordRequestEnded(ctx context.Context, ended Ended) {
	request := ended.Request
	phases := []workload.StoredPhase{requestPhase(request, "request", ended.At)}
	if request.FirstClaimedAt != nil {
		phases = []workload.StoredPhase{requestPhase(request, "request", *request.FirstClaimedAt), requestPhase(request, "instance", ended.At)}
	}
	outcome := runtimecontrol.CodeMachineNotProvided
	if request.Kind == RequestWake {
		outcome = runtimecontrol.CodeMachineNotStarted
	}
	ctx = requestContext(ctx, request)
	workload.RecordStoredPhase(ctx, "session_computer", phases[len(phases)-1])
	workload.RecordStoredColdStart(ctx, "session_computer", ended.StartKind, outcome, phases, "request", "instance")
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
