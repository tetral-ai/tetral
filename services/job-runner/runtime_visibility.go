package jobrunner

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	kubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
)

type runtimeProcessDecision string

const (
	runtimeProcessReuse       runtimeProcessDecision = "reuse"
	runtimeProcessUnavailable runtimeProcessDecision = "unavailable"
	runtimeProcessLoss        runtimeProcessDecision = "loss"
	runtimeProcessConfirm     runtimeProcessDecision = "confirm"
)

type runtimePodObservation struct {
	BindingID                  string
	Generation                 int64
	ProcessID                  string
	Absent, Replaced, Reusable bool
}
type runtimePodObservationKey struct{}
type runtimePodConfirmationRequired struct{ binding runtimecontrol.Binding }

func (e runtimePodConfirmationRequired) Error() string {
	return "fresh Runtime Pod observation required"
}

// One decision table governs ordinary delivery, cleanup and proactive census.
// It consumes durable process facts under the owning transaction's shared lock
// and, for a current process, the report time read under the liveness row's
// shared lock; confirming external observations are produced before Session
// arbitration. A retired process is classified from committed replacement
// facts alone. A current process without a recorded report is an invariant
// failure that is never evidence of loss.
func classifyRuntimeProcess(ready bool, visibility kubernetes.BindingVisibilityState, process runtimecontrol.Process, found bool, replacement bool, reportedAt sql.NullTime, now time.Time, freshness time.Duration, observation *runtimePodObservation) runtimeProcessDecision {
	if !ready || !found {
		return runtimeProcessUnavailable
	}
	if !process.Current || process.RetiredAt.Valid {
		if replacement {
			return runtimeProcessLoss
		}
		return runtimeProcessUnavailable
	}
	if !reportedAt.Valid {
		return runtimeProcessUnavailable
	}
	if process.Phase == runtimecontrol.ProcessStarting {
		return runtimeProcessUnavailable
	}
	if observation != nil && (observation.Absent || observation.Replaced) {
		return runtimeProcessLoss
	}
	if visibility == kubernetes.BindingVisibilityReusable && process.Phase == runtimecontrol.ProcessAccepting {
		return runtimeProcessReuse
	}
	if observation != nil && observation.Reusable {
		return runtimeProcessUnavailable
	}
	fresh := now.Before(reportedAt.Time.Add(freshness))
	if observation == nil {
		return runtimeProcessConfirm
	}
	if fresh {
		return runtimeProcessUnavailable
	}
	return runtimeProcessLoss
}

func (r KubernetesRuntimeTargetResolver) confirmRuntimePod(ctx context.Context, binding runtimecontrol.Binding) (runtimePodObservation, error) {
	if r.GetPod == nil {
		return runtimePodObservation{}, runtimecontrol.PreparationError{Kind: "runtime_confirmation_unavailable", Message: "fresh Runtime Pod observation is unavailable", Retryable: true}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pod, err := r.GetPod(ctx, binding.Namespace, binding.PodName)
	observation := runtimePodObservation{BindingID: binding.BindingID, Generation: binding.BindingGeneration, ProcessID: binding.RuntimeProcessID}
	if err == nil && pod != nil && pod.Absent {
		observation.Absent = true
		return observation, nil
	}
	if err != nil || pod == nil {
		return runtimePodObservation{}, runtimecontrol.PreparationError{Kind: "runtime_confirmation_failed", Message: "fresh Runtime Pod observation failed", Retryable: true}
	}
	if pod.Namespace != binding.Namespace || pod.Name != binding.PodName || pod.UID == "" {
		return runtimePodObservation{}, fmt.Errorf("fresh Runtime Pod observation identity is malformed")
	}
	observation.Replaced = pod.UID != binding.PodUID
	observation.Reusable = !observation.Replaced && !pod.Deleting && pod.Running && pod.Ready && pod.IP == binding.PodIP

	return observation, nil
}

func (r KubernetesRuntimeTargetResolver) runtimeProcessDecisionTx(ctx context.Context, tx *dbconnect.Tx, binding runtimecontrol.Binding) (runtimeProcessDecision, error) {
	snapshot := r.BindingVisibilitySnapshot()
	if !snapshot.Ready {
		return runtimeProcessUnavailable, nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_processes WHERE namespace=$1 AND pod_uid=$2 AND runtime_process_id=$3)`, binding.Namespace, binding.PodUID, binding.RuntimeProcessID).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		return runtimeProcessUnavailable, nil
	}
	identity := runtimecontrol.ProcessIdentity{Namespace: binding.Namespace, PodUID: binding.PodUID, ID: binding.RuntimeProcessID}
	process, err := runtimecontrol.LockProcessTx(ctx, tx, identity)
	if err != nil {
		return "", err
	}
	var replacement bool
	var reportedAt sql.NullTime
	if !process.Current || process.RetiredAt.Valid {
		// A committed promotion permanently supersedes this process. Subsequent
		// draining or candidate registration cannot undo that fact; accepting is
		// a placement admission condition, not a condition for repairing old custody.
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_process_pods WHERE namespace=$1 AND pod_uid=$2 AND last_promoted_order>$3)`, binding.Namespace, binding.PodUID, process.RegistrationOrder).Scan(&replacement); err != nil {
			return "", err
		}
	} else {
		// Session -> binding -> process SHARE -> liveness SHARE. A report already
		// holding the liveness row commits or rolls back before this returns;
		// a later report waits until this decision commits.
		reportedAt, err = runtimecontrol.LockProcessLivenessTx(ctx, tx, identity)
		if err != nil {
			return "", err
		}
	}
	// Database time is read after any liveness lock wait, in its own statement.
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return "", err
	}
	policy := r.ProcessPolicy
	if policy == (runtimecontrol.ProcessPolicy{}) {
		policy = runtimecontrol.DefaultProcessPolicy()
	}
	if err := policy.Validate(); err != nil {
		return "", err
	}
	var observation *runtimePodObservation
	if fact, ok := ctx.Value(runtimePodObservationKey{}).(runtimePodObservation); ok && fact.BindingID == binding.BindingID && fact.Generation == binding.BindingGeneration && fact.ProcessID == binding.RuntimeProcessID {
		observation = &fact
	}
	visibility := snapshot.VisibilityFor(kubernetes.BoundRuntimePod{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, PodIP: binding.PodIP})
	decision := classifyRuntimeProcess(snapshot.Ready, visibility, process, true, replacement, reportedAt, now, policy.Freshness, observation)
	if decision == runtimeProcessConfirm {
		return decision, runtimePodConfirmationRequired{binding: binding}
	}
	return decision, nil
}
