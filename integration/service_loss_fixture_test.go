package integration

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

// The fixture supplies watcher visibility; the real Runner census and mutation
// perform loss repair. Other existing bindings remain eligible in this snapshot.
func repairLostBindingThroughProduction(ctx context.Context, store *jobrunner.PostgreSQLRuntimeDeliveryStore, workspaceID, sessionID string, binding runtimecontrol.Binding, now time.Time) error {
	var candidates []kubernetes.BindingCandidate
	err := store.Client.WithWorkspaceReadOnlyTx(ctx, workspaceID, "test.loss_visibility", func(tx *dbconnect.Tx) (queryErr error) {
		rows, err := tx.Query(ctx, `SELECT session_id,binding_id,binding_generation,agent_runtime_namespace,agent_runtime_pod_name,agent_runtime_pod_uid,agent_runtime_pod_ip FROM session_runtime_bindings WHERE workspace_id=$1`, workspaceID)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := rows.Close(); queryErr == nil {
				queryErr = closeErr
			}
		}()
		for rows.Next() {
			var session, id string
			var generation int64
			var candidate kubernetes.BindingCandidate
			if err := rows.Scan(&session, &id, &generation, &candidate.Namespace, &candidate.PodName, &candidate.PodUID, &candidate.PodIP); err != nil {
				return err
			}
			if session == sessionID && id == binding.BindingID && generation == binding.BindingGeneration {
				continue
			}
			candidates = append(candidates, candidate)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	err = store.Client.WithWorkspaceTx(ctx, workspaceID, "test.loss_residency", func(tx *dbconnect.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO session_runtime_status(workspace_id,session_id,status,running_since,active_seconds_total,binding_id,binding_generation,created_at,updated_at) SELECT workspace_id,session_id,'running',$3,0,binding_id,binding_generation,$3,$3 FROM session_runtime_bindings WHERE workspace_id=$1 AND session_id=$2 ON CONFLICT(workspace_id,session_id) DO NOTHING`, workspaceID, sessionID, now)
		return err
	})
	if err != nil {
		return err
	}
	previousResolver, previousClock := store.TargetResolver, store.Clock
	defer func() { store.TargetResolver, store.Clock = previousResolver, previousClock }()
	store.Clock = func() time.Time { return now }
	store.TargetResolver = jobrunner.KubernetesRuntimeTargetResolver{Snapshot: func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.NewBindingVisibilitySnapshotForTest(true, candidates)
	}}
	_, err = store.RepairLostRuntimeBindings(ctx, workspaceID)
	return err
}
