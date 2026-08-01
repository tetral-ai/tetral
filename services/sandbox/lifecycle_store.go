package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

var errSandboxReleaseBlocked = errors.New("sandbox release is waiting for active provider work")

type SandboxSessionResourceSource interface {
	ListSessionResourcesTx(context.Context, *dbconnect.Tx, workspace.ID, string) (sandbox.ResourceSetup, error)
}

// PostgreSQLSandboxLifecycleStore coordinates provider lifecycle receipts and
// their execution waiters. Transactions only manipulate durable state and
// Queue references; provider inspection and mutation stay in the runners.
type PostgreSQLSandboxLifecycleStore struct {
	client                  *dbconnect.Client
	resources               SandboxSessionResourceSource
	credentialRefreshMargin time.Duration
}

func NewPostgreSQLSandboxLifecycleStore(client *dbconnect.Client, resources SandboxSessionResourceSource, credentialRefreshMargin time.Duration) *PostgreSQLSandboxLifecycleStore {
	return &PostgreSQLSandboxLifecycleStore{client: client, resources: resources, credentialRefreshMargin: credentialRefreshMargin}
}

func (s *PostgreSQLSandboxLifecycleStore) ClaimActivation(ctx context.Context, job SandboxLifecycleJob, now time.Time) (SandboxActivationWork, bool, error) {
	if s == nil || s.client == nil {
		return SandboxActivationWork{}, false, errors.New("sandbox lifecycle store is required")
	}
	var work SandboxActivationWork
	var claimed bool
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.activation.claim", func(tx *dbconnect.Tx) error {
		var preliminaryKind string
		var preliminaryGeneration sql.NullInt64
		if err := tx.QueryRow(ctx,
			`SELECT kind, target_environment_generation
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2`,
			job.WorkspaceID, job.OperationID,
		).Scan(&preliminaryKind, &preliminaryGeneration); err != nil {
			if dbconnect.IsNoRows(err) {
				return nil
			}
			return err
		}
		var environmentID string
		if err := tx.QueryRow(ctx,
			`SELECT environment_id FROM sessions
			  WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			job.WorkspaceID, job.SessionID,
		).Scan(&environmentID); err != nil {
			return err
		}
		var currentGeneration int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation FROM environments
			  WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			job.WorkspaceID, environmentID,
		).Scan(&currentGeneration); err != nil {
			return err
		}
		targetGeneration := currentGeneration
		if preliminaryGeneration.Valid {
			targetGeneration = preliminaryGeneration.Int64
		} else if err := tx.QueryRow(ctx,
			`SELECT environment_generation FROM session_sandbox_bindings
			  WHERE workspace_id = $1 AND session_id = $2`,
			job.WorkspaceID, job.SessionID,
		).Scan(&targetGeneration); err != nil {
			return err
		}
		artifactStatus := "ready"
		artifactProvider := sandboxdriver.DaytonaProviderName
		var networkJSON string
		var providerArtifactRef sql.NullString
		if preliminaryKind != string(ActivationStart) {
			if err := tx.QueryRow(ctx,
				`SELECT status, provider, provider_artifact_ref, runtime_network_policy_json
					   FROM environment_artifacts
					  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
					  FOR UPDATE`,
				job.WorkspaceID, environmentID, targetGeneration,
			).Scan(&artifactStatus, &artifactProvider, &providerArtifactRef, &networkJSON); err != nil {
				return err
			}
			if artifactStatus != "ready" || !providerArtifactRef.Valid || artifactProvider != sandboxdriver.DaytonaProviderName {
				return errors.New("sandbox activation environment artifact is not ready")
			}
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID)
		if err != nil || !exists {
			if err != nil {
				return err
			}
			return errors.New("sandbox activation binding does not exist")
		}
		if binding.LogicalSandboxID != job.LogicalSandboxID {
			return errors.New("sandbox activation logical identity does not match")
		}
		if binding.EnvironmentID != environmentID {
			return nil
		}
		var (
			kind                    string
			state                   string
			observedRevision        int64
			targetProviderResource  sql.NullString
			providerCreateName      sql.NullString
			providerLabelsJSON      sql.NullString
			outcomeEffectBoundary   sql.NullString
			storedQueueJobID        sql.NullString
			storedQueueKind         sql.NullString
			storedQueuePartitionKey sql.NullString
			storedQueueDedupeKey    sql.NullString
		)
		if err := tx.QueryRow(ctx,
			`SELECT kind, state, observed_binding_revision, target_provider_resource_id,
			        provider_create_name, provider_request_labels_json,
			        outcome_effect_boundary, queue_job_id, queue_kind,
			        queue_partition_key, queue_dedupe_key
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2
			    AND session_id = $3 AND logical_sandbox_id = $4
			  FOR UPDATE`,
			job.WorkspaceID, job.OperationID, job.SessionID, job.LogicalSandboxID,
		).Scan(
			&kind, &state, &observedRevision, &targetProviderResource,
			&providerCreateName, &providerLabelsJSON, &outcomeEffectBoundary,
			&storedQueueJobID, &storedQueueKind, &storedQueuePartitionKey, &storedQueueDedupeKey,
		); err != nil {
			return err
		}
		if preliminaryKind != kind || (state != "pending" && state != "running") ||
			!sandboxLifecycleQueueIdentityMatches(job, queue.KindSandboxActivate, storedQueueJobID, storedQueueKind, storedQueuePartitionKey, storedQueueDedupeKey) {
			return nil
		}
		if state == "pending" && (observedRevision != binding.BindingRevision || binding.ReleaseRequested() ||
			(!preliminaryGeneration.Valid && binding.EnvironmentGeneration != targetGeneration) ||
			(targetProviderResource.Valid && targetProviderResource.String != binding.ProviderResourceID)) {
			return nil
		}
		if err := recordLifecycleLease(ctx, tx, job, now); err != nil {
			return err
		}
		network, err := decodeSandboxNetworkSetup(networkJSON)
		if err != nil {
			return err
		}
		activationKind := ActivationKind(kind)
		labels := map[string]string{}
		if providerLabelsJSON.Valid {
			if err := json.Unmarshal([]byte(providerLabelsJSON.String), &labels); err != nil {
				return err
			}
		}
		currentHandleID := binding.ProviderResourceID
		if state == "running" && targetProviderResource.Valid {
			currentHandleID = targetProviderResource.String
		}
		work = SandboxActivationWork{
			Job: job, Kind: activationKind, Provider: binding.Provider,
			CurrentHandle: sandbox.ProviderHandle{Provider: binding.Provider, SandboxID: currentHandleID},
			StableName:    providerCreateName.String, Labels: labels,
			MayCreate: !outcomeEffectBoundary.Valid || outcomeEffectBoundary.String != string(ProviderOutcomeUnknown),
			Setup: sandbox.SandboxSetup{
				WorkspaceID: workspace.ID(job.WorkspaceID), SessionID: job.SessionID,
				SandboxID: job.LogicalSandboxID, LifecycleOperationID: job.OperationID,
				EnvironmentID: environmentID, EnvironmentGeneration: targetGeneration,
				ProviderArtifactRef: providerArtifactRef.String, Network: network,
			},
		}
		claimed = true
		return nil
	})
	return work, claimed, err
}

func (s *PostgreSQLSandboxLifecycleStore) CompleteActivation(ctx context.Context, work SandboxActivationWork, handle sandbox.ProviderHandle, now time.Time) error {
	if handle.Provider != sandboxdriver.DaytonaProviderName || handle.SandboxID == "" {
		return errors.New("sandbox activation returned an invalid provider handle")
	}
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.activation.complete", func(tx *dbconnect.Tx) error {
		var environmentID string
		var resourceRevision int64
		var databaseNow time.Time
		if err := tx.QueryRow(ctx,
			`SELECT environment_id, sandbox_resource_revision, CURRENT_TIMESTAMP FROM sessions
			  WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.SessionID,
		).Scan(&environmentID, &resourceRevision, &databaseNow); err != nil {
			return err
		}
		var currentGeneration int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation FROM environments
			  WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, environmentID,
		).Scan(&currentGeneration); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID)
		if err != nil || !exists {
			if err != nil {
				return err
			}
			return errors.New("sandbox activation binding disappeared")
		}
		var state, kind string
		var observedRevision int64
		var targetGeneration sql.NullInt64
		if err := tx.QueryRow(ctx,
			`SELECT state, kind, observed_binding_revision, target_environment_generation
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2
			  FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&state, &kind, &observedRevision, &targetGeneration); err != nil {
			return err
		}
		if state == "completed" {
			return nil
		}
		if state != "running" {
			return nil
		}
		if observedRevision != binding.BindingRevision {
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_lifecycle_operations
				    SET state = 'completed', adopted_provider_resource_id = $3,
				        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
				        completed_at = $4, updated_at = $4
				  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
				work.Job.WorkspaceID, work.Job.OperationID, handle.SandboxID, now.UTC(),
			); err != nil {
				return err
			}
			if binding.ReleaseRequested() {
				return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
			}
			waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_activation_operation_id")
			if err != nil {
				return err
			}
			return releaseExecutionWaitersTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, waiters, now)
		}
		releaseRequested := binding.ReleaseRequested()
		displacedHandle := binding.ProviderResourceID
		generation := binding.EnvironmentGeneration
		if targetGeneration.Valid {
			generation = targetGeneration.Int64
		}
		metadata := handle.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		preserveMaterialization := kind == string(ActivationStart) && binding.ProviderResourceID == handle.SandboxID
		if preserveMaterialization {
			if _, err := tx.Exec(ctx,
				`UPDATE session_sandbox_bindings
				    SET provider_resource_id = $3, binding_revision = binding_revision + 1,
				        environment_generation = $4, provider_metadata_json = $5,
				        updated_at = $6
				  WHERE workspace_id = $1 AND session_id = $2`,
				work.Job.WorkspaceID, work.Job.SessionID, handle.SandboxID, generation, string(metadataJSON), now.UTC(),
			); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(ctx,
				`UPDATE session_sandbox_bindings
				    SET provider_resource_id = $3, binding_revision = binding_revision + 1,
				        environment_generation = $4, materialized_resource_revision = 0,
				        resource_credential_expires_at = NULL, resource_roots_json = '[]',
				        helper_verified_at = NULL, provider_metadata_json = $5,
				        updated_at = $6
				  WHERE workspace_id = $1 AND session_id = $2`,
				work.Job.WorkspaceID, work.Job.SessionID, handle.SandboxID, generation, string(metadataJSON), now.UTC(),
			); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'completed', adopted_provider_resource_id = $3,
			        outcome_effect_boundary = NULL, outcome_disposition = NULL,
			        error_kind = NULL, safe_message = NULL,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        completed_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND operation_id = $2`,
			work.Job.WorkspaceID, work.Job.OperationID, handle.SandboxID, now.UTC(),
		); err != nil {
			return err
		}
		binding.BindingRevision++
		binding.ProviderResourceID = handle.SandboxID
		binding.EnvironmentGeneration = generation
		if !preserveMaterialization {
			binding.MaterializedResourceRevision = 0
			binding.ResourceCredentialExpiresAt = nil
			binding.ResourceRootsJSON = "[]"
		}
		if kind == string(ActivationReplace) && displacedHandle != "" && displacedHandle != handle.SandboxID {
			if _, _, err := EnsureSandboxReleaseTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, SandboxReleaseReplacedHandle, displacedHandle, now); err != nil {
				return err
			}
		}
		// Provider work that crossed its submission boundary remains durable even
		// when release was requested during the call. Release owns the adopted
		// handle next; cancelled execution waiters must not be revived here.
		if releaseRequested {
			if _, _, err := EnsureSandboxReleaseTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, SandboxReleaseSessionDelete, handle.SandboxID, now); err != nil {
				return err
			}
			return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
		}
		if err := requeueActivationMaterializationsTx(ctx, tx, work.Job, binding, now); err != nil {
			return err
		}
		ready := preserveMaterialization && binding.MaterializedResourceRevision == resourceRevision &&
			binding.ResourceCredentialExpiresAt != nil && binding.ResourceCredentialExpiresAt.After(databaseNow.UTC().Add(s.credentialRefreshMargin)) &&
			binding.HelperVerifiedAt != nil
		waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_activation_operation_id")
		if err != nil {
			return err
		}
		if len(waiters) == 0 {
			return nil
		}
		if ready {
			return releaseExecutionWaitersTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, waiters, now)
		}
		materializationID, err := s.ensureMaterializationOperationTx(ctx, tx, work.Job, binding, resourceRevision, now)
		if err != nil {
			return err
		}
		return attachExecutionWaitersToMaterializationTx(ctx, tx, work.Job.WorkspaceID, waiters, materializationID, now)
	})
}

func requeueActivationMaterializationsTx(ctx context.Context, tx *dbconnect.Tx, activation SandboxLifecycleJob, binding SandboxBinding, now time.Time) error {
	rows, err := tx.Query(ctx,
		`SELECT operation_id
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1
		    AND waiting_activation_operation_id = $2
		    AND kind = 'materialize'
		    AND state = 'waiting_activation'
		  ORDER BY operation_id
		  FOR UPDATE`,
		activation.WorkspaceID, activation.OperationID,
	)
	if err != nil {
		return err
	}
	var operationIDs []string
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			_ = rows.Close()
			return err
		}
		operationIDs = append(operationIDs, operationID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, operationID := range operationIDs {
		jobID := queue.NewJobID()
		partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(activation.WorkspaceID), activation.LogicalSandboxID)
		dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, workspace.ID(activation.WorkspaceID), activation.LogicalSandboxID, operationID)
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'pending', waiting_activation_operation_id = NULL,
			        observed_binding_revision = $7,
			        target_environment_generation = $8,
			        target_provider_resource_id = $9,
			        queue_job_id = $3, queue_kind = $4,
			        queue_partition_key = $5, queue_dedupe_key = $6,
			        updated_at = $10
			  WHERE workspace_id = $1 AND operation_id = $2
			    AND state = 'waiting_activation'`,
			activation.WorkspaceID, operationID, jobID, queue.KindSandboxMaterialize,
			partitionKey, dedupeKey, binding.BindingRevision, binding.EnvironmentGeneration,
			binding.ProviderResourceID, now.UTC(),
		); err != nil {
			return err
		}
		payload, err := sandboxLifecycleQueuePayload(SandboxExecutionRef{
			WorkspaceID: activation.WorkspaceID, SessionID: activation.SessionID,
		}, activation.LogicalSandboxID, operationID)
		if err != nil {
			return err
		}
		if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
			ID: jobID, WorkspaceID: workspace.ID(activation.WorkspaceID), Kind: queue.KindSandboxMaterialize,
			PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
			PayloadJSON: payload, MaxAttempts: sandboxMaterializationMaxAttempts, Now: now.UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgreSQLSandboxLifecycleStore) ReplaceMissingActivation(ctx context.Context, work SandboxActivationWork, now time.Time) (SandboxActivationWork, bool, error) {
	var replacement SandboxActivationWork
	var current bool
	err := s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.activation.replace_missing", func(tx *dbconnect.Tx) error {
		var environmentID string
		if err := tx.QueryRow(ctx,
			`SELECT environment_id FROM sessions WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.SessionID,
		).Scan(&environmentID); err != nil {
			return err
		}
		var generation int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation FROM environments WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, environmentID,
		).Scan(&generation); err != nil {
			return err
		}
		var artifactStatus, artifactProvider, networkJSON string
		var providerArtifactRef sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT status, provider, provider_artifact_ref, runtime_network_policy_json
			   FROM environment_artifacts
			  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
			  FOR UPDATE`,
			work.Job.WorkspaceID, environmentID, generation,
		).Scan(&artifactStatus, &artifactProvider, &providerArtifactRef, &networkJSON); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID)
		if err != nil || !exists {
			return err
		}
		var state, kind string
		var observedRevision int64
		if err := tx.QueryRow(ctx,
			`SELECT state, kind, observed_binding_revision
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&state, &kind, &observedRevision); err != nil {
			return err
		}
		if state != "running" || kind != string(ActivationStart) ||
			observedRevision != binding.BindingRevision ||
			binding.ProviderResourceID != work.CurrentHandle.SandboxID || binding.ReleaseRequested() {
			return nil
		}
		if artifactProvider != sandboxdriver.DaytonaProviderName {
			return errors.New("sandbox replacement environment artifact provider is invalid")
		}
		labels := sandboxActivationLabels(work.Job.WorkspaceID, work.Job.SessionID, environmentID, binding.LogicalSandboxID, work.Job.OperationID)
		labelsJSON, err := json.Marshal(labels)
		if err != nil {
			return err
		}
		if artifactStatus == "pending" || artifactStatus == "building" {
			_, err := tx.Exec(ctx,
				`UPDATE sandbox_lifecycle_operations
				    SET kind = 'replace', state = 'waiting_artifact',
				        target_environment_generation = $3,
				        target_provider_resource_id = NULL, provider_create_name = $4,
				        provider_request_labels_json = $5,
				        queue_job_id = NULL, queue_kind = NULL,
				        queue_partition_key = NULL, queue_dedupe_key = NULL,
				        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
				        outcome_effect_boundary = NULL, outcome_disposition = NULL,
				        error_kind = NULL, safe_message = NULL, updated_at = $6
				  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
				work.Job.WorkspaceID, work.Job.OperationID, generation,
				binding.LogicalSandboxID, string(labelsJSON), now.UTC(),
			)
			return err
		}
		if artifactStatus == "failed" {
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_lifecycle_operations
				    SET kind = 'replace', state = 'failed',
				        target_environment_generation = $3,
				        target_provider_resource_id = NULL, provider_create_name = $4,
				        provider_request_labels_json = $5,
				        queue_job_id = NULL, queue_kind = NULL,
				        queue_partition_key = NULL, queue_dedupe_key = NULL,
				        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
				        outcome_effect_boundary = 'proved_not_started',
				        outcome_disposition = 'terminal',
				        error_kind = 'environment_artifact_failed',
				        safe_message = 'sandbox environment artifact is unavailable',
				        completed_at = $6, updated_at = $6
				  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
				work.Job.WorkspaceID, work.Job.OperationID, generation,
				binding.LogicalSandboxID, string(labelsJSON), now.UTC(),
			); err != nil {
				return err
			}
			waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_activation_operation_id")
			if err != nil {
				return err
			}
			if err := settleExecutionWaitersTx(ctx, tx, waiters, "environment_artifact_failed", "sandbox environment artifact is unavailable", now); err != nil {
				return err
			}
			if err := failActivationMaterializationDependentsTx(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "environment_artifact_failed", "sandbox environment artifact is unavailable", now); err != nil {
				return err
			}
			return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
		}
		if artifactStatus != "ready" || !providerArtifactRef.Valid || providerArtifactRef.String == "" {
			return errors.New("sandbox replacement environment artifact is malformed")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET kind = 'replace', target_environment_generation = $3,
			        target_provider_resource_id = NULL, provider_create_name = $4,
			        provider_request_labels_json = $5, outcome_effect_boundary = NULL,
			        outcome_disposition = NULL, error_kind = NULL, safe_message = NULL,
			        updated_at = $6
			  WHERE workspace_id = $1 AND operation_id = $2`,
			work.Job.WorkspaceID, work.Job.OperationID, generation,
			binding.LogicalSandboxID, string(labelsJSON), now.UTC(),
		); err != nil {
			return err
		}
		network, err := decodeSandboxNetworkSetup(networkJSON)
		if err != nil {
			return err
		}
		replacement = SandboxActivationWork{
			Job: work.Job, Kind: ActivationReplace, Provider: binding.Provider,
			CurrentHandle: work.CurrentHandle, StableName: binding.LogicalSandboxID,
			Labels: labels, MayCreate: true,
			Setup: sandbox.SandboxSetup{
				WorkspaceID: workspace.ID(work.Job.WorkspaceID), SessionID: work.Job.SessionID,
				SandboxID: binding.LogicalSandboxID, LifecycleOperationID: work.Job.OperationID,
				EnvironmentID: environmentID, EnvironmentGeneration: generation,
				ProviderArtifactRef: providerArtifactRef.String, Network: network,
			},
		}
		current = true
		return nil
	})
	return replacement, current, err
}

func (s *PostgreSQLSandboxLifecycleStore) ObserveUnknownActivation(ctx context.Context, work SandboxActivationWork, kind string, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.activation.observe_unknown", func(tx *dbconnect.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'running', outcome_effect_boundary = 'outcome_unknown',
			        outcome_disposition = 'terminal', error_kind = $3,
			        safe_message = 'sandbox creation outcome requires observation',
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        updated_at = $4
			  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
			work.Job.WorkspaceID, work.Job.OperationID, kind, now.UTC(),
		)
		return err
	})
}

func (s *PostgreSQLSandboxLifecycleStore) FailActivation(ctx context.Context, work SandboxActivationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string, now time.Time) error {
	if boundary == "" || disposition == "" {
		return errors.New("sandbox activation failure classification is required")
	}
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.activation.fail", func(tx *dbconnect.Tx) error {
		if err := lockLifecycleSessionBindingOperation(ctx, tx, work.Job); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'failed', outcome_effect_boundary = $3,
			        outcome_disposition = $4, error_kind = $5, safe_message = $6,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        completed_at = $7, updated_at = $7
			  WHERE workspace_id = $1 AND operation_id = $2
			    AND state IN ('pending', 'running')`,
			work.Job.WorkspaceID, work.Job.OperationID, boundary, disposition, kind, message, now.UTC(),
		); err != nil {
			return err
		}
		waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_activation_operation_id")
		if err != nil {
			return err
		}
		if err := settleExecutionWaitersTx(ctx, tx, waiters, kind, message, now); err != nil {
			return err
		}
		if err := failActivationMaterializationDependentsTx(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, kind, message, now); err != nil {
			return err
		}
		return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
	})
}

func (s *PostgreSQLSandboxLifecycleStore) ClaimMaterialization(ctx context.Context, job SandboxLifecycleJob, now time.Time) (SandboxMaterializationWork, bool, error) {
	if s == nil || s.client == nil || s.resources == nil {
		return SandboxMaterializationWork{}, false, errors.New("sandbox materialization store dependencies are required")
	}
	var work SandboxMaterializationWork
	var claimed bool
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.materialization.claim", func(tx *dbconnect.Tx) error {
		var preliminaryTargetEnvironmentGeneration int64
		if err := tx.QueryRow(ctx,
			`SELECT target_environment_generation
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2 AND kind = 'materialize'`,
			job.WorkspaceID, job.OperationID,
		).Scan(&preliminaryTargetEnvironmentGeneration); err != nil {
			if dbconnect.IsNoRows(err) {
				return nil
			}
			return err
		}
		var environmentID string
		if err := tx.QueryRow(ctx,
			`SELECT environment_id FROM sessions WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			job.WorkspaceID, job.SessionID,
		).Scan(&environmentID); err != nil {
			return err
		}
		var generation int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation FROM environments WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			job.WorkspaceID, environmentID,
		).Scan(&generation); err != nil {
			return err
		}
		var providerArtifactRef sql.NullString
		var networkJSON, artifactStatus, artifactProvider string
		if err := tx.QueryRow(ctx,
			`SELECT provider_artifact_ref, runtime_network_policy_json, status, provider
			   FROM environment_artifacts
			  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
			  FOR UPDATE`,
			job.WorkspaceID, environmentID, preliminaryTargetEnvironmentGeneration,
		).Scan(&providerArtifactRef, &networkJSON, &artifactStatus, &artifactProvider); err != nil {
			return err
		}
		if artifactStatus != "ready" || artifactProvider != sandboxdriver.DaytonaProviderName {
			return errors.New("sandbox materialization environment artifact is not ready")
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID)
		if err != nil || !exists {
			if err != nil {
				return err
			}
			return errors.New("sandbox materialization binding does not exist")
		}
		if binding.LogicalSandboxID != job.LogicalSandboxID {
			return nil
		}
		var state, materializationResourcesJSON string
		var observedRevision, targetEnvironmentGeneration, targetRevision int64
		var targetHandle string
		var storedQueueJobID, storedQueueKind, storedQueuePartitionKey, storedQueueDedupeKey sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, observed_binding_revision, target_environment_generation, target_resource_revision,
			        target_provider_resource_id, queue_job_id, queue_kind,
			        queue_partition_key, queue_dedupe_key, materialization_resources_json
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2
			    AND kind = 'materialize' AND session_id = $3
			  FOR UPDATE`,
			job.WorkspaceID, job.OperationID, job.SessionID,
		).Scan(&state, &observedRevision, &targetEnvironmentGeneration, &targetRevision, &targetHandle,
			&storedQueueJobID, &storedQueueKind, &storedQueuePartitionKey, &storedQueueDedupeKey,
			&materializationResourcesJSON); err != nil {
			return err
		}
		if (state != "pending" && state != "running") ||
			!sandboxLifecycleQueueIdentityMatches(job, queue.KindSandboxMaterialize, storedQueueJobID, storedQueueKind, storedQueuePartitionKey, storedQueueDedupeKey) {
			return nil
		}
		if state == "pending" && (binding.ProviderResourceID == "" || binding.ReleaseRequested() ||
			observedRevision != binding.BindingRevision || targetHandle != binding.ProviderResourceID ||
			targetEnvironmentGeneration != binding.EnvironmentGeneration) {
			return nil
		}
		if binding.EnvironmentID != environmentID || targetEnvironmentGeneration != preliminaryTargetEnvironmentGeneration {
			return nil
		}
		if err := recordLifecycleLease(ctx, tx, job, now); err != nil {
			return err
		}
		network, err := decodeSandboxNetworkSetup(networkJSON)
		if err != nil {
			return err
		}
		resources, err := decodeMaterializationResources(materializationResourcesJSON)
		if err != nil {
			return err
		}
		work = SandboxMaterializationWork{
			Job: job, Provider: binding.Provider,
			Handle:                      sandbox.ProviderHandle{Provider: binding.Provider, SandboxID: targetHandle},
			BindingRevision:             observedRevision,
			TargetEnvironmentGeneration: targetEnvironmentGeneration,
			TargetResourceRevision:      targetRevision,
			Setup: sandbox.SandboxSetup{
				WorkspaceID: workspace.ID(job.WorkspaceID), SessionID: job.SessionID,
				SandboxID: job.LogicalSandboxID, LifecycleOperationID: job.OperationID,
				EnvironmentID: binding.EnvironmentID, EnvironmentGeneration: targetEnvironmentGeneration,
				ResourceRevision: targetRevision, ProviderArtifactRef: providerArtifactRef.String, Network: network,
				Resources: resources,
			},
		}
		_ = generation
		claimed = true
		return nil
	})
	return work, claimed, err
}

func (s *PostgreSQLSandboxLifecycleStore) WaitMaterializationForActivation(ctx context.Context, work SandboxMaterializationWork, readiness ExecutionReadiness, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.materialization.wait_activation", func(tx *dbconnect.Tx) error {
		var environmentID string
		if err := tx.QueryRow(ctx,
			`SELECT environment_id FROM sessions WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.SessionID,
		).Scan(&environmentID); err != nil {
			return err
		}
		var generation int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation FROM environments WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, environmentID,
		).Scan(&generation); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID)
		if err != nil || !exists {
			if err != nil {
				return err
			}
			return errors.New("sandbox materialization binding disappeared")
		}
		var operationState string
		var leaseOwner, leaseToken sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, lease_owner, lease_token FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&operationState, &leaseOwner, &leaseToken); err != nil {
			return err
		}
		if operationState != "running" {
			return nil
		}
		if !lifecycleLeaseIdentityMatches(work.Job, leaseOwner, leaseToken) {
			return nil
		}
		waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_materialization_operation_id")
		if err != nil {
			return err
		}
		if len(waiters) == 0 {
			_, err := tx.Exec(ctx,
				`UPDATE sandbox_lifecycle_operations
				    SET state = 'skipped_cold', lease_owner = NULL, lease_token = NULL,
				        lease_expires_at = NULL, completed_at = $3, updated_at = $3
				  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
				work.Job.WorkspaceID, work.Job.OperationID, now.UTC(),
			)
			return err
		}
		activationID, err := ensureActivationOperationTx(ctx, tx, work.Job, binding, generation, readiness, now)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'waiting_activation', waiting_activation_operation_id = $3,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        updated_at = $4
			  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
			work.Job.WorkspaceID, work.Job.OperationID, activationID, now.UTC(),
		)
		return err
	})
}

func (s *PostgreSQLSandboxLifecycleStore) CompleteMaterialization(ctx context.Context, work SandboxMaterializationWork, result MaterializationResult, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.materialization.complete", func(tx *dbconnect.Tx) error {
		var desiredRevision int64
		if err := tx.QueryRow(ctx,
			`SELECT sandbox_resource_revision FROM sessions
			  WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.SessionID,
		).Scan(&desiredRevision); err != nil {
			return err
		}
		var environmentID string
		if err := tx.QueryRow(ctx,
			`SELECT environment_id FROM sessions WHERE workspace_id = $1 AND id = $2`,
			work.Job.WorkspaceID, work.Job.SessionID,
		).Scan(&environmentID); err != nil {
			return err
		}
		var generation int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation FROM environments WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
			work.Job.WorkspaceID, environmentID,
		).Scan(&generation); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID)
		if err != nil || !exists {
			if err != nil {
				return err
			}
			return errors.New("sandbox materialization binding disappeared")
		}
		var state string
		var observedRevision, targetEnvironmentGeneration, targetRevision int64
		var targetHandle string
		var leaseOwner, leaseToken sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, observed_binding_revision, target_environment_generation, target_resource_revision,
			        target_provider_resource_id, lease_owner, lease_token
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2
			  FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&state, &observedRevision, &targetEnvironmentGeneration, &targetRevision, &targetHandle, &leaseOwner, &leaseToken); err != nil {
			return err
		}
		if state == "completed" {
			return nil
		}
		if state != "running" {
			return nil
		}
		if work.Job.LeaseOwner != "" && work.Job.LeaseToken != "" &&
			(!leaseOwner.Valid || leaseOwner.String != work.Job.LeaseOwner || !leaseToken.Valid || leaseToken.String != work.Job.LeaseToken) {
			return nil
		}
		if observedRevision != binding.BindingRevision || targetHandle != binding.ProviderResourceID ||
			targetEnvironmentGeneration != binding.EnvironmentGeneration {
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_lifecycle_operations
				    SET state = 'completed', lease_owner = NULL, lease_token = NULL,
				        lease_expires_at = NULL, completed_at = $3, updated_at = $3
				  WHERE workspace_id = $1 AND operation_id = $2 AND state = 'running'`,
				work.Job.WorkspaceID, work.Job.OperationID, now.UTC(),
			); err != nil {
				return err
			}
			if binding.ReleaseRequested() {
				return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
			}
			waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_materialization_operation_id")
			if err != nil {
				return err
			}
			return releaseExecutionWaitersTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, waiters, now)
		}
		releaseRequested := binding.ReleaseRequested()
		if result.MaterializedEnvironmentGeneration != targetEnvironmentGeneration || result.MaterializedResourceRevision != targetRevision {
			return errors.New("sandbox materialization result target does not match its operation")
		}
		deletedResourceIDs := materializedDeletedResourceIDs(work.Setup.Resources)
		if len(deletedResourceIDs) > 0 {
			update, err := tx.Exec(ctx,
				`UPDATE session_resources
					    SET detached_at = $4, updated_at = $4
					  WHERE workspace_id = $1 AND session_id = $2
					    AND resource_id = ANY($3)
					    AND delete_requested_at IS NOT NULL AND detached_at IS NULL`,
				work.Job.WorkspaceID, work.Job.SessionID, deletedResourceIDs, now.UTC(),
			)
			if err != nil {
				return err
			}
			updated, err := update.RowsAffected()
			if err != nil {
				return err
			}
			if updated != int64(len(deletedResourceIDs)) {
				return errors.New("sandbox materialization deleted-resource snapshot does not match durable resources")
			}
		}
		rootsJSON := result.Resources.ResourceRootsJSON
		if rootsJSON == "" {
			rootsJSON = "[]"
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_sandbox_bindings
			    SET materialized_resource_revision = $3,
			        resource_credential_expires_at = $4,
			        resource_roots_json = $5, helper_verified_at = $6,
			        updated_at = $6
			  WHERE workspace_id = $1 AND session_id = $2`,
			work.Job.WorkspaceID, work.Job.SessionID, targetRevision,
			result.Resources.ResourceCredExpiresAt, rootsJSON, now.UTC(),
		); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'completed', outcome_effect_boundary = NULL,
			        outcome_disposition = NULL, error_kind = NULL, safe_message = NULL,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        completed_at = $3, updated_at = $3
			  WHERE workspace_id = $1 AND operation_id = $2`,
			work.Job.WorkspaceID, work.Job.OperationID, now.UTC(),
		); err != nil {
			return err
		}
		// Materialization that already reached the provider is recorded before a
		// concurrent release takes ownership. It must not release cancelled waiters.
		if releaseRequested {
			return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
		}
		waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_materialization_operation_id")
		if err != nil {
			return err
		}
		if desiredRevision > targetRevision {
			binding.MaterializedResourceRevision = targetRevision
			successorID, err := s.ensureMaterializationOperationTx(ctx, tx, work.Job, binding, desiredRevision, now)
			if err != nil {
				return err
			}
			return attachExecutionWaitersToMaterializationTx(ctx, tx, work.Job.WorkspaceID, waiters, successorID, now)
		}
		if desiredRevision != targetRevision {
			return errors.New("sandbox resource revision moved backwards during materialization")
		}
		return releaseExecutionWaitersTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, waiters, now)
	})
}

func materializedDeletedResourceIDs(resources sandbox.ResourceSetup) []string {
	ids := make([]string, 0, len(resources.DeletedFiles)+len(resources.DeletedMemoryStores)+len(resources.DeletedGitHubRepositories))
	for _, resource := range resources.DeletedFiles {
		ids = append(ids, resource.ResourceID)
	}
	for _, resource := range resources.DeletedMemoryStores {
		ids = append(ids, resource.ResourceID)
	}
	for _, resource := range resources.DeletedGitHubRepositories {
		ids = append(ids, resource.ResourceID)
	}
	sort.Strings(ids)
	out := ids[:0]
	for _, resourceID := range ids {
		if resourceID == "" || (len(out) > 0 && out[len(out)-1] == resourceID) {
			continue
		}
		out = append(out, resourceID)
	}
	return out
}

func (s *PostgreSQLSandboxLifecycleStore) FailMaterialization(ctx context.Context, work SandboxMaterializationWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.materialization.fail", func(tx *dbconnect.Tx) error {
		if err := lockLifecycleSessionBindingOperation(ctx, tx, work.Job); err != nil {
			return err
		}
		var leaseOwner, leaseToken sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT lease_owner, lease_token FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&leaseOwner, &leaseToken); err != nil {
			return err
		}
		if !lifecycleLeaseIdentityMatches(work.Job, leaseOwner, leaseToken) {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'failed', outcome_effect_boundary = $3,
			        outcome_disposition = $4, error_kind = $5, safe_message = $6,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        completed_at = $7, updated_at = $7
			  WHERE workspace_id = $1 AND operation_id = $2
			    AND state IN ('pending', 'running', 'waiting_activation')`,
			work.Job.WorkspaceID, work.Job.OperationID, boundary, disposition, kind, message, now.UTC(),
		); err != nil {
			return err
		}
		waiters, err := lockExecutionWaiters(ctx, tx, work.Job.WorkspaceID, work.Job.OperationID, "waiting_materialization_operation_id")
		if err != nil {
			return err
		}
		if err := settleExecutionWaitersTx(ctx, tx, waiters, kind, message, now); err != nil {
			return err
		}
		return enqueueReadySandboxReleasesTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, now)
	})
}

func (s *PostgreSQLSandboxLifecycleStore) ClaimRelease(ctx context.Context, job SandboxLifecycleJob, now time.Time) (SandboxReleaseWork, bool, error) {
	if s == nil || s.client == nil {
		return SandboxReleaseWork{}, false, errors.New("sandbox lifecycle store is required")
	}
	var work SandboxReleaseWork
	var claimed bool
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.release.claim", func(tx *dbconnect.Tx) error {
		if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID)
		if err != nil || !exists {
			return err
		}
		var state, kind, targetHandle, releaseReason string
		var effectBoundary sql.NullString
		var storedJobID, storedKind, storedPartition, storedDedupe sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, kind, target_provider_resource_id, release_reason,
			        outcome_effect_boundary, queue_job_id, queue_kind,
			        queue_partition_key, queue_dedupe_key
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id=$1 AND operation_id=$2
			    AND session_id=$3 AND logical_sandbox_id=$4
			  FOR UPDATE`,
			job.WorkspaceID, job.OperationID, job.SessionID, job.LogicalSandboxID,
		).Scan(&state, &kind, &targetHandle, &releaseReason, &effectBoundary,
			&storedJobID, &storedKind, &storedPartition, &storedDedupe); err != nil {
			if dbconnect.IsNoRows(err) {
				return nil
			}
			return err
		}
		if kind != "release" || (state != "pending" && state != "running") ||
			!sandboxLifecycleQueueIdentityMatches(job, queue.KindSandboxRelease, storedJobID, storedKind, storedPartition, storedDedupe) {
			return nil
		}
		blocked, err := sandboxReleaseBlockedTx(ctx, tx, job.WorkspaceID, job.SessionID, job.LogicalSandboxID, job.OperationID, targetHandle)
		if err != nil {
			return err
		}
		if blocked {
			return errSandboxReleaseBlocked
		}
		if err := recordLifecycleLease(ctx, tx, job, now); err != nil {
			return err
		}
		work = SandboxReleaseWork{
			Job: job, Provider: binding.Provider,
			Handle: sandbox.ProviderHandle{Provider: binding.Provider, SandboxID: targetHandle},
			Reason: SandboxReleaseReason(releaseReason),
			ObserveOnly: effectBoundary.Valid &&
				(effectBoundary.String == string(ProviderSubmitted) || effectBoundary.String == string(ProviderOutcomeUnknown)),
		}
		claimed = true
		return nil
	})
	return work, claimed, err
}

func (s *PostgreSQLSandboxLifecycleStore) ParkBlockedRelease(ctx context.Context, job SandboxLifecycleJob, now time.Time) (bool, error) {
	if s == nil || s.client == nil {
		return false, errors.New("sandbox lifecycle store is required")
	}
	var parked bool
	err := s.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.release.park", func(tx *dbconnect.Tx) error {
		if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}); err != nil {
			return err
		}
		if _, exists, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID); err != nil || !exists {
			return err
		}
		var state, kind, targetHandle string
		var storedJobID, storedKind, storedPartition, storedDedupe sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, kind, target_provider_resource_id, queue_job_id, queue_kind,
			        queue_partition_key, queue_dedupe_key
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id=$1 AND operation_id=$2 AND session_id=$3
			    AND logical_sandbox_id=$4 FOR UPDATE`,
			job.WorkspaceID, job.OperationID, job.SessionID, job.LogicalSandboxID,
		).Scan(&state, &kind, &targetHandle, &storedJobID, &storedKind, &storedPartition, &storedDedupe); err != nil {
			if dbconnect.IsNoRows(err) {
				return nil
			}
			return err
		}
		if state != "pending" || kind != "release" ||
			!sandboxLifecycleQueueIdentityMatches(job, queue.KindSandboxRelease, storedJobID, storedKind, storedPartition, storedDedupe) {
			return nil
		}
		blocked, err := sandboxReleaseBlockedTx(ctx, tx, job.WorkspaceID, job.SessionID, job.LogicalSandboxID, job.OperationID, targetHandle)
		if err != nil || !blocked {
			return err
		}
		updated, err := queue.AckTx(ctx, tx, queue.AckRequest{
			WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID,
			LeaseToken: job.LeaseToken, Now: now.UTC(),
		})
		if err != nil {
			return err
		}
		if !updated {
			return errors.New("sandbox release queue lease is stale")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET queue_job_id=NULL, queue_kind=NULL, queue_partition_key=NULL,
			        queue_dedupe_key=NULL, updated_at=$3
			  WHERE workspace_id=$1 AND operation_id=$2 AND state='pending'`,
			job.WorkspaceID, job.OperationID, now.UTC(),
		); err != nil {
			return err
		}
		parked = true
		return nil
	})
	return parked, err
}

func sandboxReleaseBlockedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, logicalSandboxID string, operationID string, targetHandle string) (bool, error) {
	var blocked bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM session_runtime_tool_results
			 WHERE workspace_id=$1 AND session_id=$2 AND tool_kind='sandbox_tool'
			   AND execution_state IN ('preparing','running')
			   AND authorized_provider_resource_id=$3
			UNION ALL
			SELECT 1 FROM session_background_tasks
			 WHERE workspace_id=$1 AND session_id=$2 AND status='running'
			   AND provider_session_id=$3
			UNION ALL
			SELECT 1 FROM sandbox_lifecycle_operations
			 WHERE workspace_id=$1 AND logical_sandbox_id=$4 AND operation_id<>$5
			   AND kind IN ('create','start','replace','materialize') AND state='running'
		)`,
		workspaceID, sessionID, targetHandle, logicalSandboxID, operationID,
	).Scan(&blocked)
	return blocked, err
}

func (s *PostgreSQLSandboxLifecycleStore) AuthorizeRelease(ctx context.Context, work SandboxReleaseWork, now time.Time) (bool, error) {
	var authorized bool
	err := s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.release.authorize", func(tx *dbconnect.Tx) error {
		if err := lockLifecycleSessionBindingOperation(ctx, tx, work.Job); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET outcome_effect_boundary='submitted', outcome_disposition='terminal', updated_at=$3
			  WHERE workspace_id=$1 AND operation_id=$2 AND kind='release' AND state='running'
			    AND target_provider_resource_id=$4
			    AND lease_owner=$5 AND lease_token=$6
			    AND outcome_effect_boundary IS NULL`,
			work.Job.WorkspaceID, work.Job.OperationID, now.UTC(), work.Handle.SandboxID,
			work.Job.LeaseOwner, work.Job.LeaseToken,
		)
		if err != nil {
			return err
		}
		authorized = transitionRowsAffected(result)
		return nil
	})
	return authorized, err
}

func (s *PostgreSQLSandboxLifecycleStore) RearmRelease(ctx context.Context, work SandboxReleaseWork, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.release.rearm", func(tx *dbconnect.Tx) error {
		if err := lockLifecycleSessionBindingOperation(ctx, tx, work.Job); err != nil {
			return err
		}
		var state, targetHandle string
		var effectBoundary, leaseOwner, leaseToken sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, target_provider_resource_id, outcome_effect_boundary, lease_owner, lease_token
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id=$1 AND operation_id=$2 AND kind='release'
			  FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&state, &targetHandle, &effectBoundary, &leaseOwner, &leaseToken); err != nil {
			return err
		}
		if state != "running" || targetHandle != work.Handle.SandboxID ||
			!lifecycleLeaseIdentityMatches(work.Job, leaseOwner, leaseToken) {
			return nil
		}
		if !effectBoundary.Valid {
			return nil
		}
		if effectBoundary.String != string(ProviderSubmitted) {
			return errors.New("sandbox release outcome boundary cannot be rearmed")
		}
		_, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET outcome_effect_boundary=NULL, outcome_disposition=NULL,
			        error_kind=NULL, safe_message=NULL, updated_at=$3
			  WHERE workspace_id=$1 AND operation_id=$2 AND state='running'
			    AND outcome_effect_boundary='submitted'`,
			work.Job.WorkspaceID, work.Job.OperationID, now.UTC(),
		)
		return err
	})
}

func (s *PostgreSQLSandboxLifecycleStore) CompleteRelease(ctx context.Context, work SandboxReleaseWork, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.release.complete", func(tx *dbconnect.Tx) error {
		if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: work.Job.WorkspaceID, SessionID: work.Job.SessionID}); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID)
		if err != nil || !exists {
			return err
		}
		var state, reason, targetHandle string
		var leaseOwner, leaseToken sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT state, release_reason, target_provider_resource_id, lease_owner, lease_token
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id=$1 AND operation_id=$2 FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.OperationID,
		).Scan(&state, &reason, &targetHandle, &leaseOwner, &leaseToken); err != nil {
			return err
		}
		if state == "completed" {
			return nil
		}
		if state != "running" || targetHandle != work.Handle.SandboxID ||
			!lifecycleLeaseIdentityMatches(work.Job, leaseOwner, leaseToken) {
			return nil
		}
		if err := applySandboxReleasePostconditionTx(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID, binding, targetHandle, SandboxReleaseReason(reason), now); err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state='completed', outcome_effect_boundary=NULL, outcome_disposition=NULL,
			        error_kind=NULL, safe_message=NULL, lease_owner=NULL, lease_token=NULL,
			        lease_expires_at=NULL, completed_at=$3, updated_at=$3
			  WHERE workspace_id=$1 AND operation_id=$2 AND state='running'`,
			work.Job.WorkspaceID, work.Job.OperationID, now.UTC(),
		)
		return err
	})
}

func (s *PostgreSQLSandboxLifecycleStore) ObserveUnknownRelease(ctx context.Context, work SandboxReleaseWork, kind string, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.release.observe_unknown", func(tx *dbconnect.Tx) error {
		if err := lockLifecycleSessionBindingOperation(ctx, tx, work.Job); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET outcome_effect_boundary='outcome_unknown', outcome_disposition='terminal',
			        error_kind=$3, safe_message='sandbox release outcome is unknown', updated_at=$4
			  WHERE workspace_id=$1 AND operation_id=$2 AND state='running'
			    AND lease_owner=$5 AND lease_token=$6`,
			work.Job.WorkspaceID, work.Job.OperationID, kind, now.UTC(), work.Job.LeaseOwner, work.Job.LeaseToken,
		)
		return err
	})
}

func (s *PostgreSQLSandboxLifecycleStore) FailRelease(ctx context.Context, work SandboxReleaseWork, boundary ProviderEffectBoundary, disposition ProviderDisposition, kind string, message string, now time.Time) error {
	return s.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.release.fail", func(tx *dbconnect.Tx) error {
		if err := lockLifecycleSessionBindingOperation(ctx, tx, work.Job); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state='failed', outcome_effect_boundary=$3, outcome_disposition=$4,
			        error_kind=$5, safe_message=$6, lease_owner=NULL, lease_token=NULL,
			        lease_expires_at=NULL, completed_at=$7, updated_at=$7
			  WHERE workspace_id=$1 AND operation_id=$2 AND state IN ('pending','running')
			    AND lease_owner=$8 AND lease_token=$9`,
			work.Job.WorkspaceID, work.Job.OperationID, boundary, disposition, kind, message, now.UTC(),
			work.Job.LeaseOwner, work.Job.LeaseToken,
		)
		return err
	})
}

func applySandboxReleasePostconditionTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding SandboxBinding, targetHandle string, reason SandboxReleaseReason, now time.Time) error {
	return sandboxrelease.ApplyPostconditionTx(ctx, tx, workspaceID, sessionID, binding.ProviderResourceID, targetHandle, reason, now)
}

func (s *PostgreSQLSandboxLifecycleStore) FinalizeExhaustedLifecycle(ctx context.Context, queueJob *queuev1.QueueJob, queueKind string, now time.Time) error {
	return s.finalizeClosedLifecycle(ctx, queueJob, queueKind, "", "", now)
}

func (s *PostgreSQLSandboxLifecycleStore) FinalizeInvalidLifecycle(ctx context.Context, queueJob *queuev1.QueueJob, queueKind string, now time.Time) error {
	return s.finalizeClosedLifecycle(ctx, queueJob, queueKind, "sandbox_queue_integrity_error", "sandbox lifecycle queue payload is invalid", now)
}

func (s *PostgreSQLSandboxLifecycleStore) finalizeClosedLifecycle(ctx context.Context, queueJob *queuev1.QueueJob, queueKind string, errorKind string, safeMessage string, now time.Time) error {
	if queueKind != queue.KindSandboxActivate && queueKind != queue.KindSandboxMaterialize && queueKind != queue.KindSandboxRelease {
		return errors.New("sandbox lifecycle exhaustion kind is invalid")
	}
	if queueJob == nil || queueJob.GetWorkspaceId() == "" || queueJob.GetId() == "" || queueJob.GetKind() != queueKind {
		return errors.New("sandbox lifecycle exhaustion Queue identity is invalid")
	}
	return s.client.WithWorkspaceTx(ctx, queueJob.GetWorkspaceId(), "sandbox.lifecycle.finalize_exhausted", func(tx *dbconnect.Tx) error {
		job, found, err := lookupSandboxLifecycleJobByQueueIdentity(
			ctx, tx, queueJob.GetWorkspaceId(), queueJob.GetId(), queueKind,
			queueJob.GetPartitionKey(), queueJob.GetDedupeKey(),
		)
		if err != nil || !found {
			return err
		}
		return finalizeExhaustedSandboxLifecycleTx(ctx, tx, job, queueKind, errorKind, safeMessage, now)
	})
}

func lookupSandboxLifecycleJobByQueueIdentity(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	jobID string,
	queueKind string,
	partitionKey string,
	dedupeKey string,
) (SandboxLifecycleJob, bool, error) {
	var job SandboxLifecycleJob
	err := tx.QueryRow(ctx,
		`SELECT session_id, logical_sandbox_id, operation_id
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1 AND queue_job_id = $2 AND queue_kind = $3
		    AND queue_partition_key = $4 AND queue_dedupe_key = $5`,
		workspaceID, jobID, queueKind, partitionKey, dedupeKey,
	).Scan(&job.SessionID, &job.LogicalSandboxID, &job.OperationID)
	if dbconnect.IsNoRows(err) {
		return SandboxLifecycleJob{}, false, nil
	}
	if err != nil {
		return SandboxLifecycleJob{}, false, err
	}
	job.JobID = jobID
	job.WorkspaceID = workspaceID
	if partitionKey != queue.FormatSandboxLifecyclePartitionKey(workspace.ID(workspaceID), job.LogicalSandboxID) ||
		dedupeKey != queue.FormatSandboxLifecycleDedupeKey(queueKind, workspace.ID(workspaceID), job.LogicalSandboxID, job.OperationID) {
		return SandboxLifecycleJob{}, false, errors.New("sandbox lifecycle exhaustion Queue identity is not canonical")
	}
	return job, true, nil
}

func finalizeExhaustedSandboxLifecycleTx(ctx context.Context, tx *dbconnect.Tx, job SandboxLifecycleJob, queueKind string, forcedErrorKind string, forcedSafeMessage string, now time.Time) error {
	if queueKind != queue.KindSandboxActivate && queueKind != queue.KindSandboxMaterialize && queueKind != queue.KindSandboxRelease {
		return errors.New("sandbox lifecycle exhaustion kind is invalid")
	}
	errorKind := "sandbox_activation_attempts_exhausted"
	safeMessage := "sandbox activation could not be completed"
	waiterColumn := "waiting_activation_operation_id"
	if queueKind == queue.KindSandboxMaterialize {
		errorKind = "sandbox_materialization_attempts_exhausted"
		safeMessage = "sandbox resources could not be materialized"
		waiterColumn = "waiting_materialization_operation_id"
	}
	if queueKind == queue.KindSandboxRelease {
		errorKind = "sandbox_release_attempts_exhausted"
		safeMessage = "sandbox release could not be completed"
		waiterColumn = ""
	}
	if forcedErrorKind != "" {
		errorKind = forcedErrorKind
		safeMessage = forcedSafeMessage
	}
	if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}); err != nil {
		return err
	}
	if _, _, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return err
	}
	var state, operationKind string
	var storedEffectBoundary sql.NullString
	var storedJobID, storedKind, partition, dedupe sql.NullString
	if err := tx.QueryRow(ctx,
		`SELECT state, kind, outcome_effect_boundary, queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2 FOR UPDATE`,
		job.WorkspaceID, job.OperationID,
	).Scan(&state, &operationKind, &storedEffectBoundary, &storedJobID, &storedKind, &partition, &dedupe); err != nil {
		if dbconnect.IsNoRows(err) {
			return nil
		}
		return err
	}
	if state == "completed" || state == "failed" || state == "abandoned" {
		return nil
	}
	if queueKind == queue.KindSandboxMaterialize && state == "waiting_activation" {
		return nil
	}
	if !sandboxLifecycleQueueIdentityMatches(job, queueKind, storedJobID, storedKind, partition, dedupe) {
		return errors.New("sandbox lifecycle exhaustion queue identity does not match")
	}
	if (queueKind == queue.KindSandboxActivate && operationKind != string(ActivationCreate) && operationKind != string(ActivationStart) && operationKind != string(ActivationReplace)) ||
		(queueKind == queue.KindSandboxMaterialize && operationKind != "materialize") ||
		(queueKind == queue.KindSandboxRelease && operationKind != "release") {
		return errors.New("sandbox lifecycle exhaustion operation kind does not match")
	}
	outcomeBoundary := ProviderProvedNotStarted
	if queueKind == queue.KindSandboxActivate && storedEffectBoundary.String == string(ProviderOutcomeUnknown) {
		outcomeBoundary = ProviderOutcomeUnknown
		errorKind = "activation_outcome_unknown"
		safeMessage = "sandbox creation outcome could not be reconciled"
	}
	if queueKind == queue.KindSandboxRelease && (storedEffectBoundary.String == string(ProviderSubmitted) || storedEffectBoundary.String == string(ProviderOutcomeUnknown)) {
		outcomeBoundary = ProviderOutcomeUnknown
		errorKind = "sandbox_release_outcome_unknown"
		safeMessage = "sandbox release outcome could not be reconciled"
	}
	if _, err := tx.Exec(ctx,
		`UPDATE sandbox_lifecycle_operations
			    SET state = 'failed', outcome_effect_boundary = $3,
			        outcome_disposition = 'terminal', error_kind = $4, safe_message = $5,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        completed_at = $6, updated_at = $6
			  WHERE workspace_id = $1 AND operation_id = $2`,
		job.WorkspaceID, job.OperationID, outcomeBoundary, errorKind, safeMessage, now.UTC(),
	); err != nil {
		return err
	}
	if queueKind == queue.KindSandboxRelease {
		return nil
	}
	waiters, err := lockExecutionWaiters(ctx, tx, job.WorkspaceID, job.OperationID, waiterColumn)
	if err != nil {
		return err
	}
	if err := settleExecutionWaitersTx(ctx, tx, waiters, errorKind, safeMessage, now); err != nil {
		return err
	}
	if queueKind != queue.KindSandboxActivate {
		return nil
	}
	return failActivationMaterializationDependentsTx(ctx, tx, job.WorkspaceID, job.OperationID, errorKind, safeMessage, now)
}

func failActivationMaterializationDependentsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, activationOperationID string, kind string, message string, now time.Time) error {
	rows, err := tx.Query(ctx,
		`SELECT operation_id FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1 AND waiting_activation_operation_id = $2
		    AND kind = 'materialize' AND state = 'waiting_activation'
		  ORDER BY operation_id FOR UPDATE`,
		workspaceID, activationOperationID,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var materializationIDs []string
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			return err
		}
		materializationIDs = append(materializationIDs, operationID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, operationID := range materializationIDs {
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'failed', outcome_effect_boundary = 'proved_not_started',
			        outcome_disposition = 'terminal', error_kind = $3, safe_message = $4,
			        lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			        completed_at = $5, updated_at = $5
			  WHERE workspace_id = $1 AND operation_id = $2`,
			workspaceID, operationID, kind, message, now.UTC(),
		); err != nil {
			return err
		}
		materializationWaiters, err := lockExecutionWaiters(ctx, tx, workspaceID, operationID, "waiting_materialization_operation_id")
		if err != nil {
			return err
		}
		if err := settleExecutionWaitersTx(ctx, tx, materializationWaiters, kind, message, now); err != nil {
			return err
		}
	}
	return nil
}

type sandboxExecutionWaiter struct {
	ref        SandboxExecutionRef
	generation int64
}

func lockExecutionWaiters(ctx context.Context, tx *dbconnect.Tx, workspaceID string, operationID string, linkColumn string) ([]sandboxExecutionWaiter, error) {
	if linkColumn != "waiting_activation_operation_id" && linkColumn != "waiting_materialization_operation_id" {
		return nil, errors.New("sandbox execution waiter link is invalid")
	}
	rows, err := tx.Query(ctx,
		`SELECT session_id, session_thread_id, tool_use_event_id, execution_attempt_generation
		   FROM session_runtime_tool_results
		  WHERE workspace_id = $1 AND `+linkColumn+` = $2
		    AND execution_state IN ('waiting_activation', 'waiting_materialization')
		  ORDER BY session_thread_id, tool_use_event_id
		  FOR UPDATE`,
		workspaceID, operationID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var waiters []sandboxExecutionWaiter
	for rows.Next() {
		var waiter sandboxExecutionWaiter
		waiter.ref.WorkspaceID = workspaceID
		if err := rows.Scan(&waiter.ref.SessionID, &waiter.ref.SessionThreadID, &waiter.ref.ToolUseEventID, &waiter.generation); err != nil {
			return nil, err
		}
		waiters = append(waiters, waiter)
	}
	return waiters, rows.Err()
}

func releaseExecutionWaitersTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, waiters []sandboxExecutionWaiter, now time.Time) error {
	for _, waiter := range waiters {
		generation := waiter.generation + 1
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET execution_state = 'pending', execution_attempt_generation = $5,
			        waiting_activation_operation_id = NULL,
			        waiting_materialization_operation_id = NULL,
			        updated_at = $6
			  WHERE workspace_id = $1 AND session_id = $2
			    AND session_thread_id = $3 AND tool_use_event_id = $4`,
			workspaceID, waiter.ref.SessionID, waiter.ref.SessionThreadID, waiter.ref.ToolUseEventID, generation, now.UTC(),
		); err != nil {
			return err
		}
		payload, err := sandboxExecutionQueuePayload(waiter.ref)
		if err != nil {
			return err
		}
		if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
			WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxToolExecute,
			PartitionKey:   queue.FormatSandboxExecutionPartitionKey(workspace.ID(workspaceID), sessionID, waiter.ref.SessionThreadID, waiter.ref.ToolUseEventID),
			DedupeKey:      queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(workspaceID), sessionID, waiter.ref.SessionThreadID, waiter.ref.ToolUseEventID, generation),
			PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxToolExecuteMaxAttempts, Now: now,
		}); err != nil {
			return err
		}
	}
	return nil
}

func attachExecutionWaitersToMaterializationTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, waiters []sandboxExecutionWaiter, operationID string, now time.Time) error {
	for _, waiter := range waiters {
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET execution_state = 'waiting_materialization',
			        waiting_activation_operation_id = NULL,
			        waiting_materialization_operation_id = $5,
			        updated_at = $6
			  WHERE workspace_id = $1 AND session_id = $2
			    AND session_thread_id = $3 AND tool_use_event_id = $4`,
			workspaceID, waiter.ref.SessionID, waiter.ref.SessionThreadID, waiter.ref.ToolUseEventID, operationID, now.UTC(),
		); err != nil {
			return err
		}
	}
	return nil
}

func settleExecutionWaitersTx(ctx context.Context, tx *dbconnect.Tx, waiters []sandboxExecutionWaiter, kind string, message string, now time.Time) error {
	for _, waiter := range waiters {
		if err := settleSandboxExecutionTx(ctx, tx, waiter.ref, waiter.generation, SandboxExecutionSettlement{
			Kind: SandboxExecutionFailed, ErrorKind: kind, SafeMessage: message,
		}, now); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgreSQLSandboxLifecycleStore) ensureMaterializationOperationTx(ctx context.Context, tx *dbconnect.Tx, job SandboxLifecycleJob, binding SandboxBinding, resourceRevision int64, now time.Time) (string, error) {
	var operationID string
	err := tx.QueryRow(ctx,
		`SELECT operation_id FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1 AND logical_sandbox_id = $2
		    AND kind = 'materialize'
		    AND state IN ('pending', 'waiting_activation', 'running')
		  FOR UPDATE`,
		job.WorkspaceID, binding.LogicalSandboxID,
	).Scan(&operationID)
	if err == nil {
		return operationID, nil
	}
	if !dbconnect.IsNoRows(err) {
		return "", err
	}
	resources, err := s.resources.ListSessionResourcesTx(ctx, tx, workspace.ID(job.WorkspaceID), job.SessionID)
	if err != nil {
		return "", err
	}
	resourcesJSON, err := encodeMaterializationResources(resources)
	if err != nil {
		return "", err
	}
	operationID = id.New(sandboxLifecycleOperationIDPrefix)
	queueJobID := queue.NewJobID()
	partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(job.WorkspaceID), binding.LogicalSandboxID)
	dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, workspace.ID(job.WorkspaceID), binding.LogicalSandboxID, operationID)
	if _, err := tx.Exec(ctx,
		`INSERT INTO sandbox_lifecycle_operations (
			workspace_id, operation_id, session_id, logical_sandbox_id,
			kind, state, observed_binding_revision, target_environment_generation, target_resource_revision,
			target_provider_resource_id, materialization_resources_json, queue_job_id, queue_kind,
			queue_partition_key, queue_dedupe_key, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'materialize', 'pending', $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)`,
		job.WorkspaceID, operationID, job.SessionID, binding.LogicalSandboxID,
		binding.BindingRevision, binding.EnvironmentGeneration, resourceRevision, binding.ProviderResourceID,
		resourcesJSON, queueJobID, queue.KindSandboxMaterialize, partitionKey, dedupeKey, now.UTC(),
	); err != nil {
		return "", err
	}
	payload, err := sandboxLifecycleQueuePayload(SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}, binding.LogicalSandboxID, operationID)
	if err != nil {
		return "", err
	}
	if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID: queueJobID, WorkspaceID: workspace.ID(job.WorkspaceID), Kind: queue.KindSandboxMaterialize,
		PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
		PayloadJSON: payload, MaxAttempts: sandboxMaterializationMaxAttempts, Now: now,
	}); err != nil {
		return "", err
	}
	return operationID, nil
}

func ensureActivationOperationTx(ctx context.Context, tx *dbconnect.Tx, job SandboxLifecycleJob, binding SandboxBinding, generation int64, readiness ExecutionReadiness, now time.Time) (string, error) {
	var operationID string
	err := tx.QueryRow(ctx,
		`SELECT operation_id FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1 AND logical_sandbox_id = $2
		    AND kind IN ('create', 'start', 'replace')
		    AND state IN ('pending', 'waiting_artifact', 'running')
		  FOR UPDATE`,
		job.WorkspaceID, binding.LogicalSandboxID,
	).Scan(&operationID)
	if err == nil {
		return operationID, nil
	}
	if !dbconnect.IsNoRows(err) {
		return "", err
	}
	kind := "start"
	if readiness == ExecutionNeedsCreation {
		kind = "replace"
	}
	operationID = id.New(sandboxLifecycleOperationIDPrefix)
	queueJobID := queue.NewJobID()
	partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(job.WorkspaceID), binding.LogicalSandboxID)
	dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, workspace.ID(job.WorkspaceID), binding.LogicalSandboxID, operationID)
	labelsJSON, err := json.Marshal(sandboxActivationLabels(job.WorkspaceID, job.SessionID, binding.EnvironmentID, binding.LogicalSandboxID, operationID))
	if err != nil {
		return "", err
	}
	var targetGeneration, targetHandle, createName, labels any
	if kind == "start" {
		targetHandle = binding.ProviderResourceID
	} else {
		targetGeneration = generation
		createName = binding.LogicalSandboxID
		labels = string(labelsJSON)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO sandbox_lifecycle_operations (
			workspace_id, operation_id, session_id, logical_sandbox_id,
			kind, state, observed_binding_revision, target_environment_generation,
			target_provider_resource_id, provider_create_name,
			provider_request_labels_json, queue_job_id, queue_kind,
			queue_partition_key, queue_dedupe_key, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $15)`,
		job.WorkspaceID, operationID, job.SessionID, binding.LogicalSandboxID,
		kind, binding.BindingRevision, targetGeneration, targetHandle, createName, labels,
		queueJobID, queue.KindSandboxActivate, partitionKey, dedupeKey, now.UTC(),
	); err != nil {
		return "", err
	}
	payload, err := sandboxLifecycleQueuePayload(SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}, binding.LogicalSandboxID, operationID)
	if err != nil {
		return "", err
	}
	if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID: queueJobID, WorkspaceID: workspace.ID(job.WorkspaceID), Kind: queue.KindSandboxActivate,
		PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
		PayloadJSON: payload, MaxAttempts: sandboxActivationMaxAttempts, Now: now,
	}); err != nil {
		return "", err
	}
	return operationID, nil
}

func recordLifecycleLease(ctx context.Context, tx *dbconnect.Tx, job SandboxLifecycleJob, now time.Time) error {
	if job.LeaseOwner == "" || job.LeaseToken == "" || job.LeaseExpiresAt.IsZero() {
		_, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'running', attempt_count = GREATEST(attempt_count, $3), updated_at = $4
			  WHERE workspace_id = $1 AND operation_id = $2`,
			job.WorkspaceID, job.OperationID, job.AttemptCount, now.UTC(),
		)
		return err
	}
	_, err := tx.Exec(ctx,
		`UPDATE sandbox_lifecycle_operations
		    SET state = 'running', lease_owner = $3, lease_token = $4,
		        lease_expires_at = $5, attempt_count = GREATEST(attempt_count, $6),
		        updated_at = $7
		  WHERE workspace_id = $1 AND operation_id = $2`,
		job.WorkspaceID, job.OperationID, job.LeaseOwner, job.LeaseToken,
		job.LeaseExpiresAt.UTC(), job.AttemptCount, now.UTC(),
	)
	return err
}

func lifecycleLeaseIdentityMatches(job SandboxLifecycleJob, leaseOwner sql.NullString, leaseToken sql.NullString) bool {
	if job.LeaseOwner == "" || job.LeaseToken == "" {
		return true
	}
	return leaseOwner.Valid && leaseOwner.String == job.LeaseOwner && leaseToken.Valid && leaseToken.String == job.LeaseToken
}

func lockLifecycleSessionBindingOperation(ctx context.Context, tx *dbconnect.Tx, job SandboxLifecycleJob) error {
	if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}); err != nil {
		return err
	}
	var environmentID string
	if err := tx.QueryRow(ctx,
		`SELECT environment_id FROM sessions WHERE workspace_id = $1 AND id = $2`,
		job.WorkspaceID, job.SessionID,
	).Scan(&environmentID); err != nil {
		return err
	}
	var generation int64
	if err := tx.QueryRow(ctx,
		`SELECT current_generation FROM environments WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
		job.WorkspaceID, environmentID,
	).Scan(&generation); err != nil {
		return err
	}
	if _, _, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return err
	}
	return lockSandboxLifecycleOperationsForSession(ctx, tx, job.WorkspaceID, job.SessionID)
}

func sandboxLifecycleQueueIdentityMatches(job SandboxLifecycleJob, kind string, jobID sql.NullString, storedKind sql.NullString, partition sql.NullString, dedupe sql.NullString) bool {
	ws := workspace.ID(job.WorkspaceID)
	return jobID.Valid && jobID.String == job.JobID && storedKind.Valid && storedKind.String == kind &&
		partition.Valid && partition.String == queue.FormatSandboxLifecyclePartitionKey(ws, job.LogicalSandboxID) &&
		dedupe.Valid && dedupe.String == queue.FormatSandboxLifecycleDedupeKey(kind, ws, job.LogicalSandboxID, job.OperationID)
}

func sandboxActivationLabels(workspaceID string, sessionID string, environmentID string, logicalSandboxID string, operationID string) map[string]string {
	return map[string]string{
		"tetral.workspace_id":           workspaceID,
		"tetral.session_id":             sessionID,
		"tetral.environment_id":         environmentID,
		"tetral.sandbox_id":             logicalSandboxID,
		"tetral.lifecycle_operation_id": operationID,
		"tetral.lifecycle_owner":        "sandbox",
	}
}

func sandboxExecutionQueuePayload(ref SandboxExecutionRef) ([]byte, error) {
	return json.Marshal(struct {
		WorkspaceID     string `json:"workspace_id"`
		SessionID       string `json:"session_id"`
		SessionThreadID string `json:"session_thread_id"`
		ToolUseEventID  string `json:"tool_use_event_id"`
	}{
		WorkspaceID: ref.WorkspaceID, SessionID: ref.SessionID,
		SessionThreadID: ref.SessionThreadID, ToolUseEventID: ref.ToolUseEventID,
	})
}

func decodeSandboxNetworkSetup(raw string) (sandbox.NetworkSetup, error) {
	if raw == "" {
		return sandbox.NetworkSetup{}, nil
	}
	var payload struct {
		Type             string `json:"type"`
		NetworkAllowList string `json:"network_allow_list"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return sandbox.NetworkSetup{}, err
	}
	return sandbox.NetworkSetup{Type: payload.Type, NetworkAllowList: payload.NetworkAllowList}, nil
}

func encodeMaterializationResources(resources sandbox.ResourceSetup) (string, error) {
	resources.ResourceCredExpiresAt = nil
	resources.ResourceRootsJSON = ""
	encoded, err := json.Marshal(resources)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeMaterializationResources(raw string) (sandbox.ResourceSetup, error) {
	if raw == "" {
		return sandbox.ResourceSetup{}, errors.New("sandbox materialization resource snapshot is missing")
	}
	var resources sandbox.ResourceSetup
	if err := json.Unmarshal([]byte(raw), &resources); err != nil {
		return sandbox.ResourceSetup{}, err
	}
	return resources, nil
}
