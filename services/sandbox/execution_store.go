package tetralsandbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxruntime "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const (
	sandboxLogicalIDPrefix                 = "sbox_"
	sandboxLifecycleOperationIDPrefix      = "sop_"
	sandboxActivationSubmissionMaxAttempts = 5
	sandboxActivationMaxAttempts           = sandboxActivationSubmissionMaxAttempts + 1
	sandboxMaterializationMaxAttempts      = 5
)

var errSandboxExecutionReinspection = errors.New("sandbox execution requires a fresh provider inspection")

// PostgreSQLSandboxExecutionCoordinator owns the durable state transitions
// around provider execution. Provider calls happen in runners, never while one
// of these transactions holds Session, binding, execution, or Queue locks.
type PostgreSQLSandboxExecutionCoordinator struct {
	client                  *dbconnect.Client
	resources               SandboxSessionResourceSource
	credentialRefreshMargin time.Duration
	clock                   func() time.Time
}

func NewPostgreSQLSandboxExecutionCoordinator(client *dbconnect.Client, credentialRefreshMargin time.Duration) *PostgreSQLSandboxExecutionCoordinator {
	return &PostgreSQLSandboxExecutionCoordinator{
		client: client, resources: sandboxruntime.NewPostgreSQLStore(client),
		credentialRefreshMargin: credentialRefreshMargin,
	}
}

func (c *PostgreSQLSandboxExecutionCoordinator) LoadExecution(ctx context.Context, job SandboxExecutionJob) (SandboxExecutionWork, bool, error) {
	if c == nil || c.client == nil {
		return SandboxExecutionWork{}, false, errors.New("sandbox execution coordinator is required")
	}
	var work SandboxExecutionWork
	var current bool
	err := c.client.WithWorkspaceReadOnlyTx(ctx, job.Ref.WorkspaceID, "sandbox.execution.load", func(tx *dbconnect.Tx) error {
		var (
			state                        string
			attemptGeneration            int64
			toolName                     string
			inputJSON                    string
			logicalSandboxID             sql.NullString
			environmentID                sql.NullString
			environmentGeneration        sql.NullInt64
			provider                     sql.NullString
			providerResourceID           sql.NullString
			bindingRevision              sql.NullInt64
			materializedResourceRevision sql.NullInt64
			sessionResourceRevision      int64
			credentialExpiresAt          sql.NullTime
			resourceRootsJSON            sql.NullString
			providerMetadataJSON         sql.NullString
			helperVerifiedAt             sql.NullTime
			releaseRequestedAt           sql.NullTime
			authorizedBindingRevision    sql.NullInt64
			authorizedProviderResourceID sql.NullString
			preparationDeadline          sql.NullTime
			providerCommandReference     sql.NullString
			databaseNow                  time.Time
		)
		err := tx.QueryRow(ctx,
			`SELECT r.execution_state, r.execution_attempt_generation, r.tool_name, r.input_json,
			        b.logical_sandbox_id, b.environment_id, b.environment_generation,
			        b.provider, b.provider_resource_id, b.binding_revision,
			        b.materialized_resource_revision, s.sandbox_resource_revision,
			        b.resource_credential_expires_at, b.resource_roots_json,
			        b.provider_metadata_json, b.helper_verified_at, b.release_requested_at,
			        r.authorized_binding_revision, r.authorized_provider_resource_id,
			        r.preparation_deadline, r.provider_command_reference_json,
			        CURRENT_TIMESTAMP
			   FROM session_runtime_tool_results r
			   JOIN sessions s
			     ON s.workspace_id = r.workspace_id AND s.id = r.session_id
			   LEFT JOIN session_sandbox_bindings b
			     ON b.workspace_id = r.workspace_id AND b.session_id = r.session_id
			  WHERE r.workspace_id = $1
			    AND r.session_id = $2
			    AND r.session_thread_id = $3
			    AND r.tool_use_event_id = $4
			    AND r.tool_kind = 'sandbox_tool'`,
			job.Ref.WorkspaceID, job.Ref.SessionID, job.Ref.SessionThreadID, job.Ref.ToolUseEventID,
		).Scan(
			&state, &attemptGeneration, &toolName, &inputJSON,
			&logicalSandboxID, &environmentID, &environmentGeneration,
			&provider, &providerResourceID, &bindingRevision,
			&materializedResourceRevision, &sessionResourceRevision,
			&credentialExpiresAt, &resourceRootsJSON, &providerMetadataJSON, &helperVerifiedAt, &releaseRequestedAt,
			&authorizedBindingRevision, &authorizedProviderResourceID, &preparationDeadline, &providerCommandReference,
			&databaseNow,
		)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if (state != "pending" && state != "preparing" && state != "running") || attemptGeneration != job.AttemptGeneration {
			return nil
		}
		work = SandboxExecutionWork{
			Ref: job.Ref, AttemptGeneration: attemptGeneration, State: state,
			AuthorizedBindingRevision:    authorizedBindingRevision.Int64,
			AuthorizedProviderResourceID: authorizedProviderResourceID.String,
			ProviderCommandReference:     providerCommandReference.String,
			Invocation: sandboxdriver.ToolInvocation{
				ToolUseEventID: job.Ref.ToolUseEventID,
				ToolName:       toolName,
				InputJSON:      inputJSON,
			},
		}
		if preparationDeadline.Valid {
			deadline := preparationDeadline.Time.UTC()
			work.PreparationDeadline = &deadline
		}
		if logicalSandboxID.Valid {
			binding := &SandboxBinding{
				LogicalSandboxID: logicalSandboxID.String,
				EnvironmentID:    environmentID.String, EnvironmentGeneration: environmentGeneration.Int64,
				Provider: provider.String, ProviderResourceID: providerResourceID.String,
				BindingRevision:              bindingRevision.Int64,
				MaterializedResourceRevision: materializedResourceRevision.Int64,
				ResourceRootsJSON:            resourceRootsJSON.String,
				ProviderMetadataJSON:         providerMetadataJSON.String,
			}
			if credentialExpiresAt.Valid {
				expiresAt := credentialExpiresAt.Time.UTC()
				binding.ResourceCredentialExpiresAt = &expiresAt
			}
			if helperVerifiedAt.Valid {
				verifiedAt := helperVerifiedAt.Time.UTC()
				binding.HelperVerifiedAt = &verifiedAt
			}
			if releaseRequestedAt.Valid {
				requestedAt := releaseRequestedAt.Time.UTC()
				binding.ReleaseRequestedAt = &requestedAt
			}
			work.Binding = binding
			credentialReady := credentialExpiresAt.Valid && credentialExpiresAt.Time.After(databaseNow.UTC().Add(c.credentialRefreshMargin))
			work.MaterializationReady = !releaseRequestedAt.Valid && providerResourceID.Valid &&
				materializedResourceRevision.Int64 == sessionResourceRevision && credentialReady && helperVerifiedAt.Valid
		}
		current = true
		return nil
	})
	return work, current, err
}

func (c *PostgreSQLSandboxExecutionCoordinator) WaitForActivation(ctx context.Context, work SandboxExecutionWork, readiness ExecutionReadiness) error {
	if c == nil || c.client == nil {
		return errors.New("sandbox execution coordinator is required")
	}
	return c.client.WithWorkspaceTx(ctx, work.Ref.WorkspaceID, "sandbox.execution.wait_activation", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		now := c.now()
		var environmentID string
		var resourceRevision int64
		if err := tx.QueryRow(ctx,
			`SELECT environment_id, sandbox_resource_revision
			   FROM sessions
			  WHERE workspace_id = $1 AND id = $2
			  FOR UPDATE`,
			work.Ref.WorkspaceID, work.Ref.SessionID,
		).Scan(&environmentID, &resourceRevision); err != nil {
			return err
		}
		var generation int64
		if err := tx.QueryRow(ctx,
			`SELECT current_generation
			   FROM environments
			  WHERE workspace_id = $1 AND id = $2
			  FOR UPDATE`,
			work.Ref.WorkspaceID, environmentID,
		).Scan(&generation); err != nil {
			return err
		}
		artifactStatus := "ready"
		artifactProvider := sandboxdriver.DaytonaProviderName
		var providerArtifactRef sql.NullString
		needsArtifact := work.Binding == nil || readiness == ExecutionNeedsCreation
		if needsArtifact {
			if err := tx.QueryRow(ctx,
				`SELECT status, provider, provider_artifact_ref
				   FROM environment_artifacts
				  WHERE workspace_id = $1 AND environment_id = $2 AND generation = $3
				  FOR UPDATE`,
				work.Ref.WorkspaceID, environmentID, generation,
			).Scan(&artifactStatus, &artifactProvider, &providerArtifactRef); err != nil {
				return err
			}
			if artifactProvider != sandboxdriver.DaytonaProviderName {
				return errors.New("current environment artifact has an unsupported sandbox provider")
			}
		}

		binding, exists, err := lockSandboxBinding(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID)
		if err != nil {
			return err
		}
		staleMissingBindingObservation := work.Binding == nil && exists
		if !exists {
			binding = SandboxBinding{
				LogicalSandboxID: id.New(sandboxLogicalIDPrefix),
				EnvironmentID:    environmentID, EnvironmentGeneration: generation,
				Provider: sandboxdriver.DaytonaProviderName, BindingRevision: 1,
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO session_sandbox_bindings (
					workspace_id, session_id, logical_sandbox_id, environment_id,
					environment_generation, provider, provider_resource_id,
					binding_revision, materialized_resource_revision,
					resource_roots_json, provider_metadata_json, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, NULL, 1, 0, '[]', '{}', $7, $7)`,
				work.Ref.WorkspaceID, work.Ref.SessionID, binding.LogicalSandboxID,
				binding.EnvironmentID, binding.EnvironmentGeneration, binding.Provider, now,
			); err != nil {
				return err
			}
		}
		if err := lockSandboxLifecycleOperationsForSession(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID); err != nil {
			return err
		}
		if work.Binding != nil &&
			(binding.BindingRevision != work.Binding.BindingRevision || binding.ProviderResourceID != work.Binding.ProviderResourceID) {
			return errSandboxExecutionReinspection
		}
		if binding.ReleaseRequested() {
			return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "session_deleted", SafeMessage: "sandbox execution is no longer available",
			}, now)
		}
		if binding.Provider != sandboxdriver.DaytonaProviderName {
			return errors.New("sandbox binding has an unsupported provider")
		}

		var operationID, operationKind string
		var operationRevision int64
		var operationGeneration sql.NullInt64
		var operationHandle sql.NullString
		var activationQueueRequest *queue.EnqueueRequest
		err = tx.QueryRow(ctx,
			`SELECT operation_id, kind, observed_binding_revision,
			        target_environment_generation, target_provider_resource_id
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1
			    AND logical_sandbox_id = $2
			    AND kind IN ('create', 'start', 'replace')
			    AND state IN ('pending', 'waiting_artifact', 'running')
			  FOR UPDATE`,
			work.Ref.WorkspaceID, binding.LogicalSandboxID,
		).Scan(&operationID, &operationKind, &operationRevision, &operationGeneration, &operationHandle)
		if err != nil && !dbconnect.IsNoRows(err) {
			return err
		}
		if err == nil {
			operationMatches := operationRevision == binding.BindingRevision
			switch operationKind {
			case "start":
				operationMatches = operationMatches && readiness == ExecutionNeedsActivation &&
					operationHandle.Valid && operationHandle.String == binding.ProviderResourceID &&
					!operationGeneration.Valid
			case "create":
				operationMatches = operationMatches && readiness == ExecutionNeedsCreation &&
					binding.ProviderResourceID == "" && !operationHandle.Valid &&
					operationGeneration.Valid && operationGeneration.Int64 == generation
			case "replace":
				operationMatches = operationMatches && readiness == ExecutionNeedsCreation &&
					binding.ProviderResourceID != "" && !operationHandle.Valid &&
					operationGeneration.Valid && operationGeneration.Int64 == generation
			default:
				operationMatches = false
			}
			if !operationMatches {
				return errSandboxExecutionReinspection
			}
		}
		if dbconnect.IsNoRows(err) {
			if staleMissingBindingObservation {
				return errSandboxExecutionReinspection
			}
			operationID = id.New(sandboxLifecycleOperationIDPrefix)
			kind := "start"
			if binding.ProviderResourceID == "" {
				kind = "create"
			} else if readiness == ExecutionNeedsCreation {
				kind = "replace"
			}
			state := "pending"
			if kind != "start" && artifactStatus != "ready" {
				state = "waiting_artifact"
			}
			if kind != "start" && artifactStatus == "failed" {
				return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, SandboxExecutionSettlement{
					Kind: SandboxExecutionFailed, ErrorKind: "environment_artifact_failed", SafeMessage: "sandbox environment artifact is unavailable",
				}, now)
			}
			labelsJSON, err := json.Marshal(stableSandboxOwnershipLabels(
				work.Ref.WorkspaceID,
				work.Ref.SessionID,
				environmentID,
				binding.LogicalSandboxID,
			))
			if err != nil {
				return err
			}
			var queueJobID, queueKind, partitionKey, dedupeKey any
			if state == "pending" {
				queueJobID = queue.NewJobID()
				queueKind = queue.KindSandboxActivate
				partitionKey = queue.FormatSandboxLifecyclePartitionKey(workspace.ID(work.Ref.WorkspaceID), binding.LogicalSandboxID)
				dedupeKey = queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxActivate, workspace.ID(work.Ref.WorkspaceID), binding.LogicalSandboxID, operationID)
			}
			var targetProviderResourceID any
			var targetEnvironmentGeneration any
			var providerCreateName any
			var providerLabels any
			if kind == "start" {
				targetProviderResourceID = binding.ProviderResourceID
			} else {
				targetEnvironmentGeneration = generation
				providerCreateName = binding.LogicalSandboxID
				providerLabels = string(labelsJSON)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO sandbox_lifecycle_operations (
					workspace_id, operation_id, session_id, logical_sandbox_id,
					kind, state, observed_binding_revision, target_environment_generation,
					target_provider_resource_id, provider_create_name,
					provider_request_labels_json, queue_job_id, queue_kind,
					queue_partition_key, queue_dedupe_key, created_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16)`,
				work.Ref.WorkspaceID, operationID, work.Ref.SessionID, binding.LogicalSandboxID,
				kind, state, binding.BindingRevision, targetEnvironmentGeneration,
				targetProviderResourceID, providerCreateName, providerLabels,
				queueJobID, queueKind, partitionKey, dedupeKey, now,
			); err != nil {
				return err
			}
			if state == "pending" {
				payload, err := sandboxLifecycleQueuePayload(work.Ref, binding.LogicalSandboxID, operationID)
				if err != nil {
					return err
				}
				request := queue.EnqueueRequest{
					ID: queueJobID.(string), WorkspaceID: workspace.ID(work.Ref.WorkspaceID),
					Kind: queue.KindSandboxActivate, PartitionKey: partitionKey.(string), DedupeKey: dedupeKey.(string),
					PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: sandboxActivationMaxAttempts, Now: now,
				}
				activationQueueRequest = &request
			}
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET execution_state = 'waiting_activation',
			        waiting_activation_operation_id = $6,
			        waiting_materialization_operation_id = NULL,
			        authorized_binding_revision = NULL,
			        authorized_provider_resource_id = NULL,
			        preparation_deadline = NULL,
			        updated_at = $7
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND tool_use_event_id = $4
			    AND execution_attempt_generation = $5
			    AND execution_state IN ('pending', 'preparing')`,
			work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID,
			work.Ref.ToolUseEventID, work.AttemptGeneration, operationID, now,
		)
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return errors.New("sandbox execution changed before activation attachment")
		}
		var requests []queue.EnqueueRequest
		if activationQueueRequest != nil {
			requests = append(requests, *activationQueueRequest)
		}
		releaseRequests, err := readySandboxReleaseRequestsTx(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID, now, nil)
		if err != nil {
			return err
		}
		requests = append(requests, releaseRequests...)
		if _, err := queue.EnqueueBatchTx(ctx, tx, requests); err != nil {
			return err
		}
		_ = resourceRevision
		_ = providerArtifactRef
		return nil
	})
}

func (c *PostgreSQLSandboxExecutionCoordinator) WaitForMaterialization(ctx context.Context, work SandboxExecutionWork) error {
	if work.Binding == nil {
		return errors.New("sandbox materialization requires a binding")
	}
	return c.client.WithWorkspaceTx(ctx, work.Ref.WorkspaceID, "sandbox.execution.wait_materialization", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		now := c.now()
		if err := lockSession(ctx, tx, work.Ref); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID)
		if err != nil {
			return err
		}
		if !exists || binding.BindingRevision != work.Binding.BindingRevision || binding.ProviderResourceID != work.Binding.ProviderResourceID {
			return errSandboxExecutionReinspection
		}
		if err := lockSandboxLifecycleOperationsForSession(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID); err != nil {
			return err
		}
		if binding.ReleaseRequested() {
			return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "session_deleted", SafeMessage: "sandbox execution is no longer available",
			}, now)
		}
		var resourceRevision int64
		if err := tx.QueryRow(ctx,
			`SELECT sandbox_resource_revision FROM sessions WHERE workspace_id = $1 AND id = $2`,
			work.Ref.WorkspaceID, work.Ref.SessionID,
		).Scan(&resourceRevision); err != nil {
			return err
		}
		var operationID string
		var operationRevision, operationEnvironmentGeneration, operationResourceRevision int64
		var operationHandle string
		var materializationQueueRequest *queue.EnqueueRequest
		err = tx.QueryRow(ctx,
			`SELECT operation_id, observed_binding_revision, target_environment_generation,
			        target_resource_revision, target_provider_resource_id
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND logical_sandbox_id = $2
			    AND kind = 'materialize'
			    AND state IN ('pending', 'waiting_activation', 'running')
			  FOR UPDATE`,
			work.Ref.WorkspaceID, binding.LogicalSandboxID,
		).Scan(&operationID, &operationRevision, &operationEnvironmentGeneration,
			&operationResourceRevision, &operationHandle)
		if err != nil && !dbconnect.IsNoRows(err) {
			return err
		}
		if err == nil && (operationRevision != binding.BindingRevision ||
			operationEnvironmentGeneration != binding.EnvironmentGeneration ||
			operationResourceRevision > resourceRevision || operationHandle != binding.ProviderResourceID) {
			return errSandboxExecutionReinspection
		}
		if dbconnect.IsNoRows(err) {
			resources, err := c.resources.ListSessionResourcesTx(ctx, tx, workspace.ID(work.Ref.WorkspaceID), work.Ref.SessionID)
			if err != nil {
				return err
			}
			resourcesJSON, err := encodeMaterializationResources(resources)
			if err != nil {
				return err
			}
			operationID = id.New(sandboxLifecycleOperationIDPrefix)
			queueJobID := queue.NewJobID()
			partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(work.Ref.WorkspaceID), binding.LogicalSandboxID)
			dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxMaterialize, workspace.ID(work.Ref.WorkspaceID), binding.LogicalSandboxID, operationID)
			if _, err := tx.Exec(ctx,
				`INSERT INTO sandbox_lifecycle_operations (
					workspace_id, operation_id, session_id, logical_sandbox_id,
					kind, state, observed_binding_revision, target_environment_generation, target_resource_revision,
				target_provider_resource_id, materialization_resources_json, queue_job_id, queue_kind,
				queue_partition_key, queue_dedupe_key, created_at, updated_at
			) VALUES ($1, $2, $3, $4, 'materialize', 'pending', $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)`,
				work.Ref.WorkspaceID, operationID, work.Ref.SessionID, binding.LogicalSandboxID,
				binding.BindingRevision, binding.EnvironmentGeneration, resourceRevision, binding.ProviderResourceID,
				resourcesJSON, queueJobID, queue.KindSandboxMaterialize, partitionKey, dedupeKey, now,
			); err != nil {
				return err
			}
			payload, err := sandboxLifecycleQueuePayload(work.Ref, binding.LogicalSandboxID, operationID)
			if err != nil {
				return err
			}
			request := queue.EnqueueRequest{
				ID: queueJobID, WorkspaceID: workspace.ID(work.Ref.WorkspaceID), Kind: queue.KindSandboxMaterialize,
				PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1, PayloadJSON: payload,
				MaxAttempts: sandboxMaterializationMaxAttempts, Now: now,
			}
			materializationQueueRequest = &request
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET execution_state = 'waiting_materialization',
			        waiting_materialization_operation_id = $6,
			        waiting_activation_operation_id = NULL,
			        updated_at = $7
			  WHERE workspace_id = $1 AND session_id = $2
			    AND session_thread_id = $3 AND tool_use_event_id = $4
			    AND execution_attempt_generation = $5 AND execution_state = 'pending'`,
			work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID,
			work.Ref.ToolUseEventID, work.AttemptGeneration, operationID, now,
		)
		if err != nil {
			return err
		}
		if !transitionRowsAffected(result) {
			return errors.New("sandbox execution changed before materialization attachment")
		}
		var requests []queue.EnqueueRequest
		if materializationQueueRequest != nil {
			requests = append(requests, *materializationQueueRequest)
		}
		releaseRequests, err := readySandboxReleaseRequestsTx(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID, now, nil)
		if err != nil {
			return err
		}
		requests = append(requests, releaseRequests...)
		if _, err := queue.EnqueueBatchTx(ctx, tx, requests); err != nil {
			return err
		}
		return nil
	})
}

func (c *PostgreSQLSandboxExecutionCoordinator) BeginPreparing(ctx context.Context, work SandboxExecutionWork, deadline time.Time) (bool, error) {
	if work.Binding == nil || deadline.IsZero() {
		return false, errors.New("sandbox execution preparation requires a binding and deadline")
	}
	return c.transitionExecutionAuthorization(ctx, work, "pending", "preparing", deadline)
}

func (c *PostgreSQLSandboxExecutionCoordinator) AuthorizeRunning(ctx context.Context, work SandboxExecutionWork) (bool, error) {
	if work.Binding == nil {
		return false, errors.New("sandbox execution authorization requires a binding")
	}
	return c.transitionExecutionAuthorization(ctx, work, "preparing", "running", time.Time{})
}

func (c *PostgreSQLSandboxExecutionCoordinator) RecordProviderCommandReference(ctx context.Context, work SandboxExecutionWork, encodedReference string) (bool, error) {
	reference, err := decodeSandboxToolObservationReference(encodedReference)
	if err != nil {
		return false, err
	}
	var recorded bool
	err = c.client.WithWorkspaceTx(ctx, work.Ref.WorkspaceID, "sandbox.execution.record_command_reference", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockSession(ctx, tx, work.Ref); err != nil {
			return err
		}
		var state string
		var generation int64
		var authorizedProviderResourceID sql.NullString
		var existingReference sql.NullString
		if err := tx.QueryRow(ctx,
			`SELECT execution_state, execution_attempt_generation,
			        authorized_provider_resource_id, provider_command_reference_json
			   FROM session_runtime_tool_results
			  WHERE workspace_id = $1 AND session_id = $2
			    AND session_thread_id = $3 AND tool_use_event_id = $4
			  FOR UPDATE`,
			work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID,
		).Scan(&state, &generation, &authorizedProviderResourceID, &existingReference); err != nil {
			return err
		}
		if state != "running" || generation != work.AttemptGeneration {
			return nil
		}
		if !authorizedProviderResourceID.Valid ||
			reference.Observation.Reference.Target.ProviderSandboxID != authorizedProviderResourceID.String {
			return errors.New("sandbox command reference does not match the authorized provider resource")
		}
		if existingReference.Valid {
			existing, err := decodeSandboxToolObservationReference(existingReference.String)
			if err != nil {
				return err
			}
			if existing.Provider != reference.Provider ||
				existing.Observation.Reference.Target.ProviderSandboxID != reference.Observation.Reference.Target.ProviderSandboxID ||
				existing.Observation.Reference.Task.TaskID != reference.Observation.Reference.Task.TaskID ||
				existing.Observation.Reference.Task.ProviderCommandID != reference.Observation.Reference.Task.ProviderCommandID {
				return errors.New("sandbox command reference identity changed during observation")
			}
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET provider_command_reference_json = $5, updated_at = $6
			  WHERE workspace_id = $1 AND session_id = $2
			    AND session_thread_id = $3 AND tool_use_event_id = $4
			    AND execution_state = 'running' AND execution_attempt_generation = $7`,
			work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID,
			encodedReference, c.now(), work.AttemptGeneration,
		)
		if err != nil {
			return err
		}
		recorded = transitionRowsAffected(result)
		return nil
	})
	return recorded, err
}

func (c *PostgreSQLSandboxExecutionCoordinator) transitionExecutionAuthorization(ctx context.Context, work SandboxExecutionWork, from string, to string, deadline time.Time) (bool, error) {
	var changed bool
	var reinspect bool
	err := c.client.WithWorkspaceTx(ctx, work.Ref.WorkspaceID, "sandbox.execution."+to, func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		var resourceRevision int64
		var databaseNow time.Time
		if err := tx.QueryRow(ctx,
			`SELECT sandbox_resource_revision, CURRENT_TIMESTAMP
			   FROM sessions
			  WHERE workspace_id = $1 AND id = $2
			  FOR UPDATE`,
			work.Ref.WorkspaceID, work.Ref.SessionID,
		).Scan(&resourceRevision, &databaseNow); err != nil {
			return err
		}
		binding, exists, err := lockSandboxBinding(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID)
		if err != nil {
			return err
		}
		if err := lockSandboxLifecycleOperationsForSession(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID); err != nil {
			return err
		}
		defer wakeReadySandboxReleasesOnSuccess(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID, databaseNow, &txErr)
		var cancelRequestedAt, storedPreparationDeadline sql.NullTime
		var storedAuthorizedRevision sql.NullInt64
		var storedAuthorizedProviderResourceID sql.NullString
		var state string
		var attemptGeneration int64
		if err := tx.QueryRow(ctx,
			`SELECT execution_state, execution_attempt_generation, cancel_requested_at, preparation_deadline,
			        authorized_binding_revision, authorized_provider_resource_id
			   FROM session_runtime_tool_results
			  WHERE workspace_id = $1 AND session_id = $2
			    AND session_thread_id = $3 AND tool_use_event_id = $4
			  FOR UPDATE`,
			work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID,
		).Scan(&state, &attemptGeneration, &cancelRequestedAt, &storedPreparationDeadline,
			&storedAuthorizedRevision, &storedAuthorizedProviderResourceID); err != nil {
			return err
		}
		if attemptGeneration != work.AttemptGeneration {
			return nil
		}
		databaseNow = databaseNow.UTC()
		if cancelRequestedAt.Valid {
			return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "cancelled", SafeMessage: "sandbox execution was cancelled",
			}, databaseNow)
		}
		if exists && binding.ReleaseRequested() {
			return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "session_deleted", SafeMessage: "sandbox execution is no longer available",
			}, databaseNow)
		}
		if state == "preparing" && (!storedPreparationDeadline.Valid || !databaseNow.Before(storedPreparationDeadline.Time)) {
			return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, SandboxExecutionSettlement{
				Kind: SandboxExecutionFailed, ErrorKind: "sandbox_execution_unavailable",
				SafeMessage: "sandbox execution could not be started",
			}, databaseNow)
		}
		gatesCurrent := exists && binding.BindingRevision == work.Binding.BindingRevision &&
			binding.ProviderResourceID == work.Binding.ProviderResourceID &&
			binding.MaterializedResourceRevision == resourceRevision &&
			binding.ResourceCredentialExpiresAt != nil &&
			binding.ResourceCredentialExpiresAt.After(databaseNow.Add(c.credentialRefreshMargin)) &&
			binding.HelperVerifiedAt != nil
		if !gatesCurrent {
			if state == "preparing" {
				if _, err := tx.Exec(ctx,
					`UPDATE session_runtime_tool_results
					    SET execution_state = 'pending', authorized_binding_revision = NULL,
					        authorized_provider_resource_id = NULL, preparation_deadline = NULL,
					        updated_at = $5
					  WHERE workspace_id = $1 AND session_id = $2
					    AND session_thread_id = $3 AND tool_use_event_id = $4
					    AND execution_state = 'preparing'`,
					work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID,
					work.Ref.ToolUseEventID, databaseNow,
				); err != nil {
					return err
				}
			}
			reinspect = true
			return nil
		}
		if to == "preparing" && state == "preparing" {
			if !storedAuthorizedRevision.Valid || storedAuthorizedRevision.Int64 != binding.BindingRevision ||
				!storedAuthorizedProviderResourceID.Valid || storedAuthorizedProviderResourceID.String != binding.ProviderResourceID {
				reinspect = true
				return nil
			}
			changed = true
			return nil
		}
		if state != from {
			return nil
		}
		if to == "preparing" && !deadline.After(databaseNow) {
			return errors.New("sandbox execution preparation deadline must be in the future")
		}
		if from == "preparing" && (!storedAuthorizedRevision.Valid || storedAuthorizedRevision.Int64 != binding.BindingRevision ||
			!storedAuthorizedProviderResourceID.Valid || storedAuthorizedProviderResourceID.String != binding.ProviderResourceID) {
			reinspect = true
			return nil
		}
		var result sql.Result
		if to == "preparing" {
			result, err = tx.Exec(ctx,
				`UPDATE session_runtime_tool_results
				    SET execution_state = 'preparing',
				        authorized_binding_revision = $5,
				        authorized_provider_resource_id = $6,
				        preparation_deadline = $7,
				        updated_at = $8
				  WHERE workspace_id = $1 AND session_id = $2
				    AND session_thread_id = $3 AND tool_use_event_id = $4
				    AND execution_state = 'pending'`,
				work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID,
				binding.BindingRevision, binding.ProviderResourceID, deadline.UTC(), databaseNow,
			)
		} else {
			result, err = tx.Exec(ctx,
				`UPDATE session_runtime_tool_results
				    SET execution_state = 'running', updated_at = $5
				  WHERE workspace_id = $1 AND session_id = $2
				    AND session_thread_id = $3 AND tool_use_event_id = $4
				    AND execution_state = 'preparing'`,
				work.Ref.WorkspaceID, work.Ref.SessionID, work.Ref.SessionThreadID, work.Ref.ToolUseEventID, databaseNow,
			)
		}
		if err != nil {
			return err
		}
		changed = transitionRowsAffected(result)
		return nil
	})
	if err == nil && reinspect {
		return false, errSandboxExecutionReinspection
	}
	return changed, err
}

func (c *PostgreSQLSandboxExecutionCoordinator) SettleExecution(ctx context.Context, work SandboxExecutionWork, settlement SandboxExecutionSettlement) error {
	if settlement.BackgroundTask != nil {
		if work.Binding == nil {
			return errors.New("background sandbox execution requires a binding")
		}
		settlement.LogicalSandboxID = work.Binding.LogicalSandboxID
		settlement.Provider = work.Binding.Provider
		settlement.BindingRevision = work.Binding.BindingRevision
		settlement.ResourceRootsJSON = work.Binding.ResourceRootsJSON
	}
	return c.client.WithWorkspaceTx(ctx, work.Ref.WorkspaceID, "sandbox.execution.settle", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockSession(ctx, tx, work.Ref); err != nil {
			return err
		}
		if _, _, err := lockSandboxBinding(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID); err != nil {
			return err
		}
		if err := lockSandboxLifecycleOperationsForSession(ctx, tx, work.Ref.WorkspaceID, work.Ref.SessionID); err != nil {
			return err
		}
		return settleSandboxExecutionTx(ctx, tx, work.Ref, work.AttemptGeneration, settlement, c.now())
	})
}

func lockSandboxLifecycleOperationsForSession(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	rows, err := tx.Query(ctx,
		`SELECT operation_id
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY operation_id
		  FOR UPDATE`,
		workspaceID, sessionID,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *PostgreSQLSandboxExecutionCoordinator) FinalizeExhaustedExecution(ctx context.Context, job *queuev1.QueueJob) error {
	return c.finalizeClosedExecution(ctx, job, "sandbox.execution.exhausted")
}

func (c *PostgreSQLSandboxExecutionCoordinator) FinalizeInvalidExecution(ctx context.Context, job *queuev1.QueueJob) error {
	return c.finalizeClosedExecution(ctx, job, "sandbox.execution.invalid_queue_payload")
}

func (c *PostgreSQLSandboxExecutionCoordinator) finalizeClosedExecution(ctx context.Context, job *queuev1.QueueJob, operation string) error {
	decoded, err := decodeSandboxExecutionQueueTransportIdentity(
		job.GetId(), job.GetWorkspaceId(), job.GetPartitionKey(), job.GetDedupeKey(),
	)
	if err != nil {
		return err
	}
	return c.client.WithWorkspaceTx(ctx, decoded.Ref.WorkspaceID, operation, func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		return finalizeExhaustedSandboxExecutionTx(ctx, tx, decoded, c.now())
	})
}

func decodeSandboxExecutionQueueTransportIdentity(jobID string, workspaceID string, partitionKey string, dedupeKey string) (SandboxExecutionJob, error) {
	prefix := "sandbox-execution:" + workspaceID + ":"
	if jobID == "" || workspaceID == "" || !strings.HasPrefix(partitionKey, prefix) {
		return SandboxExecutionJob{}, errors.New("sandbox execution Queue transport identity is invalid")
	}
	parts := strings.Split(strings.TrimPrefix(partitionKey, prefix), ":")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return SandboxExecutionJob{}, errors.New("sandbox execution Queue partition identity is invalid")
	}
	separator := strings.LastIndexByte(dedupeKey, ':')
	if separator < 0 {
		return SandboxExecutionJob{}, errors.New("sandbox execution Queue generation is missing")
	}
	generation, err := strconv.ParseInt(dedupeKey[separator+1:], 10, 64)
	if err != nil || generation <= 0 || dedupeKey != queue.FormatSandboxToolExecuteDedupeKey(
		workspace.ID(workspaceID), parts[0], parts[1], parts[2], generation,
	) {
		return SandboxExecutionJob{}, errors.New("sandbox execution Queue dedupe identity is invalid")
	}
	return SandboxExecutionJob{
		JobID: jobID,
		Ref: SandboxExecutionRef{
			WorkspaceID: workspaceID, SessionID: parts[0], SessionThreadID: parts[1], ToolUseEventID: parts[2],
		},
		AttemptGeneration: generation,
	}, nil
}

func finalizeExhaustedSandboxExecutionTx(ctx context.Context, tx *dbconnect.Tx, job SandboxExecutionJob, now time.Time) error {
	if err := lockSession(ctx, tx, job.Ref); err != nil {
		return err
	}
	binding, bindingExists, err := lockSandboxBinding(ctx, tx, job.Ref.WorkspaceID, job.Ref.SessionID)
	if err != nil {
		return err
	}
	if err := lockSandboxLifecycleOperationsForSession(ctx, tx, job.Ref.WorkspaceID, job.Ref.SessionID); err != nil {
		return err
	}
	var state string
	var deadline, cancelRequestedAt sql.NullTime
	var commandReference sql.NullString
	err = tx.QueryRow(ctx,
		`SELECT execution_state, preparation_deadline, provider_command_reference_json, cancel_requested_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = $1 AND session_id = $2
		    AND session_thread_id = $3 AND tool_use_event_id = $4
		    AND execution_attempt_generation = $5
		  FOR UPDATE`,
		job.Ref.WorkspaceID, job.Ref.SessionID, job.Ref.SessionThreadID,
		job.Ref.ToolUseEventID, job.AttemptGeneration,
	).Scan(&state, &deadline, &commandReference, &cancelRequestedAt)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if state == "waiting_activation" || state == "waiting_materialization" || state == "terminal_unconsumed" || state == "consumed" {
		return nil
	}
	if state == "preparing" && deadline.Valid && now.Before(deadline.Time) {
		return errors.New("sandbox execution preparation deadline has not elapsed")
	}
	settlement := SandboxExecutionSettlement{
		Kind: SandboxExecutionFailed, ErrorKind: "sandbox_execution_unavailable",
		SafeMessage: "sandbox execution could not be started",
	}
	if state != "running" && cancelRequestedAt.Valid {
		settlement.ErrorKind = "cancelled"
		settlement.SafeMessage = "sandbox execution was cancelled"
	} else if state != "running" && bindingExists && binding.ReleaseRequested() {
		settlement.ErrorKind = "session_deleted"
		settlement.SafeMessage = "sandbox execution is no longer available"
	}
	if state == "running" {
		settlement.Kind = SandboxExecutionUnknownOutcome
		settlement.ErrorKind = "sandbox_execution_outcome_unknown"
		settlement.SafeMessage = "sandbox execution outcome is unknown"
		if commandReference.Valid {
			settlement.ProviderCommandReference = commandReference.String
		}
	}
	return settleSandboxExecutionTx(ctx, tx, job.Ref, job.AttemptGeneration, settlement, now)
}

func lockSession(ctx context.Context, tx *dbconnect.Tx, ref SandboxExecutionRef) error {
	var exists bool
	return tx.QueryRow(ctx,
		`SELECT TRUE FROM sessions WHERE workspace_id = $1 AND id = $2 FOR UPDATE`,
		ref.WorkspaceID, ref.SessionID,
	).Scan(&exists)
}

func lockSandboxBinding(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (SandboxBinding, bool, error) {
	var binding SandboxBinding
	var providerResourceID, releaseReason sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT logical_sandbox_id, environment_id, environment_generation,
		        provider, provider_resource_id, binding_revision,
		        materialized_resource_revision, resource_credential_expires_at,
		        resource_roots_json, helper_verified_at, provider_metadata_json,
		        release_requested_at, release_reason
		   FROM session_sandbox_bindings
		  WHERE workspace_id = $1 AND session_id = $2
		  FOR UPDATE`,
		workspaceID, sessionID,
	).Scan(
		&binding.LogicalSandboxID, &binding.EnvironmentID, &binding.EnvironmentGeneration,
		&binding.Provider, &providerResourceID, &binding.BindingRevision,
		&binding.MaterializedResourceRevision, &binding.ResourceCredentialExpiresAt,
		&binding.ResourceRootsJSON, &binding.HelperVerifiedAt, &binding.ProviderMetadataJSON,
		&binding.ReleaseRequestedAt, &releaseReason,
	)
	if dbconnect.IsNoRows(err) {
		return SandboxBinding{}, false, nil
	}
	if err != nil {
		return SandboxBinding{}, false, err
	}
	binding.ProviderResourceID = providerResourceID.String
	binding.ReleaseReason = releaseReason.String
	return binding, true, nil
}

func settleSandboxExecutionTx(ctx context.Context, tx *dbconnect.Tx, ref SandboxExecutionRef, generation int64, settlement SandboxExecutionSettlement, now time.Time) error {
	if settlement.Kind != SandboxExecutionCompleted && settlement.Kind != SandboxExecutionFailed && settlement.Kind != SandboxExecutionUnknownOutcome {
		return errors.New("sandbox execution settlement kind is invalid")
	}
	resultJSON := settlement.ResultJSON
	if settlement.Kind != SandboxExecutionCompleted && resultJSON == "" {
		encoded, err := json.Marshal(map[string]any{
			"error": map[string]string{
				"kind":    settlement.ErrorKind,
				"message": settlement.SafeMessage,
			},
			"status": "error",
		})
		if err != nil {
			return err
		}
		resultJSON = string(encoded)
	}
	if resultJSON == "" {
		return errors.New("sandbox execution settlement result is required")
	}
	digest := settlement.ResultDigest
	if digest == "" {
		sum := sha256.Sum256([]byte(resultJSON))
		digest = hex.EncodeToString(sum[:])
	}
	backgroundTaskStarted := settlement.BackgroundTask != nil
	var taskID any
	if backgroundTaskStarted {
		taskID = settlement.BackgroundTask.TaskID
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'terminal_unconsumed',
		        result_json = $6,
		        result_digest = $7,
		        provider_command_reference_json = NULLIF($8, ''),
		        background_task_started = $9,
		        task_id = $10,
		        updated_at = $11
		  WHERE workspace_id = $1 AND session_id = $2
		    AND session_thread_id = $3 AND tool_use_event_id = $4
		    AND execution_attempt_generation = $5
		    AND execution_state IN ('pending', 'preparing', 'running', 'waiting_activation', 'waiting_materialization')`,
		ref.WorkspaceID, ref.SessionID, ref.SessionThreadID, ref.ToolUseEventID,
		generation, resultJSON, digest, settlement.ProviderCommandReference,
		backgroundTaskStarted, taskID, now.UTC(),
	)
	if err != nil {
		return err
	}
	if !transitionRowsAffected(result) {
		return nil
	}
	var requests []queue.EnqueueRequest
	if settlement.BackgroundTask != nil {
		request, err := insertBackgroundTaskAndReconcileTx(ctx, tx, ref, settlement, now)
		if err != nil {
			return err
		}
		requests = append(requests, request)
	}
	releaseRequests, err := readySandboxReleaseRequestsTx(ctx, tx, ref.WorkspaceID, ref.SessionID, now, nil)
	if err != nil {
		return err
	}
	requests = append(requests, releaseRequests...)
	_, err = queue.EnqueueBatchTx(ctx, tx, requests)
	return err
}

func insertBackgroundTaskAndReconcileTx(ctx context.Context, tx *dbconnect.Tx, ref SandboxExecutionRef, settlement SandboxExecutionSettlement, now time.Time) (queue.EnqueueRequest, error) {
	task := settlement.BackgroundTask
	if task == nil || task.TaskID == "" || task.SourceToolUseEventID != ref.ToolUseEventID ||
		task.ProviderSessionID == "" || task.ProviderCommandID == "" || settlement.LogicalSandboxID == "" ||
		settlement.Provider == "" || settlement.BindingRevision <= 0 {
		return queue.EnqueueRequest{}, errors.New("sandbox background task identity is incomplete")
	}
	metadataJSON := task.ProviderCommandMetadataJSON
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	var metadata map[string]any
	if len([]byte(metadataJSON)) > 4096 || json.Unmarshal([]byte(metadataJSON), &metadata) != nil || metadata == nil {
		return queue.EnqueueRequest{}, errors.New("sandbox background task metadata is invalid")
	}
	resourceRootsJSON := settlement.ResourceRootsJSON
	if resourceRootsJSON == "" {
		resourceRootsJSON = "[]"
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_background_tasks (
			workspace_id, session_id, session_thread_id, task_id, source_tool_use_event_id,
			binding_id, sandbox_id, provider, binding_revision,
			provider_session_id, provider_command_id, provider_command_metadata_json,
			resource_roots_json, status, reconcile_generation, next_poll_at,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8, $9, $10, $11, $12,
			'running', 1, $13, $13, $13)`,
		ref.WorkspaceID, ref.SessionID, ref.SessionThreadID, task.TaskID, ref.ToolUseEventID,
		settlement.LogicalSandboxID, settlement.Provider, settlement.BindingRevision,
		task.ProviderSessionID, task.ProviderCommandID, metadataJSON, resourceRootsJSON, now.UTC(),
	); err != nil {
		return queue.EnqueueRequest{}, err
	}
	payload, err := json.Marshal(struct {
		WorkspaceID         string `json:"workspace_id"`
		SessionID           string `json:"session_id"`
		TaskID              string `json:"task_id"`
		ReconcileGeneration int64  `json:"reconcile_generation"`
	}{WorkspaceID: ref.WorkspaceID, SessionID: ref.SessionID, TaskID: task.TaskID, ReconcileGeneration: 1})
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	workspaceID := workspace.ID(ref.WorkspaceID)
	return queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxBackgroundReconcile,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(workspaceID, ref.SessionID, task.TaskID),
		DedupeKey:      queue.FormatSandboxBackgroundReconcileDedupeKey(workspaceID, ref.SessionID, task.TaskID, 1),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: SandboxBackgroundReconcileMaxAttempts,
		AvailableAt: now.UTC(), Now: now.UTC(),
	}, nil
}

func sandboxLifecycleQueuePayload(ref SandboxExecutionRef, logicalSandboxID string, operationID string) ([]byte, error) {
	return json.Marshal(struct {
		WorkspaceID      string `json:"workspace_id"`
		SessionID        string `json:"session_id"`
		LogicalSandboxID string `json:"logical_sandbox_id"`
		OperationID      string `json:"operation_id"`
	}{
		WorkspaceID: ref.WorkspaceID, SessionID: ref.SessionID,
		LogicalSandboxID: logicalSandboxID, OperationID: operationID,
	})
}

func (c *PostgreSQLSandboxExecutionCoordinator) now() time.Time {
	if c != nil && c.clock != nil {
		return c.clock().UTC()
	}
	return storage.Now()
}
