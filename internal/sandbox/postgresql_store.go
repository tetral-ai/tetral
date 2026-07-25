package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	runtimeInputIDPrefix               = "rin_"
	eventTypeUserMessage               = "user.message"
	eventTypeUserInterrupt             = "user.interrupt"
	eventTypeUserToolConfirmation      = "user.tool_confirmation"
	runtimeInputKindMessages           = "messages"
	runtimeInputKindInterruptControl   = "interrupt_control"
	runtimeInputKindToolConfirmation   = "tool_confirmation"
	runtimeInputInterruptQueuePriority = 100
)

type PostgreSQLSandboxStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLStore(client *dbconnect.Client) *PostgreSQLSandboxStore {
	return &PostgreSQLSandboxStore{client: client}
}

func (s *PostgreSQLSandboxStore) ClaimSessionPreparation(ctx context.Context, ws workspace.ID, sessionID string, preparationAttemptID string, now time.Time) (SessionPreparation, bool, error) {
	if s == nil || s.client == nil {
		return SessionPreparation{}, false, &ValidationError{Message: "session preparation store is required"}
	}
	if ws == "" {
		return SessionPreparation{}, false, &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return SessionPreparation{}, false, &ValidationError{Message: "session_id is required"}
	}
	if preparationAttemptID == "" {
		return SessionPreparation{}, false, &ValidationError{Message: "preparation_attempt_id is required"}
	}
	if now.IsZero() {
		now = storage.Now()
	}
	now = now.UTC()
	var preparation SessionPreparation
	var claimed bool
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.claim_session_preparation", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`WITH active AS (
				SELECT preparation_attempt_id
				  FROM session_preparations
					 WHERE workspace_id = $1
					   AND session_id = $2
					   AND superseded_at IS NULL
					 ORDER BY created_at DESC, preparation_attempt_id DESC
				 LIMIT 1
			)
			SELECT p.workspace_id, p.session_id, p.preparation_attempt_id, p.environment_id,
			       p.environment_generation, p.sandbox_id, p.status, p.failure_reason,
			       p.resource_cred_expires_at, COALESCE(previous.resource_roots_json, ''),
			       (p.preparation_attempt_id = active.preparation_attempt_id), p.created_at,
			       env.current_generation
			  FROM session_preparations p
			  JOIN sessions sess ON sess.workspace_id = p.workspace_id AND sess.id = p.session_id AND sess.lifecycle_state <> 'deleted'
			  JOIN environments env ON env.workspace_id = p.workspace_id AND env.id = p.environment_id AND env.archived_at IS NULL
			  JOIN active ON TRUE
			  LEFT JOIN LATERAL (
			    SELECT previous.resource_roots_json
			      FROM session_preparations previous
			     WHERE previous.workspace_id = p.workspace_id
			       AND previous.session_id = p.session_id
			       AND previous.sandbox_id = p.sandbox_id
			       AND previous.preparation_attempt_id <> p.preparation_attempt_id
			       AND previous.status = 'ready'
			       AND previous.resource_roots_json IS NOT NULL
			       AND previous.resource_roots_json <> ''
			     ORDER BY previous.superseded_at DESC NULLS LAST,
			              previous.ready_at DESC NULLS LAST,
			              previous.created_at DESC,
			              previous.preparation_attempt_id DESC
			     LIMIT 1
			  ) previous ON TRUE
			 WHERE p.workspace_id = $1
			   AND p.session_id = $2
			   AND p.preparation_attempt_id = $3
			 FOR UPDATE OF p, sess, env`,
			string(ws), sessionID, preparationAttemptID,
		)
		got, isCurrent, currentEnvironmentGeneration, err := scanSessionPreparationWithCurrency(row)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !isCurrent {
			preparation = got
			return nil
		}
		if got.Status != "pending" && got.Status != "preparing" {
			preparation = got
			return nil
		}
		artifactStatus, artifactFailure, err := loadEnvironmentArtifactForPreparationTx(ctx, tx, ws, got.EnvironmentID, currentEnvironmentGeneration, &got)
		if err != nil {
			return err
		}
		got.EnvironmentGeneration = currentEnvironmentGeneration
		switch artifactStatus {
		case "pending", "building":
			result, err := tx.Exec(ctx,
				`UPDATE session_preparations
				    SET environment_generation = $4,
				        status = 'waiting_environment',
				        failure_stage = NULL,
				        last_error_kind = NULL,
				        failure_reason = NULL,
				        retryable = NULL,
				        failed_at = NULL,
				        updated_at = $5
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND preparation_attempt_id = $3
				    AND status IN ('pending', 'preparing')
				    AND superseded_at IS NULL`,
				string(ws), sessionID, preparationAttemptID, currentEnvironmentGeneration, now,
			)
			if err != nil {
				return err
			}
			if affected, _ := result.RowsAffected(); affected == 0 {
				return nil
			}
			got.Status = "waiting_environment"
			preparation = got
			return nil
		case "failed":
			result, err := tx.Exec(ctx,
				`UPDATE session_preparations
				    SET environment_generation = $4,
				        status = 'failed',
				        failure_stage = $5,
				        last_error_kind = $6,
				        failure_reason = $7,
				        retryable = FALSE,
				        failed_at = $8,
				        updated_at = $8
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND preparation_attempt_id = $3
				    AND status IN ('pending', 'preparing')
				    AND superseded_at IS NULL`,
				string(ws), sessionID, preparationAttemptID, currentEnvironmentGeneration,
				nullableEmptyString(artifactFailure.FailureStage), nullableEmptyString(artifactFailure.LastErrorKind),
				nullableEmptyString(artifactFailure.FailureReason), now,
			)
			if err != nil {
				return err
			}
			if affected, _ := result.RowsAffected(); affected == 0 {
				return nil
			}
			got.Status = "failed"
			got.FailureReason = artifactFailure.FailureReason
			preparation = got
			segments, err := LoadPendingRuntimeInputSegments(ctx, tx, ws, sessionID)
			if err != nil {
				return err
			}
			return EnqueueRuntimeInputSegments(ctx, tx, ws, sessionID, segments, now)
		case "ready":
			if got.ProviderArtifactRef == "" {
				return errors.New("ready session preparation environment artifact has no provider ref")
			}
		default:
			return errors.New("session preparation environment artifact is unavailable")
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_preparations
			    SET environment_generation = $4,
			        status = 'preparing',
			        failure_stage = NULL,
			        last_error_kind = NULL,
			        failure_reason = NULL,
			        retryable = NULL,
			        failed_at = NULL,
			        updated_at = $5
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND preparation_attempt_id = $3
				    AND status IN ('pending', 'preparing')
				    AND superseded_at IS NULL`,
			string(ws), sessionID, preparationAttemptID, currentEnvironmentGeneration, now,
		)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return nil
		}
		got.Status = "preparing"
		preparation = got
		claimed = true
		return nil
	})
	return preparation, claimed, err
}

func (s *PostgreSQLSandboxStore) SessionPreparationStillCurrent(ctx context.Context, ws workspace.ID, sessionID string, preparationAttemptID string) (bool, error) {
	var current bool
	err := s.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "sandbox.preparation_current", func(tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT EXISTS (
			 SELECT 1 FROM sessions sess
			 JOIN session_preparations p ON p.workspace_id=sess.workspace_id AND p.session_id=sess.id
			 WHERE sess.workspace_id=$1 AND sess.id=$2 AND sess.lifecycle_state <> 'deleted'
			   AND p.preparation_attempt_id=$3 AND p.superseded_at IS NULL AND p.status='preparing'
			   AND p.preparation_attempt_id=(SELECT p2.preparation_attempt_id FROM session_preparations p2 WHERE p2.workspace_id=$1 AND p2.session_id=$2 AND p2.superseded_at IS NULL ORDER BY p2.created_at DESC,p2.preparation_attempt_id DESC LIMIT 1)
			)`, string(ws), sessionID, preparationAttemptID).Scan(&current)
	})
	return current, err
}

func (s *PostgreSQLSandboxStore) SaveProviderHandleForSessionPreparation(
	ctx context.Context,
	ws workspace.ID,
	sessionID string,
	preparationAttemptID string,
	sandboxID string,
	handle ProviderHandle,
	updatedAt time.Time,
) (*Sandbox, postCreatePreparationDisposition, error) {
	if err := ValidateProviderHandle(handle); err != nil {
		return nil, "", err
	}
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	metadataJSON, err := json.Marshal(nonNilStringMap(handle.Metadata))
	if err != nil {
		return nil, "", err
	}
	var (
		got         *Sandbox
		disposition postCreatePreparationDisposition
	)
	err = s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.save_preparation_provider_handle", func(tx *dbconnect.Tx) error {
		var (
			lifecycleState  string
			status          string
			isCurrent       bool
			lockedSandboxID string
		)
		err := tx.QueryRow(ctx,
			`SELECT sess.lifecycle_state, p.status,
			        (p.superseded_at IS NULL AND p.preparation_attempt_id = (
			           SELECT active.preparation_attempt_id
			             FROM session_preparations active
			            WHERE active.workspace_id = p.workspace_id
			              AND active.session_id = p.session_id
			              AND active.superseded_at IS NULL
			            ORDER BY active.created_at DESC, active.preparation_attempt_id DESC
			            LIMIT 1
			        )), sb.id
			   FROM sessions sess
			   JOIN session_preparations p
			     ON p.workspace_id = sess.workspace_id AND p.session_id = sess.id
			   JOIN sandboxes sb
			     ON sb.workspace_id = p.workspace_id AND sb.id = p.sandbox_id
			  WHERE sess.workspace_id = $1
			    AND sess.id = $2
			    AND p.preparation_attempt_id = $3
			  FOR UPDATE OF sess, p, sb`,
			string(ws), sessionID, preparationAttemptID,
		).Scan(&lifecycleState, &status, &isCurrent, &lockedSandboxID)
		if err != nil {
			return err
		}
		if lifecycleState == "deleted" {
			disposition = postCreatePreparationDeleted
			return nil
		}
		if !isCurrent || status != "preparing" {
			disposition = postCreatePreparationStale
			return nil
		}
		if lockedSandboxID != sandboxID {
			disposition = postCreatePreparationStale
			return nil
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET provider = $1,
			        provider_sandbox_id = $2,
			        provider_metadata_json = $3,
			        updated_at = $4
			  WHERE workspace_id = $5
			    AND id = $6
			    AND status IN ('creating', 'resuming')`,
			handle.Provider, handle.SandboxID, string(metadataJSON), updatedAt.UTC(), string(ws), sandboxID,
		)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			disposition = postCreatePreparationStale
			return nil
		}
		got, err = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		if err != nil {
			return err
		}
		disposition = postCreatePreparationCurrent
		return nil
	})
	return got, disposition, err
}

func (s *PostgreSQLSandboxStore) SettleDeletedSessionPreparationAfterCreate(
	ctx context.Context,
	ws workspace.ID,
	sessionID string,
	preparationAttemptID string,
	sandboxID string,
	failedAt time.Time,
) error {
	if failedAt.IsZero() {
		failedAt = storage.Now()
	}
	failedAt = failedAt.UTC()
	return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.settle_deleted_preparation_after_create", func(tx *dbconnect.Tx) error {
		var lifecycleState string
		if err := tx.QueryRow(ctx,
			`SELECT lifecycle_state FROM sessions
			  WHERE workspace_id = $1 AND id = $2
			  FOR UPDATE`,
			string(ws), sessionID,
		).Scan(&lifecycleState); err != nil {
			return err
		}
		if lifecycleState != "deleted" {
			return ErrStalePreparationAttempt
		}
		preparationResult, err := tx.Exec(ctx,
			`UPDATE session_preparations
			    SET status = 'failed',
			        failure_stage = 'sandbox_readiness',
			        last_error_kind = $4,
			        failure_reason = $4,
			        retryable = FALSE,
			        failed_at = $5,
			        ready_at = NULL,
			        updated_at = $5
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND preparation_attempt_id = $3
			    AND status IN ('preparing', 'failed')
			    AND superseded_at IS NULL`,
			string(ws), sessionID, preparationAttemptID, sessionDeletedPreparationFailureReason, failedAt,
		)
		if err != nil {
			return err
		}
		if affected, _ := preparationResult.RowsAffected(); affected != 1 {
			return ErrStalePreparationAttempt
		}
		sandboxResult, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = 'failed',
			        failed_at = $4,
			        startup_failure_reason = $5,
			        cleanup_status = 'released',
			        cleanup_method = 'delete',
			        cleanup_error_kind = NULL,
			        cleanup_retryable = FALSE,
			        cleanup_last_attempt_at = $4,
			        cleanup_next_attempt_at = NULL,
			        cleanup_attempt_count = GREATEST(cleanup_attempt_count, 1),
			        cleanup_lease_token = NULL,
			        cleanup_lease_expires_at = NULL,
			        status_refreshed_at = NULL,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND id = $2
			    AND session_id = $3
			    AND status IN ('creating', 'resuming', 'failed')`,
			string(ws), sandboxID, sessionID, failedAt, sessionDeletedPreparationFailureReason,
		)
		if err != nil {
			return err
		}
		if affected, _ := sandboxResult.RowsAffected(); affected != 1 {
			return ErrStalePreparationAttempt
		}
		return nil
	})
}

func (s *PostgreSQLSandboxStore) ListSessionPreparationResources(ctx context.Context, ws workspace.ID, sessionID string) (ResourceSetup, error) {
	if s == nil || s.client == nil {
		return ResourceSetup{}, &ValidationError{Message: "session preparation store is required"}
	}
	var setup ResourceSetup
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.list_session_preparation_resources", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT sr.resource_id, sr.type, sr.detached_at, sr.delete_requested_at,
			        sfr.source_file_id, source_file.object_id, sfr.file_id, sfr.mount_path,
			        smr.memory_store_id, smr.access, smr.instructions, smr.name, smr.description, smr.mount_path,
			        sgr.url, sgr.mount_path, sgr.checkout_type, sgr.checkout_ref
			   FROM session_resources sr
			   LEFT JOIN session_file_resources sfr
			     ON sfr.workspace_id = sr.workspace_id
			    AND sfr.session_id = sr.session_id
			    AND sfr.resource_id = sr.resource_id
			   LEFT JOIN files source_file
			     ON source_file.workspace_id = sfr.workspace_id
			    AND source_file.file_id = sfr.source_file_id
			   LEFT JOIN session_memory_store_resources smr
			     ON smr.workspace_id = sr.workspace_id
			    AND smr.session_id = sr.session_id
			    AND smr.resource_id = sr.resource_id
			   LEFT JOIN session_github_repository_resources sgr
			     ON sgr.workspace_id = sr.workspace_id
			    AND sgr.session_id = sr.session_id
			    AND sgr.resource_id = sr.resource_id
			  WHERE sr.workspace_id = $1
			    AND sr.session_id = $2
			  ORDER BY sr.storage_sequence ASC`,
			string(ws), sessionID,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var (
				resourceID         string
				resourceType       string
				detachedAt         sql.NullTime
				deleteRequestedAt  sql.NullTime
				sourceFileID       sql.NullString
				objectID           sql.NullString
				sessionFileID      sql.NullString
				fileMountPath      sql.NullString
				memoryStoreID      sql.NullString
				memoryAccess       sql.NullString
				memoryInstructions sql.NullString
				memoryName         sql.NullString
				memoryDescription  sql.NullString
				memoryMountPath    sql.NullString
				githubURL          sql.NullString
				githubMountPath    sql.NullString
				checkoutType       sql.NullString
				checkoutRef        sql.NullString
			)
			if err := rows.Scan(
				&resourceID, &resourceType, &detachedAt, &deleteRequestedAt,
				&sourceFileID, &objectID, &sessionFileID, &fileMountPath,
				&memoryStoreID, &memoryAccess, &memoryInstructions, &memoryName, &memoryDescription, &memoryMountPath,
				&githubURL, &githubMountPath, &checkoutType, &checkoutRef,
			); err != nil {
				return err
			}
			switch resourceType {
			case "file":
				mount := FileMount{
					ResourceID:    resourceID,
					SourceFileID:  nullableStringValue(sourceFileID),
					SessionFileID: nullableStringValue(sessionFileID),
					ObjectID:      nullableStringValue(objectID),
					MountPath:     nullableStringValue(fileMountPath),
					ReadOnly:      true,
				}
				if deleteRequestedAt.Valid && !detachedAt.Valid {
					setup.DeletedFiles = append(setup.DeletedFiles, mount)
				} else if detachedAt.Valid {
					continue
				} else {
					setup.Files = append(setup.Files, mount)
				}
			case "memory_store":
				mount := MemoryStoreMount{
					ResourceID:    resourceID,
					MemoryStoreID: nullableStringValue(memoryStoreID),
					MountPath:     nullableStringValue(memoryMountPath),
					Access:        nullableStringValue(memoryAccess),
					Instructions:  nullableStringValue(memoryInstructions),
					Name:          nullableStringValue(memoryName),
					Description:   nullableStringValue(memoryDescription),
				}
				if deleteRequestedAt.Valid && !detachedAt.Valid {
					setup.DeletedMemoryStores = append(setup.DeletedMemoryStores, mount)
				} else if detachedAt.Valid {
					continue
				} else {
					setup.MemoryStores = append(setup.MemoryStores, mount)
				}
			case "github_repository":
				mount := GitHubRepositoryMount{
					ResourceID:   resourceID,
					URL:          nullableStringValue(githubURL),
					MountPath:    nullableStringValue(githubMountPath),
					CheckoutType: nullableStringValue(checkoutType),
					CheckoutRef:  nullableStringValue(checkoutRef),
				}
				if deleteRequestedAt.Valid && !detachedAt.Valid {
					setup.DeletedGitHubRepositories = append(setup.DeletedGitHubRepositories, mount)
				} else if detachedAt.Valid {
					continue
				} else {
					setup.GitHubRepositories = append(setup.GitHubRepositories, mount)
				}
			default:
				return &ValidationError{Message: "session resource type is invalid"}
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		skills, err := listSessionPreparationSkills(ctx, tx, ws, sessionID)
		if err != nil {
			return err
		}
		if err := persistSessionPreparationSkillsIndex(ctx, tx, ws, sessionID, skills); err != nil {
			return err
		}
		setup.Skills = skills
		return nil
	})
	return setup, err
}

type sessionPreparationSkillRef struct {
	SkillID string `json:"skill_id"`
	Version string `json:"version"`
}

func listSessionPreparationSkills(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) ([]SkillMount, error) {
	var configJSON string
	err := tx.QueryRow(ctx,
		`SELECT av.config_json
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id
		  WHERE s.workspace_id = $1
		    AND s.id = $2`,
		string(ws), sessionID,
	).Scan(&configJSON)
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "session not found"}
	}
	if err != nil {
		return nil, err
	}
	var config struct {
		Skills []sessionPreparationSkillRef `json:"skills"`
	}
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, &ValidationError{Message: "session agent config is invalid"}
	}
	if len(config.Skills) == 0 {
		return nil, nil
	}
	out := make([]SkillMount, 0, len(config.Skills))
	for _, ref := range config.Skills {
		if ref.SkillID == "" || ref.Version == "" {
			return nil, &ValidationError{Message: "session agent skill reference is invalid"}
		}
		versionExpr := `CASE WHEN $3 = 'latest' THEN s.latest_version ELSE $3 END`
		var mount SkillMount
		err := tx.QueryRow(ctx,
			`SELECT v.skill_id, v.skill_version_id, v.version, v.name, v.description, v.directory, v.blob_key, v.sha256, v.size_bytes
			   FROM skills s
			   JOIN skill_versions v
			     ON v.workspace_id = s.workspace_id
			    AND v.skill_id = s.skill_id
			    AND v.version = `+versionExpr+`
			    AND v.deleted_at IS NULL
			  WHERE s.workspace_id = $1
			    AND s.skill_id = $2
			    AND s.deleted_at IS NULL`,
			string(ws), ref.SkillID, ref.Version,
		).Scan(&mount.SkillID, &mount.SkillVersionID, &mount.Version, &mount.Name, &mount.Description, &mount.Directory, &mount.BlobKey, &mount.SHA256, &mount.SizeBytes)
		if dbconnect.IsNoRows(err) {
			return nil, &ValidationError{Message: "session agent skill reference is not active"}
		}
		if err != nil {
			return nil, err
		}
		out = append(out, mount)
	}
	sort.Slice(out, func(i int, j int) bool {
		if out[i].SkillID != out[j].SkillID {
			return out[i].SkillID < out[j].SkillID
		}
		return out[i].SkillVersionID < out[j].SkillVersionID
	})
	return out, nil
}

type sessionPreparationSkillIndexEntry struct {
	SkillID        string `json:"skill_id"`
	SkillVersionID string `json:"skill_version_id"`
	Version        string `json:"version"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	Directory      string `json:"directory"`
}

func persistSessionPreparationSkillsIndex(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, skills []SkillMount) error {
	index := make([]sessionPreparationSkillIndexEntry, 0, len(skills))
	for _, skill := range skills {
		index = append(index, sessionPreparationSkillIndexEntry{
			SkillID:        skill.SkillID,
			SkillVersionID: skill.SkillVersionID,
			Version:        skill.Version,
			Name:           skill.Name,
			Description:    skill.Description,
			Directory:      skill.Directory,
		})
	}
	encoded, err := json.Marshal(index)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_preparations
		    SET skills_index_json = $3
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND status = 'preparing'
		    AND superseded_at IS NULL`,
		string(ws), sessionID, string(encoded),
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return ErrStalePreparationAttempt
	}
	return nil
}

func (s *PostgreSQLSandboxStore) CheckSessionPreparationResourceCleanup(ctx context.Context, ws workspace.ID, sessionID string, preparationAttemptID string, resourceID string) (bool, error) {
	if s == nil || s.client == nil {
		return false, &ValidationError{Message: "session preparation store is required"}
	}
	if ws == "" {
		return false, &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return false, &ValidationError{Message: "session_id is required"}
	}
	if preparationAttemptID == "" {
		return false, &ValidationError{Message: "preparation_attempt_id is required"}
	}
	if resourceID == "" {
		return false, &ValidationError{Message: "resource_id is required"}
	}
	var pending bool
	var staleAttempt bool
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.check_session_preparation_resource_cleanup", func(tx *dbconnect.Tx) error {
		current, err := currentSessionPreparationAttempt(ctx, tx, ws, sessionID, preparationAttemptID)
		if err != nil {
			return err
		}
		if !current {
			staleAttempt = true
			return nil
		}
		return tx.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1
				  FROM session_resources
				 WHERE workspace_id = $1
				   AND session_id = $2
				   AND resource_id = $3
				   AND type IN ('file', 'memory_store', 'github_repository')
				   AND delete_requested_at IS NOT NULL
				   AND detached_at IS NULL
			)`,
			string(ws), sessionID, resourceID,
		).Scan(&pending)
	})
	if err != nil {
		return false, err
	}
	if staleAttempt {
		return false, ErrStalePreparationAttempt
	}
	return pending, nil
}

func (s *PostgreSQLSandboxStore) DetachSessionPreparationResource(ctx context.Context, ws workspace.ID, sessionID string, preparationAttemptID string, resourceID string, detachedAt time.Time) error {
	if s == nil || s.client == nil {
		return &ValidationError{Message: "session preparation store is required"}
	}
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if preparationAttemptID == "" {
		return &ValidationError{Message: "preparation_attempt_id is required"}
	}
	if resourceID == "" {
		return &ValidationError{Message: "resource_id is required"}
	}
	if detachedAt.IsZero() {
		detachedAt = storage.Now()
	}
	detachedAt = detachedAt.UTC()
	staleAttempt := false
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.detach_session_preparation_resource", func(tx *dbconnect.Tx) error {
		current, err := currentSessionPreparationAttempt(ctx, tx, ws, sessionID, preparationAttemptID)
		if err != nil {
			return err
		}
		if !current {
			staleAttempt = true
			return nil
		}
		if err := storage.AcquireWorkspaceFilesLock(ctx, tx, string(ws)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE files f
			    SET deleted_at = COALESCE(f.deleted_at, $4)
			   FROM session_resources sr
			   JOIN session_file_resources sfr
			     ON sfr.workspace_id = sr.workspace_id
			    AND sfr.session_id = sr.session_id
			    AND sfr.resource_id = sr.resource_id
			  WHERE sr.workspace_id = $1
			    AND sr.session_id = $2
			    AND sr.resource_id = $3
			    AND sr.type = 'file'
			    AND sr.delete_requested_at IS NOT NULL
			    AND sr.detached_at IS NULL
			    AND sfr.file_id = f.file_id
			    AND f.workspace_id = sr.workspace_id
			    AND f.scope_type = 'session'
			    AND f.scope_id = sr.session_id`,
			string(ws), sessionID, resourceID, detachedAt,
		); err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE session_resources
			    SET detached_at = $4,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND resource_id = $3
			    AND type IN ('file', 'memory_store', 'github_repository')
			    AND delete_requested_at IS NOT NULL
			    AND detached_at IS NULL`,
			string(ws), sessionID, resourceID, detachedAt,
		)
		return err
	})
	if err != nil {
		return err
	}
	if staleAttempt {
		return ErrStalePreparationAttempt
	}
	return nil
}

func currentSessionPreparationAttempt(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, preparationAttemptID string) (bool, error) {
	var current bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_preparations p
			 WHERE p.workspace_id = $1
			   AND p.session_id = $2
			   AND p.preparation_attempt_id = $3
			   AND p.status = 'preparing'
			   AND p.superseded_at IS NULL
			   AND p.preparation_attempt_id = (
			       SELECT active.preparation_attempt_id
			         FROM session_preparations active
			        WHERE active.workspace_id = $1
			          AND active.session_id = $2
			          AND active.superseded_at IS NULL
			        ORDER BY active.created_at DESC, active.preparation_attempt_id DESC
			        LIMIT 1
			   )
		)`,
		string(ws), sessionID, preparationAttemptID,
	).Scan(&current)
	return current, err
}

func (s *PostgreSQLSandboxStore) MarkSessionPreparationReady(ctx context.Context, ws workspace.ID, sessionID string, preparationAttemptID string, update SessionPreparationReadyUpdate) error {
	if s == nil || s.client == nil {
		return &ValidationError{Message: "session preparation store is required"}
	}
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if preparationAttemptID == "" {
		return &ValidationError{Message: "preparation_attempt_id is required"}
	}
	if update.ReadyAt.IsZero() {
		update.ReadyAt = storage.Now()
	}
	update.ReadyAt = update.ReadyAt.UTC()
	var updated bool
	var staleAttempt bool
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_session_preparation_ready", func(tx *dbconnect.Tx) error {
		if err := lockSessionReadyFanoutFence(ctx, tx, ws, sessionID); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_preparations
			    SET status = 'ready',
			        resource_cred_expires_at = $5,
			        resource_roots_json = $6,
			        failure_stage = NULL,
			        last_error_kind = NULL,
			        failure_reason = NULL,
			        retryable = NULL,
			        failed_at = NULL,
			        ready_at = $4,
			        updated_at = $4
			  WHERE workspace_id = $1
				    AND session_id = $2
				    AND preparation_attempt_id = $3
				    AND status = 'preparing'
				    AND superseded_at IS NULL
				    AND preparation_attempt_id = (
				        SELECT active.preparation_attempt_id
				          FROM session_preparations active
				         WHERE active.workspace_id = $1
				           AND active.session_id = $2
				           AND active.superseded_at IS NULL
				         ORDER BY active.created_at DESC, active.preparation_attempt_id DESC
				         LIMIT 1
			    )`,
			string(ws),
			sessionID,
			preparationAttemptID,
			update.ReadyAt,
			update.ResourceCredExpiresAt,
			nullableEmptyString(update.ResourceRootsJSON),
		)
		if err != nil {
			return err
		}
		updated = rowsAffected(result)
		if !updated {
			var err error
			staleAttempt, err = sessionPreparationAttemptStaleTx(ctx, tx, ws, sessionID, preparationAttemptID)
			return err
		}
		segments, err := LoadPendingRuntimeInputSegments(ctx, tx, ws, sessionID)
		if err != nil {
			return err
		}
		return EnqueueRuntimeInputSegments(ctx, tx, ws, sessionID, segments, update.ReadyAt)
	})
	if err == nil && !updated {
		if staleAttempt {
			return ErrStalePreparationAttempt
		}
		return &NotFoundError{Message: "preparing session preparation not found"}
	}
	return err
}

func (s *PostgreSQLSandboxStore) MarkSessionPreparationFailed(ctx context.Context, ws workspace.ID, sessionID string, preparationAttemptID string, update SessionPreparationFailureUpdate) error {
	if s == nil || s.client == nil {
		return &ValidationError{Message: "session preparation store is required"}
	}
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if sessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if preparationAttemptID == "" {
		return &ValidationError{Message: "preparation_attempt_id is required"}
	}
	if update.FailedAt.IsZero() {
		update.FailedAt = storage.Now()
	}
	update.FailedAt = update.FailedAt.UTC()
	var updated bool
	var staleAttempt bool
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_session_preparation_failed", func(tx *dbconnect.Tx) error {
		if err := lockSessionReadyFanoutFence(ctx, tx, ws, sessionID); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_preparations
			    SET status = 'failed',
			        failure_stage = $5,
			        last_error_kind = $6,
			        failure_reason = $7,
			        failure_resource_id = $8,
			        failure_resource_url = $9,
			        retryable = $10,
			        failed_at = $4,
			        ready_at = NULL,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND preparation_attempt_id = $3
			    AND status = 'preparing'
			    AND superseded_at IS NULL
			    AND preparation_attempt_id = (
			        SELECT active.preparation_attempt_id
			          FROM session_preparations active
			         WHERE active.workspace_id = $1
			           AND active.session_id = $2
			           AND active.superseded_at IS NULL
			         ORDER BY active.created_at DESC, active.preparation_attempt_id DESC
			         LIMIT 1
			    )`,
			string(ws),
			sessionID,
			preparationAttemptID,
			update.FailedAt,
			nullableEmptyString(update.FailureStage),
			nullableEmptyString(update.LastErrorKind),
			nullableEmptyString(update.FailureReason),
			nullableEmptyString(update.FailureResourceID),
			nullableEmptyString(update.FailureResourceURL),
			update.Retryable,
		)
		if err != nil {
			return err
		}
		updated = rowsAffected(result)
		if !updated {
			staleAttempt, err = sessionPreparationAttemptStaleTx(ctx, tx, ws, sessionID, preparationAttemptID)
			return err
		}
		segments, err := LoadPendingRuntimeInputSegments(ctx, tx, ws, sessionID)
		if err != nil {
			return err
		}
		return EnqueueRuntimeInputSegments(ctx, tx, ws, sessionID, segments, update.FailedAt)
	})
	if err != nil {
		return err
	}
	if !updated {
		if staleAttempt {
			return ErrStalePreparationAttempt
		}
		return &NotFoundError{Message: "preparing session preparation not found"}
	}
	return nil
}

func sessionPreparationAttemptStaleTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, preparationAttemptID string) (bool, error) {
	var currentAttemptID string
	err := tx.QueryRowScanner(ctx,
		`SELECT preparation_attempt_id
		   FROM session_preparations
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND superseded_at IS NULL
			  ORDER BY created_at DESC, preparation_attempt_id DESC
		  LIMIT 1`,
		string(ws),
		sessionID,
	).Scan(&currentAttemptID)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return currentAttemptID != preparationAttemptID, nil
}

func lockSessionReadyFanoutFence(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) error {
	var lockedSessionID string
	if err := tx.QueryRowScanner(ctx,
		`SELECT id
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		string(ws),
		sessionID,
	).Scan(&lockedSessionID); dbconnect.IsNoRows(err) {
		return &NotFoundError{Message: "session not found"}
	} else if err != nil {
		return err
	}
	var lockedRuntimeSessionID string
	if err := tx.QueryRowScanner(ctx,
		`SELECT session_id
		   FROM session_runtime_status
		  WHERE workspace_id = $1
		    AND session_id = $2
		  FOR UPDATE`,
		string(ws),
		sessionID,
	).Scan(&lockedRuntimeSessionID); dbconnect.IsNoRows(err) {
		return errors.New("sandbox: session_runtime_status invariant missing")
	} else if err != nil {
		return err
	}
	return nil
}

type pendingRuntimeInputEvent struct {
	eventID              string
	sessionThreadID      string
	sequence             int64
	inputKind            string
	preparationAttemptID string
}

type PendingRuntimeInputSegment struct {
	SessionThreadID      string
	InputKind            string
	EventIDs             []string
	EventSequences       []int64
	SequenceFrom         int64
	SequenceTo           int64
	PreparationAttemptID string
}

func LoadPendingRuntimeInputSegments(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string) ([]PendingRuntimeInputSegment, error) {
	return loadPendingRuntimeInputSegments(ctx, tx, ws, sessionID, "")
}

func LoadPendingRuntimeInputSegmentsThroughPreparationAttempt(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, preparationAttemptID string) ([]PendingRuntimeInputSegment, error) {
	if preparationAttemptID == "" {
		return nil, &ValidationError{Message: "preparation_attempt_id boundary is required"}
	}
	return loadPendingRuntimeInputSegments(ctx, tx, ws, sessionID, preparationAttemptID)
}

func loadPendingRuntimeInputSegments(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, throughPreparationAttemptID string) ([]PendingRuntimeInputSegment, error) {
	rows, err := tx.Query(ctx,
		`SELECT e.event_id, e.session_thread_id, e.sequence, e.type, e.preparation_attempt_id
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.processed_at IS NULL
		    AND e.type IN ('user.message', 'user.interrupt', 'user.tool_confirmation')
		    AND (
		        $4 = ''
		        OR e.preparation_attempt_id IS NULL
		        OR EXISTS (
		            SELECT 1
		              FROM session_preparations birth
		              JOIN session_preparations boundary
		                ON boundary.workspace_id = birth.workspace_id
		               AND boundary.session_id = birth.session_id
		             WHERE birth.workspace_id = e.workspace_id
		               AND birth.session_id = e.session_id
		               AND birth.preparation_attempt_id = e.preparation_attempt_id
		               AND boundary.preparation_attempt_id = $4
		               AND (
		                   birth.created_at < boundary.created_at
		                   OR (
		                       birth.created_at = boundary.created_at
		                       AND birth.preparation_attempt_id <= boundary.preparation_attempt_id
		                   )
		               )
		        )
		    )
		    AND NOT (
		        e.type = 'user.message'
		        AND e.sequence < (
		            SELECT COALESCE(MAX(interrupt.sequence), 0)
		              FROM session_events interrupt
		             WHERE interrupt.workspace_id = e.workspace_id
		               AND interrupt.session_id = e.session_id
		               AND interrupt.session_thread_id = e.session_thread_id
		               AND interrupt.type = 'user.interrupt'
		               AND interrupt.processed_at IS NOT NULL
		        )
		    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM queue_jobs q
		         WHERE q.workspace_id = e.workspace_id
		           AND q.kind = 'runtime_input'
		           AND q.partition_key = $3
		           AND q.status IN ('pending', 'leased')
		           AND (q.payload_json::jsonb -> 'event_ids') ? e.event_id
		    )
		  ORDER BY e.insert_stream_position ASC, e.event_id ASC`,
		string(ws),
		sessionID,
		queue.FormatSessionPartitionKey(ws, sessionID),
		throughPreparationAttemptID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var segments []PendingRuntimeInputSegment
	for rows.Next() {
		var event pendingRuntimeInputEvent
		var sessionThreadID sql.NullString
		var preparationAttemptID sql.NullString
		var eventType string
		if err := rows.Scan(&event.eventID, &sessionThreadID, &event.sequence, &eventType, &preparationAttemptID); err != nil {
			return nil, err
		}
		if !sessionThreadID.Valid || sessionThreadID.String == "" {
			return nil, errors.New("sandbox: pending runtime input event missing session_thread_id")
		}
		event.sessionThreadID = sessionThreadID.String
		if !preparationAttemptID.Valid || preparationAttemptID.String == "" {
			return nil, errors.New("sandbox: pending runtime input event missing preparation_attempt_id")
		}
		event.preparationAttemptID = preparationAttemptID.String
		event.inputKind = runtimeInputKindForEventType(eventType)
		if event.inputKind == "" {
			continue
		}
		last := len(segments) - 1
		if last < 0 ||
			segments[last].InputKind != event.inputKind ||
			segments[last].SessionThreadID != event.sessionThreadID ||
			segments[last].PreparationAttemptID != event.preparationAttemptID {
			segments = append(segments, PendingRuntimeInputSegment{
				SessionThreadID:      event.sessionThreadID,
				InputKind:            event.inputKind,
				SequenceFrom:         event.sequence,
				PreparationAttemptID: event.preparationAttemptID,
			})
			last = len(segments) - 1
		}
		segments[last].EventIDs = append(segments[last].EventIDs, event.eventID)
		segments[last].EventSequences = append(segments[last].EventSequences, event.sequence)
		segments[last].SequenceTo = event.sequence
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return segments, nil
}

func EnqueueRuntimeInputSegments(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID string, segments []PendingRuntimeInputSegment, now time.Time) error {
	jobIndex := 0
	for _, segment := range segments {
		if len(segment.EventIDs) == 0 {
			continue
		}
		if segment.PreparationAttemptID == "" {
			return &ValidationError{Message: "runtime input birth preparation attempt is required"}
		}
		if len(segment.EventSequences) != len(segment.EventIDs) {
			return &ValidationError{Message: "runtime input event sequence references are incomplete"}
		}
		for offset := 0; offset < len(segment.EventIDs); offset += queue.MaxRuntimeInputEventRefsPerJob {
			end := min(offset+queue.MaxRuntimeInputEventRefsPerJob, len(segment.EventIDs))
			runtimeInputID := id.New(runtimeInputIDPrefix)
			payloadFields := map[string]any{
				"workspace_id":           string(ws),
				"session_id":             sessionID,
				"session_thread_id":      segment.SessionThreadID,
				"runtime_input_id":       runtimeInputID,
				"event_ids":              segment.EventIDs[offset:end],
				"sequence_from":          segment.EventSequences[offset],
				"sequence_to":            segment.EventSequences[end-1],
				"input_kind":             segment.InputKind,
				"preparation_attempt_id": segment.PreparationAttemptID,
			}
			payload, err := json.Marshal(payloadFields)
			if err != nil {
				return err
			}
			jobIndex++
			// Fractional timestamps preserve stream order in the session partition.
			// Durable timestamps are microsecond-precision, so fan-out jobs are spaced
			// one microsecond apart to keep created_at ordering equal to insert order.
			segmentTime := now.Add(time.Duration(jobIndex) * time.Microsecond)
			if _, err := queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
				WorkspaceID:    ws,
				Kind:           queue.KindRuntimeInput,
				PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
				DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, sessionID, runtimeInputID),
				PayloadVersion: 1,
				PayloadJSON:    payload,
				Priority:       runtimeInputPriority(segment.InputKind),
				AvailableAt:    segmentTime,
				Now:            segmentTime,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func runtimeInputKindForEventType(eventType string) string {
	switch eventType {
	case eventTypeUserMessage:
		return runtimeInputKindMessages
	case eventTypeUserInterrupt:
		return runtimeInputKindInterruptControl
	case eventTypeUserToolConfirmation:
		return runtimeInputKindToolConfirmation
	default:
		return ""
	}
}

func runtimeInputPriority(inputKind string) int {
	if inputKind == runtimeInputKindInterruptControl {
		return runtimeInputInterruptQueuePriority
	}
	return 0
}

func (s *PostgreSQLSandboxStore) ReadMemoryStoreSnapshot(ctx context.Context, ws workspace.ID, memoryStoreID string) ([]MemorySnapshotFile, error) {
	var files []MemorySnapshotFile
	if err := s.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "sandbox.read_memory_store_snapshot", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT m.path, v.content, m.content_sha256
			   FROM memories m
			   JOIN memory_versions v
			     ON v.workspace_id = m.workspace_id
			    AND v.memory_store_id = m.memory_store_id
			    AND v.memory_id = m.memory_id
			    AND v.memory_version_id = m.current_version_id
			  WHERE m.workspace_id = $1
			    AND m.memory_store_id = $2
			    AND m.deleted_at IS NULL
			  ORDER BY m.path ASC`,
			string(ws),
			memoryStoreID,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var file MemorySnapshotFile
			if err := rows.Scan(&file.Path, &file.Content, &file.ContentSHA256); err != nil {
				return err
			}
			files = append(files, file)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return files, nil
}

func (s *PostgreSQLSandboxStore) WithMemoryStoreMutationLocks(ctx context.Context, ws workspace.ID, memoryStoreIDs []string, fn func(context.Context) error) error {
	if s == nil || s.client == nil {
		return &ValidationError{Message: "memory store mutation locker is required"}
	}
	if ws == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if fn == nil {
		return &ValidationError{Message: "memory store mutation callback is required"}
	}
	ids := uniqueMemoryStoreIDs(memoryStoreIDs)
	if len(ids) == 0 {
		return &ValidationError{Message: "memory_store_id is required"}
	}
	return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.memory_projection_mutation_locks", func(tx *dbconnect.Tx) error {
		for _, memoryStoreID := range ids {
			if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, string(ws), memoryStoreID); err != nil {
				return err
			}
		}
		return fn(ctx)
	})
}

func uniqueMemoryStoreIDs(memoryStoreIDs []string) []string {
	seen := map[string]struct{}{}
	ids := make([]string, 0, len(memoryStoreIDs))
	for _, id := range memoryStoreIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

type postgresqlRuntimeTransaction struct {
	tx *dbconnect.Tx
}

func (t postgresqlRuntimeTransaction) Exec(ctx context.Context, query string, args ...any) (interface {
	RowsAffected() (int64, error)
}, error) {
	return t.tx.Exec(ctx, query, args...)
}

func (t postgresqlRuntimeTransaction) QueryRowScanner(ctx context.Context, query string, args ...any) interface {
	Scan(dest ...any) error
} {
	return t.tx.QueryRowScanner(ctx, query, args...)
}

func (s *PostgreSQLSandboxStore) CreateSandbox(ctx context.Context, sandbox *Sandbox) error {
	return validateAndCreateSandbox(ctx, sandbox, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(sandbox.WorkspaceID), "sandbox.create", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func validateAndCreateSandbox(ctx context.Context, sandbox *Sandbox, run func(func(PostgreSQLTransaction) error) error) error {
	if sandbox == nil {
		return &ValidationError{Message: "sandbox is required"}
	}
	if sandbox.WorkspaceID == "" {
		return &ValidationError{Message: "workspace_id is required"}
	}
	if sandbox.ID == "" {
		return &ValidationError{Message: "sandbox_id is required"}
	}
	if sandbox.SessionID == "" {
		return &ValidationError{Message: "session_id is required"}
	}
	if sandbox.Provider == "" {
		return &ValidationError{Message: "provider is required"}
	}
	if err := ValidateProviderName(sandbox.Provider); err != nil {
		return err
	}
	if sandbox.ProviderSandboxID != "" {
		return &ValidationError{Message: "provider handle must be saved after sandbox creation"}
	}
	status := sandbox.Status
	if status == "" {
		status = StatusCreating
	}
	if err := validateStatus(status); err != nil {
		return err
	}
	if err := ValidateProviderMetadata(sandbox.ProviderMetadata); err != nil {
		return err
	}
	createdAt := sandbox.CreatedAt
	if createdAt.IsZero() {
		createdAt = storage.Now()
	}
	updatedAt := sandbox.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	metadataJSON, err := json.Marshal(nonNilStringMap(sandbox.ProviderMetadata))
	if err != nil {
		return err
	}
	return run(func(tx PostgreSQLTransaction) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO sandboxes (
				workspace_id, id, session_id, environment_id, environment_generation, status, provider, provider_sandbox_id,
				provider_metadata_json, created_at, updated_at, released_at, failed_at,
				startup_failure_reason, cleanup_status, cleanup_method, cleanup_error_kind,
				cleanup_retryable, cleanup_last_attempt_at, cleanup_next_attempt_at,
				cleanup_attempt_count, status_refreshed_at, machine_was_usable
			) VALUES ($1, $2, $3,
			          COALESCE(NULLIF($4, ''), (SELECT environment_id FROM sessions WHERE workspace_id = $1 AND id = $3)),
			          COALESCE(NULLIF($5, 0), (SELECT e.current_generation FROM environments e JOIN sessions s ON s.workspace_id=e.workspace_id AND s.environment_id=e.id WHERE s.workspace_id=$1 AND s.id=$3)),
			          $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
			          CASE WHEN $6 = 'active' THEN TRUE ELSE NULL END)`,
			string(sandbox.WorkspaceID),
			sandbox.ID,
			sandbox.SessionID,
			sandbox.EnvironmentID,
			sandbox.EnvironmentGeneration,
			string(status),
			sandbox.Provider,
			nullableEmptyString(sandbox.ProviderSandboxID),
			string(metadataJSON),
			createdAt,
			updatedAt,
			sandbox.ReleasedAt,
			sandbox.FailedAt,
			nullableEmptyString(sandbox.StartupFailureReason),
			string(nonEmptyCleanupStatus(sandbox.CleanupStatus)),
			nullableEmptyString(sandbox.CleanupMethod),
			nullableEmptyString(sandbox.CleanupErrorKind),
			sandbox.CleanupRetryable,
			sandbox.CleanupLastAttemptAt,
			sandbox.CleanupNextAttemptAt,
			sandbox.CleanupAttemptCount,
			sandbox.StatusRefreshedAt,
		)
		return mapPostgreSQLError(err)
	})
}

func (s *PostgreSQLSandboxStore) SaveProviderHandle(ctx context.Context, ws workspace.ID, sandboxID string, handle ProviderHandle, updatedAt time.Time) (*Sandbox, error) {
	return saveProviderHandle(ctx, ws, sandboxID, handle, updatedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.save_provider_handle", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func saveProviderHandle(ctx context.Context, ws workspace.ID, sandboxID string, handle ProviderHandle, updatedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if err := ValidateProviderHandle(handle); err != nil {
		return nil, err
	}
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	metadataJSON, err := json.Marshal(nonNilStringMap(handle.Metadata))
	if err != nil {
		return nil, err
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET provider = $1,
			        provider_sandbox_id = $2,
			        provider_metadata_json = $3,
			        updated_at = $4
			  WHERE workspace_id = $5 AND id = $6`,
			handle.Provider,
			handle.SandboxID,
			string(metadataJSON),
			updatedAt,
			string(ws),
			sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

// sandboxes.status is the machine-lifecycle axis of a session's sandbox row.
// Every writer below is a guarded UPDATE in this file; the WHERE-clause pre-state
// set is what makes each transition idempotent and forward-only (monotonic) — a
// write whose row is no longer in the expected pre-state affects zero rows and
// yields rather than clobbering a concurrent writer.
//
//	state      meaning                                     writers (this file)                             legal next states
//	---------  ------------------------------------------  ----------------------------------------------  ------------------------------------------
//	creating   row exists, provider machine being born     CreateSandbox (insert)                          active, releasing, released, failed
//	resuming   waking a stopped/archived machine           RefreshSandboxState, resumeSandboxAndPrepare    active, releasing, released, failed
//	active     machine started and usable                  MarkActive, updateStatus, RefreshSandboxState   stopped, archived, resuming, releasing, released, failed
//	stopped    provider paused the machine (recoverable)   RefreshSandboxState                             resuming, active, releasing, released, failed
//	archived   checkpoint-archived (recoverable, may bill) RefreshSandboxState, MarkArchived, markStartupFailed(derived)  resuming, active, releasing, released
//	releasing  release in flight                           MarkReleasing, MarkReleasingForDelete           released, archived
//	released   terminal: machine gone                      MarkReleased, RefreshSandboxState, provider-missing settle  creating (REPLACE only)
//	failed     terminal-for-attempt                        markStartupFailed, markFailed, SettleDeleted... archived (cleanup settle), released, creating (REPLACE)
//
// Readers: session_prepare.go routes cold return on status (its closed wake-vs-
// replace table); the cleanup reconciler reads status together with cleanup_status.
//
// TERMINAL-STATUS DERIVATION — the preparation-failure settle (markStartupFailed,
// markStartupCleanupAttempt) derives the settled status from the cleanup outcome
// rather than taking it as input, reason-derived and fact-derived:
//   - cleanup_status='released' AND cleanup_error_kind='not_found' -> released
//     (provider confirmed the machine is already gone; released_at stamped).
//   - machine_was_usable IS TRUE AND cleanup_status='released'      -> archived
//     (cleanup reason stop+archives at the provider, so a machine that ever reached
//     active is kept as a reconcilable, possibly-billing checkpoint, not dropped:
//     cleanup->archived).
//   - otherwise                                                     -> failed.
//     The delete path is the counterpart: SettleDeletedSessionPreparationAfterCreate
//     settles cleanup_method='delete' after the just-created machine is provider-
//     deleted, and MarkReleasingForDelete/MarkReleased carry a deletion to released
//     (delete->released). The settle is exactly-once and monotonic — markStartupFailed
//     excludes rows already in {failed, archived, releasing, released}, so a
//     redelivered settle is a no-op.
//
// machine_was_usable (BOOLEAN, nullable) — the durable "this machine reached active
// at least once" fact the derivation above reads. What it bounds and how it moves:
//   - Set TRUE by EVERY write that sets status='active' (the CreateSandbox insert
//     CASE, MarkActive/updateStatus, RefreshSandboxState, refreshStaleStartup); the
//     all-writers rule is guarded by an invariant test in this package.
//   - Enforced by a NOT VALID CHECK (defined in internal/storage/postgresql_schema.go)
//     in the NULL-safe IS-TRUE form: status <> 'active' OR machine_was_usable IS TRUE.
//   - READ, never inferred from status: the settle reads the column
//     (COALESCE(machine_was_usable, FALSE)) because a failed row is no longer
//     'active' yet must still record whether it once was.
//   - Reset to NULL only by PrepareSandboxReplacement, which reuses the row for a
//     brand-new machine; an absent/NULL value reads FALSE.
//
// UPDATE-WITH: internal/storage/postgresql_schema.go (the CHECK); every status='active'
// writer in this file; the settle readers here and in internal/sandbox/session_prepare.go.
func (s *PostgreSQLSandboxStore) MarkActive(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error) {
	return s.updateStatus(ctx, ws, sandboxID, StatusActive, updatedAt, "AND status IN ('creating', 'resuming')")
}

func (s *PostgreSQLSandboxStore) PrepareSandboxReplacement(ctx context.Context, ws workspace.ID, sandboxID string, environmentID string, environmentGeneration int64, updatedAt time.Time) (*Sandbox, error) {
	return prepareSandboxReplacement(ctx, ws, sandboxID, environmentID, environmentGeneration, updatedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.prepare_replacement", func(tx *dbconnect.Tx) error { return fn(postgresqlRuntimeTransaction{tx: tx}) })
	})
}

func prepareSandboxReplacement(ctx context.Context, ws workspace.ID, sandboxID string, environmentID string, environmentGeneration int64, updatedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if environmentID == "" || environmentGeneration <= 0 {
		return nil, &ValidationError{Message: "replacement environment identity is required"}
	}
	var got *Sandbox
	err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status='creating', environment_id=$1, environment_generation=$2,
			        provider_sandbox_id=NULL, provider_metadata_json='{}', released_at=NULL,
			        failed_at=NULL, startup_failure_reason=NULL,
			        cleanup_status='none', cleanup_method=NULL, cleanup_error_kind=NULL,
			        cleanup_retryable=FALSE, cleanup_last_attempt_at=NULL,
			        cleanup_next_attempt_at=NULL, cleanup_attempt_count=0,
			        cleanup_lease_token=NULL, cleanup_lease_expires_at=NULL,
			        status_refreshed_at=NULL, machine_was_usable=NULL, updated_at=$3
			  WHERE workspace_id=$4 AND id=$5
			    AND status IN ('active','stopped','archived','released','failed')`,
			environmentID, environmentGeneration, updatedAt, string(ws), sandboxID)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id=$1 AND id=$2`, string(ws), sandboxID))
		return loadErr
	})
	return got, err
}

func (s *PostgreSQLSandboxStore) MarkStatusRefreshed(ctx context.Context, ws workspace.ID, sandboxID string, refreshedAt time.Time) (*Sandbox, error) {
	return markStatusRefreshed(ctx, ws, sandboxID, refreshedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_status_refreshed", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func markStatusRefreshed(ctx context.Context, ws workspace.ID, sandboxID string, refreshedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if refreshedAt.IsZero() {
		refreshedAt = storage.Now()
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status_refreshed_at = $1, updated_at = $1
			  WHERE workspace_id = $2 AND id = $3 AND status = 'active'`,
			refreshedAt, string(ws), sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) RefreshSandboxState(ctx context.Context, ws workspace.ID, sandboxID string, status Status, refreshedAt time.Time) (*Sandbox, error) {
	return refreshSandboxState(ctx, ws, sandboxID, status, refreshedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.refresh_state", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func refreshSandboxState(ctx context.Context, ws workspace.ID, sandboxID string, status Status, refreshedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if err := validateStatus(status); err != nil {
		return nil, err
	}
	if refreshedAt.IsZero() {
		refreshedAt = storage.Now()
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		setClause := `status = $1, status_refreshed_at = $2, updated_at = $2`
		switch status {
		case StatusActive:
			setClause = `status = $1,
			 status_refreshed_at = $2,
			 updated_at = $2,
			 machine_was_usable = TRUE,
			 failed_at = NULL,
			 startup_failure_reason = NULL,
			 cleanup_status = 'none',
			 cleanup_method = NULL,
			 cleanup_error_kind = NULL,
			 cleanup_retryable = FALSE,
			 cleanup_last_attempt_at = NULL,
			 cleanup_next_attempt_at = NULL,
			 cleanup_lease_token = NULL,
			 cleanup_lease_expires_at = NULL`
		case StatusReleased:
			setClause = `status = $1,
			 status_refreshed_at = $2,
			 updated_at = $2,
			 released_at = COALESCE(released_at, $2)`
		case StatusFailed:
			setClause = `status = $1,
			 status_refreshed_at = $2,
			 updated_at = $2,
			 failed_at = COALESCE(failed_at, $2)`
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET `+setClause+`
			  WHERE workspace_id = $3
			    AND id = $4
			    AND status IN ('creating', 'active', 'stopped', 'archived', 'resuming')`,
			string(status),
			refreshedAt,
			string(ws),
			sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) InspectStaleStartup(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time) (StaleStartupInspection, error) {
	if staleBefore.IsZero() {
		return StaleStartupInspection{}, &ValidationError{Message: "stale_before is required"}
	}
	var inspection StaleStartupInspection
	err := s.client.WithWorkspaceReadOnlyTx(ctx, string(ws), "sandbox.inspect_stale_startup", func(tx *dbconnect.Tx) error {
		got, err := scanSandboxRow(tx.QueryRowScanner(ctx,
			sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2 AND status IN ('creating', 'resuming') AND updated_at <= $3`,
			string(ws), sandboxID, staleBefore.UTC(),
		))
		if err != nil {
			return err
		}
		var lifecycleState string
		if err := tx.QueryRowScanner(ctx,
			`SELECT lifecycle_state FROM sessions WHERE workspace_id = $1 AND id = $2`,
			string(ws), got.SessionID,
		).Scan(&lifecycleState); err != nil {
			return err
		}
		inspection = StaleStartupInspection{Sandbox: got, SessionDeleted: lifecycleState == "deleted"}
		return nil
	})
	return inspection, err
}

func (s *PostgreSQLSandboxStore) RefreshStaleStartup(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StaleStartupRefreshUpdate) (*Sandbox, error) {
	return refreshStaleStartup(ctx, ws, sandboxID, staleBefore, update, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.refresh_stale_startup", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func refreshStaleStartup(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StaleStartupRefreshUpdate, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if staleBefore.IsZero() {
		return nil, &ValidationError{Message: "stale_before is required"}
	}
	if update.RefreshedAt.IsZero() {
		update.RefreshedAt = storage.Now()
	}
	if update.Status != StatusCreating && update.Status != StatusResuming && update.Status != StatusActive && update.Status != StatusReleased {
		return nil, &ValidationError{Message: "stale startup refresh status is invalid"}
	}
	var (
		provider          any
		providerSandboxID any
		metadataJSON      any
	)
	if update.AdoptedProviderHandle != nil {
		if err := ValidateProviderHandle(*update.AdoptedProviderHandle); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(nonNilStringMap(update.AdoptedProviderHandle.Metadata))
		if err != nil {
			return nil, err
		}
		provider = update.AdoptedProviderHandle.Provider
		providerSandboxID = update.AdoptedProviderHandle.SandboxID
		metadataJSON = string(encoded)
	}
	var got *Sandbox
	err := run(func(tx PostgreSQLTransaction) error {
		setClause := `provider = COALESCE($1, provider),
			        provider_sandbox_id = COALESCE($2, provider_sandbox_id),
			        provider_metadata_json = COALESCE($3, provider_metadata_json),
			        status = $4,
			        status_refreshed_at = $5,
			        machine_was_usable = CASE WHEN $4 = 'active' THEN TRUE ELSE machine_was_usable END,
			        updated_at = $5`
		if update.Status == StatusReleased {
			setClause = `provider = COALESCE($1, provider),
			        provider_sandbox_id = COALESCE($2, provider_sandbox_id),
			        provider_metadata_json = COALESCE($3, provider_metadata_json),
			        status = $4,
			        status_refreshed_at = $5,
			        released_at = COALESCE(released_at, $5),
			        cleanup_status = 'released',
			        cleanup_method = NULL,
			        cleanup_error_kind = 'not_found',
			        cleanup_retryable = FALSE,
			        cleanup_next_attempt_at = NULL,
			        cleanup_lease_token = NULL,
			        cleanup_lease_expires_at = NULL,
			        updated_at = $5`
		}
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET `+setClause+`
			  WHERE workspace_id = $6
			    AND id = $7
			    AND status IN ('creating', 'resuming')
			    AND updated_at <= $8
			    AND EXISTS (
			      SELECT 1 FROM sessions sess
			       WHERE sess.workspace_id = sandboxes.workspace_id
			         AND sess.id = sandboxes.session_id
			         AND sess.lifecycle_state <> 'deleted'
			    )`,
			provider, providerSandboxID, metadataJSON, string(update.Status), update.RefreshedAt.UTC(),
			string(ws), sandboxID, staleBefore.UTC(),
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return &NotFoundError{Message: "stale startup sandbox not found"}
		}
		got, err = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return err
	})
	return got, err
}

func (s *PostgreSQLSandboxStore) ClaimStaleCreating(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StartupFailureUpdate) (*Sandbox, error) {
	return claimStaleCreating(ctx, ws, sandboxID, staleBefore, update, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.claim_stale_creating", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func claimStaleCreating(ctx context.Context, ws workspace.ID, sandboxID string, staleBefore time.Time, update StartupFailureUpdate, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if staleBefore.IsZero() {
		return nil, &ValidationError{Message: "stale_before is required"}
	}
	if update.FailedAt.IsZero() {
		update.FailedAt = storage.Now()
	}
	if update.StartupFailureReason == "" {
		return nil, &ValidationError{Message: "startup_failure_reason is required"}
	}
	if update.CleanupStatus == "" {
		update.CleanupStatus = CleanupStatusPending
	}
	if err := validateCleanupStatus(update.CleanupStatus); err != nil {
		return nil, err
	}
	var (
		provider          any
		providerSandboxID any
		metadataJSON      any
	)
	if update.AdoptedProviderHandle != nil {
		if err := ValidateProviderHandle(*update.AdoptedProviderHandle); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(nonNilStringMap(update.AdoptedProviderHandle.Metadata))
		if err != nil {
			return nil, err
		}
		provider = update.AdoptedProviderHandle.Provider
		providerSandboxID = update.AdoptedProviderHandle.SandboxID
		metadataJSON = string(encoded)
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET provider = COALESCE($1, provider),
			        provider_sandbox_id = COALESCE($2, provider_sandbox_id),
			        provider_metadata_json = COALESCE($3, provider_metadata_json),
			        status = 'failed',
			        failed_at = $4,
			        updated_at = $4,
			        startup_failure_reason = $5,
			        cleanup_status = $6,
			        cleanup_method = NULL,
			        cleanup_error_kind = NULL,
			        cleanup_retryable = FALSE,
			        cleanup_last_attempt_at = NULL,
			        cleanup_next_attempt_at = NULL
			  WHERE workspace_id = $7
			    AND id = $8
			    AND status IN ('creating', 'resuming')
			    AND updated_at <= $9`,
			provider, providerSandboxID, metadataJSON,
			update.FailedAt,
			update.StartupFailureReason,
			string(update.CleanupStatus),
			string(ws),
			sandboxID,
			staleBefore,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "stale creating sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) ClaimDueStartupCleanup(ctx context.Context, ws workspace.ID, sandboxID string, now time.Time, leaseDuration time.Duration, maxAttempts int) (*StartupCleanupClaim, error) {
	return claimDueStartupCleanup(ctx, ws, sandboxID, now, leaseDuration, maxAttempts, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.claim_due_startup_cleanup", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

// claimDueStartupCleanup is the claim leg of the finite, exclusively-owned
// startup-cleanup retry machine on the sandboxes row. It and
// markStartupCleanupAttempt write the machine's cleanup_status transitions on a
// failed row, and these durable transitions live here, not in any reconciler.
// The other writers of cleanup_status on a failed row are the delete and replace
// exits, which leave this machine entirely: settleDeletedCleanupFieldsSQL (the
// provider-missing delete path via markReleasedForDeleteProviderMissing),
// SettleDeletedSessionPreparationAfterCreate (create-redelivery settle to
// released), and prepareSandboxReplacement (reset to none for REPLACE).
//
//	cleanup_status    meaning                        leaves via
//	pending           cleanup owed, none in flight   claim -> in_progress
//	retryable_failed  last attempt failed, backing   claim (when due) -> in_progress
//	                  off (cleanup_next_attempt_at)
//	in_progress       an attempt is leased           completion -> released /
//	                  (cleanup_lease_token +          retryable_failed /
//	                  cleanup_lease_expires_at)       permanent_failed; or a later
//	                                                  claim steals it once the
//	                                                  lease has expired
//	released          terminal success               (terminal)
//	permanent_failed  terminal failure / at cap      (terminal)
//
// Claim CAS (this function): eligible when pending, or retryable_failed past
// cleanup_next_attempt_at, or in_progress with an expired lease. It mints a
// fresh cleanup_lease_token and cleanup_lease_expires_at and reserves the
// attempt at claim time -- cleanup_attempt_count increments only while it is
// still below maxAttempts (SANDBOX_CLEANUP_MAX_ATTEMPTS). Once the count has
// reached maxAttempts a claim no longer reserves (ProviderAttemptReserved is
// false) and the attempt becomes a read-only provider observation that
// terminalizes to released or permanent_failed instead of another release.
//
// Completion CAS (markStartupCleanupAttempt): matches only status='failed' AND
// cleanup_status='in_progress' AND cleanup_lease_token equal to the claimed
// token, and it clears the lease, so only the lease holder can settle the
// attempt.
//
// UPDATE-WITH: markStartupCleanupAttempt, MarkStartupFailed (this file),
// service.go (cleanupInterruptedStartup, observeStartupCleanupAtCap),
// sandbox.go (CleanupStatus, CleanupLeaseCompletionWriteMargin).
func claimDueStartupCleanup(ctx context.Context, ws workspace.ID, sandboxID string, now time.Time, leaseDuration time.Duration, maxAttempts int, run func(func(PostgreSQLTransaction) error) error) (*StartupCleanupClaim, error) {
	if now.IsZero() {
		now = storage.Now()
	}
	if leaseDuration <= 0 {
		return nil, &ValidationError{Message: "cleanup lease duration must be positive"}
	}
	if maxAttempts <= 0 {
		return nil, &ValidationError{Message: "cleanup max attempts must be positive"}
	}
	now = now.UTC()
	var got *StartupCleanupClaim
	if err := run(func(tx PostgreSQLTransaction) error {
		var sessionID string
		if err := tx.QueryRowScanner(ctx,
			`SELECT s.id
			   FROM sessions s
			   JOIN sandboxes sb
			     ON sb.workspace_id = s.workspace_id
			    AND sb.session_id = s.id
			  WHERE s.workspace_id = $1
			    AND sb.id = $2
			    AND s.lifecycle_state <> 'deleted'
			  FOR UPDATE OF s`,
			string(ws),
			sandboxID,
		).Scan(&sessionID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return &NotFoundError{Message: "due startup cleanup not found"}
			}
			return mapPostgreSQLError(err)
		}
		current, err := scanSandboxRow(tx.QueryRowScanner(ctx,
			sandboxSelectSQL+`
			  WHERE workspace_id = $1
			    AND id = $2
			    AND session_id = $3
			    AND status = 'failed'
			    AND startup_failure_reason IS NOT NULL
			  FOR UPDATE`,
			string(ws),
			sandboxID,
			sessionID,
		))
		if err != nil {
			return err
		}
		eligible := current.CleanupStatus == CleanupStatusPending ||
			(current.CleanupStatus == CleanupStatusRetryableFailed &&
				(current.CleanupNextAttemptAt == nil || !current.CleanupNextAttemptAt.After(now))) ||
			(current.CleanupStatus == CleanupStatusInProgress &&
				current.CleanupLeaseExpiresAt != nil && !current.CleanupLeaseExpiresAt.After(now))
		if !eligible {
			return &NotFoundError{Message: "due startup cleanup not found"}
		}
		attemptReserved := current.CleanupAttemptCount < maxAttempts
		reservedIncrement := 0
		if attemptReserved {
			reservedIncrement = 1
		}
		leaseToken := id.New("lease_")
		leaseExpiresAt := now.Add(leaseDuration)
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET cleanup_status = 'in_progress',
			        cleanup_lease_token = $1,
			        cleanup_lease_expires_at = $2,
			        cleanup_retryable = FALSE,
			        cleanup_method = NULL,
			        cleanup_error_kind = NULL,
			        cleanup_next_attempt_at = NULL,
			        cleanup_last_attempt_at = CASE
			            WHEN $3 = 1 THEN $4
			            ELSE cleanup_last_attempt_at
			        END,
			        cleanup_attempt_count = cleanup_attempt_count + $3,
			        updated_at = $4
			  WHERE workspace_id = $5
			    AND id = $6
			    AND status = 'failed'
			    AND cleanup_status = $7`,
			leaseToken,
			leaseExpiresAt,
			reservedIncrement,
			now,
			string(ws),
			sandboxID,
			string(current.CleanupStatus),
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "due startup cleanup not found"}
		}
		claimed, loadErr := scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		if loadErr != nil {
			return loadErr
		}
		got = &StartupCleanupClaim{Sandbox: claimed, ProviderAttemptReserved: attemptReserved}
		return nil
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) FindLiveBySessionID(ctx context.Context, ws workspace.ID, sessionID string) (*Sandbox, error) {
	return findLiveBySessionID(ctx, ws, sessionID, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.find_live_by_session", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func (s *PostgreSQLSandboxStore) FindLatestBySessionID(ctx context.Context, ws workspace.ID, sessionID string) (*Sandbox, error) {
	return findLatestBySessionID(ctx, ws, sessionID, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.find_latest_by_session", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func findLatestBySessionID(ctx context.Context, ws workspace.ID, sessionID string, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		var err error
		got, err = scanSandboxRow(tx.QueryRowScanner(ctx,
			sandboxSelectSQL+`
			  WHERE workspace_id = $1
			    AND session_id = $2
			  ORDER BY storage_sequence DESC
			  LIMIT 1`,
			string(ws), sessionID,
		))
		return err
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func findLiveBySessionID(ctx context.Context, ws workspace.ID, sessionID string, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		var err error
		got, err = scanSandboxRow(tx.QueryRowScanner(ctx,
			sandboxSelectSQL+`
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND status IN ('creating', 'active', 'stopped', 'archived', 'resuming', 'releasing')
			  ORDER BY storage_sequence DESC
			  LIMIT 1`,
			string(ws), sessionID,
		))
		return err
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) MarkReleasing(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error) {
	return s.updateStatus(ctx, ws, sandboxID, StatusReleasing, updatedAt, "AND (status IN ('active', 'stopped', 'archived') OR (status = 'failed' AND cleanup_status IN ('pending', 'retryable_failed')))")
}

func (s *PostgreSQLSandboxStore) MarkReleasingForDelete(ctx context.Context, ws workspace.ID, sandboxID string, updatedAt time.Time) (*Sandbox, error) {
	return s.updateStatus(ctx, ws, sandboxID, StatusReleasing, updatedAt, "AND status IN ('active', 'stopped', 'archived', 'releasing', 'released', 'failed')")
}

func (s *PostgreSQLSandboxStore) MarkReleasedForDeleteProviderMissing(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time) (*Sandbox, error) {
	return markReleasedForDeleteProviderMissing(ctx, ws, sandboxID, releasedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_released_for_delete_provider_missing", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

const settleDeletedCleanupFieldsSQL = `
			        cleanup_status = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN 'released'
			            ELSE cleanup_status
			        END,
			        cleanup_method = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN 'delete'
			            ELSE cleanup_method
			        END,
			        cleanup_error_kind = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN NULL
			            ELSE cleanup_error_kind
			        END,
			        cleanup_retryable = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN FALSE
			            ELSE cleanup_retryable
			        END,
			        cleanup_last_attempt_at = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN $1
			            ELSE cleanup_last_attempt_at
			        END,
			        cleanup_next_attempt_at = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN NULL
			            ELSE cleanup_next_attempt_at
			        END,
			        cleanup_attempt_count = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN GREATEST(cleanup_attempt_count, 1)
			            ELSE cleanup_attempt_count
			        END,
			        cleanup_lease_token = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN NULL
			            ELSE cleanup_lease_token
			        END,
			        cleanup_lease_expires_at = CASE
			            WHEN cleanup_status IN ('pending', 'in_progress', 'retryable_failed') THEN NULL
			            ELSE cleanup_lease_expires_at
			        END`

func markReleasedForDeleteProviderMissing(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if releasedAt.IsZero() {
		releasedAt = storage.Now()
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = 'released',
			        released_at = COALESCE(released_at, $1),
			        updated_at = $1,
			`+settleDeletedCleanupFieldsSQL+`
			  WHERE workspace_id = $2
			    AND id = $3
			    AND status IN ('failed', 'released')
			    AND provider_sandbox_id IS NULL`,
			releasedAt, string(ws), sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) MarkReleased(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time) (*Sandbox, error) {
	return markReleased(ctx, ws, sandboxID, releasedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_released", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func markReleased(ctx context.Context, ws workspace.ID, sandboxID string, releasedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if releasedAt.IsZero() {
		releasedAt = storage.Now()
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = 'released',
			        released_at = $1,
			        updated_at = $1,
			`+settleDeletedCleanupFieldsSQL+`
			  WHERE workspace_id = $2 AND id = $3 AND status = 'releasing'`,
			releasedAt, string(ws), sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) MarkArchived(ctx context.Context, ws workspace.ID, sandboxID string, archivedAt time.Time) (*Sandbox, error) {
	if archivedAt.IsZero() {
		archivedAt = storage.Now()
	}
	var got *Sandbox
	err := s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_archived", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = 'archived',
			        status_refreshed_at = $1,
			        updated_at = $1
			  WHERE workspace_id = $2
			    AND id = $3
			    AND status = 'releasing'`,
			archivedAt, string(ws), sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	})
	return got, err
}

func (s *PostgreSQLSandboxStore) MarkFailed(ctx context.Context, ws workspace.ID, sandboxID string, failedAt time.Time) (*Sandbox, error) {
	return markFailed(ctx, ws, sandboxID, failedAt, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_failed", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func (s *PostgreSQLSandboxStore) MarkStartupFailed(ctx context.Context, ws workspace.ID, sandboxID string, update StartupFailureUpdate) (*Sandbox, error) {
	return markStartupFailed(ctx, ws, sandboxID, update, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_startup_failed", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func markStartupFailed(ctx context.Context, ws workspace.ID, sandboxID string, update StartupFailureUpdate, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if update.FailedAt.IsZero() {
		update.FailedAt = storage.Now()
	}
	if update.CleanupStatus == "" {
		update.CleanupStatus = CleanupStatusPending
	}
	var cleanupLastAttemptAt any
	cleanupAttemptCount := 0
	if update.CleanupAttempted {
		cleanupLastAttemptAt = update.FailedAt
		cleanupAttemptCount = 1
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = CASE
			            WHEN $3 = 'released' AND $5 = 'not_found' THEN 'released'
			            WHEN COALESCE(machine_was_usable, FALSE) AND $3 = 'released' THEN 'archived'
			            ELSE 'failed'
			        END,
			        failed_at = $1,
			        released_at = CASE
			            WHEN $3 = 'released' AND $5 = 'not_found' THEN COALESCE(released_at, $1)
			            ELSE released_at
			        END,
			        updated_at = $1,
			        startup_failure_reason = $2,
			        cleanup_status = $3,
			        cleanup_method = $4,
			        cleanup_error_kind = $5,
			        cleanup_retryable = $6,
			        cleanup_last_attempt_at = $7,
			        cleanup_next_attempt_at = $8,
			        cleanup_attempt_count = cleanup_attempt_count + $9
			  WHERE workspace_id = $10
			    AND id = $11
			    AND status NOT IN ('failed', 'archived', 'releasing', 'released')`,
			update.FailedAt,
			nullableEmptyString(update.StartupFailureReason),
			string(update.CleanupStatus),
			nullableEmptyString(update.CleanupMethod),
			nullableEmptyString(update.CleanupErrorKind),
			update.CleanupRetryable,
			cleanupLastAttemptAt,
			update.CleanupNextAttemptAt,
			cleanupAttemptCount,
			string(ws),
			sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		if affected, _ := result.RowsAffected(); affected == 0 && loadErr == nil {
			return nil
		}
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) MarkStartupCleanupAttempt(ctx context.Context, ws workspace.ID, sandboxID string, update CleanupAttemptUpdate) (*Sandbox, error) {
	return markStartupCleanupAttempt(ctx, ws, sandboxID, update, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.mark_startup_cleanup_attempt", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func markStartupCleanupAttempt(ctx context.Context, ws workspace.ID, sandboxID string, update CleanupAttemptUpdate, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if update.AttemptedAt.IsZero() {
		update.AttemptedAt = storage.Now()
	}
	if update.CleanupLeaseToken == "" {
		return nil, &ValidationError{Message: "cleanup lease token is required"}
	}
	if update.CleanupStatus == "" {
		update.CleanupStatus = CleanupStatusPending
	}
	if err := validateCleanupStatus(update.CleanupStatus); err != nil {
		return nil, err
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = CASE
			            WHEN $2 = 'released' AND $4 = 'not_found' THEN 'released'
			            WHEN machine_was_usable AND $2 = 'released' THEN 'archived'
			            ELSE status
			        END,
			        updated_at = $1,
			        released_at = CASE
			            WHEN $2 = 'released' AND $4 = 'not_found' THEN COALESCE(released_at, $1)
			            ELSE released_at
			        END,
			        cleanup_status = $2,
			        cleanup_method = $3,
			        cleanup_error_kind = $4,
			        cleanup_retryable = $5,
			        cleanup_next_attempt_at = $6,
			        cleanup_lease_token = NULL,
			        cleanup_lease_expires_at = NULL
			  WHERE workspace_id = $7
			    AND id = $8
			    AND status = 'failed'
			    AND startup_failure_reason IS NOT NULL
			    AND cleanup_status = 'in_progress'
			    AND cleanup_lease_token = $9`,
			update.AttemptedAt,
			string(update.CleanupStatus),
			nullableEmptyString(update.CleanupMethod),
			nullableEmptyString(update.CleanupErrorKind),
			update.CleanupRetryable,
			update.CleanupNextAttemptAt,
			string(ws),
			sandboxID,
			update.CleanupLeaseToken,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		if affected, _ := result.RowsAffected(); affected == 0 && loadErr == nil {
			return nil
		}
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func markFailed(ctx context.Context, ws workspace.ID, sandboxID string, failedAt time.Time, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if failedAt.IsZero() {
		failedAt = storage.Now()
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		result, err := tx.Exec(ctx,
			`UPDATE sandboxes
			    SET status = 'failed', failed_at = $1, updated_at = $1
			  WHERE workspace_id = $2 AND id = $3`,
			failedAt, string(ws), sandboxID,
		)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func (s *PostgreSQLSandboxStore) updateStatus(ctx context.Context, ws workspace.ID, sandboxID string, status Status, updatedAt time.Time, extraPredicate string) (*Sandbox, error) {
	return updateStatus(ctx, ws, sandboxID, status, updatedAt, extraPredicate, func(fn func(PostgreSQLTransaction) error) error {
		return s.client.WithWorkspaceTx(ctx, string(ws), "sandbox.update_status", func(tx *dbconnect.Tx) error {
			return fn(postgresqlRuntimeTransaction{tx: tx})
		})
	})
}

func updateStatus(ctx context.Context, ws workspace.ID, sandboxID string, status Status, updatedAt time.Time, extraPredicate string, run func(func(PostgreSQLTransaction) error) error) (*Sandbox, error) {
	if err := validateStatus(status); err != nil {
		return nil, err
	}
	if updatedAt.IsZero() {
		updatedAt = storage.Now()
	}
	var got *Sandbox
	if err := run(func(tx PostgreSQLTransaction) error {
		setClause := `status = $1, updated_at = $2`
		if status == StatusActive {
			setClause = `status = $1,
			 updated_at = $2,
			 status_refreshed_at = $2,
			 machine_was_usable = TRUE,
			 failed_at = NULL,
			 startup_failure_reason = NULL,
			 cleanup_status = 'none',
			 cleanup_method = NULL,
			 cleanup_error_kind = NULL,
			 cleanup_retryable = FALSE,
			 cleanup_last_attempt_at = NULL,
			 cleanup_next_attempt_at = NULL,
			 cleanup_lease_token = NULL,
			 cleanup_lease_expires_at = NULL`
		}
		query := `UPDATE sandboxes
			    SET ` + setClause + `
			  WHERE workspace_id = $3 AND id = $4 ` + extraPredicate
		result, err := tx.Exec(ctx, query, string(status), updatedAt, string(ws), sandboxID)
		if err != nil {
			return mapPostgreSQLError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return &NotFoundError{Message: "sandbox not found"}
		}
		var loadErr error
		got, loadErr = scanSandboxRow(tx.QueryRowScanner(ctx, sandboxSelectSQL+` WHERE workspace_id = $1 AND id = $2`, string(ws), sandboxID))
		return loadErr
	}); err != nil {
		return nil, err
	}
	return got, nil
}

func scanSessionPreparationWithCurrency(row interface{ Scan(dest ...any) error }) (SessionPreparation, bool, int64, error) {
	var (
		workspaceIDValue      string
		sessionID             string
		preparationAttemptID  string
		environmentID         string
		environmentGeneration int64
		sandboxID             string
		status                string
		failureReason         sql.NullString
		resourceCredExpiresAt sql.NullTime
		resourceRootsJSON     string
		isCurrent             bool
		createdAt             time.Time
		currentGeneration     int64
	)
	if err := row.Scan(
		&workspaceIDValue, &sessionID, &preparationAttemptID, &environmentID, &environmentGeneration, &sandboxID,
		&status, &failureReason, &resourceCredExpiresAt, &resourceRootsJSON, &isCurrent, &createdAt, &currentGeneration,
	); err != nil {
		return SessionPreparation{}, false, 0, err
	}
	var parsedResourceCredExpiresAt *time.Time
	if resourceCredExpiresAt.Valid {
		value := resourceCredExpiresAt.Time.UTC()
		parsedResourceCredExpiresAt = &value
	}
	parsedCreatedAt := createdAt.UTC()
	preparation := SessionPreparation{
		WorkspaceID:           workspace.ID(workspaceIDValue),
		SessionID:             sessionID,
		PreparationAttemptID:  preparationAttemptID,
		EnvironmentID:         environmentID,
		EnvironmentGeneration: environmentGeneration,
		SandboxID:             sandboxID,
		Status:                status,
		FailureReason:         nullableStringValue(failureReason),
		ResourceCredExpiresAt: parsedResourceCredExpiresAt,
		ResourceRootsJSON:     resourceRootsJSON,
		IsCurrent:             isCurrent,
		CreatedAt:             parsedCreatedAt,
	}
	return preparation, isCurrent, currentGeneration, nil
}

func loadEnvironmentArtifactForPreparationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	ws workspace.ID,
	environmentID string,
	generation int64,
	preparation *SessionPreparation,
) (string, SessionPreparationFailureUpdate, error) {
	var providerArtifactRef sql.NullString
	var networkPolicyJSON sql.NullString
	var status string
	var failureStage sql.NullString
	var lastErrorKind sql.NullString
	var failureReason sql.NullString
	var retryable sql.NullBool
	err := tx.QueryRow(ctx,
		`SELECT provider_artifact_ref, runtime_network_policy_json, status,
		        failure_stage, last_error_kind, failure_reason, retryable
		   FROM environment_artifacts
		  WHERE workspace_id = $1
		    AND environment_id = $2
		    AND generation = $3
		  FOR SHARE`,
		string(ws),
		environmentID,
		generation,
	).Scan(&providerArtifactRef, &networkPolicyJSON, &status, &failureStage, &lastErrorKind, &failureReason, &retryable)
	if dbconnect.IsNoRows(err) {
		return "", SessionPreparationFailureUpdate{}, nil
	}
	if err != nil {
		return "", SessionPreparationFailureUpdate{}, err
	}
	network, err := parseSessionPreparationNetwork(networkPolicyJSON)
	if err != nil {
		return "", SessionPreparationFailureUpdate{}, err
	}
	preparation.Network = network
	if status == "ready" && providerArtifactRef.Valid {
		preparation.ProviderArtifactRef = providerArtifactRef.String
	}
	return status, SessionPreparationFailureUpdate{
		FailureStage:  nullableStringValue(failureStage),
		LastErrorKind: nullableStringValue(lastErrorKind),
		FailureReason: nullableStringValue(failureReason),
		Retryable:     retryable.Valid && retryable.Bool,
	}, nil
}

func parseSessionPreparationNetwork(raw sql.NullString) (NetworkSetup, error) {
	if !raw.Valid || raw.String == "" {
		return NetworkSetup{}, nil
	}
	var payload struct {
		Type             string `json:"type"`
		NetworkAllowList string `json:"network_allow_list"`
	}
	if err := json.Unmarshal([]byte(raw.String), &payload); err != nil {
		return NetworkSetup{}, err
	}
	return NetworkSetup{Type: payload.Type, NetworkAllowList: payload.NetworkAllowList}, nil
}

func rowsAffected(result interface {
	RowsAffected() (int64, error)
}) bool {
	if result == nil {
		return false
	}
	affected, err := result.RowsAffected()
	return err == nil && affected > 0
}

const sandboxSelectSQL = `SELECT
	storage_sequence, workspace_id, id, session_id, COALESCE(environment_id, ''), COALESCE(environment_generation, 0), status, provider, provider_sandbox_id,
	provider_metadata_json, created_at, updated_at, released_at, failed_at,
	startup_failure_reason, cleanup_status, cleanup_method, cleanup_error_kind,
	cleanup_retryable, cleanup_last_attempt_at, cleanup_next_attempt_at,
	cleanup_attempt_count, cleanup_lease_token, cleanup_lease_expires_at,
	COALESCE(machine_was_usable, FALSE), status_refreshed_at
	FROM sandboxes`

func scanSandboxRow(row interface{ Scan(dest ...any) error }) (*Sandbox, error) {
	var (
		sequence                  int64
		workspaceIDValue          string
		sandboxID                 string
		sessionID                 string
		environmentID             string
		environmentGeneration     int64
		statusValue               string
		provider                  string
		providerSandboxID         sql.NullString
		metadataJSON              string
		createdAtValue            time.Time
		updatedAtValue            time.Time
		releasedAtValue           sql.NullTime
		failedAtValue             sql.NullTime
		startupFailureReason      sql.NullString
		cleanupStatusValue        string
		cleanupMethod             sql.NullString
		cleanupErrorKind          sql.NullString
		cleanupRetryable          bool
		cleanupLastAttemptAtValue sql.NullTime
		cleanupNextAttemptAtValue sql.NullTime
		cleanupAttemptCount       int
		cleanupLeaseToken         sql.NullString
		cleanupLeaseExpiresAt     sql.NullTime
		machineWasUsable          bool
		statusRefreshedAtValue    sql.NullTime
	)
	err := row.Scan(
		&sequence,
		&workspaceIDValue,
		&sandboxID,
		&sessionID,
		&environmentID,
		&environmentGeneration,
		&statusValue,
		&provider,
		&providerSandboxID,
		&metadataJSON,
		&createdAtValue,
		&updatedAtValue,
		&releasedAtValue,
		&failedAtValue,
		&startupFailureReason,
		&cleanupStatusValue,
		&cleanupMethod,
		&cleanupErrorKind,
		&cleanupRetryable,
		&cleanupLastAttemptAtValue,
		&cleanupNextAttemptAtValue,
		&cleanupAttemptCount,
		&cleanupLeaseToken,
		&cleanupLeaseExpiresAt,
		&machineWasUsable,
		&statusRefreshedAtValue,
	)
	_ = sequence
	if dbconnect.IsNoRows(err) {
		return nil, &NotFoundError{Message: "sandbox not found"}
	}
	if err != nil {
		return nil, err
	}
	status := Status(statusValue)
	if err := validateStatus(status); err != nil {
		return nil, err
	}
	metadata := map[string]string{}
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return nil, err
		}
	}
	createdAt := createdAtValue.UTC()
	updatedAt := updatedAtValue.UTC()
	var releasedAt *time.Time
	if releasedAtValue.Valid {
		parsed := releasedAtValue.Time.UTC()
		releasedAt = &parsed
	}
	var failedAt *time.Time
	if failedAtValue.Valid {
		parsed := failedAtValue.Time.UTC()
		failedAt = &parsed
	}
	cleanupStatus := CleanupStatus(cleanupStatusValue)
	if cleanupStatus == "" {
		cleanupStatus = CleanupStatusNone
	}
	if err := validateCleanupStatus(cleanupStatus); err != nil {
		return nil, err
	}
	var cleanupLastAttemptAt *time.Time
	if cleanupLastAttemptAtValue.Valid {
		value := cleanupLastAttemptAtValue.Time.UTC()
		cleanupLastAttemptAt = &value
	}
	var cleanupNextAttemptAt *time.Time
	if cleanupNextAttemptAtValue.Valid {
		value := cleanupNextAttemptAtValue.Time.UTC()
		cleanupNextAttemptAt = &value
	}
	var cleanupLeaseExpiry *time.Time
	if cleanupLeaseExpiresAt.Valid {
		value := cleanupLeaseExpiresAt.Time.UTC()
		cleanupLeaseExpiry = &value
	}
	var statusRefreshedAt *time.Time
	if statusRefreshedAtValue.Valid {
		value := statusRefreshedAtValue.Time.UTC()
		statusRefreshedAt = &value
	}
	handle := ProviderHandle{Provider: provider, Metadata: cloneStringMap(metadata)}
	if providerSandboxID.Valid {
		handle.SandboxID = providerSandboxID.String
	}
	return &Sandbox{
		ID:                    sandboxID,
		WorkspaceID:           workspace.ID(workspaceIDValue),
		SessionID:             sessionID,
		EnvironmentID:         environmentID,
		EnvironmentGeneration: environmentGeneration,
		Status:                status,
		Provider:              provider,
		ProviderSandboxID:     nullableStringValue(providerSandboxID),
		ProviderMetadata:      metadata,
		ProviderHandle:        handle,
		StartupFailureReason:  nullableStringValue(startupFailureReason),
		CleanupStatus:         cleanupStatus,
		CleanupMethod:         nullableStringValue(cleanupMethod),
		CleanupErrorKind:      nullableStringValue(cleanupErrorKind),
		CleanupRetryable:      cleanupRetryable,
		CleanupLastAttemptAt:  cleanupLastAttemptAt,
		CleanupNextAttemptAt:  cleanupNextAttemptAt,
		CleanupAttemptCount:   cleanupAttemptCount,
		CleanupLeaseToken:     nullableStringValue(cleanupLeaseToken),
		CleanupLeaseExpiresAt: cleanupLeaseExpiry,
		MachineWasUsable:      machineWasUsable,
		StatusRefreshedAt:     statusRefreshedAt,
		CreatedAt:             createdAt,
		UpdatedAt:             updatedAt,
		ReleasedAt:            releasedAt,
		FailedAt:              failedAt,
	}, nil
}

func validateCleanupStatus(status CleanupStatus) error {
	switch status {
	case CleanupStatusNone, CleanupStatusPending, CleanupStatusInProgress, CleanupStatusReleased, CleanupStatusRetryableFailed, CleanupStatusPermanentFailed:
		return nil
	default:
		return &ValidationError{Message: "invalid sandbox cleanup status"}
	}
}

func nonEmptyCleanupStatus(status CleanupStatus) CleanupStatus {
	if status == "" {
		return CleanupStatusNone
	}
	return status
}

func validateStatus(status Status) error {
	switch status {
	case StatusCreating, StatusActive, StatusStopped, StatusArchived, StatusResuming, StatusReleasing, StatusReleased, StatusFailed:
		return nil
	default:
		return &ValidationError{Message: "invalid sandbox status"}
	}
}

func nonNilStringMap(in map[string]string) map[string]string {
	if in == nil {
		return map[string]string{}
	}
	return cloneStringMap(in)
}

func nullableStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullableEmptyString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func mapPostgreSQLError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return &ConflictError{Message: "sandbox current lifecycle constraint violated"}
	}
	return err
}
