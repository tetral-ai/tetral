package environment

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// PostgreSQLEnvironmentStore implements Environment template CRUD
// against the PostgreSQL `environments` table.
type PostgreSQLEnvironmentStore struct {
	client             *dbconnect.Client
	defaultArtifactRef string
}

// StoreOption configures PostgreSQLEnvironmentStore.
type StoreOption func(*PostgreSQLEnvironmentStore)

// WithDefaultArtifactRef sets the prebuilt provider artifact used by Cloud
// environments with no custom packages.
func WithDefaultArtifactRef(ref string) StoreOption {
	return func(s *PostgreSQLEnvironmentStore) {
		s.defaultArtifactRef = ref
	}
}

// NewPostgreSQLEnvironmentStore constructs the PG-backed environment
// store.
func NewPostgreSQLEnvironmentStore(runtimeClient *dbconnect.Client, options ...StoreOption) *PostgreSQLEnvironmentStore {
	store := &PostgreSQLEnvironmentStore{client: runtimeClient}
	for _, option := range options {
		option(store)
	}
	return store
}

func (s *PostgreSQLEnvironmentStore) Create(ctx context.Context, ws workspace.ID, request CreateEnvironmentRequest) (*Environment, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	normalized, err := NormalizeCreateEnvironmentRequest(request)
	if err != nil {
		return nil, err
	}
	now := storage.Now()
	environmentID := id.New("env_")
	configJSON, err := marshalStoredEnvironmentConfig(normalized.Config)
	if err != nil {
		return nil, err
	}
	metadataJSON, err := json.Marshal(normalized.Metadata)
	if err != nil {
		return nil, err
	}
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "environment.create", func(tx *dbconnect.Tx) error {
		_, execErr := tx.Exec(ctx,
			`INSERT INTO environments (id, workspace_id, name, description, config_json, metadata_json, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			environmentID, string(ws), normalized.Name, normalized.Description, string(configJSON), string(metadataJSON), now.Format(time.RFC3339),
		)
		if execErr != nil {
			var pgErr *pgconn.PgError
			if errors.As(execErr, &pgErr) && pgErr.Code == "23505" {
				return &ConflictError{Message: "environment name '" + normalized.Name + "' already exists"}
			}
			return execErr
		}
		return s.createEnvironmentArtifactAdmission(ctx, tx, ws, environmentID, 1, normalized.Config, now)
	}); err != nil {
		return nil, err
	}
	return &Environment{
		ID:                environmentID,
		Type:              "environment",
		Name:              normalized.Name,
		Description:       normalized.Description,
		Config:            normalized.Config,
		Metadata:          normalized.Metadata,
		CurrentGeneration: 1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}, nil
}

func (s *PostgreSQLEnvironmentStore) Get(ctx context.Context, ws workspace.ID, environmentID string) (*Environment, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Environment
	err := s.client.WithWorkspaceTx(ctx, string(ws), "environment.get", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT id, name, description, config_json, current_generation, metadata_json, archived_at, created_at, updated_at
			   FROM environments WHERE id = $1 AND workspace_id = $2`,
			environmentID, string(ws),
		)
		env, scanErr := scanEnvironmentRow(row)
		if scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "environment not found"}
		}
		if scanErr != nil {
			return scanErr
		}
		result = env
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLEnvironmentStore) List(ctx context.Context, ws workspace.ID, options ListOptions) (ListResult, error) {
	if ws == "" {
		return ListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	if options.Limit <= 0 {
		options.Limit = 20
	}
	if options.Limit > 100 {
		options.Limit = 100
	}
	var lastSequence int64
	if options.Page != "" {
		token, err := decodeEnvironmentPageToken(options.Page, ws, options.IncludeArchived)
		if err != nil {
			return ListResult{}, err
		}
		lastSequence = token.LastSequence
	}
	var (
		rowsRead []environmentListRow
		nextPage *string
	)
	err := s.client.WithWorkspaceTx(ctx, string(ws), "environment.list", func(tx *dbconnect.Tx) error {
		query := `SELECT storage_sequence, id, name, description, config_json, current_generation, metadata_json, archived_at, created_at, updated_at
		            FROM environments
		           WHERE workspace_id = $1
		             AND storage_sequence > $2`
		args := []any{string(ws), lastSequence}
		if !options.IncludeArchived {
			query += ` AND archived_at IS NULL`
		}
		query += ` ORDER BY storage_sequence ASC LIMIT $3`
		args = append(args, options.Limit+1)
		rows, queryErr := tx.Query(ctx, query, args...)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			item, scanErr := scanEnvironmentListRow(rows)
			if scanErr != nil {
				return scanErr
			}
			rowsRead = append(rowsRead, item)
		}
		return rows.Err()
	})
	if err != nil {
		return ListResult{}, err
	}
	hasMore := len(rowsRead) > options.Limit
	if hasMore {
		rowsRead = rowsRead[:options.Limit]
		last := rowsRead[len(rowsRead)-1]
		token, err := encodeEnvironmentPageToken(environmentPageToken{
			Version:         environmentPageTokenVersion,
			Resource:        environmentPageTokenResource,
			WorkspaceID:     string(ws),
			IncludeArchived: options.IncludeArchived,
			LastSequence:    last.storageSequence,
		})
		if err != nil {
			return ListResult{}, err
		}
		nextPage = &token
	}
	results := make([]*Environment, 0, len(rowsRead))
	for _, row := range rowsRead {
		results = append(results, row.env)
	}
	return ListResult{Data: results, NextPage: nextPage}, nil
}

func (s *PostgreSQLEnvironmentStore) Update(ctx context.Context, ws workspace.ID, environmentID string, patch EnvironmentPatch) (*Environment, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var noChange *Environment
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "environment.update", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT id, name, description, config_json, current_generation, metadata_json, archived_at, created_at, updated_at
			   FROM environments WHERE id = $1 AND workspace_id = $2 FOR UPDATE`,
			environmentID, string(ws),
		)
		current, scanErr := scanEnvironmentRow(row)
		if scanErr != nil {
			return scanErr
		}
		if current.ArchivedAt != nil {
			return &ValidationError{Message: "environment is archived"}
		}
		materialized, materializeErr := patch.Materialize(*current)
		if materializeErr != nil {
			return materializeErr
		}
		if current.Name == materialized.Name && current.Description == materialized.Description && equalConfig(current.Config, materialized.Config) && equalMetadata(current.Metadata, materialized.Metadata) {
			noChange = current
			return nil
		}
		now := storage.Now()
		configJSON, marshalErr := marshalStoredEnvironmentConfig(materialized.Config)
		if marshalErr != nil {
			return marshalErr
		}
		metadataJSON, marshalErr := json.Marshal(materialized.Metadata)
		if marshalErr != nil {
			return marshalErr
		}
		nextGeneration := current.CurrentGeneration
		configChanged := !equalConfig(current.Config, materialized.Config)
		if configChanged {
			nextGeneration++
		}
		result, execErr := tx.Exec(ctx,
			`UPDATE environments SET name = $1, description = $2, config_json = $3, metadata_json = $4, current_generation = $5, updated_at = $6
			  WHERE id = $7 AND workspace_id = $8 AND archived_at IS NULL`,
			materialized.Name, materialized.Description, string(configJSON), string(metadataJSON), nextGeneration, now.Format(time.RFC3339),
			environmentID, string(ws),
		)
		if execErr != nil {
			var pgErr *pgconn.PgError
			if errors.As(execErr, &pgErr) && pgErr.Code == "23505" {
				return &ConflictError{Message: "environment name '" + materialized.Name + "' already exists"}
			}
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "environment " + environmentID + " not found"}
		}
		if configChanged {
			return s.createEnvironmentArtifactAdmission(ctx, tx, ws, environmentID, nextGeneration, materialized.Config, now)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if noChange != nil {
		return noChange, nil
	}
	return s.Get(ctx, ws, environmentID)
}

func (s *PostgreSQLEnvironmentStore) Archive(ctx context.Context, ws workspace.ID, environmentID string) (*Environment, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	now := storage.Now().Format(time.RFC3339)
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "environment.archive", func(tx *dbconnect.Tx) error {
		result, execErr := tx.Exec(ctx,
			`UPDATE environments
			    SET archived_at = COALESCE(archived_at, $1),
			        updated_at = CASE WHEN archived_at IS NULL THEN $1 ELSE updated_at END
			  WHERE id = $2 AND workspace_id = $3`,
			now, environmentID, string(ws),
		)
		if execErr != nil {
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "environment " + environmentID + " not found"}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return s.Get(ctx, ws, environmentID)
}

func (s *PostgreSQLEnvironmentStore) Delete(ctx context.Context, ws workspace.ID, environmentID string) (*DeleteResult, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "environment.delete", func(tx *dbconnect.Tx) error {
		var referenced int
		refErr := tx.QueryRow(ctx,
			`SELECT 1 FROM sessions WHERE workspace_id = $1 AND environment_id = $2 LIMIT 1`,
			string(ws), environmentID,
		).Scan(&referenced)
		if refErr != nil && refErr != sql.ErrNoRows {
			return refErr
		}
		if refErr == nil {
			return &ValidationError{Message: "environment has session references"}
		}
		result, execErr := tx.Exec(ctx,
			`DELETE FROM environments WHERE id = $1 AND workspace_id = $2`,
			environmentID, string(ws),
		)
		if execErr != nil {
			var pgErr *pgconn.PgError
			if errors.As(execErr, &pgErr) && (pgErr.Code == "23503" || pgErr.Code == "23001") {
				return &ValidationError{Message: "environment has session references"}
			}
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "environment " + environmentID + " not found"}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &DeleteResult{ID: environmentID, Type: "environment_deleted"}, nil
}

type environmentScanner interface {
	Scan(dest ...any) error
}

func scanEnvironmentRow(row environmentScanner) (*Environment, error) {
	var env Environment
	var configJSON, metadataJSON string
	var archivedAt sql.NullTime
	var createdAt, updatedAt time.Time
	err := row.Scan(&env.ID, &env.Name, &env.Description, &configJSON, &env.CurrentGeneration, &metadataJSON, &archivedAt, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, &NotFoundError{Message: "environment not found"}
	}
	if err != nil {
		return nil, err
	}
	return finalizeEnvironment(&env, configJSON, metadataJSON, archivedAt, createdAt, updatedAt)
}

type environmentListRow struct {
	storageSequence int64
	env             *Environment
}

func scanEnvironmentListRow(rows environmentScanner) (environmentListRow, error) {
	var storageSequence int64
	var env Environment
	var configJSON, metadataJSON string
	var archivedAt sql.NullTime
	var createdAt, updatedAt time.Time
	if err := rows.Scan(&storageSequence, &env.ID, &env.Name, &env.Description, &configJSON, &env.CurrentGeneration, &metadataJSON, &archivedAt, &createdAt, &updatedAt); err != nil {
		return environmentListRow{}, err
	}
	finalized, err := finalizeEnvironment(&env, configJSON, metadataJSON, archivedAt, createdAt, updatedAt)
	if err != nil {
		return environmentListRow{}, err
	}
	return environmentListRow{storageSequence: storageSequence, env: finalized}, nil
}

func equalConfig(a, b EnvironmentConfig) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

func equalMetadata(a, b StringMap) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

func (s *PostgreSQLEnvironmentStore) createEnvironmentArtifactAdmission(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string, generation int64, config EnvironmentConfig, now time.Time) error {
	artifact, err := environmentArtifactSnapshot(config)
	if err != nil {
		return err
	}
	if artifact.packagesEmpty {
		if s.defaultArtifactRef == "" {
			return &ValidationError{Message: "default environment artifact ref is required"}
		}
		return upsertEnvironmentArtifact(ctx, tx, ws, environmentID, generation, "ready", s.defaultArtifactRef, artifact, now)
	}
	if providerRef, ok, err := reusableEnvironmentArtifactRef(ctx, tx, ws, environmentID, artifact.artifactInputHash); err != nil {
		return err
	} else if ok {
		return upsertEnvironmentArtifact(ctx, tx, ws, environmentID, generation, "ready", providerRef, artifact, now)
	}
	if inFlight, err := reusableInFlightEnvironmentArtifact(ctx, tx, ws, environmentID, artifact.artifactInputHash); err != nil {
		return err
	} else if inFlight {
		return upsertEnvironmentArtifact(ctx, tx, ws, environmentID, generation, "pending", "", artifact, now)
	}
	if err := upsertEnvironmentArtifact(ctx, tx, ws, environmentID, generation, "pending", "", artifact, now); err != nil {
		return err
	}
	return enqueueEnvironmentBuild(ctx, tx, ws, environmentID, generation, now)
}

func reusableInFlightEnvironmentArtifact(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string, artifactInputHash string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM environment_artifacts
			 WHERE workspace_id = $1
			   AND environment_id = $2
			   AND status IN ('pending', 'building')
			   AND artifact_input_hash = $3
		)`,
		string(ws), environmentID, artifactInputHash,
	).Scan(&exists)
	return exists, err
}

func upsertEnvironmentArtifact(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string, generation int64, status string, providerArtifactRef string, artifact environmentArtifactSnapshotResult, now time.Time) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			normalized_config_hash, artifact_input_hash, runtime_network_policy_json,
			packages_json, provider_artifact_ref, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral', $5, $6, $7, $8, NULLIF($9, ''), $10, $10)
		ON CONFLICT (workspace_id, environment_id, generation) DO UPDATE SET
			status = EXCLUDED.status,
			provider_artifact_ref = EXCLUDED.provider_artifact_ref,
			normalized_config_hash = EXCLUDED.normalized_config_hash,
			artifact_input_hash = EXCLUDED.artifact_input_hash,
			runtime_network_policy_json = EXCLUDED.runtime_network_policy_json,
			packages_json = EXCLUDED.packages_json,
			failure_stage = NULL,
			last_error_kind = NULL,
			failure_reason = NULL,
			retryable = NULL,
			updated_at = EXCLUDED.updated_at`,
		string(ws), environmentID, generation, status,
		artifact.normalizedConfigHash,
		artifact.artifactInputHash,
		artifact.runtimeNetworkPolicyJSON,
		artifact.packagesJSON,
		providerArtifactRef,
		now.Format(time.RFC3339),
	); err != nil {
		return err
	}
	return nil
}

func reusableEnvironmentArtifactRef(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string, artifactInputHash string) (string, bool, error) {
	var providerRef string
	err := tx.QueryRow(ctx,
		`SELECT provider_artifact_ref
		   FROM environment_artifacts
		  WHERE workspace_id = $1
		    AND environment_id = $2
		    AND status = 'ready'
		    AND artifact_input_hash = $3
		    AND provider_artifact_ref IS NOT NULL
		  ORDER BY generation DESC
		  LIMIT 1`,
		string(ws), environmentID, artifactInputHash,
	).Scan(&providerRef)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return providerRef, true, nil
}

func enqueueEnvironmentBuild(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string, generation int64, now time.Time) error {
	payload, err := json.Marshal(map[string]string{
		"workspace_id":   string(ws),
		"environment_id": environmentID,
		"generation":     strconv.FormatInt(generation, 10),
	})
	if err != nil {
		return err
	}
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		WorkspaceID:    ws,
		Kind:           queue.KindEnvironmentBuild,
		PartitionKey:   queue.FormatEnvironmentPartitionKey(ws, environmentID),
		DedupeKey:      queue.FormatEnvironmentBuildDedupeKey(ws, environmentID, strconv.FormatInt(generation, 10)),
		PayloadVersion: 1,
		PayloadJSON:    payload,
		Now:            now,
	})
	return err
}

type environmentArtifactSnapshotResult struct {
	normalizedConfigHash     string
	artifactInputHash        string
	runtimeNetworkPolicyJSON string
	packagesJSON             string
	packagesEmpty            bool
}

func environmentArtifactSnapshot(config EnvironmentConfig) (environmentArtifactSnapshotResult, error) {
	normalized, err := NormalizeEnvironmentConfig(config)
	if err != nil {
		return environmentArtifactSnapshotResult{}, err
	}
	normalizedJSON, err := marshalStoredEnvironmentConfig(normalized)
	if err != nil {
		return environmentArtifactSnapshotResult{}, err
	}
	networkJSON, err := json.Marshal(normalized.Networking)
	if err != nil {
		return environmentArtifactSnapshotResult{}, err
	}
	packagesJSON, err := marshalStoredPackageMap(normalized.Packages)
	if err != nil {
		return environmentArtifactSnapshotResult{}, err
	}
	return environmentArtifactSnapshotResult{
		normalizedConfigHash:     sha256Hex(normalizedJSON),
		artifactInputHash:        sha256Hex(packagesJSON),
		runtimeNetworkPolicyJSON: string(networkJSON),
		packagesJSON:             string(packagesJSON),
		packagesEmpty:            packagesEmpty(normalized.Packages),
	}, nil
}

func marshalStoredEnvironmentConfig(config EnvironmentConfig) ([]byte, error) {
	normalized, err := NormalizeEnvironmentConfig(config)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Type       string              `json:"type"`
		Networking *NetworkingConfig   `json:"networking"`
		Packages   map[string][]string `json:"packages"`
	}{
		Type: normalized.Type, Networking: normalized.Networking,
		Packages: map[string][]string(normalized.Packages),
	})
}

func marshalStoredPackageMap(packages PackageMap) ([]byte, error) {
	return json.Marshal(map[string][]string(packages))
}

func packagesEmpty(packages PackageMap) bool {
	for _, entries := range packages {
		if len(entries) > 0 {
			return false
		}
	}
	return true
}

func sha256Hex(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
