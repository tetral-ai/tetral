package agent

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// PostgreSQLAgentStore owns the PostgreSQL persistence primitives for
// current Agent rows and immutable Agent version rows. Agent business
// behavior lives in Service.
type PostgreSQLAgentStore struct {
	client *dbconnect.Client
}

type storedAgent struct {
	Agent      *Agent
	ConfigJSON string
	ConfigHash string
}

func NewPostgreSQLAgentStore(runtimeClient *dbconnect.Client) *PostgreSQLAgentStore {
	return &PostgreSQLAgentStore{client: runtimeClient}
}

func (s *PostgreSQLAgentStore) withWorkspaceTx(ctx context.Context, ws workspace.ID, fn func(Transaction) error) error {
	return s.client.WithWorkspaceTx(ctx, string(ws), "agent.transaction", func(tx *dbconnect.Tx) error {
		return fn(tx)
	})
}

func (s *PostgreSQLAgentStore) insertAgentSnapshot(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, cfg AgentConfig, canonicalBytes []byte, configHash string, createdAt time.Time) (string, error) {
	versionID := id.New("agv_")
	storedAt := createdAt
	if _, err := tx.Exec(ctx,
		`INSERT INTO agents (id, workspace_id, name, description, version, archived_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, 1, NULL, $5, $5)`,
		agentID, string(ws), cfg.Name, nullableStringValue(cfg.Description), storedAt,
	); err != nil {
		return "", mapPostgreSQLAgentError(err)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO agent_versions (id, workspace_id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, 1, $4, $5, $6)`,
		versionID, string(ws), agentID, string(canonicalBytes), configHash, storedAt,
	); err != nil {
		return "", mapPostgreSQLAgentError(err)
	}
	return versionID, nil
}

func (s *PostgreSQLAgentStore) loadCurrentAgent(ctx context.Context, tx Transaction, ws workspace.ID, agentID string) (*storedAgent, error) {
	row := tx.QueryRowScanner(ctx,
		`SELECT a.id, av.id, a.version, a.archived_at, a.created_at, a.updated_at, av.config_json, av.config_hash
		   FROM agents a JOIN agent_versions av
		        ON a.id = av.agent_id AND a.version = av.version
		  WHERE a.id = $1 AND a.workspace_id = $2 AND av.workspace_id = $2`,
		agentID, string(ws),
	)
	return scanStoredAgentRow(row, "agent not found")
}

func (s *PostgreSQLAgentStore) loadCurrentAgentForUpdate(ctx context.Context, tx Transaction, ws workspace.ID, agentID string) (*storedAgent, error) {
	row := tx.QueryRowScanner(ctx,
		`SELECT a.id, av.id, a.version, a.archived_at, a.created_at, a.updated_at, av.config_json, av.config_hash
		   FROM agents a JOIN agent_versions av
		        ON a.id = av.agent_id AND a.version = av.version
		  WHERE a.id = $1 AND a.workspace_id = $2 AND av.workspace_id = $2
		  FOR UPDATE OF a`,
		agentID, string(ws),
	)
	return scanStoredAgentRow(row, "agent not found")
}

func (s *PostgreSQLAgentStore) updateAgentSnapshot(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, expectedVersion int, cfg AgentConfig, canonicalBytes []byte, configHash string, updatedAt time.Time) (string, error) {
	newVersion := expectedVersion + 1
	versionID := id.New("agv_")
	storedUpdatedAt := updatedAt
	result, err := tx.Exec(ctx,
		`UPDATE agents SET version = $1, name = $2, description = $3, updated_at = $4
		  WHERE id = $5 AND version = $6 AND workspace_id = $7 AND archived_at IS NULL`,
		newVersion, cfg.Name, nullableStringValue(cfg.Description), storedUpdatedAt, agentID, expectedVersion, string(ws),
	)
	if err != nil {
		return "", mapPostgreSQLAgentError(err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return "", &ConflictError{Message: "agent version conflict"}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO agent_versions (id, workspace_id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		versionID, string(ws), agentID, newVersion, string(canonicalBytes), configHash, storedUpdatedAt,
	); err != nil {
		return "", mapPostgreSQLAgentError(err)
	}
	return versionID, nil
}

func (s *PostgreSQLAgentStore) archiveAgent(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, archivedAt time.Time) error {
	if _, err := tx.Exec(ctx,
		`UPDATE agents SET archived_at = $1
		  WHERE id = $2 AND workspace_id = $3 AND archived_at IS NULL`,
		archivedAt, agentID, string(ws),
	); err != nil {
		return mapPostgreSQLAgentError(err)
	}
	return nil
}

func (s *PostgreSQLAgentStore) loadAgentVersion(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, version int) (*Agent, error) {
	row := tx.QueryRowScanner(ctx,
		`SELECT a.id, av.id, av.version, a.archived_at, a.created_at, av.created_at, av.config_json
		   FROM agent_versions av
		   JOIN agents a ON a.id = av.agent_id
		  WHERE av.agent_id = $1 AND av.version = $2
		    AND a.workspace_id = $3 AND av.workspace_id = $3`,
		agentID, version, string(ws),
	)
	var (
		idValue    string
		versionID  string
		versionNum int
		archivedAt sql.NullTime
		createdAt  time.Time
		updatedAt  time.Time
		configJSON string
	)
	if err := row.Scan(&idValue, &versionID, &versionNum, &archivedAt, &createdAt, &updatedAt, &configJSON); err == sql.ErrNoRows {
		return nil, &NotFoundError{Message: "agent version not found"}
	} else if err != nil {
		return nil, err
	}
	return agentFromStoredConfig(idValue, versionID, versionNum, configJSON, archivedAt, createdAt, updatedAt)
}

func (s *PostgreSQLAgentStore) listCurrentAgents(ctx context.Context, tx Transaction, ws workspace.ID, options ListOptions) ([]*Agent, bool, error) {
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	hasLower := options.CreatedAtGTE != nil
	lower := time.Unix(0, 0).UTC()
	if options.CreatedAtGTE != nil {
		lower = *options.CreatedAtGTE
	}
	hasUpper := options.CreatedAtLTE != nil
	upper := time.Unix(0, 0).UTC()
	if options.CreatedAtLTE != nil {
		upper = *options.CreatedAtLTE
	}
	hasCursor := options.cursorID != ""
	cursorCreatedAt := time.Unix(0, 0).UTC()
	if options.cursorID != "" {
		cursorCreatedAt = options.cursorCreatedAt
	}
	rows, err := tx.QueryRows(ctx,
		`SELECT a.id, av.id, a.version, a.archived_at, a.created_at, a.updated_at, av.config_json, av.config_hash
		   FROM agents a JOIN agent_versions av
		        ON a.id = av.agent_id AND a.version = av.version
		  WHERE a.workspace_id = $1 AND av.workspace_id = $1
		    AND ($2 OR a.archived_at IS NULL)
		    AND (NOT $3 OR a.created_at >= $4)
		    AND (NOT $5 OR a.created_at <= $6)
		    AND (NOT $7 OR (a.created_at, a.id) > ($8, $9))
		  ORDER BY a.created_at ASC, a.id ASC LIMIT $10`,
		string(ws),
		options.IncludeArchived,
		hasLower,
		lower,
		hasUpper,
		upper,
		hasCursor,
		cursorCreatedAt,
		options.cursorID,
		limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	results := []*Agent{}
	for rows.Next() {
		stored, err := scanStoredAgentRows(rows)
		if err != nil {
			return nil, false, err
		}
		results = append(results, stored.Agent)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}
	return results, hasMore, nil
}

func (s *PostgreSQLAgentStore) listAgentVersions(ctx context.Context, tx Transaction, ws workspace.ID, agentID string, options ListVersionsOptions) ([]*Agent, bool, error) {
	limit := options.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if _, err := s.loadCurrentAgent(ctx, tx, ws, agentID); err != nil {
		return nil, false, err
	}
	rows, err := tx.QueryRows(ctx,
		`SELECT a.id, av.id, av.version, a.archived_at, a.created_at, av.created_at, av.config_json
		   FROM agent_versions av
		   JOIN agents a ON a.id = av.agent_id
		  WHERE av.agent_id = $1 AND av.version > $2
		    AND a.workspace_id = $3 AND av.workspace_id = $3
		  ORDER BY av.version ASC LIMIT $4`,
		agentID, options.cursorVersion, string(ws), limit+1,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	results := []*Agent{}
	for rows.Next() {
		var (
			idValue    string
			versionID  string
			versionNum int
			archivedAt sql.NullTime
			createdAt  time.Time
			updatedAt  time.Time
			configJSON string
		)
		if err := rows.Scan(&idValue, &versionID, &versionNum, &archivedAt, &createdAt, &updatedAt, &configJSON); err != nil {
			return nil, false, err
		}
		agentValue, err := agentFromStoredConfig(idValue, versionID, versionNum, configJSON, archivedAt, createdAt, updatedAt)
		if err != nil {
			return nil, false, err
		}
		results = append(results, agentValue)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}
	return results, hasMore, nil
}

type storedAgentScanner interface {
	Scan(dest ...any) error
}

func scanStoredAgentRow(row storedAgentScanner, notFoundMessage string) (*storedAgent, error) {
	var (
		agentID    string
		versionID  string
		version    int
		archivedAt sql.NullTime
		createdAt  time.Time
		updatedAt  time.Time
		configJSON string
		configHash string
	)
	err := row.Scan(&agentID, &versionID, &version, &archivedAt, &createdAt, &updatedAt, &configJSON, &configHash)
	if err == sql.ErrNoRows {
		return nil, &NotFoundError{Message: notFoundMessage}
	}
	if err != nil {
		return nil, err
	}
	agentValue, err := agentFromStoredConfig(agentID, versionID, version, configJSON, archivedAt, createdAt, updatedAt)
	if err != nil {
		return nil, err
	}
	return &storedAgent{Agent: agentValue, ConfigJSON: configJSON, ConfigHash: configHash}, nil
}

func scanStoredAgentRows(rows storedAgentScanner) (*storedAgent, error) {
	return scanStoredAgentRow(rows, "agent not found")
}

func nullableStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func mapPostgreSQLAgentError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return &ConflictError{Message: "agent unique constraint violated"}
	}
	return err
}
