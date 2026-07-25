package environment

import (
	"database/sql"
	"encoding/json"
	"time"
)

// finalizeEnvironment decodes canonical JSON columns into the
// Environment value and parses RFC3339 timestamps.
// Centralizes the "stored row → Environment value" projection so PG
// store code does not duplicate parsing logic.
func finalizeEnvironment(env *Environment, configJSON string, metadataJSON string, archivedAt sql.NullTime, createdAt time.Time, updatedAt time.Time) (*Environment, error) {
	env.Type = "environment"

	if err := json.Unmarshal([]byte(configJSON), &env.Config); err != nil {
		return nil, err
	}
	cfg, err := NormalizeEnvironmentConfig(env.Config)
	if err != nil {
		return nil, err
	}
	env.Config = cfg

	if err := json.Unmarshal([]byte(metadataJSON), &env.Metadata); err != nil {
		return nil, err
	}
	env.Metadata = normalizeMetadata(env.Metadata)

	env.CreatedAt = createdAt.UTC()
	env.UpdatedAt = updatedAt.UTC()
	if archivedAt.Valid {
		parsedArchivedAt := archivedAt.Time.UTC()
		env.ArchivedAt = &parsedArchivedAt
	}

	return env, nil
}
