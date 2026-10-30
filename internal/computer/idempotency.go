package computer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// IdempotencyRecord is the result identity of the request that wrote it:
// the Session of a creation, or the Session and computer of an addition.
type IdempotencyRecord struct {
	Fingerprint [sha256.Size]byte
	SessionID   string
	ComputerID  string
}

// FindIdempotencyRecord returns the record of scope, which the instance
// routes' rules define; a record lives as long as its Session.
func FindIdempotencyRecord(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, scope instance.IdempotencyScope) (IdempotencyRecord, bool, error) {
	var record IdempotencyRecord
	var fingerprint []byte
	var computerID sql.NullString
	err := tx.QueryRow(ctx, `SELECT fingerprint,session_id,computer_id FROM computer_idempotency_records
		WHERE workspace_id=$1 AND principal_credential_id=$2 AND route=$3 AND idempotency_key=$4`,
		string(ws), scope.PrincipalCredentialID, scope.Route, []byte(scope.Key)).Scan(&fingerprint, &record.SessionID, &computerID)
	if errors.Is(err, sql.ErrNoRows) {
		return IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return IdempotencyRecord{}, false, err
	}
	copy(record.Fingerprint[:], fingerprint)
	record.ComputerID = computerID.String
	return record, true, nil
}

// SaveIdempotencyRecord writes the record of scope in the transaction of its
// effect. When a concurrent request with the same scope committed first, the
// insert fails with an error IsIdempotencyRace reports; the caller retries
// its transaction once and then finds that record.
func SaveIdempotencyRecord(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, scope instance.IdempotencyScope, record IdempotencyRecord) error {
	var computerID any
	if record.ComputerID != "" {
		computerID = record.ComputerID
	}
	_, err := tx.Exec(ctx, `INSERT INTO computer_idempotency_records
		(workspace_id,principal_credential_id,route,idempotency_key,fingerprint,session_id,computer_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		string(ws), scope.PrincipalCredentialID, scope.Route, []byte(scope.Key), record.Fingerprint[:], record.SessionID, computerID)
	return err
}

// IsIdempotencyRace reports the unique violation of a record that a
// concurrent identical request committed first.
func IsIdempotencyRace(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "computer_idempotency_records_pkey"
}

// Fingerprint is instance.Fingerprint of body with the value of every
// authorization_token member replaced by its hex SHA-256, so no record holds
// a repository token. ok is false when body is not one JSON value or an
// object repeats a key.
func Fingerprint(body []byte) (fingerprint [sha256.Size]byte, ok bool) {
	if _, ok := instance.Fingerprint(body); !ok {
		return fingerprint, false
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fingerprint, false
	}
	hashed, err := json.Marshal(hashTokens(value))
	if err != nil {
		return fingerprint, false
	}
	return instance.Fingerprint(hashed)
}

func hashTokens(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, member := range typed {
			if token, ok := member.(string); ok && key == "authorization_token" {
				digest := sha256.Sum256([]byte(token))
				typed[key] = hex.EncodeToString(digest[:])
				continue
			}
			typed[key] = hashTokens(member)
		}
	case []any:
		for i, item := range typed {
			typed[i] = hashTokens(item)
		}
	}
	return value
}
