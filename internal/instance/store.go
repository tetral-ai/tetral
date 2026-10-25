package instance

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	KindUser  = "user"
	KindCloud = "cloud"

	RegistrationActive       = "active"
	RegistrationDeregistered = "deregistered"

	CredentialPending = "pending"
	CredentialActive  = "active"
	CredentialRevoked = "revoked"
)

// ErrNotFound is returned when no row with the ID exists in the transaction's
// Workspace.
var ErrNotFound = errors.New("instance: not found")

// Instance is one instances row.
type Instance struct {
	WorkspaceID       workspace.ID
	ID                string
	Kind              string
	Name              *string
	RegistrationState string
	CreatedAt         time.Time
	DeregisteredAt    *time.Time
	StorageSequence   int64
}

// Credential is one instance_credentials row without its digest, which no
// read returns. State is the stored state; expiry is read from ExpiresAt.
type Credential struct {
	WorkspaceID     workspace.ID
	InstanceID      string
	ID              string
	State           string
	IssuedAt        time.Time
	ExpiresAt       time.Time
	RevokedAt       *time.Time
	RevocationCause *string
	StorageSequence int64
}

// NewInstance describes an instance row to insert. Name is set for a user
// instance and nil for a cloud instance.
type NewInstance struct {
	Kind string
	Name *string
}

// NewCredential describes a credential row to insert; only the key's digest
// is written.
type NewCredential struct {
	InstanceID string
	State      string
	Key        ConnectionKey
	ExpiresAt  time.Time
}

const instanceColumns = `workspace_id,id,kind,name,registration_state,created_at,deregistered_at,storage_sequence`
const credentialColumns = `workspace_id,instance_id,id,state,issued_at,expires_at,revoked_at,revocation_cause,storage_sequence`

// The row functions run inside a transaction scoped to ws
// (dbconnect.Client.WithWorkspaceTx), so Workspace row-level security bounds
// every statement.

// InsertInstance creates an active instance with a new inst_ ID.
func InsertInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, row NewInstance) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `INSERT INTO instances (workspace_id,id,kind,name,registration_state)
		VALUES ($1,$2,$3,$4,'active') RETURNING `+instanceColumns, string(ws), id.New("inst_"), row.Kind, row.Name))
}

// InsertCredential creates a credential with a new ccred_ ID, issued at the
// database clock.
func InsertCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, row NewCredential) (Credential, error) {
	return scanCredential(tx.QueryRow(ctx, `INSERT INTO instance_credentials (workspace_id,instance_id,id,state,key_digest,expires_at)
		VALUES ($1,$2,$3,$4,$5,$6) RETURNING `+credentialColumns, string(ws), row.InstanceID, id.New("ccred_"), row.State, row.Key.Digest(), row.ExpiresAt))
}

func ReadInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `SELECT `+instanceColumns+` FROM instances WHERE workspace_id=$1 AND id=$2`, string(ws), instanceID))
}

func ReadCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, credentialID string) (Credential, error) {
	return scanCredential(tx.QueryRow(ctx, `SELECT `+credentialColumns+` FROM instance_credentials WHERE workspace_id=$1 AND id=$2`, string(ws), credentialID))
}

func scanInstance(row *dbconnect.Row) (Instance, error) {
	var value Instance
	var name sql.NullString
	var deregistered sql.NullTime
	err := row.Scan(&value.WorkspaceID, &value.ID, &value.Kind, &name, &value.RegistrationState, &value.CreatedAt, &deregistered, &value.StorageSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return Instance{}, ErrNotFound
	}
	if err != nil {
		return Instance{}, err
	}
	if name.Valid {
		value.Name = &name.String
	}
	if deregistered.Valid {
		at := deregistered.Time.UTC()
		value.DeregisteredAt = &at
	}
	value.CreatedAt = value.CreatedAt.UTC()
	return value, nil
}

func scanCredential(row *dbconnect.Row) (Credential, error) {
	var value Credential
	var revoked sql.NullTime
	var cause sql.NullString
	err := row.Scan(&value.WorkspaceID, &value.InstanceID, &value.ID, &value.State, &value.IssuedAt, &value.ExpiresAt, &revoked, &cause, &value.StorageSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, err
	}
	if revoked.Valid {
		at := revoked.Time.UTC()
		value.RevokedAt = &at
	}
	if cause.Valid {
		value.RevocationCause = &cause.String
	}
	value.IssuedAt, value.ExpiresAt = value.IssuedAt.UTC(), value.ExpiresAt.UTC()
	return value, nil
}
