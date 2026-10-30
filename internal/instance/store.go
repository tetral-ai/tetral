package instance

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
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
	// CredentialExpired is only read, never stored (CredentialState).
	CredentialExpired = "expired"
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
	// CLIVersion is the cli_version of the instance's latest ready.
	CLIVersion *string
	// LastReuse is the latest use of a credential that a pending
	// credential's activation revoked, nil before the first.
	LastReuse *Reuse
	// ComputerID and SessionID name, for a cloud instance, the computer
	// whose claim created it; nil for a user instance. Unused is the
	// instance object's unused (SetUnused).
	ComputerID      *string
	SessionID       *string
	Unused          bool
	StorageSequence int64
}

// Reuse is one recorded use of a credential that an activation revoked.
type Reuse struct {
	CredentialID string
	DetectedAt   time.Time
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
	// Delivery is DeliveryResponse, DeliveryStream or DeliveryClaim. A stream delivery
	// renews the connection RenewalConnectionID, replacing
	// ReplacesCredentialID; FetchedAt is when its key was fetched.
	Delivery             string
	RenewalConnectionID  *string
	ReplacesCredentialID *string
	FetchedAt            *time.Time
	// UsedAt is the first authenticated use of a pending credential, which
	// activated it.
	UsedAt          *time.Time
	StorageSequence int64
}

const instanceColumns = `workspace_id,id,kind,name,registration_state,created_at,deregistered_at,cli_version,
 last_credential_reuse_credential_id,last_credential_reuse_at,computer_id,session_id,unused,storage_sequence`
const credentialColumns = `workspace_id,instance_id,id,state,issued_at,expires_at,revoked_at,revocation_cause,
 delivery,renewal_connection_id,replaces_credential_id,fetched_at,used_at,storage_sequence`

// The row functions run inside a transaction scoped to ws
// (dbconnect.Client.WithWorkspaceTx), so Workspace row-level security bounds
// every statement.

func ReadInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `SELECT `+instanceColumns+` FROM instances WHERE workspace_id=$1 AND id=$2`, string(ws), instanceID))
}

func ReadCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, credentialID string) (Credential, error) {
	return scanCredential(tx.QueryRow(ctx, `SELECT `+credentialColumns+` FROM instance_credentials WHERE workspace_id=$1 AND id=$2`, string(ws), credentialID))
}

type scanner interface{ Scan(dest ...any) error }

func scanInstance(row scanner) (Instance, error) {
	var value Instance
	var name, cliVersion, reused, computerID, sessionID sql.NullString
	var deregistered, reusedAt sql.NullTime
	err := row.Scan(&value.WorkspaceID, &value.ID, &value.Kind, &name, &value.RegistrationState, &value.CreatedAt, &deregistered, &cliVersion, &reused, &reusedAt,
		&computerID, &sessionID, &value.Unused, &value.StorageSequence)
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
	value.CLIVersion = stringOrNil(cliVersion)
	value.ComputerID, value.SessionID = stringOrNil(computerID), stringOrNil(sessionID)
	if reused.Valid {
		value.LastReuse = &Reuse{CredentialID: reused.String, DetectedAt: reusedAt.Time.UTC()}
	}
	value.CreatedAt = value.CreatedAt.UTC()
	return value, nil
}

func scanCredential(row scanner) (Credential, error) {
	var value Credential
	var revoked, fetched, used sql.NullTime
	var cause, renewal, replaces sql.NullString
	err := row.Scan(&value.WorkspaceID, &value.InstanceID, &value.ID, &value.State, &value.IssuedAt, &value.ExpiresAt, &revoked, &cause,
		&value.Delivery, &renewal, &replaces, &fetched, &used, &value.StorageSequence)
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
	value.RenewalConnectionID, value.ReplacesCredentialID = stringOrNil(renewal), stringOrNil(replaces)
	value.FetchedAt, value.UsedAt = timeOrNil(fetched), timeOrNil(used)
	value.IssuedAt, value.ExpiresAt = value.IssuedAt.UTC(), value.ExpiresAt.UTC()
	return value, nil
}
