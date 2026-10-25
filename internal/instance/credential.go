package instance

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// CredentialLimit is the most credentials an instance holds that are neither
// revoked nor expired.
const CredentialLimit = 2

// The delivery modes of an issuance on an existing instance.
const (
	DeliveryStream   = "stream"
	DeliveryResponse = "response"
)

// CredentialLimitError answers an issuance on an instance that already holds
// CredentialLimit usable credentials. No credential is revoked to make room.
type CredentialLimitError struct{}

func (*CredentialLimitError) Error() string {
	return "the instance already holds 2 credentials that are neither revoked nor expired"
}

// OfflineError answers a stream delivery to an instance without an online
// connection.
type OfflineError struct{}

func (*OfflineError) Error() string { return "the instance has no online connection" }

// ResolveExpiry returns the expiry of a credential issued at issuedAt:
// requested, truncated to the microsecond the database stores, when it is
// later than issuedAt and at most maxLifetime after it; issuedAt plus
// maxLifetime when requested is nil. ok is false for any other requested time.
func ResolveExpiry(issuedAt time.Time, requested *time.Time, maxLifetime time.Duration) (expiresAt time.Time, ok bool) {
	latest := issuedAt.Add(maxLifetime)
	if requested == nil {
		return latest, true
	}
	expiresAt = requested.UTC().Truncate(time.Microsecond)
	return expiresAt, expiresAt.After(issuedAt) && !expiresAt.After(latest)
}

// CheckCredentialLimit locks the instance row and answers
// CredentialLimitError when the instance already holds CredentialLimit
// pending or active credentials that expire after now, the database clock of
// tx. The lock holds until tx ends, so no concurrent issuance counts the same
// rows.
func CheckCredentialLimit(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, now time.Time) error {
	if _, err := LockInstance(ctx, tx, ws, instanceID); err != nil {
		return err
	}
	var usable int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM instance_credentials
		WHERE workspace_id=$1 AND instance_id=$2 AND state IN ('pending','active') AND expires_at>$3`, string(ws), instanceID, now).Scan(&usable); err != nil {
		return err
	}
	if usable >= CredentialLimit {
		return &CredentialLimitError{}
	}
	return nil
}

// IssueCredential creates a credential for instanceID in state, issued at
// issuedAt, the database clock of tx, after CheckCredentialLimit; at the
// limit it writes nothing. The returned key is the only copy of its
// plaintext.
func IssueCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, state string, issuedAt, expiresAt time.Time) (Credential, ConnectionKey, error) {
	if err := CheckCredentialLimit(ctx, tx, ws, instanceID, issuedAt); err != nil {
		return Credential{}, ConnectionKey{}, err
	}
	key := GenerateKey()
	credential, err := InsertCredential(ctx, tx, ws, NewCredential{InstanceID: instanceID, State: state, Key: key, IssuedAt: issuedAt, ExpiresAt: expiresAt})
	if err != nil {
		return Credential{}, ConnectionKey{}, err
	}
	return credential, key, nil
}

// RevokeCredential revokes credentialID of instanceID at the database clock
// with cause delete. It reports false and changes nothing when the
// credential is already revoked or expired, and answers ErrNotFound when the
// instance holds no such credential.
func RevokeCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string) (bool, error) {
	var revoked string
	err := tx.QueryRow(ctx, `UPDATE instance_credentials SET state='revoked',revoked_at=clock_timestamp(),revocation_cause='delete'
		WHERE workspace_id=$1 AND instance_id=$2 AND id=$3 AND revoked_at IS NULL AND expires_at>clock_timestamp() RETURNING id`,
		string(ws), instanceID, credentialID).Scan(&revoked)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if _, err := ReadInstanceCredential(ctx, tx, ws, instanceID, credentialID); err != nil {
		return false, err
	}
	return false, nil
}

// ReadInstanceCredential reads credentialID only when instanceID holds it.
func ReadInstanceCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string) (Credential, error) {
	return scanCredential(tx.QueryRow(ctx, `SELECT `+credentialColumns+` FROM instance_credentials WHERE workspace_id=$1 AND instance_id=$2 AND id=$3`, string(ws), instanceID, credentialID))
}

// ShareCredential reads credentialID of instanceID and holds a share lock on
// its row until the transaction ends, so a revocation waits for the request.
func ShareCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string) (Credential, error) {
	return scanCredential(tx.QueryRow(ctx, `SELECT `+credentialColumns+` FROM instance_credentials WHERE workspace_id=$1 AND instance_id=$2 AND id=$3 FOR SHARE`, string(ws), instanceID, credentialID))
}

// ListCredentials returns up to limit of instanceID's credentials after
// storage sequence after, in creation order.
func ListCredentials(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, after int64, limit int) ([]Credential, error) {
	rows, err := tx.Query(ctx, `SELECT `+credentialColumns+` FROM instance_credentials WHERE workspace_id=$1 AND instance_id=$2 AND storage_sequence>$3 ORDER BY storage_sequence LIMIT $4`, string(ws), instanceID, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var credentials []Credential
	for rows.Next() {
		value, err := scanCredential(rows)
		if err != nil {
			return nil, err
		}
		credentials = append(credentials, value)
	}
	return credentials, rows.Err()
}

// LiveCredentialID returns the credential of instanceID's live connection at
// now, or "" when it has none.
func LiveCredentialID(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string, now time.Time) (string, error) {
	var credentialID string
	err := tx.QueryRow(ctx, `SELECT credential_id FROM instance_connections
		WHERE workspace_id=$1 AND instance_id=$2 AND ended_at IS NULL AND lease_expires_at>$3`, string(ws), instanceID, now).Scan(&credentialID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return credentialID, err
}

// CredentialState is the state a read shows at now, the database clock:
// revoked once revoked_at is set, expired once expires_at is not later than
// now, otherwise the stored pending or active.
func CredentialState(value Credential, now time.Time) string {
	switch {
	case value.RevokedAt != nil:
		return CredentialRevoked
	case !value.ExpiresAt.After(now):
		return CredentialExpired
	default:
		return value.State
	}
}

// CredentialObject is the public credential object of a read; it never holds
// a key or digest.
type CredentialObject struct {
	ID         string  `json:"id"`
	InstanceID string  `json:"instance_id"`
	State      string  `json:"state"`
	InUse      bool    `json:"in_use"`
	IssuedAt   string  `json:"issued_at"`
	ExpiresAt  string  `json:"expires_at"`
	RevokedAt  *string `json:"revoked_at"`
}

// IssuedCredentialObject is the credential object of an issuance response.
// ConnectionKey holds the key only in the first successful response and is
// null in every replay.
type IssuedCredentialObject struct {
	CredentialObject
	ConnectionKey *string `json:"connection_key"`
}

// CredentialObjectOf returns the credential object observed at now;
// liveCredentialID is the credential of the instance's live connection, or
// "" (LiveCredentialID).
func CredentialObjectOf(value Credential, liveCredentialID string, now time.Time) CredentialObject {
	object := CredentialObject{
		ID:         value.ID,
		InstanceID: value.InstanceID,
		State:      CredentialState(value, now),
		InUse:      value.ID == liveCredentialID,
		IssuedAt:   FormatTime(value.IssuedAt),
		ExpiresAt:  FormatTime(value.ExpiresAt),
	}
	if value.RevokedAt != nil {
		revokedAt := FormatTime(*value.RevokedAt)
		object.RevokedAt = &revokedAt
	}
	return object
}
