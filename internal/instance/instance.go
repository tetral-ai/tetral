package instance

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// UnavailableError answers an issuance on a deregistered instance.
type UnavailableError struct{}

func (*UnavailableError) Error() string { return "the instance is deregistered" }

// EventChannel is the PostgreSQL notification channel of instance events.
// Its payload is an EventNotice; the receiver reads the instance object
// itself.
const EventChannel = "tetral_hub_instance_events"

// Instance events, the SSE event names of the instance stream.
const (
	EventCredentialReused = "credential_reused" //nolint:gosec // G101: event name, not credential material.
	EventUnused           = "unused"
)

// EventNotice is the payload of one EventChannel notification.
type EventNotice struct {
	WorkspaceID string `json:"workspace_id"`
	InstanceID  string `json:"instance_id"`
	Event       string `json:"event"`
}

// Now returns the database clock, the time base of every issuance, expiry
// and observation.
func Now(ctx context.Context, tx *dbconnect.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}

// LockInstance reads the instance and holds its row lock until the
// transaction ends, so issuances on one instance run one at a time.
func LockInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `SELECT `+instanceColumns+` FROM instances WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, string(ws), instanceID))
}

// CreateUserInstance creates an active user instance named name.
func CreateUserInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, name string) (Instance, error) {
	return InsertInstance(ctx, tx, ws, NewInstance{Kind: KindUser, Name: &name})
}

// Deregister marks an active instance deregistered at the database clock and
// revokes every credential not yet revoked with cause deregistration. It
// returns the credentials it revoked; an instance already deregistered is
// left unchanged and returns none.
func Deregister(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) ([]string, error) {
	current, err := LockInstance(ctx, tx, ws, instanceID)
	if err != nil {
		return nil, err
	}
	if current.RegistrationState == RegistrationDeregistered {
		return nil, nil
	}
	now, err := Now(ctx, tx)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE instances SET registration_state='deregistered',deregistered_at=$3 WHERE workspace_id=$1 AND id=$2`, string(ws), instanceID, now); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `UPDATE instance_credentials SET state='revoked',revoked_at=$3,revocation_cause='deregistration'
		WHERE workspace_id=$1 AND instance_id=$2 AND revoked_at IS NULL RETURNING id`, string(ws), instanceID, now)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var revoked []string
	for rows.Next() {
		var credentialID string
		if err := rows.Scan(&credentialID); err != nil {
			return nil, err
		}
		revoked = append(revoked, credentialID)
	}
	return revoked, rows.Err()
}

// ListInstances returns up to limit instances after storage sequence after,
// deregistered tombstones included, in creation order.
func ListInstances(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, after int64, limit int) ([]Instance, error) {
	rows, err := tx.Query(ctx, `SELECT `+instanceColumns+` FROM instances WHERE workspace_id=$1 AND storage_sequence>$2 ORDER BY storage_sequence LIMIT $3`, string(ws), after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var instances []Instance
	for rows.Next() {
		value, err := scanInstance(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, value)
	}
	return instances, rows.Err()
}

// NotifyInstanceEvent queues event for instanceID on EventChannel; PostgreSQL
// delivers it to the listeners when tx commits.
func NotifyInstanceEvent(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, event string) error {
	payload, err := json.Marshal(EventNotice{WorkspaceID: string(ws), InstanceID: instanceID, Event: event})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, EventChannel, string(payload))
	return err
}

// InstanceObject is the public instance object. The connection, CLI and
// count fields hold their values before any connection.
type InstanceObject struct {
	ID                  string           `json:"id"`
	Kind                string           `json:"kind"`
	ComputerID          *string          `json:"computer_id"`
	Name                *string          `json:"name"`
	RegistrationState   string           `json:"registration_state"`
	Unused              bool             `json:"unused"`
	Connection          json.RawMessage  `json:"connection"`
	CLIVersion          *string          `json:"cli_version"`
	CLIUpdateAvailable  bool             `json:"cli_update_available"`
	ComputerCount       int              `json:"computer_count"`
	BindingCount        int              `json:"binding_count"`
	LastCredentialReuse *CredentialReuse `json:"last_credential_reuse"`
	ObservedAt          string           `json:"observed_at"`
}

// CredentialReuse is the latest use of a credential that a pending
// credential's first use revoked.
type CredentialReuse struct {
	CredentialID string `json:"credential_id"`
	DetectedAt   string `json:"detected_at"`
}

// ObjectOf returns the instance object observed at observedAt, the database
// clock of the read.
func ObjectOf(value Instance, observedAt time.Time) InstanceObject {
	return InstanceObject{
		ID:                value.ID,
		Kind:              value.Kind,
		Name:              value.Name,
		RegistrationState: value.RegistrationState,
		ObservedAt:        FormatTime(observedAt),
	}
}

// FormatTime renders a time as the API does: RFC 3339 in UTC.
func FormatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
