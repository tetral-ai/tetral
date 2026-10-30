package instance

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
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

// ShareInstance reads the instance and holds a share lock on its row until
// the transaction ends, so a deregistration waits for the request.
func ShareInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `SELECT `+instanceColumns+` FROM instances WHERE workspace_id=$1 AND id=$2 FOR SHARE`, string(ws), instanceID))
}

// LockInstanceNoKeyUpdate reads the instance and locks its row FOR NO KEY
// UPDATE until the transaction ends, for a request that updates the row's
// columns: it waits for share locks but not for the key share locks of
// inserts that reference the instance.
func LockInstanceNoKeyUpdate(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `SELECT `+instanceColumns+` FROM instances WHERE workspace_id=$1 AND id=$2 FOR NO KEY UPDATE`, string(ws), instanceID))
}

// CreateUserInstance creates an active user instance named name with a new
// inst_ ID.
func CreateUserInstance(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, name string) (Instance, error) {
	return scanInstance(tx.QueryRow(ctx, `INSERT INTO instances (workspace_id,id,kind,name,registration_state)
		VALUES ($1,$2,$3,$4,'active') RETURNING `+instanceColumns, string(ws), id.New("inst_"), KindUser, name))
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

// InstanceObject is the public instance object.
type InstanceObject struct {
	ID                  string            `json:"id"`
	Kind                string            `json:"kind"`
	ComputerID          *string           `json:"computer_id"`
	Name                *string           `json:"name"`
	RegistrationState   string            `json:"registration_state"`
	Unused              bool              `json:"unused"`
	Connection          *ConnectionObject `json:"connection"`
	CLIVersion          *string           `json:"cli_version"`
	CLIUpdateAvailable  bool              `json:"cli_update_available"`
	ComputerCount       int               `json:"computer_count"`
	BindingCount        int               `json:"binding_count"`
	LastCredentialReuse *CredentialReuse  `json:"last_credential_reuse"`
	ObservedAt          string            `json:"observed_at"`
}

// CredentialReuse is the latest use of a credential that a pending
// credential's first use revoked.
type CredentialReuse struct {
	CredentialID string `json:"credential_id"`
	DetectedAt   string `json:"detected_at"`
}

// ReadObjects returns the instance objects of values observed at now, the
// database clock of tx: each one's latest connection, the capabilities of
// its latest accepted ready, and cli_update_available against
// recommendedCLI, the recommended CLI version.
func ReadObjects(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, values []Instance, recommendedCLI string, now time.Time) ([]InstanceObject, error) {
	ids := make([]string, len(values))
	for i, value := range values {
		ids[i] = value.ID
	}
	connections, capabilities, err := connectionFacts(ctx, tx, ws, ids)
	if err != nil {
		return nil, err
	}
	computers, err := computerCounts(ctx, tx, ws, ids)
	if err != nil {
		return nil, err
	}
	objects := make([]InstanceObject, len(values))
	for i, value := range values {
		objects[i] = InstanceObject{
			ID:                 value.ID,
			Kind:               value.Kind,
			Name:               value.Name,
			RegistrationState:  value.RegistrationState,
			CLIVersion:         value.CLIVersion,
			CLIUpdateAvailable: value.CLIVersion != nil && VersionBelow(*value.CLIVersion, recommendedCLI),
			ComputerCount:      computers[value.ID],
			ObservedAt:         FormatTime(now),
		}
		if connection, ok := connections[value.ID]; ok {
			objects[i].Connection = connectionObjectOf(connection, capabilities[value.ID], now)
		}
		if value.LastReuse != nil {
			objects[i].LastCredentialReuse = &CredentialReuse{CredentialID: value.LastReuse.CredentialID, DetectedAt: FormatTime(value.LastReuse.DetectedAt)}
		}
	}
	return objects, nil
}

// computerCounts returns, per instance, the attached computers whose
// current instance it is.
func computerCounts(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceIDs []string) (map[string]int, error) {
	rows, err := tx.Query(ctx, `SELECT current_instance_id,count(*) FROM computers
		WHERE workspace_id=$1 AND current_instance_id=ANY($2) AND association_state='attached' GROUP BY current_instance_id`, string(ws), instanceIDs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	counts := map[string]int{}
	for rows.Next() {
		var instanceID string
		var count int
		if err := rows.Scan(&instanceID, &count); err != nil {
			return nil, err
		}
		counts[instanceID] = count
	}
	return counts, rows.Err()
}

// ReadObject returns the instance object of value observed at now.
func ReadObject(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, value Instance, recommendedCLI string, now time.Time) (InstanceObject, error) {
	objects, err := ReadObjects(ctx, tx, ws, []Instance{value}, recommendedCLI, now)
	if err != nil {
		return InstanceObject{}, err
	}
	return objects[0], nil
}

// FormatTime renders a time as the API does: RFC 3339 in UTC.
func FormatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
