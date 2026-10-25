package instance

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/mod/semver"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The causes of a connection's end.
const (
	EndLeaseExpired = "lease_expired"
	EndRevoked      = "revoked"
	EndDeregistered = "deregistered"
)

// The connection states of the instance read.
const (
	ConnectionRegistering = "registering"
	ConnectionOnline      = "online"
	ConnectionOffline     = "offline"
)

// RefusedCLIUpgradeRequired is the code of a ready refused because its CLI is
// older than the minimum version.
const RefusedCLIUpgradeRequired = "cli_upgrade_required"

// LeaseGrant is the longest one grant extends a lease: a new connection, an
// accepted ready and an accepted heartbeat each grant at most this much,
// capped by the expiry of the connection's credential.
const LeaseGrant = 45 * time.Second

// HeartbeatSeqStride separates the heartbeat sequences of a connection's
// streams: the n-th heartbeat of the stream counted s carries
// s*HeartbeatSeqStride+n, so sequences rise across streams and pods without a
// write per heartbeat.
const HeartbeatSeqStride = 1 << 20

// StaleConnectionError answers a request whose connection_id does not name
// the instance's live connection.
type StaleConnectionError struct{}

func (*StaleConnectionError) Error() string {
	return "the connection is not the instance's live connection"
}

// Connection is one instance_connections row. The ready fields are set once
// its ready is accepted, the refusal fields once it is refused, and the
// heartbeat fields hold the latest accepted heartbeat. LastIssueSeq is the
// issue sequence of its latest Session instruction, FirstSessionReportAt the
// time its first Session report was stored.
type Connection struct {
	WorkspaceID          workspace.ID
	InstanceID           string
	ID                   string
	CredentialID         string
	HubPod               string
	ConnectedAt          time.Time
	LeaseExpiresAt       time.Time
	StreamCount          int64
	ReadyMessageID       *string
	ReadyFingerprint     *[sha256.Size]byte
	ReadyAcceptedAt      *time.Time
	ReadyValidForMS      *int64
	OS                   *string
	Arch                 *string
	CLIVersion           *string
	Capabilities         json.RawMessage
	RefusedCode          *string
	RefusedMessage       *string
	Heartbeat            *HeartbeatRecord
	EndedAt              *time.Time
	EndCause             *string
	StorageSequence      int64
	LastIssueSeq         int64
	FirstSessionReportAt *time.Time
}

// HeartbeatRecord is a connection's latest accepted heartbeat: its replay
// record and the grant it answered.
type HeartbeatRecord struct {
	MessageID   string
	Fingerprint [sha256.Size]byte
	Seq         int64
	At          time.Time
	ValidForMS  int64
}

// HeartbeatMessageID is the message_id of the heartbeat with sequence seq on
// connectionID, which the CLI's heartbeat report names as its
// request_message_id.
func HeartbeatMessageID(connectionID string, seq int64) string {
	return "hb_" + connectionID + "_" + strconv.FormatInt(seq, 10)
}

// Live reports whether the connection is the instance's live connection at
// now: not ended and its lease later than now.
func (c Connection) Live(now time.Time) bool {
	return c.EndedAt == nil && c.LeaseExpiresAt.After(now)
}

// ReadyAnswered reports whether the connection's ready was accepted or
// refused.
func (c Connection) ReadyAnswered() bool { return c.ReadyMessageID != nil }

// NewConnectionID returns conn_ followed by 32 hexadecimal characters from
// 128 bits of crypto/rand.
func NewConnectionID() string {
	var random [16]byte
	_, _ = rand.Read(random[:]) // crypto/rand.Read never returns an error.
	return "conn_" + hex.EncodeToString(random[:])
}

// Grant returns the lease that a grant at now gives under a credential that
// expires at expiresAt: the earlier of now plus LeaseGrant and expiresAt,
// and its length in whole milliseconds, rounded down. ok is false when that
// length is below one millisecond, so no positive grant remains.
func Grant(now, expiresAt time.Time) (leaseExpiresAt time.Time, validForMS int64, ok bool) {
	leaseExpiresAt = now.Add(LeaseGrant)
	if expiresAt.Before(leaseExpiresAt) {
		leaseExpiresAt = expiresAt
	}
	validForMS = leaseExpiresAt.Sub(now).Milliseconds()
	return leaseExpiresAt, validForMS, validForMS >= 1
}

// VersionBelow reports whether version precedes floor by semantic version
// precedence; both are semantic versions without a "v" prefix.
func VersionBelow(version, floor string) bool {
	return semver.Compare("v"+version, "v"+floor) < 0
}

// NewConnection describes a connection to create: its ID, the credential
// that opened it, the pod that holds its stream, the time and its first
// lease.
type NewConnection struct {
	ID             string
	InstanceID     string
	CredentialID   string
	HubPod         string
	ConnectedAt    time.Time
	LeaseExpiresAt time.Time
}

const connectionColumns = `workspace_id,instance_id,id,credential_id,hub_pod,connected_at,lease_expires_at,stream_count,
 ready_message_id,ready_fingerprint,ready_accepted_at,ready_valid_for_ms,os,arch,cli_version,capabilities,ready_refused_code,ready_refused_message,
 last_heartbeat_message_id,last_heartbeat_fingerprint,last_heartbeat_seq,last_heartbeat_at,last_heartbeat_valid_for_ms,ended_at,end_cause,storage_sequence,
 last_issue_seq,first_session_report_at`

// CreateConnection inserts a connection with stream count 1. When another
// connection of the instance has not ended, the insert fails with an error
// IsConnectionRace reports.
func CreateConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, row NewConnection) (Connection, error) {
	return scanConnection(tx.QueryRow(ctx, `INSERT INTO instance_connections (workspace_id,instance_id,id,credential_id,hub_pod,connected_at,lease_expires_at,stream_count)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1) RETURNING `+connectionColumns,
		string(ws), row.InstanceID, row.ID, row.CredentialID, row.HubPod, row.ConnectedAt, row.LeaseExpiresAt))
}

// IsConnectionRace reports the unique violation of a connection created while
// a concurrent transaction committed another connection of the instance.
func IsConnectionRace(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "idx_instance_connections_unended"
}

// LockUnendedConnection reads the instance's connection that has not ended,
// whatever its lease, and holds its row lock until the transaction ends.
func LockUnendedConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) (Connection, bool, error) {
	return optionalConnection(scanConnection(tx.QueryRow(ctx, `SELECT `+connectionColumns+` FROM instance_connections
		WHERE workspace_id=$1 AND instance_id=$2 AND ended_at IS NULL FOR UPDATE`, string(ws), instanceID)))
}

// LockLiveConnection reads connectionID when it is instanceID's live
// connection at now and locks its row, FOR UPDATE when update is set and FOR
// SHARE otherwise; any other connection answers StaleConnectionError.
func LockLiveConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, connectionID string, now time.Time, update bool) (Connection, error) {
	lock := ` FOR SHARE`
	if update {
		lock = ` FOR UPDATE`
	}
	value, found, err := optionalConnection(scanConnection(tx.QueryRow(ctx, `SELECT `+connectionColumns+` FROM instance_connections
		WHERE workspace_id=$1 AND instance_id=$2 AND id=$3 AND ended_at IS NULL AND lease_expires_at>$4`+lock, string(ws), instanceID, connectionID, now)))
	if err != nil {
		return Connection{}, err
	}
	if !found {
		return Connection{}, &StaleConnectionError{}
	}
	return value, nil
}

// LockLiveConnectionOf reads instanceID's live connection at now and locks
// its row FOR UPDATE; with credentialID set, only a live connection under
// that credential is returned.
func LockLiveConnectionOf(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, credentialID string, now time.Time) (Connection, bool, error) {
	return optionalConnection(scanConnection(tx.QueryRow(ctx, `SELECT `+connectionColumns+` FROM instance_connections
		WHERE workspace_id=$1 AND instance_id=$2 AND ended_at IS NULL AND lease_expires_at>$3 AND ($4='' OR credential_id=$4) FOR UPDATE`,
		string(ws), instanceID, now, credentialID)))
}

// ShareConnection reads one connection of the Workspace and locks its row
// FOR SHARE until the transaction ends.
func ShareConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) (Connection, error) {
	return scanConnection(tx.QueryRow(ctx, `SELECT `+connectionColumns+` FROM instance_connections WHERE workspace_id=$1 AND id=$2 FOR SHARE`, string(ws), connectionID))
}

// ReadConnection reads one connection of the Workspace.
func ReadConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) (Connection, error) {
	return scanConnection(tx.QueryRow(ctx, `SELECT `+connectionColumns+` FROM instance_connections WHERE workspace_id=$1 AND id=$2`, string(ws), connectionID))
}

// ReadyAcceptance is what accepting a ready stores.
type ReadyAcceptance struct {
	MessageID      string
	Fingerprint    [sha256.Size]byte
	AcceptedAt     time.Time
	LeaseExpiresAt time.Time
	ValidForMS     int64
	OS             string
	Arch           string
	CLIVersion     string
	Capabilities   json.RawMessage
}

// AcceptReady stores the accepted ready on the connection, renews its lease
// and sets the instance's cli_version.
func AcceptReady(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, ready ReadyAcceptance) error {
	if _, err := tx.Exec(ctx, `UPDATE instance_connections SET ready_message_id=$3,ready_fingerprint=$4,ready_accepted_at=$5,lease_expires_at=$6,
		ready_valid_for_ms=$7,os=$8,arch=$9,cli_version=$10,capabilities=$11 WHERE workspace_id=$1 AND id=$2`,
		string(ws), connection.ID, ready.MessageID, ready.Fingerprint[:], ready.AcceptedAt, ready.LeaseExpiresAt, ready.ValidForMS,
		ready.OS, ready.Arch, ready.CLIVersion, []byte(ready.Capabilities)); err != nil {
		return err
	}
	return setInstanceCLIVersion(ctx, tx, ws, connection.InstanceID, ready.CLIVersion)
}

// ReadyRefusal is what refusing a ready stores.
type ReadyRefusal struct {
	MessageID   string
	Fingerprint [sha256.Size]byte
	Code        string
	Message     string
	CLIVersion  string
}

// RefuseReady stores the refused ready's replay record and its error on the
// connection, which keeps its lease, and sets the instance's cli_version to
// the refused ready's.
func RefuseReady(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, refusal ReadyRefusal) error {
	if _, err := tx.Exec(ctx, `UPDATE instance_connections SET ready_message_id=$3,ready_fingerprint=$4,ready_refused_code=$5,ready_refused_message=$6
		WHERE workspace_id=$1 AND id=$2`, string(ws), connection.ID, refusal.MessageID, refusal.Fingerprint[:], refusal.Code, refusal.Message); err != nil {
		return err
	}
	return setInstanceCLIVersion(ctx, tx, ws, connection.InstanceID, refusal.CLIVersion)
}

func setInstanceCLIVersion(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID, version string) error {
	_, err := tx.Exec(ctx, `UPDATE instances SET cli_version=$3 WHERE workspace_id=$1 AND id=$2`, string(ws), instanceID, version)
	return err
}

// AcceptHeartbeat stores heartbeat as the connection's latest accepted one
// and renews its lease to leaseExpiresAt.
func AcceptHeartbeat(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, heartbeat HeartbeatRecord, leaseExpiresAt time.Time) error {
	_, err := tx.Exec(ctx, `UPDATE instance_connections SET last_heartbeat_message_id=$3,last_heartbeat_fingerprint=$4,last_heartbeat_seq=$5,
		last_heartbeat_at=$6,last_heartbeat_valid_for_ms=$7,lease_expires_at=$8 WHERE workspace_id=$1 AND id=$2`,
		string(ws), connection.ID, heartbeat.MessageID, heartbeat.Fingerprint[:], heartbeat.Seq, heartbeat.At, heartbeat.ValidForMS, leaseExpiresAt)
	return err
}

// EndConnection is the only writer of a connection's end: it sets ended_at,
// the database clock, and cause, deletes the connection's ledger rows and
// notifies the pod that holds its stream. The caller holds the connection's
// row lock.
func EndConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, cause string) error {
	if _, err := tx.Exec(ctx, `UPDATE instance_connections SET ended_at=clock_timestamp(),end_cause=$3 WHERE workspace_id=$1 AND id=$2 AND ended_at IS NULL`,
		string(ws), connection.ID, cause); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM connection_upstream_messages WHERE workspace_id=$1 AND connection_id=$2`, string(ws), connection.ID); err != nil {
		return err
	}
	return NotifyConnection(ctx, tx, ws, connection.ID)
}

// ResumeConnection moves connection's lease to the stream of pod without
// renewing it: hub_pod becomes pod and stream_count increases. It notifies
// the pod that held the previous stream, which then closes it, and returns
// the moved row. The caller holds the connection's row lock FOR UPDATE.
func ResumeConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, pod string) (Connection, error) {
	moved, err := scanConnection(tx.QueryRow(ctx, `UPDATE instance_connections SET hub_pod=$3,stream_count=stream_count+1
		WHERE workspace_id=$1 AND id=$2 RETURNING `+connectionColumns, string(ws), connection.ID, pod))
	if err != nil {
		return Connection{}, err
	}
	return moved, notifyPod(ctx, tx, connection.HubPod, ConnectionNotice{WorkspaceID: string(ws), ConnectionID: connection.ID, IssueSeq: moved.LastIssueSeq})
}

// ConnectionNotice is the payload of a connection notice: the pod that holds
// the connection's stream reads the row itself. IssueSeq bounds the Session
// instructions the notice releases: the issued instruction's issue_seq, or
// the connection's last_issue_seq as the notifying transaction read it.
type ConnectionNotice struct {
	WorkspaceID  string `json:"workspace_id"`
	ConnectionID string `json:"connection_id"`
	IssueSeq     int64  `json:"issue_seq"`
}

// ConnectionNoticeChannel is the notification channel of the hub pod named
// pod: tetral_hub_ followed by the first 16 hexadecimal characters of the
// SHA-256 of the name.
func ConnectionNoticeChannel(pod string) string {
	digest := sha256.Sum256([]byte(pod))
	return "tetral_hub_" + hex.EncodeToString(digest[:])[:16]
}

// NotifyConnection queues a connection notice on the channel of the pod
// that holds connectionID's stream, carrying the connection's last_issue_seq,
// both as tx reads them; PostgreSQL delivers it when tx commits.
func NotifyConnection(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) error {
	var (
		pod      string
		issueSeq int64
	)
	if err := tx.QueryRow(ctx, `SELECT hub_pod,last_issue_seq FROM instance_connections WHERE workspace_id=$1 AND id=$2`, string(ws), connectionID).Scan(&pod, &issueSeq); err != nil {
		return err
	}
	return notifyPod(ctx, tx, pod, ConnectionNotice{WorkspaceID: string(ws), ConnectionID: connectionID, IssueSeq: issueSeq})
}

func notifyPod(ctx context.Context, tx *dbconnect.Tx, pod string, notice ConnectionNotice) error {
	payload, err := json.Marshal(notice)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, ConnectionNoticeChannel(pod), string(payload))
	return err
}

// ConnectionObject is the connection of the instance object.
type ConnectionObject struct {
	ID              string           `json:"id"`
	CredentialID    string           `json:"credential_id"`
	State           string           `json:"state"`
	ReadyAt         *string          `json:"ready_at"`
	LastHeartbeatAt *string          `json:"last_heartbeat_at"`
	LeaseExpiresAt  string           `json:"lease_expires_at"`
	Capabilities    json.RawMessage  `json:"capabilities"`
	Error           *ConnectionError `json:"error"`
}

// ConnectionError is the error of a refused ready.
type ConnectionError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ConnectionState is the state of connection at now: online with an accepted
// ready and a live lease, registering while its ready is neither accepted nor
// refused and it has not ended, otherwise offline.
func ConnectionState(connection Connection, now time.Time) string {
	switch {
	case connection.ReadyAcceptedAt != nil && connection.Live(now):
		return ConnectionOnline
	case !connection.ReadyAnswered() && connection.EndedAt == nil:
		return ConnectionRegistering
	default:
		return ConnectionOffline
	}
}

// connectionObjectOf returns the connection object at now; capabilities are
// those of the instance's latest accepted ready.
func connectionObjectOf(connection Connection, capabilities json.RawMessage, now time.Time) *ConnectionObject {
	object := &ConnectionObject{
		ID:             connection.ID,
		CredentialID:   connection.CredentialID,
		State:          ConnectionState(connection, now),
		LeaseExpiresAt: FormatTime(connection.LeaseExpiresAt),
		Capabilities:   capabilities,
	}
	if object.Capabilities == nil {
		object.Capabilities = json.RawMessage(`null`)
	}
	if connection.ReadyAcceptedAt != nil {
		readyAt := FormatTime(*connection.ReadyAcceptedAt)
		object.ReadyAt = &readyAt
	}
	if connection.Heartbeat != nil {
		heartbeatAt := FormatTime(connection.Heartbeat.At)
		object.LastHeartbeatAt = &heartbeatAt
	}
	if connection.RefusedCode != nil {
		object.Error = &ConnectionError{Code: *connection.RefusedCode, Message: *connection.RefusedMessage}
	}
	return object
}

// connectionFacts reads, for each of instanceIDs, its latest connection and
// the capabilities of its latest accepted ready.
func connectionFacts(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceIDs []string) (map[string]Connection, map[string]json.RawMessage, error) {
	latest := map[string]Connection{}
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (instance_id) `+connectionColumns+` FROM instance_connections
		WHERE workspace_id=$1 AND instance_id=ANY($2) ORDER BY instance_id,storage_sequence DESC`, string(ws), instanceIDs)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		value, err := scanConnection(rows)
		if err != nil {
			_ = rows.Close()
			return nil, nil, err
		}
		latest[value.InstanceID] = value
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	capabilities := map[string]json.RawMessage{}
	rows, err = tx.Query(ctx, `SELECT DISTINCT ON (instance_id) instance_id,capabilities FROM instance_connections
		WHERE workspace_id=$1 AND instance_id=ANY($2) AND ready_accepted_at IS NOT NULL ORDER BY instance_id,storage_sequence DESC`, string(ws), instanceIDs)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var instanceID string
		var value []byte
		if err := rows.Scan(&instanceID, &value); err != nil {
			return nil, nil, err
		}
		capabilities[instanceID] = value
	}
	return latest, capabilities, rows.Err()
}

func optionalConnection(value Connection, err error) (Connection, bool, error) {
	if errors.Is(err, ErrNotFound) {
		return Connection{}, false, nil
	}
	return value, err == nil, err
}

func scanConnection(row scanner) (Connection, error) {
	var (
		value                                                    Connection
		readyMessage, osName, arch, cliVersion, refused, message sql.NullString
		heartbeatMessage, endCause                               sql.NullString
		readyFingerprint, heartbeatFingerprint, capabilities     []byte
		readyAccepted, heartbeatAt, ended, firstReport           sql.NullTime
		readyValidFor, heartbeatSeq, heartbeatValidFor           sql.NullInt64
	)
	err := row.Scan(&value.WorkspaceID, &value.InstanceID, &value.ID, &value.CredentialID, &value.HubPod, &value.ConnectedAt, &value.LeaseExpiresAt, &value.StreamCount,
		&readyMessage, &readyFingerprint, &readyAccepted, &readyValidFor, &osName, &arch, &cliVersion, &capabilities, &refused, &message,
		&heartbeatMessage, &heartbeatFingerprint, &heartbeatSeq, &heartbeatAt, &heartbeatValidFor, &ended, &endCause, &value.StorageSequence,
		&value.LastIssueSeq, &firstReport)
	if errors.Is(err, sql.ErrNoRows) {
		return Connection{}, ErrNotFound
	}
	if err != nil {
		return Connection{}, err
	}
	value.ConnectedAt, value.LeaseExpiresAt = value.ConnectedAt.UTC(), value.LeaseExpiresAt.UTC()
	value.ReadyMessageID, value.OS, value.Arch, value.CLIVersion = stringOrNil(readyMessage), stringOrNil(osName), stringOrNil(arch), stringOrNil(cliVersion)
	value.RefusedCode, value.RefusedMessage, value.EndCause = stringOrNil(refused), stringOrNil(message), stringOrNil(endCause)
	value.ReadyAcceptedAt, value.EndedAt, value.FirstSessionReportAt = timeOrNil(readyAccepted), timeOrNil(ended), timeOrNil(firstReport)
	if len(readyFingerprint) == sha256.Size {
		value.ReadyFingerprint = new([sha256.Size]byte)
		copy(value.ReadyFingerprint[:], readyFingerprint)
	}
	if readyValidFor.Valid {
		value.ReadyValidForMS = &readyValidFor.Int64
	}
	if capabilities != nil {
		value.Capabilities = append(json.RawMessage(nil), capabilities...)
	}
	if heartbeatMessage.Valid {
		value.Heartbeat = &HeartbeatRecord{MessageID: heartbeatMessage.String, Seq: heartbeatSeq.Int64, At: heartbeatAt.Time.UTC(), ValidForMS: heartbeatValidFor.Int64}
		copy(value.Heartbeat.Fingerprint[:], heartbeatFingerprint)
	}
	return value, nil
}

func stringOrNil(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func timeOrNil(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	at := value.Time.UTC()
	return &at
}
