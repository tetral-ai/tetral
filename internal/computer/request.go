package computer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Request kinds.
const (
	RequestNew  = "new"
	RequestWake = "wake"
)

// Request states.
const (
	RequestPending   = "pending"
	RequestClaimed   = "claimed"
	RequestFulfilled = "fulfilled"
	RequestFailed    = "failed"
	RequestExpired   = "expired"
)

// RequestEventChannel is the PostgreSQL notification channel of request
// transitions. Its payload is a RequestNotice; every hub pod reads the request
// itself and writes the event to its subscribers.
const RequestEventChannel = "tetral_hub_instance_requests"

// Request events, the SSE event names of the request stream.
const (
	RequestEventCreated = "created"
	RequestEventClaimed = "claimed"
	RequestEventPending = "pending"
	RequestEventExpired = "expired"
)

// RequestNotice is the payload of one RequestEventChannel notification.
type RequestNotice struct {
	WorkspaceID string `json:"workspace_id"`
	RequestID   string `json:"request_id"`
	Event       string `json:"event"`
}

// RequestAlreadyClaimedError answers a claim of a request another worker
// holds.
type RequestAlreadyClaimedError struct{}

func (*RequestAlreadyClaimedError) Error() string { return "the request is claimed by another worker" }

// RequestNotPendingError answers a claim of a request that has ended.
type RequestNotPendingError struct{}

func (*RequestNotPendingError) Error() string { return "the request is not pending" }

// RequestNotClaimedError answers a release or fail of a request that is not
// claimed.
type RequestNotClaimedError struct{}

func (*RequestNotClaimedError) Error() string { return "the request is not claimed" }

// Request is one computer_requests row.
type Request struct {
	ID                string
	SessionID         string
	ComputerID        string
	Kind              string
	EnvironmentID     string
	InstanceID        *string
	State             string
	WorkerID          *string
	ClaimCredentialID *string
	CreatedAt         time.Time
	ClaimedAt         *time.Time
	PendingSince      time.Time
	StorageSequence   int64
}

// RequestObject is the public request object.
type RequestObject struct {
	ID            string  `json:"id"`
	Kind          string  `json:"kind"`
	EnvironmentID string  `json:"environment_id"`
	InstanceID    *string `json:"instance_id"`
	State         string  `json:"state"`
	WorkerID      *string `json:"worker_id"`
	CreatedAt     string  `json:"created_at"`
	ClaimedAt     *string `json:"claimed_at"`
}

// Object returns the request as the routes serve it.
func (r Request) Object() RequestObject {
	object := RequestObject{ID: r.ID, Kind: r.Kind, EnvironmentID: r.EnvironmentID, InstanceID: r.InstanceID, State: r.State, WorkerID: r.WorkerID,
		CreatedAt: instance.FormatTime(r.CreatedAt)}
	if r.ClaimedAt != nil {
		claimedAt := instance.FormatTime(*r.ClaimedAt)
		object.ClaimedAt = &claimedAt
	}
	return object
}

const requestColumns = `id,session_id,computer_id,kind,environment_id,instance_id,state,worker_id,claim_credential_id,created_at,claimed_at,pending_since,storage_sequence`

// EnsureRequest returns the open request of computerID, a cloud computer of
// sessionID without a usable instance, or creates one: new without a current
// instance, otherwise wake for the current instance. Creation sends the
// created event. Concurrent callers share one request: the open-request index
// makes the later insert wait and do nothing, and it then reads the winner's.
func EnsureRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, computerID string) (Request, error) {
	var kind, association string
	var environmentID, currentInstanceID sql.NullString
	err := tx.QueryRow(ctx, `SELECT kind,environment_id,current_instance_id,association_state FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		string(ws), sessionID, computerID).Scan(&kind, &environmentID, &currentInstanceID, &association)
	if errors.Is(err, sql.ErrNoRows) {
		return Request{}, ErrNotFound
	}
	if err != nil {
		return Request{}, err
	}
	if kind != KindCloud || association != Attached {
		return Request{}, &InvalidError{Message: "only an attached cloud computer gets a request"}
	}
	requestKind := RequestNew
	if currentInstanceID.Valid {
		requestKind = RequestWake
	}
	var requestID string
	err = tx.QueryRow(ctx, `INSERT INTO computer_requests (workspace_id,id,session_id,computer_id,kind,environment_id,instance_id,state,pending_since)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',clock_timestamp())
		ON CONFLICT (workspace_id,computer_id) WHERE state IN ('pending','claimed') DO NOTHING RETURNING id`,
		string(ws), id.New("ireq_"), sessionID, computerID, requestKind, environmentID.String, currentInstanceID).Scan(&requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return scanRequest(tx.QueryRow(ctx, `SELECT `+requestColumns+` FROM computer_requests
			WHERE workspace_id=$1 AND computer_id=$2 AND state IN ('pending','claimed')`, string(ws), computerID))
	}
	if err != nil {
		return Request{}, err
	}
	if err := notifyRequest(ctx, tx, ws, requestID, RequestEventCreated); err != nil {
		return Request{}, err
	}
	return ReadRequest(ctx, tx, ws, requestID)
}

// ReadRequest reads one request of the Workspace.
func ReadRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) (Request, error) {
	return scanRequest(tx.QueryRow(ctx, `SELECT `+requestColumns+` FROM computer_requests WHERE workspace_id=$1 AND id=$2`, string(ws), requestID))
}

// ListPendingRequests returns up to limit pending requests of environmentID
// after storage sequence after, in creation order.
func ListPendingRequests(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, environmentID string, after int64, limit int) ([]Request, error) {
	rows, err := tx.Query(ctx, `SELECT `+requestColumns+` FROM computer_requests
		WHERE workspace_id=$1 AND environment_id=$2 AND state='pending' AND storage_sequence>$3 ORDER BY storage_sequence LIMIT $4`,
		string(ws), environmentID, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var requests []Request
	for rows.Next() {
		request, err := scanRequest(rows)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, rows.Err()
}

// Claim is the result of a claim. Key is nil for a repeated claim by the same
// worker, which issues nothing; WorkingDirectory is set for a new request.
// Revoked lists the never-used pending credentials a wake claim revoked.
type Claim struct {
	Request          Request
	Credential       instance.Credential
	Key              *instance.ConnectionKey
	WorkingDirectory *string
	Revoked          []string
}

// ClaimRequest claims requestID for workerID: a pending new request gets a new
// cloud instance with an active credential expiring maxLifetime after issuance,
// a pending wake request a pending credential for its instance after that
// instance's never-used pending credentials are revoked
// (instance.CredentialLimitError when two active ones remain, and the caller's
// transaction must roll back). A claimed request answers the same claim without
// a key to its worker and RequestAlreadyClaimedError to any other; an ended one
// RequestNotPendingError.
func ClaimRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID, workerID string, maxLifetime time.Duration) (Claim, error) {
	request, err := lockRequest(ctx, tx, ws, requestID)
	if err != nil {
		return Claim{}, err
	}
	switch request.State {
	case RequestClaimed:
		if *request.WorkerID != workerID {
			return Claim{}, &RequestAlreadyClaimedError{}
		}
		credential, err := instance.ReadCredential(ctx, tx, ws, *request.ClaimCredentialID)
		if err != nil {
			return Claim{}, err
		}
		claim := Claim{Request: request, Credential: credential}
		if request.Kind == RequestNew {
			claim.WorkingDirectory, err = workingDirectory(ctx, tx, ws, request)
		}
		return claim, err
	case RequestPending:
	default:
		return Claim{}, &RequestNotPendingError{}
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return Claim{}, err
	}
	expiresAt := now.Add(maxLifetime)
	var claim Claim
	var instanceID string
	if request.Kind == RequestNew {
		created, err := instance.CreateCloudInstance(ctx, tx, ws, request.ComputerID, request.SessionID)
		if err != nil {
			return Claim{}, err
		}
		instanceID = created.ID
		credential, key, err := instance.IssueClaimCredential(ctx, tx, ws, instanceID, instance.CredentialActive, now, expiresAt)
		if err != nil {
			return Claim{}, err
		}
		claim.Credential, claim.Key = credential, &key
		if claim.WorkingDirectory, err = workingDirectory(ctx, tx, ws, request); err != nil {
			return Claim{}, err
		}
	} else {
		instanceID = *request.InstanceID
		if _, err := instance.LockInstance(ctx, tx, ws, instanceID); err != nil {
			return Claim{}, err
		}
		if claim.Revoked, err = instance.RevokePendingCredentials(ctx, tx, ws, instanceID, instance.CauseRequestEnded, now); err != nil {
			return Claim{}, err
		}
		credential, key, err := instance.IssueClaimCredential(ctx, tx, ws, instanceID, instance.CredentialPending, now, expiresAt)
		if err != nil {
			return Claim{}, err
		}
		claim.Credential, claim.Key = credential, &key
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_requests SET state='claimed',worker_id=$3,claimed_at=$4,instance_id=$5,claim_credential_id=$6
		WHERE workspace_id=$1 AND id=$2`, string(ws), requestID, workerID, now, instanceID, claim.Credential.ID); err != nil {
		return Claim{}, err
	}
	if err := RefreshUnused(ctx, tx, ws, instanceID); err != nil {
		return Claim{}, err
	}
	if err := notifyRequest(ctx, tx, ws, requestID, RequestEventClaimed); err != nil {
		return Claim{}, err
	}
	claim.Request, err = ReadRequest(ctx, tx, ws, requestID)
	return claim, err
}

// ReturnToPending returns a claimed request to pending with pending_since now
// and sends the pending event; worker_id, claimed_at and instance_id keep the
// claim's values. For a new request it revokes the claim's credential with
// cause new_claim_returned and refreshes the claim instance's unused, which
// retires it. It returns the credential it revoked, "" when none, and does
// nothing to a request that is not claimed. Callers run it through
// hubflow.ReturnRequestToPending, which ends that credential's connection.
func ReturnToPending(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) (string, error) {
	request, err := lockRequest(ctx, tx, ws, requestID)
	if err != nil || request.State != RequestClaimed {
		return "", err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_requests SET state='pending',pending_since=$3 WHERE workspace_id=$1 AND id=$2`, string(ws), requestID, now); err != nil {
		return "", err
	}
	revoked := ""
	if request.Kind == RequestNew {
		if revoked, err = retireClaim(ctx, tx, ws, request, instance.CauseNewClaimReturned, now); err != nil {
			return "", err
		}
	} else if err := RefreshUnused(ctx, tx, ws, *request.InstanceID); err != nil {
		return "", err
	}
	return revoked, notifyRequest(ctx, tx, ws, requestID, RequestEventPending)
}

// Fail ends a claimed request, or with fromPending a pending wake request
// (deregistration of its instance), as failed. For a new request it revokes
// the claim's credential with cause request_ended and retires the claim's
// instance; for a wake request it revokes the claim's credential with cause
// request_ended while that credential is still pending. It returns the
// credential it revoked, "" when none, and does nothing to a request in any
// other state. Callers run it through hubflow.FailRequest.
func Fail(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string, fromPending bool) (string, error) {
	request, err := lockRequest(ctx, tx, ws, requestID)
	if err != nil {
		return "", err
	}
	if request.State != RequestClaimed && (!fromPending || request.State != RequestPending || request.Kind != RequestWake) {
		return "", nil
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_requests SET state='failed' WHERE workspace_id=$1 AND id=$2`, string(ws), requestID); err != nil {
		return "", err
	}
	if request.Kind == RequestNew {
		return retireClaim(ctx, tx, ws, request, instance.CauseRequestEnded, now)
	}
	if err := RefreshUnused(ctx, tx, ws, *request.InstanceID); err != nil {
		return "", err
	}
	return revokeUnusedWakeCredential(ctx, tx, ws, request, now)
}

// Expire ends a pending request as expired and sends the expired event; for
// a wake request it revokes the credential of the request's latest claim with
// cause request_ended while that credential is still pending. It does nothing
// to a request that is not pending. The credential it revokes was never used,
// so no connection ends.
func Expire(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) error {
	request, err := lockRequest(ctx, tx, ws, requestID)
	if err != nil || request.State != RequestPending {
		return err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE computer_requests SET state='expired' WHERE workspace_id=$1 AND id=$2`, string(ws), requestID); err != nil {
		return err
	}
	if request.Kind == RequestWake {
		if _, err := revokeUnusedWakeCredential(ctx, tx, ws, request, now); err != nil {
			return err
		}
	}
	return notifyRequest(ctx, tx, ws, requestID, RequestEventExpired)
}

// FulfilRequests runs in the transaction that accepts a ready of instanceID
// (hubflow.AcceptReady): a claimed new request whose claim created the
// instance becomes fulfilled and the instance its computer's current
// instance; a pending or claimed wake request for the instance becomes
// fulfilled.
func FulfilRequests(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, instanceID string) error {
	rows, err := tx.Query(ctx, `UPDATE computer_requests SET state='fulfilled'
		WHERE workspace_id=$1 AND instance_id=$2 AND ((kind='new' AND state='claimed') OR (kind='wake' AND state IN ('pending','claimed')))
		RETURNING kind,session_id,computer_id`, string(ws), instanceID)
	if err != nil {
		return err
	}
	type fulfilled struct{ kind, sessionID, computerID string }
	var requests []fulfilled
	for rows.Next() {
		var request fulfilled
		if err := rows.Scan(&request.kind, &request.sessionID, &request.computerID); err != nil {
			_ = rows.Close()
			return err
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()
	if len(requests) == 0 {
		return nil
	}
	for _, request := range requests {
		if request.kind != RequestNew {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE computers SET current_instance_id=$4 WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
			string(ws), request.sessionID, request.computerID, instanceID); err != nil {
			return err
		}
	}
	return RefreshUnused(ctx, tx, ws, instanceID)
}

// retireClaim revokes the credential of a new request's latest claim with
// cause and refreshes the claim instance's unused, now that no open claim
// holds it.
func retireClaim(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, request Request, cause string, now time.Time) (string, error) {
	revoked, err := instance.RevokeClaimCredential(ctx, tx, ws, *request.InstanceID, *request.ClaimCredentialID, cause, false, now)
	if err != nil {
		return "", err
	}
	if err := RefreshUnused(ctx, tx, ws, *request.InstanceID); err != nil {
		return "", err
	}
	if !revoked {
		return "", nil
	}
	return *request.ClaimCredentialID, nil
}

// revokeUnusedWakeCredential revokes the credential of a wake request's
// latest claim, if any, with cause request_ended while it is still pending.
func revokeUnusedWakeCredential(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, request Request, now time.Time) (string, error) {
	if request.ClaimCredentialID == nil {
		return "", nil
	}
	revoked, err := instance.RevokeClaimCredential(ctx, tx, ws, *request.InstanceID, *request.ClaimCredentialID, instance.CauseRequestEnded, true, now)
	if err != nil || !revoked {
		return "", err
	}
	return *request.ClaimCredentialID, nil
}

func workingDirectory(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, request Request) (*string, error) {
	var directory string
	if err := tx.QueryRow(ctx, `SELECT working_directory FROM computers WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		string(ws), request.SessionID, request.ComputerID).Scan(&directory); err != nil {
		return nil, err
	}
	return &directory, nil
}

func lockRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) (Request, error) {
	return scanRequest(tx.QueryRow(ctx, `SELECT `+requestColumns+` FROM computer_requests WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, string(ws), requestID))
}

func notifyRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID, event string) error {
	payload, err := json.Marshal(RequestNotice{WorkspaceID: string(ws), RequestID: requestID, Event: event})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1,$2)`, RequestEventChannel, string(payload))
	return err
}

type rowScanner interface{ Scan(dest ...any) error }

func scanRequest(row rowScanner) (Request, error) {
	var request Request
	var instanceID, workerID, credentialID sql.NullString
	var claimedAt sql.NullTime
	err := row.Scan(&request.ID, &request.SessionID, &request.ComputerID, &request.Kind, &request.EnvironmentID, &instanceID, &request.State,
		&workerID, &credentialID, &request.CreatedAt, &claimedAt, &request.PendingSince, &request.StorageSequence)
	if errors.Is(err, sql.ErrNoRows) {
		return Request{}, ErrNotFound
	}
	if err != nil {
		return Request{}, err
	}
	if instanceID.Valid {
		request.InstanceID = &instanceID.String
	}
	if workerID.Valid {
		request.WorkerID = &workerID.String
	}
	if credentialID.Valid {
		request.ClaimCredentialID = &credentialID.String
	}
	if claimedAt.Valid {
		at := claimedAt.Time.UTC()
		request.ClaimedAt = &at
	}
	request.CreatedAt, request.PendingSince = request.CreatedAt.UTC(), request.PendingSince.UTC()
	return request, nil
}

// RequestPressure counts the requests of every Workspace: Queued pending,
// InProgress claimed and not yet fulfilled, and OldestQueuedSeconds the age
// of the oldest pending one.
type RequestPressure struct {
	Queued              int64
	InProgress          int64
	OldestQueuedSeconds float64
}

// ReadRequestPressure reads the pressure through the security-definer
// tetral_computer_hub_request_pressure, the only cross-Workspace read of
// computer_requests, which measures ages by the database clock.
func ReadRequestPressure(ctx context.Context, tx *dbconnect.Tx) (RequestPressure, error) {
	var value RequestPressure
	err := tx.QueryRow(ctx, `SELECT queued,in_progress,oldest_queued_seconds FROM public.tetral_computer_hub_request_pressure()`).
		Scan(&value.Queued, &value.InProgress, &value.OldestQueuedSeconds)
	return value, err
}

// LockRequest reads one request of the Workspace and holds its row lock
// until the transaction ends.
func LockRequest(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, requestID string) (Request, error) {
	return lockRequest(ctx, tx, ws, requestID)
}
