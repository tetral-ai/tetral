package computerhub

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Every /v1/client/* request runs the checks below in this order, and the
// first failing check answers:
//
//  1. size: readClientBody reads at most protocol.DefaultMessageJSONBytes of
//     a JSON body; a longer body answers 413 request_too_large;
//  2. shape: the route's protocol decoder answers 400 invalid_message, or
//     413 for a body over its complete limit;
//  3. identity, on GET /v1/client/events only (clientEvents);
//  4. authority: authorize locks the principal's instance and credential
//     in the request's transaction, FOR SHARE unless the request updates the
//     row: a ready and a use of a credential an activation revoked lock the
//     instance FOR NO KEY UPDATE, and an activating credential is locked
//     FOR UPDATE;
//  5. connection, on every POST: the instance's live connection
//     (instance.LockLiveConnection), else 409 stale_connection; a ready and
//     a heartbeat lock it after its credential (lockGrantedConnection);
//  6. the replay ledger (replayOf);
//  7. the route's own checks.
//
// A refusal changes nothing, except one that records a credential reuse.
// A database failure at any step answers 503 and changes nothing, so the
// authority checks fail closed. Locks follow the order of internal/instance:
// instance, credentials, connection, each in its final mode at once; of two
// credentials, a pending one comes before the other, and the principal's
// before the connection's; a fetch locks the credential it names only when
// that credential is pending and the fetch's principal is active.

// clientFailure is a refusal with its code of the client error table and,
// for previous_connection_active, its retry_after_ms. commit marks a refusal
// whose transaction keeps what it wrote (clientTx).
type clientFailure struct {
	code         protocol.ErrorCode
	message      string
	retryAfterMS protocol.Optional[int64]
	commit       bool
}

func (f *clientFailure) Error() string { return string(f.code) + ": " + f.message }

func refuse(code protocol.ErrorCode, message string) error {
	return &clientFailure{code: code, message: message}
}

func refuseRetryAfter(code protocol.ErrorCode, message string, retryAfterMS int64) error {
	return &clientFailure{code: code, message: message, retryAfterMS: protocol.Some(retryAfterMS)}
}

// clientTx runs fn in one transaction of the request's Workspace. A refusal
// fn returns rolls back what it wrote, except a refusal marked commit, which
// commits it before the refusal is answered. A transaction that authorize
// ends with errReuseRaced runs once more.
func (h *clientHandler) clientTx(ctx context.Context, ws workspace.ID, operation string, fn func(*dbconnect.Tx) error) error {
	err := h.clientTxOnce(ctx, ws, operation, fn)
	if errors.Is(err, errReuseRaced) {
		err = h.clientTxOnce(ctx, ws, operation, fn)
	}
	return err
}

func (h *clientHandler) clientTxOnce(ctx context.Context, ws workspace.ID, operation string, fn func(*dbconnect.Tx) error) error {
	var kept error
	err := h.Database.WithWorkspaceTx(ctx, string(ws), operation, func(tx *dbconnect.Tx) error {
		kept = nil
		err := fn(tx)
		var failure *clientFailure
		if errors.As(err, &failure) && failure.commit {
			kept = err
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return kept
}

// clientAnswer is the status and exact body of a client route's answer, as
// sent and as a replay sends it again.
type clientAnswer struct {
	status int
	body   []byte
}

func acknowledge(connectionID, messageID string) clientAnswer {
	return clientAnswer{status: http.StatusOK, body: protocol.Encode(protocol.Acknowledgement{ConnectionID: connectionID, MessageID: messageID})}
}

func grantAcknowledge(connectionID, messageID string, validForMS int64) clientAnswer {
	return clientAnswer{status: http.StatusOK, body: protocol.Encode(protocol.GrantAcknowledgement{ConnectionID: connectionID, MessageID: messageID, ValidForMS: validForMS})}
}

func errorAnswer(code protocol.ErrorCode, message string) clientAnswer {
	return clientAnswer{status: protocol.ClientErrors[code], body: protocol.Encode(protocol.ErrorBody{Code: code, Message: message})}
}

func writeClientAnswer(w http.ResponseWriter, answer clientAnswer) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(answer.status)
	_, _ = w.Write(answer.body)
}

// writeClientFailure answers a failed client request. Any failure that is
// not a refusal the routes name is a database failure: 503, the API
// envelope, since the client protocol defines no 5xx body.
func writeClientFailure(w http.ResponseWriter, r *http.Request, err error) {
	var (
		failure *clientFailure
		decode  *protocol.DecodeError
		stale   *instance.StaleConnectionError
	)
	switch {
	case errors.As(err, &failure):
		answer := errorAnswer(failure.code, failure.message)
		if failure.retryAfterMS.Set {
			answer.body = protocol.Encode(protocol.ErrorBody{Code: failure.code, Message: failure.message, RetryAfterMS: failure.retryAfterMS})
		}
		writeClientAnswer(w, answer)
	case errors.As(err, &decode):
		writeClientAnswer(w, errorAnswer(decode.Code, decode.Error()))
	case errors.As(err, &stale):
		writeClientAnswer(w, errorAnswer(protocol.ErrorStaleConnection, stale.Error()))
	case instance.IsConnectionRace(err):
		writeClientAnswer(w, errorAnswer(protocol.ErrorConnectionConflict, "another connection of the instance is active"))
	default:
		httpapi.WriteError(w, r, &httpapi.UnavailableError{Message: "the connection store is unavailable"})
	}
}

// readClientBody reads a JSON body of at most protocol.DefaultMessageJSONBytes;
// a longer one answers 413 request_too_large and is read no further.
func readClientBody(r *http.Request) ([]byte, error) {
	tooLarge := refuse(protocol.ErrorRequestTooLarge, "the request body exceeds 65,536 bytes")
	if r.ContentLength > protocol.DefaultMessageJSONBytes {
		return nil, tooLarge
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, protocol.DefaultMessageJSONBytes+1))
	if err != nil {
		return nil, refuse(protocol.ErrorInvalidMessage, "the request body could not be read")
	}
	if len(body) > protocol.DefaultMessageJSONBytes {
		return nil, tooLarge
	}
	return body, nil
}

// clientIdentity is the instance and credential a connection key principal
// names.
type clientIdentity struct {
	workspace    workspace.ID
	instanceID   string
	credentialID string
}

func identityOf(r *http.Request) clientIdentity {
	// requireConnectionKey admitted only a connection key principal.
	principal, _ := auth.PrincipalFromContext(r.Context())
	return clientIdentity{workspace: principal.Workspace.ID, instanceID: principal.Authority.Scope.ID, credentialID: principal.Credential.ID}
}

// authority is what the authority check read: the principal's credential and
// the database clock of the request's transaction.
type authority struct {
	credential instance.Credential
	now        time.Time
}

// errReuseRaced is an activation that revoked the principal's credential
// after authorize's unlocked read: the instance row it locked FOR SHARE
// cannot take the reuse record, and the transaction runs again, whose read
// sees the revocation.
var errReuseRaced = errors.New("computer-hub: an activation revoked the credential during the authority check")

// authorize locks the principal's instance and credential and refuses a
// deregistered instance (403 instance_unavailable), then a revoked
// (401 credential_revoked), expired (401 credential_expired) or pending
// (401 credential_invalid) credential. An unlocked read of the credential
// first chooses the lock modes. The instance row is locked FOR NO KEY
// UPDATE when updatesInstance is set (a ready, which sets its cli_version)
// or the credential was revoked by an activation, whose use the request's
// transaction records as the instance's credential reuse and whose refusal
// commits it; otherwise FOR SHARE. With activating, the events request that
// may activate it, a pending credential passes, locked FOR UPDATE for its
// activation. With fetching, the credential a fetch names, an unlocked read
// of it decides: a pending one is locked FOR UPDATE before an active
// principal's, the order of its activation. Any other is left unlocked,
// since it never becomes pending again and the fetch writes only a pending
// one; so is any credential a fetch by a principal that is not active
// names, which is refused, so that no transaction locks two pending
// credentials, as an expired renewal and a later one are.
func authorize(ctx context.Context, tx *dbconnect.Tx, id clientIdentity, activating, updatesInstance bool, fetching string) (authority, error) {
	unknown := refuse(protocol.ErrorCredentialInvalid, "the connection key does not authenticate this request")
	peek, err := instance.ReadInstanceCredential(ctx, tx, id.workspace, id.instanceID, id.credentialID)
	if err != nil && !errors.Is(err, instance.ErrNotFound) {
		return authority{}, err
	}
	reused := err == nil && revokedByActivation(peek)
	lockInstance := instance.ShareInstance
	if updatesInstance || reused {
		lockInstance = instance.LockInstanceNoKeyUpdate
	}
	current, err := lockInstance(ctx, tx, id.workspace, id.instanceID)
	if errors.Is(err, instance.ErrNotFound) {
		return authority{}, unknown
	}
	if err != nil {
		return authority{}, err
	}
	if fetching != "" && fetching != id.credentialID {
		named, err := instance.ReadInstanceCredential(ctx, tx, id.workspace, id.instanceID, fetching)
		if err != nil && !errors.Is(err, instance.ErrNotFound) {
			return authority{}, err
		}
		if err == nil && named.State == instance.CredentialPending && peek.State == instance.CredentialActive {
			if _, err := instance.LockCredential(ctx, tx, id.workspace, id.instanceID, fetching); err != nil && !errors.Is(err, instance.ErrNotFound) {
				return authority{}, err
			}
		}
	}
	lockCredential := instance.ShareCredential
	if activating && peek.State == instance.CredentialPending {
		lockCredential = instance.LockCredential
	}
	credential, err := lockCredential(ctx, tx, id.workspace, id.instanceID, id.credentialID)
	if errors.Is(err, instance.ErrNotFound) {
		return authority{}, unknown
	}
	if err != nil {
		return authority{}, err
	}
	now, err := instance.Now(ctx, tx)
	if err != nil {
		return authority{}, err
	}
	if current.RegistrationState == instance.RegistrationDeregistered {
		return authority{}, refuse(protocol.ErrorInstanceUnavailable, "the instance is deregistered")
	}
	switch instance.CredentialState(credential, now) {
	case instance.CredentialRevoked:
		failure := &clientFailure{code: protocol.ErrorCredentialRevoked, message: "the credential has been revoked"}
		if revokedByActivation(credential) {
			if !updatesInstance && !reused {
				return authority{}, errReuseRaced
			}
			if err := instance.RecordCredentialReuse(ctx, tx, id.workspace, id.instanceID, credential.ID); err != nil {
				return authority{}, err
			}
			failure.commit = true
		}
		return authority{}, failure
	case instance.CredentialExpired:
		return authority{}, refuse(protocol.ErrorCredentialExpired, "the credential has expired")
	case instance.CredentialPending:
		if !activating {
			return authority{}, refuse(protocol.ErrorCredentialInvalid, "the credential is not active")
		}
	}
	return authority{credential: credential, now: now}, nil
}

func revokedByActivation(credential instance.Credential) bool {
	return credential.RevokedAt != nil && credential.RevocationCause != nil && *credential.RevocationCause == instance.CausePendingActivation
}

// lockGrantedConnection locks the instance's live connection connectionID
// FOR UPDATE for a ready or heartbeat, whose grant is capped by the
// connection's credential, and returns that credential. The connection is
// read without a lock first; when its credential is not the principal's,
// which authorize locked, that credential is locked FOR SHARE before the
// connection. A credential_id changed in between fails the request
// (errCredentialMoved): only an activation changes it, and the credential
// limit leaves no room for a renewal beside a principal that is not the
// connection's credential.
func lockGrantedConnection(ctx context.Context, tx *dbconnect.Tx, id clientIdentity, granted authority, connectionID string) (instance.Connection, instance.Credential, error) {
	credential := granted.credential
	unlocked, err := instance.ReadConnection(ctx, tx, id.workspace, connectionID)
	if err != nil && !errors.Is(err, instance.ErrNotFound) {
		return instance.Connection{}, instance.Credential{}, err
	}
	if err == nil && unlocked.InstanceID == id.instanceID && unlocked.CredentialID != credential.ID {
		if credential, err = instance.ShareCredential(ctx, tx, id.workspace, id.instanceID, unlocked.CredentialID); err != nil {
			return instance.Connection{}, instance.Credential{}, err
		}
	}
	connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, connectionID, granted.now, true)
	if err != nil {
		return instance.Connection{}, instance.Credential{}, err
	}
	if connection.CredentialID != credential.ID {
		return instance.Connection{}, instance.Credential{}, errCredentialMoved
	}
	return connection, credential, nil
}

// grantFor returns the lease a grant at now gives under credential's expiry,
// refusing 401 credential_expired when no positive grant remains.
func grantFor(credential instance.Credential, now time.Time) (time.Time, int64, error) {
	leaseExpiresAt, validForMS, ok := instance.Grant(now, credential.ExpiresAt)
	if !ok {
		return time.Time{}, 0, refuse(protocol.ErrorCredentialExpired, "no lease grant remains before the credential expires")
	}
	return leaseExpiresAt, validForMS, nil
}

// replayOf checks messageID against the connection's replay records: found
// with an equal fingerprint on route is a replay, the same message_id with
// another fingerprint or on another route answers 409 message_conflict.
func replayOf(ctx context.Context, tx *dbconnect.Tx, connection instance.Connection, route, messageID string, fingerprint [sha256.Size]byte) (instance.LedgerEntry, bool, error) {
	entry, found, err := instance.FindUpstreamMessage(ctx, tx, connection.WorkspaceID, connection, messageID)
	if err != nil || !found {
		return instance.LedgerEntry{}, false, err
	}
	if entry.Route != route || entry.Fingerprint != fingerprint {
		return instance.LedgerEntry{}, false, refuse(protocol.ErrorMessageConflict, "the message_id was used on this connection with different content")
	}
	return entry, true, nil
}

// fingerprintOf is the canonical fingerprint of a body the decoder accepted,
// which is one JSON object without repeated keys.
func fingerprintOf(body []byte) [sha256.Size]byte {
	fingerprint, _ := instance.Fingerprint(body)
	return fingerprint
}
