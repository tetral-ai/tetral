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
//     FOR SHARE in the request's transaction;
//  5. connection, on every POST: the instance's live connection
//     (instance.LockLiveConnection), else 409 stale_connection;
//  6. the replay ledger (replayOf);
//  7. the route's own checks.
//
// A database failure at any step answers 503 and changes nothing, so the
// authority checks fail closed.

// clientFailure is a refusal with its code of the client error table.
type clientFailure struct {
	code    protocol.ErrorCode
	message string
}

func (f *clientFailure) Error() string { return string(f.code) + ": " + f.message }

func refuse(code protocol.ErrorCode, message string) error {
	return &clientFailure{code: code, message: message}
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
		writeClientAnswer(w, errorAnswer(failure.code, failure.message))
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

// authorize locks the principal's instance and credential FOR SHARE and
// refuses a deregistered instance (403 instance_unavailable), then a revoked
// (401 credential_revoked), expired (401 credential_expired) or pending
// (401 credential_invalid) credential.
func authorize(ctx context.Context, tx *dbconnect.Tx, id clientIdentity) (authority, error) {
	current, err := instance.ShareInstance(ctx, tx, id.workspace, id.instanceID)
	if errors.Is(err, instance.ErrNotFound) {
		return authority{}, refuse(protocol.ErrorCredentialInvalid, "the connection key does not authenticate this request")
	}
	if err != nil {
		return authority{}, err
	}
	credential, err := instance.ShareCredential(ctx, tx, id.workspace, id.instanceID, id.credentialID)
	if errors.Is(err, instance.ErrNotFound) {
		return authority{}, refuse(protocol.ErrorCredentialInvalid, "the connection key does not authenticate this request")
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
		return authority{}, refuse(protocol.ErrorCredentialRevoked, "the credential has been revoked")
	case instance.CredentialExpired:
		return authority{}, refuse(protocol.ErrorCredentialExpired, "the credential has expired")
	case instance.CredentialPending:
		return authority{}, refuse(protocol.ErrorCredentialInvalid, "the credential is not active")
	}
	return authority{credential: credential, now: now}, nil
}

// grantFor locks the connection's credential FOR SHARE and returns the lease
// a grant at now gives under its expiry, refusing 401 credential_expired when
// no positive grant remains.
func grantFor(ctx context.Context, tx *dbconnect.Tx, connection instance.Connection, now time.Time) (time.Time, int64, error) {
	credential, err := instance.ShareCredential(ctx, tx, connection.WorkspaceID, connection.InstanceID, connection.CredentialID)
	if err != nil {
		return time.Time{}, 0, err
	}
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
