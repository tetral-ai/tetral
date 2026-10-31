package computerhub

import (
	"context"
	"errors"
	"net/http"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// credential answers POST /v1/client/credential on the live connection: the
// one fetch of a pending credential delivered on the stream. The key is
// generated in the fetching transaction, which stores only its digest, and
// leaves the hub in this answer alone, with Cache-Control: no-store. An
// unknown credential, another instance's, one delivered in a response or
// renewing another connection, one that is not pending, is revoked or
// expired, or was already fetched answers 404 not_found and changes nothing. The replay record keeps the request's
// route and fingerprint, never the answer: a retry with the same message_id
// and content is checked again and answers 404, the same message_id with
// other content 409 message_conflict.
func (h *clientHandler) credential(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	request, err := protocol.DecodeCredentialRequest(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var (
		fetched instance.Credential
		key     instance.ConnectionKey
	)
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_credential", func(tx *dbconnect.Tx) error {
		granted, err := authorize(ctx, tx, id, false, false, request.CredentialID)
		if err != nil {
			return err
		}
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, request.ConnectionID, granted.now, false)
		if err != nil {
			return err
		}
		_, replayed, err := replayOf(ctx, tx, connection, instance.RouteCredential, request.MessageID, fingerprint)
		if err != nil {
			return err
		}
		fetched, key, err = instance.FetchCredential(ctx, tx, id.workspace, id.instanceID, request.CredentialID, connection.ID, granted.now)
		if errors.Is(err, instance.ErrNotFound) {
			return refuse(protocol.ErrorNotFound, "no pending credential to fetch")
		}
		if err != nil || replayed {
			return err
		}
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, request.MessageID,
			instance.LedgerEntry{Route: instance.RouteCredential, Fingerprint: fingerprint, Status: http.StatusOK, Body: []byte{}})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	instance.AuditCredentialOperation(ctx, h.logger, clientActor(ctx), id.instanceID, fetched.ID, instance.AuditFetch)
	w.Header().Set("Cache-Control", "no-store")
	writeClientAnswer(w, clientAnswer{status: http.StatusOK, body: protocol.Encode(protocol.CredentialResponse{
		CredentialID: fetched.ID, ConnectionKey: key.Reveal(), ExpiresAt: instance.FormatTime(fetched.ExpiresAt),
	})})
}

// auditActivation writes the audit records of an activation the events
// request committed: activated, and the credential it revoked when there is
// one.
func (h *clientHandler) auditActivation(ctx context.Context, id clientIdentity, activated string, revoked []string) {
	if activated == "" {
		return
	}
	actor := clientActor(ctx)
	instance.AuditCredentialOperation(ctx, h.logger, actor, id.instanceID, activated, instance.AuditActivate)
	for _, credentialID := range revoked {
		instance.AuditCredentialOperation(ctx, h.logger, actor, id.instanceID, credentialID, instance.AuditRevoke)
	}
}

// clientActor is the connection key principal of a client request as an
// audit actor.
func clientActor(ctx context.Context) instance.Actor {
	principal, _ := auth.PrincipalFromContext(ctx)
	return instance.Actor{CredentialKind: principal.Credential.Kind, CredentialID: principal.Credential.ID}
}
