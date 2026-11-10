package computerhub

import (
	"net/http"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/hubflow"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// disconnect answers POST /v1/client/disconnect on the live connection: it
// ends the connection with cause disconnected (hubflow.EndConnection), as a
// lease expiry does, and answers the ACK. The end deletes the replay ledger,
// so the request is never replayed: a copy finds no live connection and
// answers 409 stale_connection.
func (h *clientHandler) disconnect(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	request, err := protocol.DecodeDisconnectRequest(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	var answer clientAnswer
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_disconnect", func(tx *dbconnect.Tx) error {
		locks, err := hubflow.LockConnectionSessions(ctx, tx, id.workspace, id.instanceID, request.ConnectionID)
		if err != nil {
			return err
		}
		granted, err := authorize(ctx, tx, id, false, true, "")
		if err != nil {
			return err
		}
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, request.ConnectionID, granted.now, true)
		if err != nil {
			return err
		}
		// No disconnect is ever recorded, so any record of its message_id
		// is another message's.
		if _, found, err := instance.FindUpstreamMessage(ctx, tx, id.workspace, connection, request.MessageID); err != nil || found {
			if err == nil {
				err = refuse(protocol.ErrorMessageConflict, "the message_id was used on this connection with different content")
			}
			return err
		}
		answer = acknowledge(connection.ID, request.MessageID)
		return hubflow.EndConnection(ctx, tx, id.workspace, connection, instance.EndDisconnected, locks)
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	writeClientAnswer(w, answer)
}
