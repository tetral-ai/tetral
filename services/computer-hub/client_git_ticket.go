package computerhub

import (
	"errors"
	"net/http"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// gitTicket answers POST /v1/client/git-ticket on the live connection: each
// fetch issues a new git ticket for the cloud computer whose preparation
// binding_id names, revoking the computer's earlier one, and answers it with
// git-proxy's origin and Cache-Control: no-store. A binding_id that names no
// preparation issued on this connection for a cloud computer with
// repositories answers 409 unknown_instruction. The replay record keeps the
// request's route and fingerprint, never the ticket: the same message_id with
// other content answers 409 message_conflict, and a retry with the same
// content is a new fetch.
func (h *clientHandler) gitTicket(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	request, err := protocol.DecodeGitTicketRequest(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var ticket string
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_git_ticket", func(tx *dbconnect.Tx) error {
		granted, err := authorize(ctx, tx, id, false, false, "")
		if err != nil {
			return err
		}
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, request.ConnectionID, granted.now, false)
		if err != nil {
			return err
		}
		_, replayed, err := replayOf(ctx, tx, connection, instance.RouteGitTicket, request.MessageID, fingerprint)
		if err != nil {
			return err
		}
		binding, err := computer.LockGitTicketBinding(ctx, tx, id.workspace, connection.ID, request.BindingID)
		var unknown *computer.UnknownPreparationError
		if errors.As(err, &unknown) {
			return refuse(protocol.ErrorUnknownInstruction, unknown.Message)
		}
		if err != nil {
			return err
		}
		if ticket, err = gitticket.IssueComputerTicket(ctx, tx, id.workspace, binding.SessionID, binding.ComputerID); err != nil || replayed {
			return err
		}
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, request.MessageID,
			instance.LedgerEntry{Route: instance.RouteGitTicket, Fingerprint: fingerprint, Status: http.StatusOK, Body: []byte{}})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeClientAnswer(w, clientAnswer{status: http.StatusOK, body: protocol.Encode(protocol.GitTicketResponse{GitProxyURL: h.GitProxyURL, Ticket: ticket})})
}
