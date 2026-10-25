package computerhub

import (
	"net/http"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// heartbeat answers POST /v1/client/heartbeat on the live connection. A
// replay of the latest accepted heartbeat returns its ACK. A heartbeat is
// accepted only after the ready, for the heartbeat its request_message_id
// names (hb_<connection_id>_<heartbeat_seq>) and with a heartbeat_seq above
// the latest accepted one; otherwise 400 invalid_message. Acceptance renews
// the lease by the grant rule of ready.
func (h *clientHandler) heartbeat(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	heartbeat, err := protocol.DecodeHeartbeat(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var answer clientAnswer
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_heartbeat", func(tx *dbconnect.Tx) error {
		granted, err := authorize(ctx, tx, id, false, false, "")
		if err != nil {
			return err
		}
		connection, credential, err := lockGrantedConnection(ctx, tx, id, granted, heartbeat.ConnectionID)
		if err != nil {
			return err
		}
		if _, found, err := replayOf(ctx, tx, connection, instance.RouteHeartbeat, heartbeat.MessageID, fingerprint); found || err != nil {
			answer = grantAcknowledge(connection.ID, heartbeat.MessageID, connection.Heartbeat.ValidForMS)
			return err
		}
		switch {
		case connection.ReadyAcceptedAt == nil:
			return refuse(protocol.ErrorInvalidMessage, "a heartbeat before the connection's ready was accepted")
		case heartbeat.RequestMessageID != instance.HeartbeatMessageID(connection.ID, heartbeat.HeartbeatSeq):
			return refuse(protocol.ErrorInvalidMessage, "request_message_id does not name the heartbeat of this heartbeat_seq")
		case connection.Heartbeat != nil && heartbeat.HeartbeatSeq <= connection.Heartbeat.Seq:
			return refuse(protocol.ErrorInvalidMessage, "heartbeat_seq is not above the last accepted heartbeat's")
		}
		leaseExpiresAt, validForMS, err := grantFor(credential, granted.now)
		if err != nil {
			return err
		}
		answer = grantAcknowledge(connection.ID, heartbeat.MessageID, validForMS)
		return instance.AcceptHeartbeat(ctx, tx, id.workspace, connection, instance.HeartbeatRecord{
			MessageID: heartbeat.MessageID, Fingerprint: fingerprint, Seq: heartbeat.HeartbeatSeq, At: granted.now, ValidForMS: validForMS,
		}, leaseExpiresAt)
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	writeClientAnswer(w, answer)
}
