package computerhub

import (
	"log/slog"
	"net/http"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// protocolErrorMessages are the safe texts of the client.protocol_error
// record, one per report code; the report's own message is never logged.
var protocolErrorMessages = map[string]string{
	string(protocol.ProtocolInvalidMessage):  "the CLI could not accept a downstream message",
	string(protocol.ProtocolMessageConflict): "the CLI received a reused message_id with different content",
}

// protocolError answers POST /v1/client/protocol-error on the live
// connection: it stores the report and its ACK in the replay ledger, writes
// one client.protocol_error record after the commit and changes nothing
// else. A replay returns the stored ACK and records nothing again.
func (h *clientHandler) protocolError(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	report, err := protocol.DecodeProtocolErrorReport(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var (
		answer   clientAnswer
		recorded bool
	)
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_protocol_error", func(tx *dbconnect.Tx) error {
		recorded = false
		granted, err := authorize(ctx, tx, id, false, false)
		if err != nil {
			return err
		}
		// FOR UPDATE: a copy of the report in flight waits here and then
		// finds the first one's replay record.
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, report.ConnectionID, granted.now, true)
		if err != nil {
			return err
		}
		if entry, found, err := replayOf(ctx, tx, connection, instance.RouteProtocolError, report.MessageID, fingerprint); found || err != nil {
			answer = clientAnswer{status: entry.Status, body: entry.Body}
			return err
		}
		var requestMessageID *string
		if report.RequestMessageID.Set {
			requestMessageID = &report.RequestMessageID.Value
		}
		if err := instance.RecordProtocolError(ctx, tx, id.workspace, connection.ID, instance.ProtocolErrorRecord{
			MessageID: report.MessageID, Code: report.Error.Code, Message: report.Error.Message, RequestMessageID: requestMessageID, At: granted.now,
		}); err != nil {
			return err
		}
		answer, recorded = acknowledge(connection.ID, report.MessageID), true
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, report.MessageID, instance.LedgerEntry{
			Route: instance.RouteProtocolError, Fingerprint: fingerprint, Status: answer.status, Body: answer.body,
		})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if recorded {
		attrs := []slog.Attr{
			slog.String("connection.id", report.ConnectionID),
			slog.String("message.id", report.MessageID),
			slog.String("error.class", "protocol_error"),
			slog.String("error.code", report.Error.Code),
			slog.String("error.message_safe", protocolErrorMessages[report.Error.Code]),
		}
		if report.RequestMessageID.Set {
			attrs = append(attrs, slog.String("request.message.id", report.RequestMessageID.Value))
		}
		h.logger.LogAttrs(ctx, slog.LevelWarn, "client.protocol_error", attrs...)
	}
	writeClientAnswer(w, answer)
}
