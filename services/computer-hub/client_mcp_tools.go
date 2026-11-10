package computerhub

import (
	"net/http"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// mcpTools answers POST /v1/client/mcp-tools on any hub pod. The first
// failing check answers, with nothing stored and no ACK:
//
//  1. the body: at most protocol.McpToolsJSONBytes read under the
//     large-report deadlines (readReportBody), then the McpTools schema,
//     which admits no tool name twice (400 invalid_message);
//  2. the credential and the instance (authorize);
//  3. the instance's live connection, else 409 stale_connection;
//  4. the replay ledger: an equal copy gets the stored ACK and nothing else.
//
// One transaction then replaces the instance's row of the server when the
// report is newer (instance.ReportMCPToolList), and saves the ACK, which
// answers an older report as well, in the replay ledger.
func (h *clientHandler) mcpTools(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readReportBody(w, r, protocol.McpToolsJSONBytes)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	report, err := protocol.DecodeMcpTools(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var answer clientAnswer
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_mcp_tools", func(tx *dbconnect.Tx) error {
		granted, err := authorize(ctx, tx, id, false, false, "")
		if err != nil {
			return err
		}
		// FOR UPDATE: a copy of the report in flight waits here and then
		// finds the first one's replay record.
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, report.ConnectionID, granted.now, true)
		if err != nil {
			return err
		}
		if entry, found, err := replayOf(ctx, tx, connection, instance.RouteMCPTools, report.MessageID, fingerprint); found || err != nil {
			answer = clientAnswer{status: entry.Status, body: entry.Body}
			return err
		}
		if _, err := instance.ReportMCPToolList(ctx, tx, id.workspace, id.instanceID, report, granted.now); err != nil {
			return err
		}
		answer = acknowledge(connection.ID, report.MessageID)
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, report.MessageID, instance.LedgerEntry{
			Route: instance.RouteMCPTools, Fingerprint: fingerprint, Status: answer.status, Body: answer.body,
		})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	writeClientAnswer(w, answer)
}
