package computerhub

import (
	"net/http"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage"
)

// driveSync answers POST /v1/client/drive-sync on the live connection. The
// first failing check answers, with nothing stored and no ACK:
//
//  1. the body: at most protocol.DriveSyncReportJSONBytes read under the
//     large-report deadlines (readReportBody), then the report schema, which
//     admits no downloading or unmounted and a drive_sync of at most
//     protocol.DriveSyncJSONBytes (400 invalid_message);
//  2. the instance's live connection, else 409 stale_connection;
//  3. the replay ledger: an equal copy gets the stored ACK and nothing else;
//  4. binding_id: a binding issued on this connection for session_id, in any
//     state, else 409 unknown_instruction;
//  5. task_id, when set: a task of a tool.execute record of the Session,
//     issued on any connection, else 409 unknown_instruction.
//
// One transaction then stores the report and its drive_sync input
// (runtimecontrol.StoreDriveSyncReportTx) for the thread
// runtimecontrol.DriveSyncThreadTx chooses, and saves the ACK in the replay
// ledger. The Session's runtime mutation lock is taken first, the order of
// every writer of a Session input.
func (h *clientHandler) driveSync(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readReportBody(w, r, protocol.DriveSyncReportJSONBytes)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	report, err := protocol.DecodeDriveSyncReport(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var answer clientAnswer
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_drive_sync", func(tx *dbconnect.Tx) error {
		if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(id.workspace), report.SessionID); err != nil {
			return err
		}
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
		if entry, found, err := replayOf(ctx, tx, connection, instance.RouteDriveSync, report.MessageID, fingerprint); found || err != nil {
			answer = clientAnswer{status: entry.Status, body: entry.Body}
			return err
		}
		computerID, found, err := computer.IssuedBindingComputer(ctx, tx, id.workspace, connection.ID, report.SessionID, report.BindingID)
		if err != nil {
			return err
		}
		if !found {
			return refuse(protocol.ErrorUnknownInstruction, "the binding_id names no binding issued on this connection for the Session")
		}
		threadID, found, err := runtimecontrol.DriveSyncThreadTx(ctx, tx, id.workspace, report.SessionID, report.TaskID.Value)
		if err != nil {
			return err
		}
		if !found {
			return refuse(protocol.ErrorUnknownInstruction, "the task_id names no task issued for the Session")
		}
		if err := runtimecontrol.StoreDriveSyncReportTx(ctx, tx, id.workspace, runtimecontrol.DriveSyncReport{
			SessionID: report.SessionID, ConnectionID: connection.ID, MessageID: report.MessageID, BindingID: report.BindingID,
			ComputerID: computerID, TaskID: report.TaskID.Value, DriveSyncJSON: string(protocol.Encode(report.DriveSync)),
		}, threadID, granted.now); err != nil {
			return err
		}
		answer = acknowledge(connection.ID, report.MessageID)
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, report.MessageID, instance.LedgerEntry{
			Route: instance.RouteDriveSync, Fingerprint: fingerprint, Status: answer.status, Body: answer.body,
		})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	writeClientAnswer(w, answer)
}
