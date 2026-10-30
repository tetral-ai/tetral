package computerhub

import (
	"errors"
	"net/http"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workload"
)

// sessionStatus answers POST /v1/client/session-status on the live
// connection. A replay returns the stored ACK and writes nothing. A report
// must be prepared or failed and name, by request_message_id, a
// session.prepare issued on this connection for its session_id and
// binding_id; otherwise 409 unknown_instruction. One transaction stores the
// report and its ACK in the replay ledger, marks the instruction reported,
// records the connection's first Session report and applies the report to
// its binding (computer.StoreReport). The Session's runtime mutation lock is
// taken first, the order of every writer of a Session event. A report that
// changed its binding ends the computer's session_computer cold start after
// the commit.
func (h *clientHandler) sessionStatus(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	body, err := readClientBody(r)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	report, err := protocol.DecodeSessionStatus(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	var (
		answer    clientAnswer
		coldStart *computer.ColdStart
	)
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_session_status", func(tx *dbconnect.Tx) error {
		coldStart = nil
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
		if entry, found, err := replayOf(ctx, tx, connection, instance.RouteSessionStatus, report.MessageID, fingerprint); found || err != nil {
			answer = clientAnswer{status: entry.Status, body: entry.Body}
			return err
		}
		if report.Status != "prepared" && report.Status != "failed" {
			return refuse(protocol.ErrorUnknownInstruction, "a "+report.Status+" report answers no session.prepare")
		}
		binding, err := computer.LockPreparation(ctx, tx, id.workspace, connection.ID, report.SessionID, report.BindingID, report.RequestMessageID)
		var unknown *computer.UnknownPreparationError
		if errors.As(err, &unknown) {
			return refuse(protocol.ErrorUnknownInstruction, unknown.Message)
		}
		if err != nil {
			return err
		}
		if err := instance.MarkReported(ctx, tx, id.workspace, connection.ID, report.RequestMessageID); err != nil {
			return err
		}
		if err := instance.MarkFirstSessionReport(ctx, tx, id.workspace, connection.ID); err != nil {
			return err
		}
		changed, err := computer.StoreReport(ctx, tx, id.workspace, binding, reportOf(report), granted.now)
		if err != nil {
			return err
		}
		if changed {
			read, err := computer.PreparationColdStart(ctx, tx, id.workspace, binding, granted.now)
			if err != nil {
				return err
			}
			coldStart = &read
		}
		answer = acknowledge(connection.ID, report.MessageID)
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, report.MessageID, instance.LedgerEntry{
			Route: instance.RouteSessionStatus, Fingerprint: fingerprint, Status: answer.status, Body: answer.body,
		})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if coldStart != nil {
		outcome := "ok"
		if report.Status == "failed" {
			outcome = report.Error.Value.Code
		}
		workload.RecordStoredColdStart(ctx, "session_computer", coldStart.StartKind, outcome, coldStart.Phases, "request", "instance")
	}
	writeClientAnswer(w, answer)
}

// reportOf is the preparation report a prepared or failed report carries.
func reportOf(status protocol.SessionStatus) computer.Report {
	if status.Status == "failed" {
		return computer.Report{Error: status.Error.Value}
	}
	return computer.Report{Prepared: true, CWD: status.Cwd.Value, DriveMountFailures: status.DriveMountFailures.Value, RepositoryFailures: status.RepositoryFailures.Value}
}
