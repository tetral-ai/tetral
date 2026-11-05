package computerhub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/imagestore"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage"
)

// The large-report deadlines of a tool-status or drive sync body: the total allows
// reportDeadlineStep per 65,536 bytes of its Content-Length (at least one
// step; a body without one gets the deadline of its limit), and each
// read waits at most reportProgressTimeout for a byte.
const (
	reportDeadlineStep    = 10 * time.Second
	reportProgressTimeout = 10 * time.Second
	reportDeadlineBytes   = 65_536
)

// toolStatus answers POST /v1/client/tool-status on the live connection. The
// first failing check answers, with nothing stored and no ACK:
//
//  1. the body: at most protocol.MediaToolStatusJSONBytes read under the
//     large-report deadlines (readReportBody), then the report schema with
//     each member's budget (400 invalid_message) and the complete limit the
//     report's own result chooses (413 request_too_large);
//  2. the instance's live connection, else 409 stale_connection;
//  3. the replay ledger: an equal copy gets the stored ACK and nothing else;
//  4. the dispatch record the report answers (runtimecontrol.ReportedDispatchTx),
//     else 409 unknown_instruction;
//  5. the complete limit of the record's tool: 14 MiB for view_image, else
//     320 KiB (413 request_too_large);
//  6. the result's tool against the record's, and a view_image image's
//     content against its MIME type (400 invalid_message).
//
// An image is uploaded to the image store before the transaction and stored
// in it; the stored report and its settlement hold its reference in place
// of data_base64. One transaction then stores the report and applies its
// rules (runtimecontrol.StoreToolReportTx), writes the Session errors those
// rules name, and saves the ACK in the replay ledger. The Session's runtime
// mutation lock is taken first, the order of every writer of a Session
// event.
func (h *clientHandler) toolStatus(w http.ResponseWriter, r *http.Request) {
	ctx, id := r.Context(), identityOf(r)
	received := time.Now()
	body, err := readReportBody(w, r, protocol.MediaToolStatusJSONBytes)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	report, err := protocol.DecodeReceivedToolStatus(body)
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	fingerprint := fingerprintOf(body)
	address := report.Status.Address()
	var (
		upload   *imagestore.Uploaded
		imageErr error
	)
	if media, ok := report.Image(); ok {
		data, _ := base64.StdEncoding.DecodeString(media.DataBase64) // the decoder checked it
		if detected := http.DetectContentType(data); detected != media.Mime {
			imageErr = refuse(protocol.ErrorInvalidMessage, "the image content is "+detected+", not "+media.Mime)
		} else {
			uploaded, err := imagestore.Upload(ctx, h.Database, h.Blobs, id.workspace, media.Mime, data)
			if err != nil {
				writeClientFailure(w, r, err)
				return
			}
			upload = &uploaded
		}
	}
	var (
		answer clientAnswer
		stored runtimecontrol.StoredReport
		stores bool
	)
	settleStart := time.Now()
	err = h.clientTx(ctx, id.workspace, "computer_hub.client_tool_status", func(tx *dbconnect.Tx) error {
		stored, stores = runtimecontrol.StoredReport{}, false
		if err := storage.AcquireSessionRuntimeMutationLock(ctx, tx, string(id.workspace), address.SessionID); err != nil {
			return err
		}
		granted, err := authorize(ctx, tx, id, false, false, "")
		if err != nil {
			return err
		}
		// FOR UPDATE: a copy of the report in flight waits here and then
		// finds the first one's replay record.
		connection, err := instance.LockLiveConnection(ctx, tx, id.workspace, id.instanceID, address.ConnectionID, granted.now, true)
		if err != nil {
			return err
		}
		if entry, found, err := replayOf(ctx, tx, connection, instance.RouteToolStatus, address.MessageID, fingerprint); found || err != nil {
			answer = clientAnswer{status: entry.Status, body: entry.Body}
			return err
		}
		record, found, err := runtimecontrol.ReportedDispatchTx(ctx, tx, id.workspace, report.Status)
		if err != nil {
			return err
		}
		if !found {
			return refuse(protocol.ErrorUnknownInstruction, "the report answers no instruction issued on this connection")
		}
		tool, err := record.InstructionTool()
		if err != nil {
			return err
		}
		limit := protocol.OrdinaryToolStatusJSONBytes
		if tool == protocol.ToolViewImage {
			limit = protocol.MediaToolStatusJSONBytes
		}
		if report.Size > limit {
			return refuse(protocol.ErrorRequestTooLarge, "the report exceeds the limit of its instruction's tool")
		}
		if helper := report.Helper(); helper.Set && helper.Value.Tool != tool {
			return refuse(protocol.ErrorInvalidMessage, "the result is of tool "+helper.Value.Tool+", the instruction of "+tool)
		}
		if imageErr != nil {
			return imageErr
		}
		saved := report
		if upload != nil {
			reference, err := imagestore.Store(ctx, tx, id.workspace, *upload, record.SessionID, record.SessionThreadID)
			if err != nil {
				return err
			}
			encoded, err := json.Marshal(reference)
			if err != nil {
				return err
			}
			if saved, err = report.WithAttachment(encoded); err != nil {
				return err
			}
		}
		stored, err = runtimecontrol.StoreToolReportTx(ctx, tx, runtimecontrol.ToolReport{
			Status: report.Status, Body: string(saved.Body()), Result: string(saved.ResultJSON()),
		}, record, granted.now)
		if err != nil {
			return err
		}
		if err := computer.WriteSessionErrors(ctx, tx, id.workspace, address.SessionID, granted.now, computer.ToolReportErrors(stored)); err != nil {
			return err
		}
		answer, stores = acknowledge(connection.ID, address.MessageID), true
		return instance.SaveUpstreamMessage(ctx, tx, id.workspace, connection.ID, address.MessageID, instance.LedgerEntry{
			Route: instance.RouteToolStatus, Fingerprint: fingerprint, Status: answer.status, Body: answer.body,
		})
	})
	if err != nil {
		writeClientFailure(w, r, err)
		return
	}
	if stores {
		h.observeToolReport(ctx, id, report, stored, received, settleStart, time.Now())
	}
	writeClientAnswer(w, answer)
}

// readReportBody reads a report body of at most limit bytes under the
// large-report deadlines. A longer body answers 413 request_too_large; one
// that ends before its Content-Length, or passes a deadline, answers 400
// invalid_message.
func readReportBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	tooLarge := refuse(protocol.ErrorRequestTooLarge, "the request body exceeds its limit of "+strconv.FormatInt(limit, 10)+" bytes")
	if r.ContentLength > limit {
		return nil, tooLarge
	}
	length := r.ContentLength
	if length < 0 {
		length = limit
	}
	steps := max(1, (length+reportDeadlineBytes-1)/reportDeadlineBytes)
	reader := &progressReader{body: io.LimitReader(r.Body, limit+1), controller: http.NewResponseController(w),
		deadline: time.Now().Add(time.Duration(steps) * reportDeadlineStep)}
	body, err := io.ReadAll(reader)
	// The server's read of the next request starts at the body's end; a
	// deadline left set would end it, and cancel this request, later.
	if err := reader.controller.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return nil, err
	}
	if err != nil {
		return nil, refuse(protocol.ErrorInvalidMessage, "the request body could not be read")
	}
	if int64(len(body)) > limit {
		return nil, tooLarge
	}
	return body, nil
}

// progressReader bounds each read of a body by reportProgressTimeout and by
// the body's total deadline, through the connection's read deadline.
type progressReader struct {
	body       io.Reader
	controller *http.ResponseController
	deadline   time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	next := time.Now().Add(reportProgressTimeout)
	if p.deadline.Before(next) {
		next = p.deadline
	}
	if err := p.controller.SetReadDeadline(next); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return 0, err
	}
	return p.body.Read(b)
}

// rejectionMessages are the safe messages of the rejections Engine logs as
// tool.report_rejected.
var rejectionMessages = map[string]string{
	string(protocol.RejectionInvalidInput):      "the CLI found the instruction invalid",
	string(protocol.RejectionTaskConflict):      "the CLI holds the task with another call",
	string(protocol.RejectionUnsupportedPolicy): "the CLI cannot enforce the sandbox policy",
	string(protocol.RejectionResourceExhausted): "the CLI could not record the instruction",
}

// observeToolReport records, after the commit, a stored report's log record
// and spans: tool.report_rejected for a logged rejection, and the spans of
// runtimecontrol.RecordToolSpans.
func (h *clientHandler) observeToolReport(ctx context.Context, id clientIdentity, report protocol.ReceivedToolStatus, stored runtimecontrol.StoredReport,
	received, settleStart, settleEnd time.Time) {
	address := report.Status.Address()
	execution, _ := report.Status.(protocol.ExecutionReport)
	if stored.LogRejection {
		code := execution.Error.Value.Code
		h.logger.LogAttrs(ctx, slog.LevelWarn, "tool.report_rejected",
			slog.String("error.class", "tool_rejection"),
			slog.String("error.code", code),
			slog.String("error.message_safe", rejectionMessages[code]),
			slog.String("instance.id", id.instanceID),
			slog.String("connection.id", address.ConnectionID),
			slog.String("session.id", address.SessionID),
			slog.String("task.id", address.TaskID))
	}
	runtimecontrol.RecordToolSpans(ctx, stored.Dispatch, report.Helper(), execution.Phase, stored.Settled, received, settleStart, settleEnd)
}
