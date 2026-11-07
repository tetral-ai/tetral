package runtimecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/instance"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// The kinds of a tool report: an execution report answers a tool.execute, a
// control reply a tool.stdin or tool.cancel.
const (
	ReportExecution = "execution"
	ReportControl   = "control"
)

// loggedRejections are the rejection codes Engine logs as tool.report_rejected;
// binding_unavailable reaches only the model and sandbox_unavailable becomes a
// Session error.
var loggedRejections = map[string]bool{
	string(protocol.RejectionInvalidInput):      true,
	string(protocol.RejectionTaskConflict):      true,
	string(protocol.RejectionUnsupportedPolicy): true,
	string(protocol.RejectionResourceExhausted): true,
}

// ToolReport is a tool-status report as computer-hub stores it: the decoded
// report, and its compact body and result envelope ("" without one) with a
// view_image's image replaced by its attachment reference.
type ToolReport struct {
	Status protocol.ToolStatus
	Body   string
	Result string
}

// reportFields are the stored columns and rule inputs of a report.
type reportFields struct {
	protocol.ReportAddress
	kind, phase string
	reportSeq   int64
	helper      protocol.Optional[protocol.HelperEnvelope]
	err         protocol.Optional[protocol.ProtocolError]
}

func fieldsOf(status protocol.ToolStatus) reportFields {
	switch s := status.(type) {
	case protocol.ExecutionReport:
		return reportFields{s.Address(), ReportExecution, s.Phase, s.ReportSeq.Value, s.Result, s.Error}
	case protocol.ControlReport:
		return reportFields{s.Address(), ReportControl, "", 0, s.Result, s.Error}
	}
	return reportFields{}
}

// StoredReport is what StoreToolReportTx changed: the record the report
// answers, whether it settled that record's call, and the facts its caller
// records: the Session errors (the Session's first sandbox_unavailable
// rejection on the connection, and the drives the report lists as unmounted)
// and whether Engine logs the rejection.
type StoredReport struct {
	Dispatch           Dispatch
	Settled            bool
	SandboxUnavailable protocol.Optional[string] // the rejection's message
	Unmounted          []protocol.DriveMountFailure
	LogRejection       bool
}

// ReportedDispatchTx reads, without a lock, the record a report answers: its
// request_message_id issued on its connection, ready or reported, for its
// Session, binding, tool use and task, a tool.execute for an execution report
// and a tool.stdin or tool.cancel for a control reply. found is false for any
// other report (unknown_instruction).
func ReportedDispatchTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, status protocol.ToolStatus) (Dispatch, bool, error) {
	f := fieldsOf(status)
	types := []string{DispatchExecute}
	if f.kind == ReportControl {
		types = []string{DispatchStdin, DispatchCancel}
	}
	dispatches, err := queryDispatches(ctx, tx, `WHERE message_id=$1 AND workspace_id=$2 AND connection_id=$3 AND session_id=$4 AND binding_id=$5
		AND tool_use_event_id=$6 AND task_id=$7 AND state IN ('ready','reported') AND type=ANY($8)`,
		f.RequestMessageID, string(ws), f.ConnectionID, f.SessionID, f.BindingID, f.ToolUseEventID, f.TaskID, types)
	if err != nil || len(dispatches) == 0 {
		return Dispatch{}, false, err
	}
	return dispatches[0], true, nil
}

// InstructionTool is the tool of d's instruction: the tool.execute payload's
// tool, stdin or cancel.
func (d Dispatch) InstructionTool() (string, error) {
	switch d.Type {
	case DispatchStdin:
		return protocol.ToolStdin, nil
	case DispatchCancel:
		return protocol.ToolCancel, nil
	}
	var payload struct {
		Tool string `json:"tool"`
	}
	if err := json.Unmarshal([]byte(d.PayloadJSON), &payload); err != nil || payload.Tool == "" {
		return "", errors.New("a tool.execute record holds no tool")
	}
	return payload.Tool, nil
}

// StoreToolReportTx stores report, which answers record d (ReportedDispatchTx),
// in session_tool_reports and applies its rules at the database clock now.
// The caller holds the report's connection FOR UPDATE and found its
// message_id in no replay record. It locks the computer row, then the call's
// row, then the record.
//
//   - A running or terminal report settles a pending call with its result
//     ({"settlement":"report"}); a running one that settles it starts the
//     task's background task. A terminal report moves the record to reported
//     and ends a running background task, which notifies the calling thread.
//     A running report after the call settled, and any report after the
//     record was reported, changes nothing more.
//   - A rejected report moves the record to reported and settles a pending
//     call {"settlement":"rejected"}.
//   - A control reply moves its record to reported; a reply to a tool.stdin
//     settles its pending write_stdin {"settlement":"reply"}. A reply to a
//     tool.cancel reaches no call.
func StoreToolReportTx(ctx context.Context, tx *dbconnect.Tx, report ToolReport, d Dispatch, now time.Time) (StoredReport, error) {
	f := fieldsOf(report.Status)
	if _, _, err := lockComputerTx(ctx, tx, d.WorkspaceID, d.SessionID, d.ComputerID); err != nil {
		return StoredReport{}, err
	}
	key := d.key()
	call, hasCall, err := readMachineCallTx(ctx, tx, key, true)
	if err != nil {
		return StoredReport{}, err
	}
	if d, err = lockDispatchTx(ctx, tx, d.MessageID); err != nil {
		return StoredReport{}, err
	}
	stored := StoredReport{Dispatch: d}
	if f.phase == "rejected" {
		stored.LogRejection = loggedRejections[f.err.Value.Code]
		if f.err.Value.Code == string(protocol.RejectionSandboxUnavailable) {
			first, err := firstSandboxRejectionTx(ctx, tx, d.WorkspaceID, d.SessionID, f.ConnectionID)
			if err != nil {
				return StoredReport{}, err
			}
			if first {
				stored.SandboxUnavailable = protocol.Some(f.err.Value.Message)
			}
		}
	}
	if f.helper.Set && f.helper.Value.DriveSync.Set {
		stored.Unmounted = f.helper.Value.DriveSync.Value.Unmounted.Value
	}
	if err := insertToolReportTx(ctx, tx, d.WorkspaceID, f, report.Body, now); err != nil {
		return StoredReport{}, err
	}
	final := f.phase != "running"
	if final && d.State == DispatchReported {
		return stored, nil
	}
	record, err := settlementOf(f, d, report.Result)
	if err != nil {
		return StoredReport{}, err
	}
	if hasCall && call.ExecutionState == "pending" && record != "" {
		if stored.Settled, err = SettleToolCallTx(ctx, tx, key, record); err != nil {
			return StoredReport{}, err
		}
	}
	if !final {
		if stored.Settled {
			return stored, startMachineTaskTx(ctx, tx, d, now)
		}
		return stored, nil
	}
	if err := setDispatchStateTx(ctx, tx, d, DispatchReported); err != nil {
		return StoredReport{}, err
	}
	if err := instance.MarkReported(ctx, tx, d.WorkspaceID, f.ConnectionID, d.MessageID); err != nil {
		return StoredReport{}, err
	}
	if f.phase == "terminal" {
		return stored, endMachineTaskTx(ctx, tx, d, f.helper.Value.Status, report.Result, now)
	}
	return stored, nil
}

// settlementOf is the settlement record a report gives its call, or "" for a
// reply to a tool.cancel, which reaches no call.
func settlementOf(f reportFields, d Dispatch, result string) (string, error) {
	switch {
	case f.phase == "rejected":
		return MarshalDataJSON(struct {
			Settlement string `json:"settlement"`
			Code       string `json:"code"`
			Message    string `json:"message"`
		}{"rejected", f.err.Value.Code, f.err.Value.Message})
	case f.kind == ReportExecution:
		return MarshalDataJSON(struct {
			Settlement string          `json:"settlement"`
			Phase      string          `json:"phase"`
			Result     json.RawMessage `json:"result"`
		}{"report", f.phase, json.RawMessage(result)})
	case d.Type == DispatchStdin:
		type replyError struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		reply := struct {
			Settlement string          `json:"settlement"`
			Result     json.RawMessage `json:"result"`
			Error      *replyError     `json:"error"`
		}{Settlement: "reply", Result: json.RawMessage("null")}
		if result != "" {
			reply.Result = json.RawMessage(result)
		}
		if f.err.Set {
			reply.Error = &replyError{f.err.Value.Code, f.err.Value.Message}
		}
		return MarshalDataJSON(reply)
	}
	return "", nil
}

func insertToolReportTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, f reportFields, body string, now time.Time) error {
	var phase, seq any
	if f.phase != "" {
		phase = f.phase
	}
	if f.reportSeq > 0 {
		seq = f.reportSeq
	}
	_, err := tx.Exec(ctx, `INSERT INTO session_tool_reports (workspace_id,connection_id,message_id,request_message_id,session_id,binding_id,
		tool_use_event_id,task_id,kind,phase,report_seq,body_json,created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		string(ws), f.ConnectionID, f.MessageID, f.RequestMessageID, f.SessionID, f.BindingID, f.ToolUseEventID, f.TaskID, f.kind, phase, seq, body, now)
	return err
}

// firstSandboxRejectionTx reports whether no report of sessionID stored on
// connectionID was rejected with sandbox_unavailable. Bodies are decoded here,
// never cast in SQL, since report text may hold U+0000.
func firstSandboxRejectionTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, connectionID string) (bool, error) {
	rows, err := tx.Query(ctx, `SELECT body_json FROM session_tool_reports
		WHERE workspace_id=$1 AND session_id=$2 AND connection_id=$3 AND phase='rejected'`,
		string(ws), sessionID, connectionID)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return false, err
		}
		var rejected struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &rejected); err != nil {
			return false, err
		}
		if rejected.Error.Code == string(protocol.RejectionSandboxUnavailable) {
			return false, nil
		}
	}
	return true, rows.Err()
}

// startMachineTaskTx records the running background task of the call whose
// running report settled it: its computer, connection and record.
func startMachineTaskTx(ctx context.Context, tx *dbconnect.Tx, d Dispatch, now time.Time) error {
	_, err := tx.Exec(ctx, `INSERT INTO session_background_tasks (workspace_id,session_id,session_thread_id,task_id,source_tool_use_event_id,
		provider,computer_id,connection_id,dispatch_message_id,status,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,'computer',$6,$7,$8,'running',$9,$9)`,
		string(d.WorkspaceID), d.SessionID, d.SessionThreadID, d.TaskID, d.ToolUseEventID, d.ComputerID, d.ConnectionID, d.MessageID, now)
	return err
}

// endMachineTaskTx ends d's task's running background task with its terminal
// result: completed for a success, failed otherwise, and notifies the calling
// thread (notifyTaskEndTx).
func endMachineTaskTx(ctx context.Context, tx *dbconnect.Tx, d Dispatch, status, result string, now time.Time) error {
	terminal := "failed"
	if status == "success" {
		terminal = "completed"
	}
	digest := sha256.Sum256([]byte(result))
	ended, err := tx.Exec(ctx, `UPDATE session_background_tasks SET status=$5,terminal_result_json=$6,terminal_result_digest=$7,
		terminal_at=COALESCE(terminal_at,$8),updated_at=$8
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND dispatch_message_id=$4 AND status='running'`,
		string(d.WorkspaceID), d.SessionID, d.TaskID, d.MessageID, terminal, result, hex.EncodeToString(digest[:]), now)
	if err != nil || !RowsAffected(ended) {
		return err
	}
	return notifyTaskEndTx(ctx, tx, d.WorkspaceID, d.SessionID, d.SessionThreadID, d.TaskID, now)
}

// ConnectionCallSessionsTx returns, in Session ID order, the Sessions with
// an execute, stdin or cancel record issued on connectionID or waiting for a
// preparation there, or a background command still running on it: those the
// connection's end changes (EndConnectionCallsTx). hubflow reads it before it
// takes those Sessions' runtime mutation locks and again under the
// connection's row lock.
func ConnectionCallSessionsTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT session_id FROM session_tool_dispatches
		WHERE workspace_id=$1 AND connection_id=$2 AND state IN ('ready','waiting_preparation')
		UNION SELECT session_id FROM session_background_tasks WHERE workspace_id=$1 AND connection_id=$2 AND status='running'
		ORDER BY session_id`, string(ws), connectionID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sessions []string
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, err
		}
		sessions = append(sessions, sessionID)
	}
	return sessions, rows.Err()
}

// endConnectionTasksTx ends every background command of connectionID still
// running as unknown_outcome, its result the "computer offline, result
// unknown" record, and notifies each calling thread once. The caller holds
// their Sessions' runtime mutation locks (ConnectionCallSessionsTx).
func endConnectionTasksTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string, now time.Time) error {
	result, err := UnknownOutcomeSettlement(MessageComputerOfflineResultUnknown)
	if err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(result))
	rows, err := tx.Query(ctx, `UPDATE session_background_tasks SET status='unknown_outcome',terminal_result_json=$3,terminal_result_digest=$4,
		terminal_at=COALESCE(terminal_at,$5),updated_at=$5
		WHERE workspace_id=$1 AND connection_id=$2 AND status='running'
		RETURNING session_id,session_thread_id,task_id`, string(ws), connectionID, result, hex.EncodeToString(digest[:]), now)
	if err != nil {
		return err
	}
	type endedTask struct{ sessionID, threadID, taskID string }
	var ended []endedTask
	for rows.Next() {
		var task endedTask
		if err := rows.Scan(&task.sessionID, &task.threadID, &task.taskID); err != nil {
			_ = rows.Close()
			return err
		}
		ended = append(ended, task)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, task := range ended {
		if err := notifyTaskEndTx(ctx, tx, ws, task.sessionID, task.threadID, task.taskID, now); err != nil {
			return err
		}
	}
	return nil
}

// notifyTaskEndTx gives the thread that started an ended background task its
// task_notification, except in an archived or deleted Session, whose task
// keeps its result and which nothing places again. The caller holds the
// Session's runtime mutation lock, under which archive and delete commit, so
// neither commits between the lifecycle read and this transaction's commit.
func notifyTaskEndTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, threadID, taskID string, now time.Time) error {
	var lifecycle string
	if err := tx.QueryRow(ctx, `SELECT lifecycle_state FROM sessions WHERE workspace_id=$1 AND id=$2`,
		string(ws), sessionID).Scan(&lifecycle); err != nil {
		return err
	}
	if lifecycle == "archived" || lifecycle == "deleted" {
		return nil
	}
	request, err := TaskNotificationInboxTx(ctx, tx, ws, sessionID, threadID, taskID, now)
	if err != nil || request == nil {
		return err
	}
	_, err = queue.EnqueueTx(ctx, tx, *request)
	return err
}

// TaskNotificationInboxTx inserts the task_notification inbox row of taskID
// for threadID and returns the runtime_input job its caller enqueues in the
// same transaction. When the thread or an ancestor is closing or closed, the
// row is born parked under the Session's Runtime binding, which fenced the
// thread, and no job is returned: resuming the thread later creates its one
// job. A second call for the task inserts nothing more.
func TaskNotificationInboxTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, threadID, taskID string, now time.Time) (*queue.EnqueueRequest, error) {
	closing, err := childcontrol.ThreadOrAncestorClosingOrClosedTx(ctx, tx, string(ws), sessionID, threadID)
	if err != nil {
		return nil, err
	}
	status, bindingID, generation, podUID := "queued", "", int64(0), ""
	if closing {
		status = "parked"
		if err := tx.QueryRow(ctx, `SELECT binding_id, binding_generation, agent_runtime_pod_uid
			FROM session_runtime_bindings WHERE workspace_id=$1 AND session_id=$2 FOR SHARE`,
			string(ws), sessionID).Scan(&bindingID, &generation, &podUID); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
		event_ids_json, status, binding_id, binding_generation, target_pod_uid, created_at, updated_at
	) VALUES ($1,$2,$3,$4,'task_notification','[]',$5,NULLIF($6,''),NULLIF($7,0),NULLIF($8,''),$9,$9)
	ON CONFLICT (workspace_id, runtime_input_id) DO NOTHING`,
		string(ws), sessionID, threadID, queue.FormatTaskNotificationRuntimeInputID(taskID), status,
		bindingID, generation, podUID, now); err != nil {
		return nil, err
	}
	if closing {
		return nil, nil
	}
	request, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(ws, sessionID, threadID, taskID, now)
	if err != nil {
		return nil, err
	}
	return &request, nil
}

// RecordToolSpans records, in the trace of d's trace context, the spans of a
// stored report. A terminal report with metrics gives the CLI's span
// execute_tool <tool>, which ends at received and lasts wall_time_ms, with
// the children queued, drive_wait, run and sync laid end to end from its
// start in that order and of those lengths. A report that settled d's call
// gives tool.settle from settleStart to settleEnd. No CLI clock is read: the
// CLI reports durations only.
func RecordToolSpans(ctx context.Context, d Dispatch, helper protocol.Optional[protocol.HelperEnvelope], phase string, settled bool, received, settleStart, settleEnd time.Time) {
	if phase == "terminal" && helper.Set && helper.Value.Metrics.Set {
		m := helper.Value.Metrics.Value
		start := received.Add(-time.Duration(m.WallTimeMS) * time.Millisecond)
		toolCtx, span := workload.StartRemoteChildSpanAt(ctx, "execute_tool "+helper.Value.Tool, d.TraceContext, start)
		if span != nil {
			span.SetString("task.id", d.TaskID)
			at := start
			for _, part := range []struct {
				name string
				ms   int64
			}{{"queued", m.QueuedMS}, {"drive_wait", m.DriveWaitMS}, {"run", m.RunMS}, {"sync", m.SyncMS}} {
				end := at.Add(time.Duration(part.ms) * time.Millisecond)
				_, child := workload.StartSpanAt(toolCtx, part.name, at)
				child.EndAt(end)
				at = end
			}
			span.EndAt(received)
		}
	}
	if settled {
		_, span := workload.StartRemoteChildSpanAt(ctx, "tool.settle", d.TraceContext, settleStart)
		span.SetString("task.id", d.TaskID)
		span.EndAt(settleEnd)
	}
}
