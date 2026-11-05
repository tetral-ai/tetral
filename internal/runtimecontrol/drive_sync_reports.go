package runtimecontrol

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// DriveSyncReport is a drive sync report as computer-hub stores it: the
// computer of its binding, and its drive_sync compact.
type DriveSyncReport struct {
	SessionID     string
	ConnectionID  string
	MessageID     string
	BindingID     string
	ComputerID    string
	TaskID        string // "" for a report without task_id
	DriveSyncJSON string
}

// DriveSyncThreadTx is the thread a drive sync report of taskID is delivered
// to, in a transaction holding the Session's runtime mutation lock: the
// thread of the Session's tool.execute record that carries taskID, issued on
// any connection, or the main thread when that thread is failed or
// terminated, or it or an ancestor is closing or closed; the main thread
// without taskID. It reports false when no such record carries taskID.
func DriveSyncThreadTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, sessionID, taskID string) (string, bool, error) {
	var mainThreadID string
	if err := tx.QueryRow(ctx, `SELECT main_thread_id FROM sessions WHERE workspace_id=$1 AND id=$2`, string(ws), sessionID).Scan(&mainThreadID); dbconnect.IsNoRows(err) {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	if taskID == "" {
		return mainThreadID, true, nil
	}
	var threadID, threadStatus string
	err := tx.QueryRow(ctx, `SELECT d.session_thread_id,t.status FROM session_tool_dispatches d
		JOIN session_threads t ON t.workspace_id=d.workspace_id AND t.session_id=d.session_id AND t.id=d.session_thread_id
		WHERE d.workspace_id=$1 AND d.session_id=$2 AND d.task_id=$3 AND d.type='tool.execute'`, string(ws), sessionID, taskID).Scan(&threadID, &threadStatus)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil || threadID == mainThreadID {
		return mainThreadID, err == nil, err
	}
	if threadStatus == "failed" || threadStatus == "terminated" {
		return mainThreadID, true, nil
	}
	closed, err := childcontrol.ThreadOrAncestorClosingOrClosedTx(ctx, tx, string(ws), sessionID, threadID)
	if err != nil {
		return "", false, err
	}
	if closed {
		return mainThreadID, true, nil
	}
	return threadID, true, nil
}

// StoreDriveSyncReportTx stores report for threadID (DriveSyncThreadTx) in
// the caller's transaction: its session_drive_sync_reports row and, with the
// same runtime_input_id, "drive_sync:" and a new dsr_ ID, its drive_sync
// input of threadID (AppendNonStartingInputTx).
func StoreDriveSyncReportTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, report DriveSyncReport, threadID string, now time.Time) error {
	runtimeInputID := queue.RuntimeInputKindDriveSync + ":" + id.New("dsr_")
	if _, err := tx.Exec(ctx, `INSERT INTO session_drive_sync_reports (workspace_id,session_id,connection_id,message_id,binding_id,computer_id,task_id,
		session_thread_id,runtime_input_id,drive_sync_json,created_at) VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,$9,$10,$11)`,
		string(ws), report.SessionID, report.ConnectionID, report.MessageID, report.BindingID, report.ComputerID, report.TaskID,
		threadID, runtimeInputID, report.DriveSyncJSON, now); err != nil {
		return err
	}
	return AppendNonStartingInputTx(ctx, tx, string(ws), report.SessionID, threadID, runtimeInputID, queue.RuntimeInputKindDriveSync, now)
}

// MoveDriveSyncToMainThreadTx moves, when threadIDs' inputs leave them, each
// of their drive_sync inputs that is queued, delivering or accepted to the
// Session's main thread: its active job is cancelled, and it becomes queued
// with no binding on the main thread with a new job. Its report row keeps
// the thread it was stored for. Thread-scoped termination and Bridge's child
// close call it, before they settle the threads' other inputs.
func MoveDriveSyncToMainThreadTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string, threadIDs []string, now time.Time) error {
	rows, err := tx.Query(ctx, `WITH moved AS (
		SELECT inbox.runtime_input_id FROM session_runtime_inbox inbox
		 WHERE inbox.workspace_id=$1 AND inbox.session_id=$2 AND inbox.session_thread_id=ANY($3)
		   AND inbox.input_kind=$4 AND inbox.status IN ('queued','delivering','accepted')
		 FOR UPDATE
	), cancelled AS (
		UPDATE queue_jobs job
		   SET status='cancelled',cancelled_at=$5,lease_token=NULL,leased_by=NULL,leased_at=NULL,leased_until=NULL,
		       lease_previous_attempt_count=NULL,updated_at=$5
		  FROM moved
		 WHERE job.workspace_id=$1 AND job.status IN ('pending','leased')
		   AND job.dedupe_key='runtime_input:' || $1 || ':' || $2 || ':' || moved.runtime_input_id
	)
	UPDATE session_runtime_inbox inbox
	   SET session_thread_id=sessions.main_thread_id,status='queued',binding_id=NULL,binding_generation=NULL,target_pod_uid=NULL,updated_at=$5
	  FROM moved, sessions
	 WHERE inbox.workspace_id=$1 AND inbox.runtime_input_id=moved.runtime_input_id
	   AND sessions.workspace_id=$1 AND sessions.id=$2
	RETURNING inbox.session_thread_id,inbox.runtime_input_id`,
		workspaceID, sessionID, threadIDs, queue.RuntimeInputKindDriveSync, now)
	if err != nil {
		return err
	}
	var inputs []AcceptedRuntimeInput
	for rows.Next() {
		input := AcceptedRuntimeInput{InputKind: queue.RuntimeInputKindDriveSync, EventIDsJSON: "[]"}
		if err := rows.Scan(&input.SessionThreadID, &input.RuntimeInputID); err != nil {
			_ = rows.Close()
			return err
		}
		inputs = append(inputs, input)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, input := range inputs {
		request, err := RuntimeInputEnqueueRequest(workspaceID, sessionID, input, now)
		if err != nil {
			return err
		}
		if _, err := queue.EnqueueTx(ctx, tx, request); err != nil {
			return err
		}
	}
	return nil
}

// DriveSyncPayloadTx is the payload_json of the drive_sync input
// runtimeInputID: {"task_id", "computer_id", "drive_sync"}, task_id null for
// a report without one.
func DriveSyncPayloadTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, runtimeInputID string) (string, bool, error) {
	var payload struct {
		TaskID     *string         `json:"task_id"`
		ComputerID string          `json:"computer_id"`
		DriveSync  json.RawMessage `json:"drive_sync"`
	}
	var driveSync string
	err := tx.QueryRow(ctx, `SELECT task_id,computer_id,drive_sync_json FROM session_drive_sync_reports
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`, workspaceID, sessionID, runtimeInputID).Scan(&payload.TaskID, &payload.ComputerID, &driveSync)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	payload.DriveSync = json.RawMessage(driveSync)
	encoded, err := json.Marshal(payload)
	return string(encoded), err == nil, err
}
