package instance

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// SessionInstructionCap is how many of a connection's Session instructions,
// in issue order, its streams write before its first Session report is
// stored.
const SessionInstructionCap = 64

// sessionInstructionTypes are the downstream types IssueInstruction stores.
var sessionInstructionTypes = map[string]bool{
	"session.prepare": true, "session.close": true, "session.drives": true,
	"tool.execute": true, "tool.stdin": true, "tool.cancel": true, "mcp.call": true,
}

// Instruction is one connection_instructions row as a stream writes it.
type Instruction struct {
	MessageID      string
	IssueSeq       int64
	Type           string
	Envelope       []byte
	TraceContext   string
	FirstWrittenAt *time.Time
}

// IssueInstruction stores payload, a Session instruction, as the next
// instruction of connection and notifies the pod holding its stream, all in
// tx. Its issue_seq adds 1 to the connection's last_issue_seq, and the row
// lock that update takes lasts until tx ends, so issue sequences follow the
// commit order of the transactions that issue them. The envelope carries
// messageID, or a new msg_ ID when it is empty, and the traceparent of ctx's
// span when there is one; every write sends these exact bytes. An ended
// connection answers StaleConnectionError and stores nothing.
func IssueInstruction(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connection Connection, payload protocol.DownstreamPayload, messageID string) (string, error) {
	kind := payload.MessageType()
	if !sessionInstructionTypes[kind] {
		return "", errors.New("instance: " + kind + " is not a Session instruction")
	}
	var issueSeq int64
	err := tx.QueryRow(ctx, `UPDATE instance_connections SET last_issue_seq=last_issue_seq+1
		WHERE workspace_id=$1 AND id=$2 AND ended_at IS NULL RETURNING last_issue_seq`, string(ws), connection.ID).Scan(&issueSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &StaleConnectionError{}
	}
	if err != nil {
		return "", err
	}
	if messageID == "" {
		messageID = id.New("msg_")
	}
	envelope := protocol.Downstream{ConnectionID: connection.ID, MessageID: messageID, Payload: payload}
	traceContext := workload.ContextTraceparent(ctx)
	if traceContext != "" {
		envelope.Traceparent = protocol.Some(traceContext)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO connection_instructions (workspace_id,connection_id,message_id,issue_seq,type,envelope,trace_context)
		VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''))`, string(ws), connection.ID, messageID, issueSeq, kind, protocol.Encode(envelope), traceContext); err != nil {
		return "", err
	}
	return messageID, NotifyConnection(ctx, tx, ws, connection.ID)
}

// MarkReported records that the final report of connectionID's instruction
// messageID is stored, so no stream writes it again; a second call changes
// nothing. An instruction not issued on the connection answers ErrNotFound.
func MarkReported(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, messageID string) error {
	result, err := tx.Exec(ctx, `UPDATE connection_instructions SET reported_at=COALESCE(reported_at,clock_timestamp())
		WHERE workspace_id=$1 AND connection_id=$2 AND message_id=$3`, string(ws), connectionID, messageID)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows == 0 {
		return errors.Join(err, ErrNotFound)
	}
	return nil
}

// MarkFirstSessionReport records the time the connection's first Session
// report is stored, which lifts the SessionInstructionCap, and notifies the
// pod holding its stream; later calls change nothing.
func MarkFirstSessionReport(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string) error {
	result, err := tx.Exec(ctx, `UPDATE instance_connections SET first_session_report_at=clock_timestamp()
		WHERE workspace_id=$1 AND id=$2 AND first_session_report_at IS NULL`, string(ws), connectionID)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows == 0 {
		return err
	}
	return NotifyConnection(ctx, tx, ws, connectionID)
}

// UnreportedInstructions reads connectionID's instructions without a stored
// report whose issue_seq is at most upTo, in issue order.
func UnreportedInstructions(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string, upTo int64) ([]Instruction, error) {
	rows, err := tx.Query(ctx, `SELECT message_id,issue_seq,type,envelope,COALESCE(trace_context,''),first_written_at FROM connection_instructions
		WHERE workspace_id=$1 AND connection_id=$2 AND reported_at IS NULL AND issue_seq<=$3 ORDER BY issue_seq`, string(ws), connectionID, upTo)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var instructions []Instruction
	for rows.Next() {
		var (
			value   Instruction
			written sql.NullTime
		)
		if err := rows.Scan(&value.MessageID, &value.IssueSeq, &value.Type, &value.Envelope, &value.TraceContext, &written); err != nil {
			return nil, err
		}
		value.FirstWrittenAt = timeOrNil(written)
		instructions = append(instructions, value)
	}
	return instructions, rows.Err()
}

// MarkWritten records the first write of connectionID's instruction
// messageID to any stream. first reports whether this write is that first
// one, and queued is then the time from storing it to this write.
func MarkWritten(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID, messageID string) (first bool, queued time.Duration, err error) {
	var stored, written time.Time
	err = tx.QueryRow(ctx, `UPDATE connection_instructions SET first_written_at=clock_timestamp()
		WHERE workspace_id=$1 AND connection_id=$2 AND message_id=$3 AND first_written_at IS NULL RETURNING created_at,first_written_at`,
		string(ws), connectionID, messageID).Scan(&stored, &written)
	if errors.Is(err, sql.ErrNoRows) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	return true, written.Sub(stored), nil
}

// InstructionPressure counts the unreported instructions of every live
// connection across Workspaces: InProgress written to a stream, Queued not
// yet written, and OldestQueuedSeconds the age of the oldest queued one.
type InstructionPressure struct {
	InProgress          int64
	Queued              int64
	OldestQueuedSeconds float64
}

// ReadInstructionPressure reads the pressure at now through the
// security-definer tetral_computer_hub_instruction_pressure, the only
// cross-Workspace read of these tables.
func ReadInstructionPressure(ctx context.Context, tx *dbconnect.Tx, now time.Time) (InstructionPressure, error) {
	var value InstructionPressure
	err := tx.QueryRow(ctx, `SELECT in_progress,queued,oldest_queued_seconds FROM public.tetral_computer_hub_instruction_pressure($1)`, now).
		Scan(&value.InProgress, &value.Queued, &value.OldestQueuedSeconds)
	return value, err
}
