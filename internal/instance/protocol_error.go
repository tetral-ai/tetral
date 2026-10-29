package instance

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// ProtocolErrorRecord is one accepted protocol-error report. RequestMessageID
// is kept as the CLI sent it, also when it names no message Engine knows.
type ProtocolErrorRecord struct {
	MessageID        string
	Code             string
	Message          string
	RequestMessageID *string
	At               time.Time
}

// RecordProtocolError stores report for connectionID.
func RecordProtocolError(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, connectionID string, report ProtocolErrorRecord) error {
	_, err := tx.Exec(ctx, `INSERT INTO connection_protocol_errors (workspace_id,connection_id,message_id,code,message,request_message_id,created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, string(ws), connectionID, report.MessageID, report.Code, []byte(report.Message), report.RequestMessageID, report.At)
	return err
}
