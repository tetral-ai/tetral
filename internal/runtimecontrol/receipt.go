package runtimecontrol

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type OperationInsert struct {
	Operation      string
	SourceKind     string
	IdempotencyKey string
	RequestHash    string
	AckStatus      string
	RuntimeInputID sql.NullString
	RuntimeWriteID sql.NullString
	ErrorCode      sql.NullString
	ResultJSON     string
	StdinWriteSeq  sql.NullInt64
	Now            time.Time
}

func InsertOperationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, op OperationInsert) error {
	if op.ResultJSON == "" {
		op.ResultJSON = "{}"
	}
	if op.SourceKind == "" {
		op.SourceKind = op.Operation
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key,
			request_hash, ack_status, runtime_input_id, runtime_write_id, error_code,
			result_json, stdin_write_seq, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		op.Operation,
		op.SourceKind,
		op.IdempotencyKey,
		op.RequestHash,
		op.AckStatus,
		op.RuntimeInputID,
		op.RuntimeWriteID,
		op.ErrorCode,
		op.ResultJSON,
		op.StdinWriteSeq,
		op.Now,
	)
	return err
}

func RequestHash(parts ...string) string {
	digest := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(digest[:])
}

const AckCommitted = "committed"

// Receipt identities shared by RPC closeout and Runner custody replay.
const (
	ChildInterruptRequestedEventType      = "agent.thread_interrupt_requested"
	AckRejected                           = "rejected"
	OperationCommitInputs                 = "commit_inputs"
	OperationCommitTaskNotificationResult = "commit_task_notification_result"
)
