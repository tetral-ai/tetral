package runtimecontrol

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Tool relation uniqueness is enforced only by these session_events
// constraints. A writer maps a unique violation from its event INSERT by
// constraint name; the failed statement has already aborted the transaction,
// so the caller returns the mapped error and rolls back everything it wrote.
const (
	toolCallIdentityConstraint = "idx_session_events_model_tool_call_unique"
	toolResultConstraint       = "idx_session_events_tool_result_unique"
	eventIdentityConstraint    = "session_events_pkey"
)

// ToolRelationInsertError maps a unique violation raised by a Tool Use, Tool
// Result or invalid-tool repair event INSERT. A reused model call ID in the
// Thread is the existing declaration conflict. A global event ID collision is
// non-disclosing. A second result for one Tool Use cannot happen after its
// writer holds the Session mutation fence and checked for an earlier result,
// so it is an invariant failure that is never retried. Other errors return
// unchanged.
func ToolRelationInsertError(err error) error {
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.Code != "23505" {
		return err
	}
	switch pgError.ConstraintName {
	case toolCallIdentityConstraint:
		return status.Error(codes.AlreadyExists, "model tool call id already has a durable declaration")
	case eventIdentityConstraint:
		return status.Error(codes.AlreadyExists, "event identity conflict")
	case toolResultConstraint:
		return status.Error(codes.Internal, "Tool Use already has a durable result")
	default:
		return err
	}
}
