package agentruntimebridge

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// The Session row is the ordering boundary for interrupt birth and every
// conversation writer. Interrupt Inbox custody is the durable mutation barrier;
// Queue ACK remains the separate delivery-barrier release owned by JobRunner.
type sessionInterruptBarrier struct {
	runtimeInputID  string
	sessionThreadID string
}

// activeSessionInterruptDeliveryBarrierTx extends only the input-admission
// edge through the receipt-to-ACK window. The committed Inbox and receipt end
// ordinary closeout exclusion, while the still-pending exact Queue custody
// prevents a preplanned command from opening a successor before JobRunner ACK.
func activeSessionInterruptDeliveryBarrierTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
) (bool, error) {
	rows, err := tx.Query(ctx,
		`SELECT inbox.runtime_input_id
		   FROM session_runtime_inbox inbox
		  WHERE inbox.workspace_id = $1
		    AND inbox.session_id = $2
		    AND inbox.input_kind = 'interrupt_control'
		    AND inbox.status = 'committed'
		    AND EXISTS (
		      SELECT 1
		        FROM session_bridge_operations operation
		       WHERE operation.workspace_id = inbox.workspace_id
		         AND operation.session_id = inbox.session_id
		         AND operation.session_thread_id = inbox.session_thread_id
		         AND operation.operation = 'commit_inputs'
		         AND operation.source_kind = 'interrupt_control'
		         AND operation.idempotency_key = inbox.runtime_input_id
		         AND operation.receipt_json IS NOT NULL
		         AND operation.receipt_json <> ''
		    )
		  ORDER BY inbox.sequence_to NULLS LAST, inbox.created_at, inbox.runtime_input_id
		  FOR UPDATE OF inbox`,
		workspaceID,
		sessionID,
	)
	if err != nil {
		return false, err
	}
	var interruptIDs []string
	for rows.Next() {
		var runtimeInputID string
		if err := rows.Scan(&runtimeInputID); err != nil {
			return false, errors.Join(err, rows.Close())
		}
		interruptIDs = append(interruptIDs, runtimeInputID)
	}
	if err := rows.Err(); err != nil {
		return false, errors.Join(err, rows.Close())
	}
	if err := rows.Close(); err != nil {
		return false, err
	}

	for _, runtimeInputID := range interruptIDs {
		dedupeKey := queue.FormatRuntimeInputDedupeKey(workspace.ID(workspaceID), sessionID, runtimeInputID)
		partitionKey := queue.FormatSessionPartitionKey(workspace.ID(workspaceID), sessionID)
		var queueStatus, storedPartition string
		err := tx.QueryRow(ctx,
			`SELECT status, partition_key
			   FROM queue_jobs
			  WHERE workspace_id = $1
			    AND kind = $2
			    AND dedupe_key = $3
			  FOR UPDATE`,
			workspaceID,
			queue.KindRuntimeInput,
			dedupeKey,
		).Scan(&queueStatus, &storedPartition)
		if dbconnect.IsNoRows(err) {
			return false, status.Error(codes.FailedPrecondition, "committed interrupt receipt is missing Queue custody")
		}
		if err != nil {
			return false, err
		}
		if storedPartition != partitionKey {
			return false, status.Error(codes.FailedPrecondition, "committed interrupt Queue custody has invalid partition")
		}
		switch queueStatus {
		case queue.StatusPending, queue.StatusLeased:
			return true, nil
		case queue.StatusAcknowledged:
			continue
		case queue.StatusCancelled, queue.StatusDeadLettered:
			return false, status.Error(codes.FailedPrecondition, "committed interrupt receipt conflicts with terminal Queue custody")
		default:
			return false, status.Errorf(codes.FailedPrecondition, "committed interrupt Queue custody has invalid status %q", queueStatus)
		}
	}
	return false, nil
}

func requireSessionInputDeliveryAllowedTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	active, err := activeSessionInterruptDeliveryBarrierTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId())
	if err != nil || !active {
		return err
	}
	return sessionInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "session interrupt delivery barrier is active"))
}

type interruptCloseoutContextKey struct{}
type interruptBarrierBirthContextKey struct{}
type runtimePodLossInterruptRepairContextKey struct{}
type interAgentMailBarrierBirthContextKey struct{}

type runtimePodLossInterruptRepairAuthority struct {
	workspaceID       string
	sessionID         string
	runtimeInputID    string
	bindingID         string
	bindingGeneration int64
	targetPodUID      string
}

type interAgentMailBarrierBirthAuthority struct {
	workspaceID       string
	sessionID         string
	runtimeInputID    string
	interruptedThread string
	sourceThread      string
	targetThread      string
}

func withInterruptBarrierBirth(ctx context.Context) context.Context {
	return context.WithValue(ctx, interruptBarrierBirthContextKey{}, true)
}

func withInterruptCloseout(ctx context.Context, runtimeInputID string) context.Context {
	return context.WithValue(ctx, interruptCloseoutContextKey{}, runtimeInputID)
}

func withRuntimePodLossInterruptRepair(ctx context.Context, authority runtimePodLossInterruptRepairAuthority) context.Context {
	return context.WithValue(ctx, runtimePodLossInterruptRepairContextKey{}, authority)
}

func runtimePodLossInterruptRepairFromContext(ctx context.Context) (runtimePodLossInterruptRepairAuthority, bool) {
	authority, ok := ctx.Value(runtimePodLossInterruptRepairContextKey{}).(runtimePodLossInterruptRepairAuthority)
	return authority, ok
}

func withInterAgentMailBarrierBirth(ctx context.Context, authority interAgentMailBarrierBirthAuthority) context.Context {
	return context.WithValue(ctx, interAgentMailBarrierBirthContextKey{}, authority)
}

func interruptCloseoutRuntimeInputID(ctx context.Context) string {
	value, _ := ctx.Value(interruptCloseoutContextKey{}).(string)
	return value
}

func activeSessionInterruptBarrierTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
) (sessionInterruptBarrier, bool, error) {
	// Queue partition sequence is the Session-wide interrupt election order.
	// Inbox sequence_to is Thread-local and cannot order custody across Threads.
	rows, err := tx.Query(ctx,
		`SELECT inbox.runtime_input_id,
		        inbox.session_thread_id,
		        inbox.status,
		        EXISTS (
		          SELECT 1
		            FROM session_bridge_operations operation
		           WHERE operation.workspace_id = inbox.workspace_id
		             AND operation.session_id = inbox.session_id
		             AND operation.session_thread_id = inbox.session_thread_id
		             AND operation.operation = 'commit_inputs'
		             AND operation.source_kind = 'interrupt_control'
		             AND operation.idempotency_key = inbox.runtime_input_id
		             AND operation.receipt_json IS NOT NULL
		             AND operation.receipt_json <> ''
		        ) AS has_receipt
		   FROM session_runtime_inbox inbox
		   LEFT JOIN queue_jobs queue
		     ON queue.workspace_id = inbox.workspace_id
		    AND queue.kind = 'runtime_input'
		    AND queue.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		    AND queue.status IN ('pending', 'leased')
		  WHERE inbox.workspace_id = $1
		    AND inbox.session_id = $2
		    AND inbox.input_kind = 'interrupt_control'
		  ORDER BY queue.queue_partition_sequence NULLS LAST, inbox.created_at, inbox.runtime_input_id
		  FOR UPDATE OF inbox`,
		workspaceID,
		sessionID,
	)
	if err != nil {
		return sessionInterruptBarrier{}, false, err
	}
	defer func() { _ = rows.Close() }()
	var active sessionInterruptBarrier
	for rows.Next() {
		var runtimeInputID string
		var sessionThreadID string
		var statusValue string
		var hasReceipt bool
		if err := rows.Scan(&runtimeInputID, &sessionThreadID, &statusValue, &hasReceipt); err != nil {
			return sessionInterruptBarrier{}, false, err
		}
		switch statusValue {
		case "queued", "delivering", "accepted":
			if hasReceipt {
				return sessionInterruptBarrier{}, false, status.Error(codes.FailedPrecondition, "active interrupt Inbox already has a closeout receipt")
			}
			if active.runtimeInputID == "" {
				active.runtimeInputID = runtimeInputID
				active.sessionThreadID = sessionThreadID
			}
		case "committed":
			if !hasReceipt {
				return sessionInterruptBarrier{}, false, status.Error(codes.FailedPrecondition, "committed interrupt Inbox is missing its closeout receipt")
			}
		case "cancelled", "dead_lettered":
			if hasReceipt {
				return sessionInterruptBarrier{}, false, status.Error(codes.FailedPrecondition, "terminal interrupt Inbox conflicts with a closeout receipt")
			}
		default:
			return sessionInterruptBarrier{}, false, status.Errorf(codes.FailedPrecondition, "interrupt Inbox has invalid custody status %q", statusValue)
		}
	}
	if err := rows.Err(); err != nil {
		return sessionInterruptBarrier{}, false, err
	}
	return active, active.runtimeInputID != "", nil
}

func requireSessionMutationAllowedTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	// Only validateInterruptLeaseRefTx and exact final-lease exhaustion mint this
	// transaction-local authority. Once minted, the closeout may pass through
	// intermediate Inbox=committed state before its receipt row is inserted.
	if interruptCloseoutRuntimeInputID(ctx) != "" {
		return nil
	}
	barrier, active, err := activeSessionInterruptBarrierTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	)
	if err != nil || !active {
		return err
	}
	if authority, ok := ctx.Value(interAgentMailBarrierBirthContextKey{}).(interAgentMailBarrierBirthAuthority); ok &&
		authority.workspaceID == scope.GetWorkspaceId() && authority.sessionID == scope.GetSessionId() &&
		authority.runtimeInputID == barrier.runtimeInputID && authority.interruptedThread == barrier.sessionThreadID &&
		(scope.GetSessionThreadId() == authority.sourceThread || scope.GetSessionThreadId() == authority.targetThread) &&
		authority.sourceThread != authority.interruptedThread &&
		authority.targetThread == authority.interruptedThread {
		return nil
	}
	if allowed, _ := ctx.Value(interruptBarrierBirthContextKey{}).(bool); allowed {
		return nil
	}
	return sessionInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "session interrupt barrier is active"))
}

func validateInterruptLeaseRefTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeInputID string,
	ref *bridgev1.InterruptLeaseRef,
) error {
	if ref == nil || ref.GetJobId() == "" || ref.GetLeaseToken() == "" || ref.GetPartitionKey() == "" || ref.GetDedupeKey() == "" {
		return status.Error(codes.InvalidArgument, "interrupt closeout lease authority is incomplete")
	}
	workspaceID := workspace.ID(scope.GetWorkspaceId())
	expectedPartition := queue.FormatSessionPartitionKey(workspaceID, scope.GetSessionId())
	expectedDedupe := queue.FormatRuntimeInputDedupeKey(workspaceID, scope.GetSessionId(), runtimeInputID)
	if ref.GetPartitionKey() != expectedPartition || ref.GetDedupeKey() != expectedDedupe {
		return status.Error(codes.FailedPrecondition, "interrupt closeout lease binding is invalid")
	}
	barrier, active, err := activeSessionInterruptBarrierTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId())
	if err != nil {
		return err
	}
	if !active || barrier.runtimeInputID != runtimeInputID {
		return sessionInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "interrupt closeout barrier is stale"))
	}
	live, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID:  workspaceID,
		JobID:        ref.GetJobId(),
		LeaseToken:   ref.GetLeaseToken(),
		Kind:         queue.KindRuntimeInput,
		PartitionKey: ref.GetPartitionKey(),
		DedupeKey:    ref.GetDedupeKey(),
	})
	if err != nil {
		return err
	}
	if !live {
		return sessionInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "interrupt closeout lease is stale"))
	}
	var payloadSessionID, payloadThreadID, payloadRuntimeInputID, payloadInputKind string
	if err := tx.QueryRow(ctx,
		`SELECT payload_json::jsonb ->> 'session_id',
		        payload_json::jsonb ->> 'session_thread_id',
		        payload_json::jsonb ->> 'runtime_input_id',
		        payload_json::jsonb ->> 'input_kind'
		   FROM queue_jobs
		  WHERE workspace_id=$1 AND id=$2
		  FOR UPDATE`,
		workspaceID,
		ref.GetJobId(),
	).Scan(&payloadSessionID, &payloadThreadID, &payloadRuntimeInputID, &payloadInputKind); err != nil {
		return err
	}
	if payloadSessionID != scope.GetSessionId() || payloadThreadID != scope.GetSessionThreadId() ||
		payloadRuntimeInputID != runtimeInputID || payloadInputKind != "interrupt_control" {
		return status.Error(codes.FailedPrecondition, "interrupt closeout lease payload binding is invalid")
	}
	return nil
}
