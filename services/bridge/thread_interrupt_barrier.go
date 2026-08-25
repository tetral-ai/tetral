package agentruntimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Interrupt Inbox custody is a Thread-local mutation barrier. Queue ACK remains
// the separate delivery-barrier release owned by JobRunner. Session-wide
// infrastructure joins the common Session arbitration lock before consulting
// barriers across all Threads.
type threadInterruptBarrier struct {
	runtimeInputID  string
	sessionThreadID string
}

type activeInterruptCustody struct {
	threadInterruptBarrier
	queueSequence int64
	inboxStatus   string
	hasReceipt    bool
}

// lockRuntimeInputQueueCustodyTx locks active Runtime-input Queue rows before a
// closeout transaction locks the corresponding Inbox or Thread rows. An empty
// Thread list means every Thread in the Session; otherwise only the declared
// target set is included.
func lockRuntimeInputQueueCustodyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	sessionThreadIDs []string,
) error {
	threadIDsJSON, err := json.Marshal(sessionThreadIDs)
	if err != nil {
		return err
	}
	rows, err := tx.Query(ctx,
		`SELECT queue.id
		   FROM queue_jobs queue
		  WHERE queue.workspace_id = $1
		    AND queue.causal_session_id = $2
		    AND queue.kind = 'runtime_input'
		    AND queue.status IN ('pending', 'leased')
		    AND ($3 OR queue.delivery_thread_id IN (
		      SELECT jsonb_array_elements_text($4::jsonb)
		    ))
		  ORDER BY queue.id
		  FOR UPDATE OF queue`,
		workspaceID,
		sessionID,
		len(sessionThreadIDs) == 0,
		string(threadIDsJSON),
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var queueID string
		if err := rows.Scan(&queueID); err != nil {
			return errors.Join(err, rows.Close())
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Join(err, rows.Close())
	}
	return rows.Close()
}

// lockActiveInterruptQueueCustodyTx starts from active Queue authority and
// locks every matching Queue row in canonical ID order. Inbox rows are locked
// separately after exact lease validation so no caller can invert Queue row
// order or the Queue-before-Inbox class order.
func lockActiveInterruptQueueCustodyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	sessionThreadID string,
) ([]activeInterruptCustody, error) {
	rows, err := tx.Query(ctx,
		`SELECT queue.id, queue.queue_partition_sequence,
		        inbox.runtime_input_id, inbox.session_thread_id
		   FROM queue_jobs queue
		   JOIN session_runtime_inbox inbox
		     ON inbox.workspace_id = queue.workspace_id
		    AND inbox.session_id = queue.causal_session_id
		    AND inbox.session_thread_id = queue.delivery_thread_id
		    AND queue.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		  WHERE queue.workspace_id = $1
		    AND queue.causal_session_id = $2
		    AND queue.kind = 'runtime_input'
		    AND queue.delivery_scope = 'thread'
		    AND queue.control_class = 'interrupt'
		    AND queue.status IN ('pending', 'leased')
		    AND ($3 = '' OR queue.delivery_thread_id = $3)
		    AND inbox.input_kind = 'interrupt_control'
		  ORDER BY queue.id
		  FOR UPDATE OF queue`,
		workspaceID,
		sessionID,
		sessionThreadID,
	)
	if err != nil {
		return nil, err
	}
	var active []activeInterruptCustody
	for rows.Next() {
		var queueID string
		var custody activeInterruptCustody
		if err := rows.Scan(&queueID, &custody.queueSequence, &custody.runtimeInputID, &custody.sessionThreadID); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		active = append(active, custody)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(err, rows.Close())
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	return active, nil
}

func lockActiveInterruptInboxCustodyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	active []activeInterruptCustody,
) error {
	for index := range active {
		custody := &active[index]
		if err := tx.QueryRow(ctx,
			`SELECT inbox.status,
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
			        )
			   FROM session_runtime_inbox inbox
			  WHERE inbox.workspace_id = $1
			    AND inbox.runtime_input_id = $2
			  FOR UPDATE OF inbox`,
			workspaceID,
			custody.runtimeInputID,
		).Scan(&custody.inboxStatus, &custody.hasReceipt); err != nil {
			return err
		}
	}
	return nil
}

func lockActiveInterruptCustodyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	sessionThreadID string,
) ([]activeInterruptCustody, error) {
	active, err := lockActiveInterruptQueueCustodyTx(ctx, tx, workspaceID, sessionID, sessionThreadID)
	if err != nil {
		return nil, err
	}
	if err := lockActiveInterruptInboxCustodyTx(ctx, tx, workspaceID, active); err != nil {
		return nil, err
	}
	sortActiveInterruptCustody(active)
	return active, nil
}

func sortActiveInterruptCustody(active []activeInterruptCustody) {
	sort.SliceStable(active, func(i, j int) bool {
		if active[i].queueSequence != active[j].queueSequence {
			return active[i].queueSequence < active[j].queueSequence
		}
		return active[i].runtimeInputID < active[j].runtimeInputID
	})
}

// activeThreadInterruptDeliveryBarrierTx extends only the input-admission
// edge through the receipt-to-ACK window. The committed Inbox and receipt end
// ordinary closeout exclusion, while the still-pending exact Queue custody
// prevents a preplanned command from opening a successor before JobRunner ACK.
func activeThreadInterruptDeliveryBarrierTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	sessionThreadID string,
) (bool, error) {
	active, err := lockActiveInterruptCustodyTx(ctx, tx, workspaceID, sessionID, sessionThreadID)
	if err != nil {
		return false, err
	}
	for _, custody := range active {
		switch custody.inboxStatus {
		case "queued", "delivering", "accepted":
			if custody.hasReceipt {
				return false, status.Error(codes.FailedPrecondition, "active interrupt Inbox already has a closeout receipt")
			}
		case "committed":
			if !custody.hasReceipt {
				return false, status.Error(codes.FailedPrecondition, "committed interrupt Inbox is missing its closeout receipt")
			}
			return true, nil
		case "cancelled", "dead_lettered":
			return false, status.Error(codes.FailedPrecondition, "active interrupt Queue custody conflicts with terminal Inbox custody")
		default:
			return false, status.Errorf(codes.FailedPrecondition, "interrupt Inbox has invalid custody status %q", custody.inboxStatus)
		}
	}
	return false, nil
}

func requireThreadInputDeliveryAllowedTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	active, err := activeThreadInterruptDeliveryBarrierTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId())
	if err != nil || !active {
		return err
	}
	return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "thread interrupt delivery barrier is active"))
}

type interruptCloseoutContextKey struct{}
type sessionRootTerminationContextKey struct{}

type interruptCloseoutAuthority struct {
	workspaceID     string
	sessionID       string
	sessionThreadID string
	runtimeInputID  string
}

type sessionRootTerminationAuthority struct {
	workspaceID  string
	sessionID    string
	mainThreadID string
	operationID  string
}

func withInterruptCloseout(ctx context.Context, workspaceID string, sessionID string, sessionThreadID string, runtimeInputID string) context.Context {
	return context.WithValue(ctx, interruptCloseoutContextKey{}, interruptCloseoutAuthority{
		workspaceID: workspaceID, sessionID: sessionID, sessionThreadID: sessionThreadID, runtimeInputID: runtimeInputID,
	})
}

func withSessionRootTermination(ctx context.Context, workspaceID string, sessionID string, mainThreadID string, operationID string) context.Context {
	return context.WithValue(ctx, sessionRootTerminationContextKey{}, sessionRootTerminationAuthority{
		workspaceID: workspaceID, sessionID: sessionID, mainThreadID: mainThreadID, operationID: operationID,
	})
}

func sessionRootTerminationAuthorityFromContext(ctx context.Context) (sessionRootTerminationAuthority, bool) {
	authority, ok := ctx.Value(sessionRootTerminationContextKey{}).(sessionRootTerminationAuthority)
	return authority, ok
}

func interruptCloseoutAuthorityFromContext(ctx context.Context) (interruptCloseoutAuthority, bool) {
	authority, ok := ctx.Value(interruptCloseoutContextKey{}).(interruptCloseoutAuthority)
	return authority, ok
}

func activeInterruptBarriersTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	sessionThreadID string,
) (map[string]threadInterruptBarrier, error) {
	rows, err := lockActiveInterruptCustodyTx(ctx, tx, workspaceID, sessionID, sessionThreadID)
	if err != nil {
		return nil, err
	}
	return activeInterruptBarriersFromCustody(rows)
}

func activeInterruptBarriersFromCustody(rows []activeInterruptCustody) (map[string]threadInterruptBarrier, error) {
	active := make(map[string]threadInterruptBarrier)
	for _, custody := range rows {
		switch custody.inboxStatus {
		case "queued", "delivering", "accepted":
			if custody.hasReceipt {
				return nil, status.Error(codes.FailedPrecondition, "active interrupt Inbox already has a closeout receipt")
			}
			if _, exists := active[custody.sessionThreadID]; !exists {
				active[custody.sessionThreadID] = custody.threadInterruptBarrier
			}
		case "committed":
			if !custody.hasReceipt {
				return nil, status.Error(codes.FailedPrecondition, "committed interrupt Inbox is missing its closeout receipt")
			}
		case "cancelled", "dead_lettered":
			return nil, status.Error(codes.FailedPrecondition, "active interrupt Queue custody conflicts with terminal Inbox custody")
		default:
			return nil, status.Errorf(codes.FailedPrecondition, "interrupt Inbox has invalid custody status %q", custody.inboxStatus)
		}
	}
	return active, nil
}

func activeInterruptBarrierTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	sessionThreadID string,
) (threadInterruptBarrier, bool, error) {
	if sessionThreadID == "" {
		return threadInterruptBarrier{}, false, status.Error(codes.InvalidArgument, "interrupt barrier lookup requires a Thread")
	}
	barriers, err := activeInterruptBarriersTx(ctx, tx, workspaceID, sessionID, sessionThreadID)
	if err != nil {
		return threadInterruptBarrier{}, false, err
	}
	barrier, active := barriers[sessionThreadID]
	return barrier, active, nil
}

func requireThreadMutationAllowedTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	// Session-root termination is minted only after the main declaration or
	// final delivery identity wins Session arbitration. It closes every Thread
	// in that one transaction; it is not an ordinary cross-Thread bypass.
	if authority, ok := sessionRootTerminationAuthorityFromContext(ctx); ok {
		if authority.workspaceID == scope.GetWorkspaceId() && authority.sessionID == scope.GetSessionId() &&
			authority.mainThreadID != "" && authority.operationID != "" {
			return nil
		}
		return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "Session-root termination authority is invalid"))
	}
	// Only validateInterruptLeaseRefTx and exact final-lease exhaustion mint this
	// transaction-local authority. Once minted, the closeout may pass through
	// intermediate Inbox=committed state before its receipt row is inserted.
	if authority, ok := interruptCloseoutAuthorityFromContext(ctx); ok {
		if authority.workspaceID == scope.GetWorkspaceId() && authority.sessionID == scope.GetSessionId() &&
			authority.sessionThreadID == scope.GetSessionThreadId() && authority.runtimeInputID != "" {
			return nil
		}
		return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "interrupt closeout authority does not own this Thread"))
	}
	_, active, err := activeInterruptBarrierTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil || !active {
		return err
	}
	return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "thread interrupt barrier is active"))
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
	activeCustody, err := lockActiveInterruptQueueCustodyTx(
		ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	)
	if err != nil {
		return err
	}
	foundRuntimeInput := false
	for _, custody := range activeCustody {
		if custody.runtimeInputID == runtimeInputID {
			foundRuntimeInput = true
			break
		}
	}
	if !foundRuntimeInput {
		return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "interrupt closeout barrier is stale"))
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
		return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "interrupt closeout lease is stale"))
	}
	if err := lockActiveInterruptInboxCustodyTx(ctx, tx, scope.GetWorkspaceId(), activeCustody); err != nil {
		return err
	}
	sortActiveInterruptCustody(activeCustody)
	barriers, err := activeInterruptBarriersFromCustody(activeCustody)
	if err != nil {
		return err
	}
	barrier, active := barriers[scope.GetSessionThreadId()]
	if !active || barrier.runtimeInputID != runtimeInputID {
		return threadInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "interrupt closeout barrier is stale"))
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
