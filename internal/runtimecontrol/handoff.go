package runtimecontrol

import (
	"context"
	"database/sql"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// ReleaseBindingTx checks canonical all-Thread checkpoint facts and commits
// custody transfer, immutable receipt, binding retirement, and continuation
// jobs together. The caller owns authentication, Workspace transaction and the
// bounded attempt. No external I/O occurs under Session arbitration.
func ReleaseBindingTx(ctx context.Context, tx *dbconnect.Tx, identity ProcessIdentity, request *bridgev1.ReleaseRuntimeBindingRequest, now time.Time) (*bridgev1.ReleaseRuntimeBindingResponse, int, error) {
	if request.GetWorkspaceId() == "" || request.GetSessionId() == "" || request.GetBindingId() == "" || request.GetBindingGeneration() <= 0 || request.GetOperationId() == "" || request.GetRuntimeProcessId() != identity.ID {
		return nil, 0, status.Error(codes.InvalidArgument, "runtime release identity is malformed")
	}
	lifecycle, err := LockSessionRuntimeArbitrationTx(ctx, tx, request.GetWorkspaceId(), request.GetSessionId())
	if err != nil {
		return nil, 0, err
	}
	digest := RequestHash(request.GetWorkspaceId(), request.GetSessionId(), request.GetBindingId(), identity.Namespace, identity.PodUID, identity.ID, request.GetOperationId())
	// Generation is independently compared to the persisted proof below.
	var handoffID, oldNamespace, oldPodUID, oldProcessID, oldBindingID, oldDigest string
	var oldGeneration int64
	err = tx.QueryRow(ctx, `SELECT handoff_id,runtime_namespace,runtime_pod_uid,runtime_process_id,binding_id,binding_generation,operation_digest
  FROM session_runtime_handoffs WHERE workspace_id=$1 AND session_id=$2 AND operation_id=$3`, request.GetWorkspaceId(), request.GetSessionId(), request.GetOperationId()).Scan(&handoffID, &oldNamespace, &oldPodUID, &oldProcessID, &oldBindingID, &oldGeneration, &oldDigest)
	if err == nil {
		if oldNamespace != identity.Namespace || oldPodUID != identity.PodUID || oldProcessID != identity.ID || oldBindingID != request.GetBindingId() || oldGeneration != request.GetBindingGeneration() || oldDigest != digest {
			return nil, 0, LifecycleError(codes.AlreadyExists, "RUNTIME_OPERATION_CONFLICT", "runtime release operation conflicts with its receipt")
		}
		response, err := readHandoffReceiptTx(ctx, tx, request, handoffID, identity.PodUID)
		return response, 0, err
	}
	if !dbconnect.IsNoRows(err) {
		return nil, 0, err
	}
	if lifecycle != "active" && lifecycle != "admitted" {
		return nil, 0, LifecycleError(codes.FailedPrecondition, "RUNTIME_BINDING_STALE", "runtime release Session lifecycle is terminal")
	}
	var sessionStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM sessions WHERE workspace_id=$1 AND id=$2`, request.GetWorkspaceId(), request.GetSessionId()).Scan(&sessionStatus); err != nil {
		return nil, 0, err
	}
	if sessionStatus == "terminated" {
		return nil, 0, LifecycleError(codes.FailedPrecondition, "RUNTIME_BINDING_STALE", "runtime release Session is terminal")
	}
	binding, found, err := ReadOptionalRuntimeBindingForDeliveryTx(ctx, tx, request.GetWorkspaceId(), request.GetSessionId())
	if err != nil {
		return nil, 0, err
	}
	if !found || binding.BindingID != request.GetBindingId() || binding.BindingGeneration != request.GetBindingGeneration() || binding.Namespace != identity.Namespace || binding.PodUID != identity.PodUID || binding.RuntimeProcessID != identity.ID {
		return nil, 0, LifecycleError(codes.FailedPrecondition, "RUNTIME_BINDING_STALE", "runtime release binding is stale")
	}
	process, err := RequireCurrentProcessTx(ctx, tx, identity)
	if err != nil {
		return nil, 0, err
	}
	if process.Phase != ProcessDraining {
		return nil, 0, LifecycleError(codes.FailedPrecondition, "RUNTIME_PROCESS_NOT_DRAINING", "runtime process has not acknowledged draining")
	}
	threads, err := checkpointThreadsTx(ctx, tx, request.GetWorkspaceId(), request.GetSessionId())
	if err != nil {
		return nil, 0, err
	}
	handedBack, err := HandBackQuiescedRuntimeInputsTx(ctx, tx, request.GetWorkspaceId(), request.GetSessionId(), binding, now)
	if err != nil {
		return nil, 0, err
	}
	handoffID = id.New("handoff_")
	if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_handoffs(workspace_id,session_id,handoff_id,operation_id,operation_digest,binding_id,binding_generation,runtime_namespace,runtime_pod_uid,runtime_process_id)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, request.GetWorkspaceId(), request.GetSessionId(), handoffID, request.GetOperationId(), digest, binding.BindingID, binding.BindingGeneration, identity.Namespace, identity.PodUID, identity.ID); err != nil {
		return nil, 0, err
	}
	for _, thread := range threads {
		var queueID sql.NullString
		if thread.Disposition == bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_RECOVER {
			enqueue, err := queue.NewRuntimeHandoffEnqueueRequest(workspace.ID(request.GetWorkspaceId()), request.GetSessionId(), thread.SessionThreadId, handoffID, now)
			if err != nil {
				return nil, 0, err
			}
			job, err := queue.EnqueueTx(ctx, tx, enqueue)
			if err != nil {
				return nil, 0, err
			}
			queueID = sql.NullString{String: job.ID, Valid: true}
			thread.QueueJobId = job.ID
		}
		disposition := "idle"
		if queueID.Valid {
			disposition = "recover"
		}
		if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_handoff_threads(workspace_id,session_id,handoff_id,session_thread_id,disposition,queue_job_id) VALUES($1,$2,$3,$4,$5,$6)`, request.GetWorkspaceId(), request.GetSessionId(), handoffID, thread.SessionThreadId, disposition, queueID); err != nil {
			return nil, 0, err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM session_runtime_bindings WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4 AND runtime_process_id=$5`, request.GetWorkspaceId(), request.GetSessionId(), binding.BindingID, binding.BindingGeneration, identity.ID); err != nil {
		return nil, 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE session_runtime_status SET binding_id=NULL,binding_generation=NULL,cleanup_after=NULL,cleanup_enqueued_at=NULL,cleanup_claimed_at=NULL,cleanup_job_id=NULL,updated_at=$5
  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`, request.GetWorkspaceId(), request.GetSessionId(), binding.BindingID, binding.BindingGeneration, now); err != nil {
		return nil, 0, err
	}
	return &bridgev1.ReleaseRuntimeBindingResponse{OperationId: request.GetOperationId(), HandoffId: handoffID, ReleasedBinding: &bridgev1.RuntimeBindingRef{BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, TargetPodUid: identity.PodUID, RuntimeProcessId: identity.ID}, Threads: threads}, handedBack, nil
}

func readHandoffReceiptTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.ReleaseRuntimeBindingRequest, handoffID, podUID string) (*bridgev1.ReleaseRuntimeBindingResponse, error) {
	rows, err := tx.Query(ctx, `SELECT session_thread_id,disposition,queue_job_id FROM session_runtime_handoff_threads WHERE workspace_id=$1 AND session_id=$2 AND handoff_id=$3 ORDER BY session_thread_id`, request.GetWorkspaceId(), request.GetSessionId(), handoffID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	response := &bridgev1.ReleaseRuntimeBindingResponse{OperationId: request.GetOperationId(), HandoffId: handoffID, ReleasedBinding: &bridgev1.RuntimeBindingRef{BindingId: request.GetBindingId(), BindingGeneration: request.GetBindingGeneration(), TargetPodUid: podUID, RuntimeProcessId: request.GetRuntimeProcessId()}, Threads: []*bridgev1.RuntimeHandoffThread{}}
	for rows.Next() {
		var threadID, disposition string
		var queueID sql.NullString
		if err := rows.Scan(&threadID, &disposition, &queueID); err != nil {
			return nil, err
		}
		result := &bridgev1.RuntimeHandoffThread{SessionThreadId: threadID, Disposition: bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_IDLE}
		if disposition == "recover" {
			result.Disposition = bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_RECOVER
			result.QueueJobId = queueID.String
		}
		response.Threads = append(response.Threads, result)
	}
	return response, rows.Err()
}

// checkpointThreadsTx rejects any unresolved producer that cannot be cold
// reconstructed. Thread idleness is derived from durable turns and routes,
// never from a caller's purported idle list or main-thread status alone.
func checkpointThreadsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string) ([]*bridgev1.RuntimeHandoffThread, error) {
	rows, err := tx.Query(ctx, `SELECT id,status FROM session_threads WHERE workspace_id=$1 AND session_id=$2 ORDER BY id FOR UPDATE`, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	type threadFact struct{ id, status string }
	var facts []threadFact
	for rows.Next() {
		var fact threadFact
		if err := rows.Scan(&fact.id, &fact.status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		facts = append(facts, fact)
	}
	err = rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	result := make([]*bridgev1.RuntimeHandoffThread, 0, len(facts))
	for _, fact := range facts {
		var openProvider, unownedTool, uncommittedReviewer, pendingTool bool
		err := tx.QueryRow(ctx, `SELECT
   EXISTS(SELECT 1 FROM session_events started WHERE started.workspace_id=$1 AND started.session_id=$2 AND started.session_thread_id=$3 AND started.type='span.model_request_start'
    AND NOT EXISTS(SELECT 1 FROM session_events ended WHERE ended.workspace_id=started.workspace_id AND ended.session_id=started.session_id AND ended.session_thread_id=started.session_thread_id AND ended.model_request_id=started.model_request_id AND ended.type='span.model_request_end')),
   EXISTS(SELECT 1 FROM session_events tool WHERE tool.workspace_id=$1 AND tool.session_id=$2 AND tool.session_thread_id=$3 AND tool.type IN ('agent.tool_use','agent.mcp_tool_use')
    AND NOT EXISTS(SELECT 1 FROM session_events settled WHERE settled.workspace_id=tool.workspace_id AND settled.session_id=tool.session_id AND settled.session_thread_id=tool.session_thread_id AND settled.type IN ('agent.tool_result','agent.mcp_tool_result')
      AND COALESCE(settled.payload_json::jsonb->>'tool_use_event_id',settled.payload_json::jsonb->>'tool_use_id',settled.payload_json::jsonb->>'mcp_tool_use_id')=tool.event_id)
    AND NOT EXISTS(SELECT 1 FROM session_runtime_tool_results execution WHERE execution.workspace_id=tool.workspace_id AND execution.session_id=tool.session_id AND execution.session_thread_id=tool.session_thread_id AND execution.tool_use_event_id=tool.event_id AND execution.tool_kind='sandbox_tool' AND execution.execution_state IN ('pending','preparing','running','waiting_activation','waiting_materialization','terminal_unconsumed'))
    AND NOT EXISTS(SELECT 1 FROM session_pending_tool_uses approval WHERE approval.workspace_id=tool.workspace_id AND approval.session_id=tool.session_id AND approval.session_thread_id=tool.session_thread_id AND approval.tool_use_event_id=tool.event_id AND approval.status IN ('pending','resolving') AND (approval.decision IS NULL OR approval.decision='deny') AND tool.projection_json::jsonb->>'evaluated_permission'='ask')),
   EXISTS(SELECT 1 FROM session_runtime_inbox reviewer WHERE reviewer.workspace_id=$1 AND reviewer.session_id=$2 AND reviewer.session_thread_id=$3 AND reviewer.input_kind='approval_review' AND reviewer.status IN ('delivering','accepted')),
   EXISTS(SELECT 1 FROM session_pending_tool_uses pending WHERE pending.workspace_id=$1 AND pending.session_id=$2 AND pending.session_thread_id=$3 AND pending.status IN ('pending','resolving'))`, workspaceID, sessionID, fact.id).Scan(&openProvider, &unownedTool, &uncommittedReviewer, &pendingTool)
		if err != nil {
			return nil, err
		}
		closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, workspaceID, sessionID, fact.id)
		if err != nil {
			return nil, err
		}
		if openProvider || unownedTool || uncommittedReviewer || closing {
			return nil, LifecycleError(codes.FailedPrecondition, "RUNTIME_NOT_CHECKPOINTED", "runtime Thread has unresolved checkpoint custody")
		}
		turnID, err := LoadOpenDurableTurnIDTx(ctx, tx, SessionScope(workspaceID, sessionID, fact.id))
		if err != nil {
			return nil, err
		}
		disposition := bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_IDLE
		terminal := fact.status == "failed" || fact.status == "terminated" || fact.status == "closed_for_runtime"
		if !terminal && (turnID != nil || pendingTool || fact.status == "running" || fact.status == "rescheduling" || fact.status == "requires_action") {
			disposition = bridgev1.RuntimeHandoffDisposition_RUNTIME_HANDOFF_DISPOSITION_RECOVER
		}
		result = append(result, &bridgev1.RuntimeHandoffThread{SessionThreadId: fact.id, Disposition: disposition})
	}
	return result, nil
}
