package runtimecontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type AcceptedRuntimeInput struct {
	SessionThreadID string
	RuntimeInputID  string
	InputKind       string
	InboxStatus     string
	EventIDsJSON    string
	SequenceFrom    sql.NullInt64
	SequenceTo      sql.NullInt64
	RejectionReason sql.NullString
	QueueJobID      sql.NullString
	QueueStatus     sql.NullString
}

// HandBackRuntimeInputsTx transfers noncommitted input custody to Queue. The
// caller must hold Session arbitration and the exact binding/process fence,
// established either by cooperative release or proven-loss repair. Existing
// job identities and attempt lineage survive; committed inputs are untouched.
// Uncommitted approval reviewers require their admitted dependency to settle
// before cooperative release and remain under their existing closeout owner.
func HandBackRuntimeInputsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	binding Binding,
	now time.Time,
) (int, error) {
	return handBackRuntimeInputsTx(ctx, tx, workspaceID, sessionID, binding, now, false)
}

// HandBackQuiescedRuntimeInputsTx reclaims delivering inputs as well: local
// producers have joined, and their accepted-but-unacknowledged delivery must
// remain discoverable when the old binding is removed. The existing job's
// identity and attempt count survive, while its old lease is fenced.
func HandBackQuiescedRuntimeInputsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string, binding Binding, now time.Time) (int, error) {
	return handBackRuntimeInputsTx(ctx, tx, workspaceID, sessionID, binding, now, true)
}

func handBackRuntimeInputsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID string, binding Binding, now time.Time, quiesced bool) (int, error) {
	rows, err := tx.Query(ctx,
		`SELECT inbox.session_thread_id,
		        inbox.runtime_input_id,
		        inbox.input_kind,
		        inbox.status,
		        inbox.event_ids_json,
		        inbox.sequence_from,
		        inbox.sequence_to,
		        inbox.rejection_reason_code,
		        active.id,
		        active.status
		   FROM session_runtime_inbox inbox
		   LEFT JOIN LATERAL (
		       SELECT job.id, job.status
		         FROM queue_jobs job
		        WHERE job.workspace_id = inbox.workspace_id
		          AND job.kind = 'runtime_input'
		          AND job.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		          AND job.status IN ('pending', 'leased')
		        ORDER BY job.created_at, job.id
		        LIMIT 1
		        FOR UPDATE OF job
		   ) active ON true
		  WHERE inbox.workspace_id = $1
		    AND inbox.session_id = $2
		    AND inbox.status IN ('delivering', 'accepted')
		    AND inbox.input_kind <> 'approval_review'
		    AND inbox.binding_id = $3
		    AND inbox.binding_generation = $4
		    AND inbox.target_pod_uid = $5
		  ORDER BY inbox.created_at, inbox.runtime_input_id
		  FOR UPDATE OF inbox`,
		workspaceID,
		sessionID,
		binding.BindingID,
		binding.BindingGeneration,
		binding.PodUID,
	)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	inputs := make([]AcceptedRuntimeInput, 0)
	for rows.Next() {
		var input AcceptedRuntimeInput
		if err := rows.Scan(
			&input.SessionThreadID,
			&input.RuntimeInputID,
			&input.InputKind,
			&input.InboxStatus,
			&input.EventIDsJSON,
			&input.SequenceFrom,
			&input.SequenceTo,
			&input.RejectionReason,
			&input.QueueJobID,
			&input.QueueStatus,
		); err != nil {
			return 0, err
		}
		inputs = append(inputs, input)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	handedOff := 0
	for _, input := range inputs {
		if input.InputKind == "" {
			return 0, PreparationError{Kind: "runtime_inbox_invariant", Message: "runtime inbox input kind is missing", Retryable: false}
		}
		if input.InboxStatus == "delivering" {
			if !input.QueueJobID.Valid {
				return 0, PreparationError{Kind: "runtime_inbox_invariant", Message: "delivering runtime input has no active queue custody", Retryable: false}
			}
			if !quiesced && input.InputKind != "interrupt_control" && input.InputKind != "agent_mail" {
				continue
			}
		}
		if input.InboxStatus != "accepted" && input.InboxStatus != "delivering" {
			continue
		}
		if input.QueueJobID.Valid {
			if input.InboxStatus == "delivering" {
				if _, err := tx.Exec(ctx,
					`UPDATE queue_jobs
					    SET status = 'pending', available_at = $3,
					        lease_token = NULL, leased_by = NULL, leased_at = NULL, leased_until = NULL,
					        updated_at = $3
					  WHERE workspace_id = $1 AND id = $2 AND status IN ('pending', 'leased')`,
					workspaceID,
					input.QueueJobID.String,
					now,
				); err != nil {
					return 0, err
				}
			} else if input.QueueStatus.String == queue.StatusLeased {
				if _, err := tx.Exec(ctx,
					`UPDATE queue_jobs
					    SET status = 'pending', available_at = $3,
					        lease_token = NULL, leased_by = NULL, leased_at = NULL, leased_until = NULL,
					        updated_at = $3
					  WHERE workspace_id = $1 AND id = $2 AND status = 'leased'`,
					workspaceID,
					input.QueueJobID.String,
					now,
				); err != nil {
					return 0, err
				}
			}
		} else {
			request, err := RuntimeInputEnqueueRequest(workspaceID, sessionID, input, now)
			if err != nil {
				return 0, err
			}
			if _, err := queue.EnqueueTx(ctx, tx, request); err != nil {
				return 0, err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'queued', binding_id = NULL, binding_generation = NULL,
			        target_pod_uid = NULL, updated_at = $3
			  WHERE workspace_id = $1 AND runtime_input_id = $2
			    AND status IN ('delivering', 'accepted')
			    AND binding_id = $4 AND binding_generation = $5 AND target_pod_uid = $6`,
			workspaceID,
			input.RuntimeInputID,
			now,
			binding.BindingID,
			binding.BindingGeneration,
			binding.PodUID,
		); err != nil {
			return 0, err
		}
		handedOff++
	}
	return handedOff, nil
}

func RuntimeInputEnqueueRequest(
	workspaceID string,
	sessionID string,
	input AcceptedRuntimeInput,
	now time.Time,
) (queue.EnqueueRequest, error) {
	ws := workspace.ID(workspaceID)
	if input.InputKind == "task_notification" {
		taskID := strings.TrimPrefix(input.RuntimeInputID, "task_notification:")
		if taskID == "" || taskID == input.RuntimeInputID {
			return queue.EnqueueRequest{}, PreparationError{Kind: "runtime_inbox_invariant", Message: "task notification runtime input identity is invalid", Retryable: false}
		}
		return queue.NewTaskNotificationRuntimeInputEnqueueRequest(ws, sessionID, input.SessionThreadID, taskID, now)
	}
	if input.InputKind == "agent_mail" {
		deliveryID := strings.TrimPrefix(input.RuntimeInputID, "agent_mail:")
		if deliveryID == "" || deliveryID == input.RuntimeInputID {
			return queue.EnqueueRequest{}, PreparationError{Kind: "runtime_inbox_invariant", Message: "agent mail runtime input identity is invalid", Retryable: false}
		}
		request, _, err := AgentMailWakeEnqueueRequest(workspaceID, sessionID, input.SessionThreadID, deliveryID, now)
		return request, err
	}
	payloadJSON, err := runtimeInputQueuePayloadJSON(workspaceID, sessionID, input)
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	return queue.EnqueueRequest{
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, sessionID, input.RuntimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    payloadJSON,
		Priority:       runtimeInputPriority(input.InputKind),
		Now:            now,
	}, nil
}

func runtimeInputQueuePayloadJSON(
	workspaceID string,
	sessionID string,
	input AcceptedRuntimeInput,
) ([]byte, error) {
	if !input.SequenceFrom.Valid || !input.SequenceTo.Valid || input.SequenceFrom.Int64 <= 0 || input.SequenceTo.Int64 < input.SequenceFrom.Int64 {
		return nil, PreparationError{Kind: "runtime_inbox_invariant", Message: "runtime input has an invalid sequence range", Retryable: false}
	}
	var eventIDs []string
	if err := json.Unmarshal([]byte(input.EventIDsJSON), &eventIDs); err != nil || len(eventIDs) == 0 {
		return nil, PreparationError{Kind: "runtime_inbox_invariant", Message: "runtime input has invalid event identities", Retryable: false}
	}
	return json.Marshal(InputQueuePayload{
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		SessionThreadID: input.SessionThreadID,
		RuntimeInputID:  input.RuntimeInputID,
		EventIDs:        eventIDs,
		SequenceFrom:    input.SequenceFrom.Int64,
		SequenceTo:      input.SequenceTo.Int64,
		InputKind:       input.InputKind,
	})
}

func runtimeInputPriority(inputKind string) int {
	if inputKind == "interrupt_control" {
		return 100
	}
	return 0
}
