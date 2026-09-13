package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

const mcpInputDiscoveryMaxAttempts = 3
const mcpInputDiscoveryBudget = 120 * time.Second

// An input owns discovery retries, not the process or Queue delivery attempt.
// Reservations commit before external I/O so restart never replenishes a budget.
type mcpInputDiscoveryAttempt struct {
	number     int
	remaining  time.Duration
	diagnostic string
}

type mcpDiscoveryAuthorityLostError struct{}

func (mcpDiscoveryAuthorityLostError) Error() string { return "MCP discovery input custody was lost" }

func requireUserInputMCPReadyTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) error {
	var state string
	if err := tx.QueryRow(ctx, `SELECT status FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(&state); err != nil {
		return err
	}
	// Delivery may already have reached Runtime. Reconcile that custody instead
	// of converting an ambiguous delivery into a new discovery failure.
	if state != "queued" {
		return nil
	}
	toolsets, err := sessionAgentMCPManifestToolsetsTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return err
	}
	var pending []MCPManifestToolsetConfig
	for _, toolset := range toolsets {
		var ready bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_mcp_manifests
			WHERE workspace_id=$1 AND session_id=$2 AND mcp_server_name=$3 AND readiness='ready')`,
			job.WorkspaceID, job.SessionID, toolset.MCPServerName).Scan(&ready); err != nil {
			return err
		}
		if !ready {
			pending = append(pending, toolset)
		}
	}
	if len(pending) > 0 {
		return runtimeInitialMCPManifestRequiredError{toolsets: pending}
	}
	return nil
}

// mcpDiscoveryInputAuthorityTx is called under Session arbitration before each
// reservation and completion. Direct store callers have no Queue identity;
// production Queue callers must still hold the exact lease.
func mcpDiscoveryInputAuthorityTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return err
	}
	var terminated bool
	if err := tx.QueryRow(ctx, `SELECT status='terminated' FROM sessions WHERE workspace_id=$1 AND id=$2`, job.WorkspaceID, job.SessionID).Scan(&terminated); err != nil {
		return err
	}
	if terminated {
		return mcpDiscoveryAuthorityLostError{}
	}
	if job.PartitionKey != "" {
		active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
			WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
			Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
		})
		if err != nil {
			return err
		}
		if !active {
			return mcpDiscoveryAuthorityLostError{}
		}
	}
	return nil
}

func requireQueuedMCPDiscoveryInputTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) error {
	var state string
	if err := tx.QueryRow(ctx, `SELECT status FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3 FOR UPDATE`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(&state); err != nil {
		return err
	}
	if state != "queued" {
		return mcpDiscoveryAuthorityLostError{}
	}
	return nil
}

func (s *PostgreSQLRuntimeDeliveryStore) reserveMCPDiscoveryAttempt(ctx context.Context, job RuntimeJob, budget time.Duration) (mcpInputDiscoveryAttempt, error) {
	var attempt mcpInputDiscoveryAttempt
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.reserve_mcp_discovery", func(tx *dbconnect.Tx) error {
		if err := mcpDiscoveryInputAuthorityTx(ctx, tx, job); err != nil {
			return err
		}
		var deadline sql.NullTime
		var state string
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT status, mcp_discovery_attempts, mcp_discovery_deadline_at,
			COALESCE(mcp_discovery_diagnostic, 'discovery_unavailable'), clock_timestamp()
			FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
			AND session_thread_id=$4 AND input_kind='messages' FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.RuntimeInputID, job.SessionThreadID,
		).Scan(&state, &attempt.number, &deadline, &attempt.diagnostic, &now); err != nil {
			return err
		}
		if state != "queued" {
			return mcpDiscoveryAuthorityLostError{}
		}
		if !deadline.Valid {
			deadline = sql.NullTime{Time: now.Add(budget), Valid: true}
		}
		attempt.remaining = deadline.Time.Sub(now)
		if attempt.number >= mcpInputDiscoveryMaxAttempts || attempt.remaining <= 0 {
			attempt.remaining = 0
			return nil
		}
		attempt.number++
		_, err := tx.Exec(ctx, `UPDATE session_runtime_inbox SET mcp_discovery_attempts=$4,
			mcp_discovery_deadline_at=$5, updated_at=$6
			WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`,
			job.WorkspaceID, job.SessionID, job.RuntimeInputID, attempt.number, deadline.Time, now)
		return err
	})
	return attempt, err
}

func (s *PostgreSQLRuntimeDeliveryStore) discoverUserInputMCP(ctx context.Context, job RuntimeJob, toolsets []MCPManifestToolsetConfig, listTimeout time.Duration) error {
	budget := min(listTimeout, mcpInputDiscoveryBudget)
	for _, toolset := range toolsets {
		for {
			attempt, err := s.reserveMCPDiscoveryAttempt(ctx, job, budget)
			if err != nil {
				return err
			}
			if attempt.remaining <= 0 {
				return s.finishMCPDiscoveryFailure(ctx, job, toolset, attempt.diagnostic)
			}
			listCtx, cancel := context.WithTimeout(ctx, attempt.remaining)
			var manifest MCPManifestListResult
			if s.MCPManifestLister == nil {
				err = mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticDiscoveryUnavailable}
			} else {
				manifest, err = s.MCPManifestLister.ListMCPTools(listCtx, MCPManifestListRequest{
					WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, MCPServerName: toolset.MCPServerName,
				})
			}
			if err == nil && listCtx.Err() != nil {
				err = listCtx.Err()
			}
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				// Bridge owns final acceptance after its family-specific filtering.
				filtered, _ := filterMCPManifestCollisions(toolset.BuiltinFamily, manifest.Tools)
				canonical, invalid := canonicalMCPManifestToolsJSON(filtered)
				if invalid != nil || strings.TrimSpace(manifest.ManifestETag) == "" || len(canonical) > MaxMcpManifestBytes {
					err = mcpManifestDiscoveryError{diagnostic: mcpManifestDiagnosticInvalid}
				}
			}
			if err == nil {
				var acceptance mcpManifestAcceptance
				commitErr := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.accept_input_mcp_discovery", func(tx *dbconnect.Tx) error {
					if err := mcpDiscoveryInputAuthorityTx(ctx, tx, job); err != nil {
						return err
					}
					if err := requireQueuedMCPDiscoveryInputTx(ctx, tx, job); err != nil {
						return err
					}
					var err error
					acceptance, err = captureInitialMCPManifestAcceptanceTx(ctx, tx, job.WorkspaceID, job.SessionID, toolset, manifest, storage.Now())
					return err
				})
				if commitErr != nil {
					return commitErr
				}
				logMCPManifestTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance, true)
				if !acceptance.Duplicate {
					logMCPManifestOmissions(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance.BuiltinFamily, acceptance.Omissions)
				}
				break
			}
			diagnostic := "internal"
			var discovery mcpManifestDiscoveryError
			if errors.As(err, &discovery) {
				diagnostic = discovery.diagnostic
			} else if errors.Is(err, context.DeadlineExceeded) {
				diagnostic = mcpManifestDiagnosticDiscoveryUnavailable
			}
			if s.Logger != nil {
				s.Logger.Error("MCP input discovery attempt failed", "event.kind", "mcp_input_discovery_failed",
					"workspace.id", job.WorkspaceID, "session.id", job.SessionID, "runtime_input.id", job.RuntimeInputID,
					"mcp.server.name", toolset.MCPServerName, "mcp.failure.kind", diagnostic, "attempt", attempt.number)
			}
			if err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.record_mcp_discovery_failure", func(tx *dbconnect.Tx) error {
				if err := mcpDiscoveryInputAuthorityTx(ctx, tx, job); err != nil {
					return err
				}
				if err := requireQueuedMCPDiscoveryInputTx(ctx, tx, job); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `UPDATE session_runtime_inbox SET mcp_discovery_diagnostic=$4
					WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`,
					job.WorkspaceID, job.SessionID, job.RuntimeInputID, diagnostic)
				return err
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// finishMCPDiscoveryFailure settles before Queue closes the input. Inbox status
// is the replay guard for both the public error and idle event; committing them
// together prevents a retry from reporting success or replaying a failed input.
func (s *PostgreSQLRuntimeDeliveryStore) finishMCPDiscoveryFailure(ctx context.Context, job RuntimeJob, toolset MCPManifestToolsetConfig, diagnostic string) error {
	recovered := false
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.finish_mcp_discovery_failure", func(tx *dbconnect.Tx) error {
		if err := mcpDiscoveryInputAuthorityTx(ctx, tx, job); err != nil {
			return err
		}
		var inboxStatus string
		if err := tx.QueryRow(ctx, `SELECT status FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3 FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(&inboxStatus); err != nil {
			return err
		}
		if inboxStatus == "dead_lettered" {
			return nil
		}
		if inboxStatus != "queued" {
			return mcpDiscoveryAuthorityLostError{}
		}
		if err := acquireMCPManifestAcceptanceLockTx(ctx, tx, job.WorkspaceID, job.SessionID, toolset.MCPServerName); err != nil {
			return err
		}
		// A concurrent notification may already have restored this server.
		current, exists, err := loadMCPManifestRowForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, toolset.MCPServerName)
		if err != nil {
			return err
		}
		if exists && current.Readiness == mcpManifestReadinessReady {
			recovered = true
			return nil
		}
		now := storage.Now()
		manifestDiagnostic := diagnostic
		if manifestDiagnostic == "internal" {
			manifestDiagnostic = mcpManifestDiagnosticDiscoveryUnavailable
		}
		if _, err := captureInitialMCPManifestUnreadyTx(ctx, tx, job.WorkspaceID, job.SessionID, toolset, manifestDiagnostic, now); err != nil {
			return err
		}
		scope := bridgeSessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
		thread, err := lockThreadMutationTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		if err := markRuntimeInputEventsProcessedByIDTx(ctx, tx, job.WorkspaceID, job.SessionID, job.EventIDs, now); err != nil {
			return err
		}
		errorType := "mcp_connection_failed_error"
		if diagnostic == mcpManifestDiagnosticCredentialUnavailable {
			errorType = "mcp_authentication_failed_error"
		}
		payload, err := marshalBridgeJSON(map[string]any{
			"type": "session.error", "error": map[string]any{
				"mcp_server_name": toolset.MCPServerName,
				"type":            errorType, "message": "Configured GitHub MCP tools could not be loaded. This input was not executed; a new input can retry.",
				"retry_status": map[string]any{"type": "exhausted"},
			},
		})
		if err != nil {
			return err
		}
		writeID := "mcp_discovery:" + job.RuntimeInputID
		if _, err := insertRuntimeTerminationEventTx(ctx, tx, scope, thread, writeID, "", "session.error", payload, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE session_runtime_inbox SET status='dead_lettered', updated_at=$4
			WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`, job.WorkspaceID, job.SessionID, job.RuntimeInputID, now); err != nil {
			return err
		}
		// This input never reached Runtime. Do not idle a different active run,
		// pending approval, or a sibling thread merely because its delivery failed.
		var active bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session_threads WHERE workspace_id=$1 AND session_id=$2
			AND status IN ('running','rescheduling','requires_action')) OR EXISTS (
			SELECT 1 FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND status IN ('delivering','accepted','parked'))`,
			job.WorkspaceID, job.SessionID).Scan(&active); err != nil {
			return err
		}
		if !active && thread.role == "main" {
			idlePayload, err := idleStatusPayloadJSON(`{"type":"end_turn"}`)
			if err != nil {
				return err
			}
			stamp, err := insertRuntimeTerminationEventTx(ctx, tx, scope, thread, writeID+":idle", "", "session.status_idle", idlePayload, now)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE sessions SET status='idle', updated_at=$3 WHERE workspace_id=$1 AND id=$2 AND status <> 'terminated'`, job.WorkspaceID, job.SessionID, now); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE session_runtime_status SET status='idle', status_event_id=$3, idle_since=$4,
				running_since=NULL, cleanup_after=$5, updated_at=$4 WHERE workspace_id=$1 AND session_id=$2 AND status='idle'`,
				job.WorkspaceID, job.SessionID, stamp.EventID, now, now.Add(defaultIdleCleanupDelay)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if recovered {
		return nil
	}
	if s.Logger != nil {
		s.Logger.Error("MCP discovery exhausted; input settled without execution", "event.kind", "mcp_input_discovery_exhausted",
			"workspace.id", job.WorkspaceID, "session.id", job.SessionID, "runtime_input.id", job.RuntimeInputID,
			"mcp.server.name", toolset.MCPServerName, "mcp.failure.kind", diagnostic)
	}
	return runtimeDeliveryPrepareError{kind: "mcp_manifest_discovery_failed", message: "configured MCP discovery failed for this input", retryable: false}
}

// Inputs that performed discovery must install the accepted generation before
// execution, including after a process restart. A cold Pod returns no_residency
// and loads the same durable generation through LoadContext. The independent
// config Queue carrier remains responsible for hot updates and eventual ACK.
func mcpDiscoveryInstallationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, binding runtimeBindingForDelivery, port int) ([]*agentruntimev1.ApplyRuntimeConfigRequest, error) {
	var attempts int
	if err := tx.QueryRow(ctx, `SELECT mcp_discovery_attempts FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(&attempts); err != nil {
		return nil, err
	}
	if attempts == 0 {
		return nil, nil
	}
	toolsets, err := sessionAgentMCPManifestToolsetsTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return nil, err
	}
	var configs []*agentruntimev1.ApplyRuntimeConfigRequest
	for _, toolset := range toolsets {
		configJob := RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, MCPServerName: toolset.MCPServerName}
		payload, inputID, err := runtimeMCPManifestCommandPayloadTx(ctx, tx, configJob)
		if err != nil {
			return nil, err
		}
		plan, err := runtimeCommandPlanForPayload(configJob, job.SessionThreadID, inputID, payload, binding, port)
		if err != nil {
			return nil, err
		}
		configs = append(configs, plan.RuntimeConfig)
	}
	return configs, nil
}
