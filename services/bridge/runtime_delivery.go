package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	internalgrpc "github.com/tetral-ai/tetral/internal/internalgrpc"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionrpc"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type RuntimeDeliveryStore interface {
	PrepareRuntimeCommand(context.Context, RuntimeJob) (RuntimeCommandPlan, error)
	MarkRuntimeInputAccepted(context.Context, RuntimeJob, *agentruntimev1.RuntimeInputCommandRequest) error
	PrepareRuntimeInputRejection(context.Context, RuntimeJob, RuntimeDeliveryResult) (bool, error)
}

type RuntimeInputSealStore interface {
	ResolveRuntimeInputSeal(context.Context, RuntimeJob) (string, error)
}

type RuntimeCleanupFinalizer interface {
	FinalizeRuntimeCleanup(context.Context, RuntimeJob) (RuntimeDeliveryResult, error)
}

type RuntimeDeliveryFinalizationStore interface {
	FinalizeRuntimeDelivery(context.Context, RuntimeJob, RuntimeDeliveryResult) (RuntimeDeliveryResult, error)
	ReplayRuntimeDeliveryFinalization(context.Context, RuntimeJob) (RuntimeDeliveryResult, bool, error)
}

type RuntimeTargetResolver interface {
	ResolveRuntimeTarget(context.Context, *dbconnect.Tx, RuntimeJob) (runtimeBindingForDelivery, error)
}

type RuntimeCleanupTargetProver interface {
	CleanupTargetProvenGone(context.Context, *dbconnect.Tx, RuntimeJob, runtimeBindingForDelivery) (bool, error)
}

type RuntimeCommandSender interface {
	SendRuntimeCommand(context.Context, RuntimePodTarget, *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error)
}

type SandboxReleaseClient interface {
	ReleaseSandbox(context.Context, SandboxReleaseRequest) (SandboxReleaseResult, error)
}

type SandboxReleaseRequest struct {
	WorkspaceID          string
	SessionID            string
	SandboxID            string
	BindingID            string
	BindingGeneration    int64
	PreparationAttemptID string
	DeleteCleanupID      string
	Reason               string
	IdempotencyKey       string
	DurableCleanupFence  bool
}

type SandboxReleaseStatus string

const (
	SandboxReleaseReleased        SandboxReleaseStatus = "released"
	SandboxReleaseAlreadyReleased SandboxReleaseStatus = "already_released"
	SandboxReleaseRetryLater      SandboxReleaseStatus = "retry_later"
	SandboxReleaseFailed          SandboxReleaseStatus = "failed"
	SandboxReleaseArchived        SandboxReleaseStatus = "archived"
)

const initialMCPManifestListTimeout = 180 * time.Second

type SandboxReleaseResult struct {
	Status        SandboxReleaseStatus
	SandboxStatus string
}

type RuntimeCommandPlan struct {
	StaleAccepted     bool
	SettledAccepted   bool
	CleanupTargetGone bool
	Target            RuntimePodTarget
	Request           *agentruntimev1.RuntimeInputCommandRequest
	TaskNotification  *RuntimeTaskNotificationPlan
}

type RuntimeTaskNotificationPlan struct {
	TaskID               string
	SourceToolUseEventID string
	ResultJSON           string
}

type RuntimePodTarget struct {
	Namespace string
	PodName   string
	PodUID    string
	PodIP     string
	Port      int
}

type RuntimePodDirectDeliverer struct {
	Store  RuntimeDeliveryStore
	Sender RuntimeCommandSender
}

func (d RuntimePodDirectDeliverer) RepairRuntimeInbox(ctx context.Context, workspaceID string, limit int) (int, error) {
	repairer, ok := d.Store.(RuntimeInboxRepairer)
	if !ok || repairer == nil {
		return 0, nil
	}
	return repairer.RepairRuntimeInbox(ctx, workspaceID, limit)
}

func (d RuntimePodDirectDeliverer) RepairCompletionMail(ctx context.Context, workspaceID string, limit int) (int, error) {
	repairer, ok := d.Store.(CompletionMailRepairer)
	if !ok || repairer == nil {
		return 0, nil
	}
	return repairer.RepairCompletionMail(ctx, workspaceID, limit)
}

func (d RuntimePodDirectDeliverer) ResolveRuntimeInputSeal(ctx context.Context, job RuntimeJob) (string, error) {
	if d.Store == nil {
		return "", runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	resolver, ok := d.Store.(RuntimeInputSealStore)
	if !ok {
		return "", runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime input seal store is unavailable", retryable: true}
	}
	return resolver.ResolveRuntimeInputSeal(ctx, job)
}

func (d RuntimePodDirectDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	finalizer, ok := d.Store.(RuntimeDeliveryFinalizationStore)
	if !ok || finalizer == nil {
		return RuntimeDeliveryResult{}, errors.New("runtime delivery finalizer is unavailable")
	}
	return finalizer.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d RuntimePodDirectDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	replayer, ok := d.Store.(RuntimeDeliveryFinalizationStore)
	if !ok || replayer == nil {
		return RuntimeDeliveryResult{}, false, errors.New("runtime delivery finalization replayer is unavailable")
	}
	return replayer.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func (d RuntimePodDirectDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	if d.Store == nil {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_reconcile_unavailable",
			ErrorMessage: "runtime delivery store is unavailable",
		}, nil
	}
	plan, err := d.Store.PrepareRuntimeCommand(ctx, job)
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if plan.SettledAccepted {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
	}
	if plan.StaleAccepted {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, nil
	}
	if (job.Kind == queue.KindCleanupSession || job.Kind == queue.KindSessionDeleteCleanup) && plan.CleanupTargetGone {
		finalizer, ok := d.Store.(RuntimeCleanupFinalizer)
		if !ok {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "cleanup_finalizer_unavailable",
				ErrorMessage: "cleanup finalizer is unavailable",
			}, nil
		}
		return finalizer.FinalizeRuntimeCleanup(ctx, job)
	}
	if d.Sender == nil {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_transport_unavailable",
			ErrorMessage: "runtime command sender is unavailable",
		}, nil
	}
	if plan.Request == nil {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_plan_invalid",
			ErrorMessage: "runtime command request is missing",
		}, nil
	}
	response, err := d.Sender.SendRuntimeCommand(ctx, plan.Target, plan.Request)
	if err != nil {
		result, deliveryErr := runtimeDeliveryResultFromSendError(err)
		if deliveryErr != nil {
			return result, deliveryErr
		}
		converted, err := d.prepareRuntimeInputRejection(ctx, job, result)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if converted {
			return d.DeliverRuntimeJob(ctx, job)
		}
		return result, nil
	}
	result := RuntimeDeliveryResultFromResponseForRequest(response, plan.Request)
	converted, err := d.prepareRuntimeInputRejection(ctx, job, result)
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if converted {
		return d.DeliverRuntimeJob(ctx, job)
	}
	if job.Kind == queue.KindRuntimeInput && (result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		if err := d.Store.MarkRuntimeInputAccepted(ctx, job, plan.Request); err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
	}
	if (job.Kind == queue.KindCleanupSession || job.Kind == queue.KindSessionDeleteCleanup) && (result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		finalizer, ok := d.Store.(RuntimeCleanupFinalizer)
		if !ok {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "cleanup_finalizer_unavailable",
				ErrorMessage: "cleanup finalizer is unavailable",
			}, nil
		}
		return finalizer.FinalizeRuntimeCleanup(ctx, job)
	}
	return result, nil
}

func (d RuntimePodDirectDeliverer) prepareRuntimeInputRejection(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (bool, error) {
	if job.Kind != queue.KindRuntimeInput || result.Status != RuntimeDeliveryRejected || result.Retryable {
		return false, nil
	}
	return d.Store.PrepareRuntimeInputRejection(ctx, job, result)
}

// runtimeDeliveryResultFromSendError separates two RESOURCE_EXHAUSTED-shaped
// conditions that must never be conflated. A client's OWN send-cap rejection,
// detected LOCALLY before transmission (runtimeCommandPayloadTooLargeError, the
// transport fuse), is a deterministic per-input terminal: it dead-letters under a
// distinct error kind and is NEVER the retryable transport arm. Only a REMOTE
// RESOURCE_EXHAUSTED — the pod reporting itself at capacity — stays retryable
// (returned as a bare error below alongside DeadlineExceeded/Unavailable).
func runtimeDeliveryResultFromSendError(err error) (RuntimeDeliveryResult, error) {
	var tooLarge *runtimeCommandPayloadTooLargeError
	if errors.As(err, &tooLarge) {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_payload_too_large",
			ErrorMessage: "runtime command exceeds the transport fuse",
		}, nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_invalid_argument",
			ErrorMessage: "runtime pod rejected an invalid command request",
		}, nil
	case codes.Internal:
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_internal_invariant",
			ErrorMessage: "runtime pod reported a terminal command invariant failure",
		}, nil
	case codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
		return RuntimeDeliveryResult{}, err
	default:
		return RuntimeDeliveryResult{}, err
	}
}

type PostgreSQLRuntimeDeliveryStore struct {
	Client                          *dbconnect.Client
	Logger                          *slog.Logger
	RuntimeGRPCPort                 int
	TargetResolver                  RuntimeTargetResolver
	MCPManifestLister               MCPManifestLister
	SandboxReleaser                 SandboxReleaseClient
	SandboxStatusFreshnessWindow    time.Duration
	ResourceCredentialRefreshMargin time.Duration
	Clock                           func() time.Time
}

// NewPostgreSQLRuntimeDeliveryStore provides a dependency-light store for
// focused callers and tests. Production Job Runner assembly must use
// NewJobRunnerRuntimeDeliveryStore so every delivery dependency is installed.
func NewPostgreSQLRuntimeDeliveryStore(client *dbconnect.Client, runtimeGRPCPort int) *PostgreSQLRuntimeDeliveryStore {
	return &PostgreSQLRuntimeDeliveryStore{
		Client:                          client,
		RuntimeGRPCPort:                 runtimeGRPCPort,
		SandboxStatusFreshnessWindow:    defaultSandboxStatusFreshness,
		ResourceCredentialRefreshMargin: defaultResourceCredentialRefreshMargin,
		Clock:                           func() time.Time { return storage.Now() },
	}
}

// NewJobRunnerRuntimeDeliveryStore assembles the complete production delivery
// store, including the MCP manifest path needed before a session's first run.
func NewJobRunnerRuntimeDeliveryStore(
	client *dbconnect.Client,
	logger *slog.Logger,
	cfg JobRunnerConfig,
	bindingSnapshot func() enginekubernetes.BindingVisibilitySnapshot,
) *PostgreSQLRuntimeDeliveryStore {
	store := NewPostgreSQLRuntimeDeliveryStore(client, cfg.AgentRuntimeGRPCPort)
	store.Logger = logger
	store.MCPManifestLister = NewGatewayMCPManifestLister(cfg.MCPConnectorGRPCAddress, internalgrpcauth.FileTokenSource{
		Path: cfg.GatewayTokenPath,
	})
	store.SandboxReleaser = NewSandboxServiceReleaseClient(cfg.SandboxServiceGRPCAddress, internalgrpcauth.FileTokenSource{
		Path: cfg.SandboxServiceTokenPath,
	})
	store.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: bindingSnapshot}
	store.SandboxStatusFreshnessWindow = cfg.SandboxStatusFreshnessWindow
	store.ResourceCredentialRefreshMargin = cfg.ResourceCredentialRefreshMargin
	return store
}

func (s *PostgreSQLRuntimeDeliveryStore) ResolveRuntimeInputSeal(ctx context.Context, job RuntimeJob) (string, error) {
	if s == nil || s.Client == nil {
		return "", runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if job.Kind != queue.KindRuntimeInput || job.WorkspaceID == "" || job.SessionID == "" {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input birth attempt is incomplete", retryable: false}
	}
	if job.InputKind == "task_notification" {
		return "", nil
	}
	if job.PreparationAttemptID == "" {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input birth attempt is incomplete", retryable: false}
	}
	var sealedAttemptID string
	err := s.Client.WithWorkspaceReadOnlyTx(ctx, job.WorkspaceID, "agentruntimebridge.resolve_runtime_input_seal", func(tx *dbconnect.Tx) error {
		readiness, found, err := loadEarliestFailedPreparationAtOrAfterTx(ctx, tx, job.WorkspaceID, job.SessionID, job.PreparationAttemptID, false)
		if err != nil {
			return err
		}
		if found {
			sealedAttemptID = readiness.PreparationAttemptID
		}
		return nil
	})
	return sealedAttemptID, err
}

func (s *PostgreSQLRuntimeDeliveryStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	if s == nil || s.Client == nil {
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" {
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime job identity is incomplete", retryable: false}
	}
	port := s.RuntimeGRPCPort
	if port <= 0 {
		port = defaultAgentRuntimeGRPCPort
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var plan RuntimeCommandPlan
	var requeuedPreparationErr *runtimeDeliveryPrepareError
	var requeuedInboxRepairErr *runtimeDeliveryPrepareError
	var initialMCPManifestToolsets []MCPManifestToolsetConfig
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.prepare_runtime_command", func(tx *dbconnect.Tx) error {
		if job.Kind == queue.KindSessionDeleteCleanup {
			deletePlan, err := s.prepareSessionDeleteCleanupCommandTx(ctx, tx, job, port, now)
			if err != nil {
				return err
			}
			plan = deletePlan
			return nil
		}
		var deleted bool
		if err := tx.QueryRow(ctx, `SELECT lifecycle_state = 'deleted' FROM sessions WHERE workspace_id=$1 AND id=$2`, job.WorkspaceID, job.SessionID).Scan(&deleted); dbconnect.IsNoRows(err) {
			plan = RuntimeCommandPlan{StaleAccepted: true}
			return nil
		} else if err != nil {
			return err
		}
		if deleted {
			plan = RuntimeCommandPlan{StaleAccepted: true}
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind != "agent_mail" {
			effectiveJob, err := effectiveRuntimeInputJobTx(ctx, tx, job)
			if err != nil {
				return err
			}
			job = effectiveJob
		}
		if job.Kind == queue.KindCleanupSession {
			cleanupPlan, err := s.prepareCleanupSessionCommandTx(ctx, tx, job, port, now)
			if err != nil {
				return err
			}
			plan = cleanupPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "agent_mail" {
			readiness, sealed, err := loadEarliestFailedPreparationAtOrAfterTx(
				ctx,
				tx,
				job.WorkspaceID,
				job.SessionID,
				job.PreparationAttemptID,
				true,
			)
			if err != nil {
				return err
			}
			if sealed {
				if err := settleSealedAgentMailTx(ctx, tx, job, readiness, now); err != nil {
					return err
				}
				plan = RuntimeCommandPlan{SettledAccepted: true}
				return nil
			}
			mailPlan, err := s.prepareAgentMailCommandTx(ctx, tx, job, port, now)
			if err != nil {
				var requeued runtimePreparationRequeuedError
				if errors.As(err, &requeued) {
					requeuedPreparationErr = &requeued.err
					return nil
				}
				return err
			}
			plan = mailPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "task_notification" {
			taskPlan, taskCommandPlan, err := s.prepareTaskNotificationCommandTx(ctx, tx, job, port, now)
			if err != nil {
				var requeued runtimePreparationRequeuedError
				if errors.As(err, &requeued) {
					requeuedPreparationErr = &requeued.err
					return nil
				}
				var initialMCP runtimeInitialMCPManifestRequiredError
				if errors.As(err, &initialMCP) {
					initialMCPManifestToolsets = initialMCP.toolsets
					return nil
				}
				return err
			}
			plan = taskCommandPlan
			plan.TaskNotification = taskPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind != "task_notification" {
			stale, err := allRuntimeInputEventsProcessedTx(ctx, tx, job)
			if err != nil {
				return err
			}
			if stale {
				plan = RuntimeCommandPlan{StaleAccepted: true}
				return nil
			}
			readiness, sealed, err := loadEarliestFailedPreparationAtOrAfterTx(ctx, tx, job.WorkspaceID, job.SessionID, job.PreparationAttemptID, true)
			if err != nil {
				return err
			}
			if sealed {
				settled, err := settleTerminalRuntimePreparationFailureTx(ctx, tx, job, readiness, now)
				if err != nil {
					return err
				}
				if settled {
					plan = RuntimeCommandPlan{SettledAccepted: true}
				} else {
					plan = RuntimeCommandPlan{StaleAccepted: true}
				}
				return nil
			}
			if job.InputKind == "messages" {
				fence, err := messageInterruptFenceStatusTx(ctx, tx, job)
				if err != nil {
					return err
				}
				switch fence {
				case messageInterruptFenceAllSuperseded:
					plan = RuntimeCommandPlan{StaleAccepted: true}
					return nil
				case messageInterruptFenceMixed:
					return runtimeDeliveryPrepareError{kind: "runtime_input_superseded_by_interrupt", message: "message runtime input mixes superseded and deliverable events", retryable: false}
				}
			}
			if err := requireRuntimePreparationReadyTx(ctx, tx, job.WorkspaceID, job.SessionID, s.sandboxStatusFreshnessWindow(), s.resourceCredentialRefreshMargin(), now); err != nil {
				var terminalPreparation runtimePreparationTerminalFailureError
				if errors.As(err, &terminalPreparation) {
					settled, err := settleTerminalRuntimePreparationFailureTx(ctx, tx, job, terminalPreparation.readiness, now)
					if err != nil {
						return err
					}
					if settled {
						plan = RuntimeCommandPlan{SettledAccepted: true}
					} else {
						plan = RuntimeCommandPlan{StaleAccepted: true}
					}
					return nil
				}
				var requeued runtimePreparationRequeuedError
				if errors.As(err, &requeued) {
					requeuedPreparationErr = &requeued.err
					return nil
				}
				return err
			}
			repaired, err := requeueEarlierPendingRuntimeInboxTx(ctx, tx, job, now)
			if err != nil {
				return err
			}
			if repaired {
				err := runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_pending", message: "earlier runtime input is pending repair and was requeued before current delivery", retryable: true}
				requeuedInboxRepairErr = &err
				return nil
			}
			if err := requireInitialMCPManifestReadyTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
				return err
			}
		}
		binding, err := s.resolveRuntimeTarget(ctx, tx, job)
		if err != nil {
			return err
		}
		sessionThreadID := job.SessionThreadID
		if sessionThreadID == "" {
			var err error
			sessionThreadID, err = readRuntimeCommandSessionThreadIDTx(ctx, tx, job.WorkspaceID, job.SessionID)
			if err != nil {
				return err
			}
		}
		if job.Kind == queue.KindRuntimeInput {
			inboxJob := job
			inboxJob.SessionThreadID = sessionThreadID
			if err := upsertRuntimeInboxDeliveryTx(ctx, tx, inboxJob, binding, now); err != nil {
				return err
			}
		}
		payloadJSON, runtimeInputID, err := runtimeCommandPayloadForJobTx(ctx, tx, job)
		if err != nil {
			return err
		}
		plan = RuntimeCommandPlan{
			Target: RuntimePodTarget{
				Namespace: binding.Namespace,
				PodName:   binding.PodName,
				PodUID:    binding.PodUID,
				PodIP:     binding.PodIP,
				Port:      port,
			},
			Request: &agentruntimev1.RuntimeInputCommandRequest{
				RequestId:          job.JobID + ":" + job.LeaseToken,
				WorkspaceId:        job.WorkspaceID,
				SessionId:          job.SessionID,
				SessionThreadId:    sessionThreadID,
				BindingId:          binding.BindingID,
				BindingGeneration:  int64(binding.BindingGeneration),
				TargetPodNamespace: binding.Namespace,
				TargetPodName:      binding.PodName,
				TargetPodUid:       binding.PodUID,
				TargetPodIp:        binding.PodIP,
				RuntimeInputId:     runtimeInputID,
				EventIds:           append([]string(nil), job.EventIDs...),
				SequenceFrom:       job.SequenceFrom,
				SequenceTo:         job.SequenceTo,
				CommandKind:        job.CommandKind,
				PayloadJson:        payloadJSON,
			},
		}
		return nil
	})
	if err != nil {
		var initialMCP runtimeInitialMCPManifestRequiredError
		var lostBinding runtimeBindingLostError
		if errors.As(err, &lostBinding) {
			if err := s.repairLostRuntimeBinding(ctx, job.WorkspaceID, job.SessionID, lostBinding.binding, now); err != nil {
				return RuntimeCommandPlan{}, err
			}
			return s.PrepareRuntimeCommand(ctx, job)
		} else if errors.As(err, &initialMCP) {
			initialMCPManifestToolsets = initialMCP.toolsets
		} else {
			return RuntimeCommandPlan{}, err
		}
	}
	if len(initialMCPManifestToolsets) > 0 {
		if err := s.enqueueInitialMCPManifestUpdates(ctx, job, initialMCPManifestToolsets, now); err != nil {
			return RuntimeCommandPlan{}, err
		}
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "mcp_manifest_discovery_pending", message: "mcp manifest update queued before runtime input delivery", retryable: true}
	}
	if requeuedPreparationErr != nil {
		return RuntimeCommandPlan{}, *requeuedPreparationErr
	}
	if requeuedInboxRepairErr != nil {
		return RuntimeCommandPlan{}, *requeuedInboxRepairErr
	}
	return plan, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) MarkRuntimeInputAccepted(ctx context.Context, job RuntimeJob, request *agentruntimev1.RuntimeInputCommandRequest) error {
	if job.Kind != queue.KindRuntimeInput {
		return nil
	}
	if s == nil || s.Client == nil {
		return runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" || request == nil ||
		request.GetBindingId() == "" || request.GetBindingGeneration() <= 0 || request.GetTargetPodUid() == "" {
		return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime job identity is incomplete", retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	return s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.mark_runtime_input_accepted", func(tx *dbconnect.Tx) error {
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = CASE
			            WHEN status = 'committed' THEN status
			            ELSE 'accepted'
			        END,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND binding_id = $5
			    AND binding_generation = $6
			    AND target_pod_uid = $7
			    AND status IN ('delivering', 'accepted', 'committed')`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			now,
			request.GetBindingId(),
			request.GetBindingGeneration(),
			request.GetTargetPodUid(),
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_accept_missing", message: "runtime inbox row is missing for accepted input", retryable: true}
		}
		return nil
	})
}

func (s *PostgreSQLRuntimeDeliveryStore) PrepareRuntimeInputRejection(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (bool, error) {
	if job.Kind != queue.KindRuntimeInput {
		return false, nil
	}
	reasonCode, eligible := boundedRuntimeRejectionReason(result)
	if !eligible || job.InputKind != "messages" {
		return false, nil
	}
	if s == nil || s.Client == nil {
		return false, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" {
		return false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input rejection identity is incomplete", retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	converted := false
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.prepare_runtime_input_rejection", func(tx *dbconnect.Tx) error {
		var inboxStatus, inboxKind string
		err := tx.QueryRow(ctx,
			`SELECT status, input_kind
			   FROM session_runtime_inbox
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			  FOR UPDATE`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
		).Scan(&inboxStatus, &inboxKind)
		if dbconnect.IsNoRows(err) {
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_rejection_missing", message: "runtime inbox row is missing for rejected input", retryable: true}
		}
		if err != nil {
			return err
		}
		if inboxKind == "rejection" || inboxStatus != "delivering" {
			return nil
		}
		if inboxKind != job.InputKind {
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_payload_conflict", message: "runtime input rejection conflicts with the durable inbox row", retryable: false}
		}
		updateResult, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET input_kind = 'rejection',
			        rejection_reason_code = $4,
			        updated_at = $5
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND status = 'delivering'
			    AND input_kind = $6`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			reasonCode,
			now,
			job.InputKind,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(updateResult) {
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_rejection_missing", message: "runtime inbox row is missing for rejected input", retryable: true}
		}
		converted = true
		return nil
	})
	return converted, err
}

func boundedRuntimeRejectionReason(result RuntimeDeliveryResult) (string, bool) {
	switch result.ErrorKind {
	case "runtime_command_payload_too_large":
		return "runtime_command_payload_too_large", true
	case "runtime_contract_failure", "runtime_command_invalid_argument", "runtime_rejected_input":
		return "runtime_command_rejected", true
	default:
		return "", false
	}
}

// FinalizeRuntimeDelivery decides settle-vs-dead_letter for a runtime_input job
// from durable history. The SEAL branch: a job is sealed (settle-only) when a
// terminally-failed preparation attempt exists at-or-after the job's birth, and
// it settles against the EARLIEST such attempt in the schema total order
// (loadEarliestFailedPreparationAtOrAfterTx). Ordering invariant: the sealed
// settlement — error events persisted AND the referenced session_events stamped
// processed (settleTerminalRuntimePreparationFailureTx) — must durably COMMIT in
// this finalizer transaction BEFORE the job may transition to dead_lettered. A
// sealed job never reaches dead_lettered through queue-side exhaustion without
// that settlement having committed first; only the non-sealed exhaustion arm
// stamps events processed and then writes status = 'dead_lettered' directly.
func (s *PostgreSQLRuntimeDeliveryStore) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if isMCPManifestRuntimeJob(job) {
		return s.finalizeMCPManifestDelivery(ctx, job, result)
	}
	if job.Kind != queue.KindRuntimeInput || job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime delivery finalization identity is incomplete", retryable: false}
	}
	if result.Status != RuntimeDeliveryRejected {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_response", message: "runtime delivery finalization requires a rejected result", retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	finalized := RuntimeDeliveryResult{}
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.finalize_runtime_delivery", func(tx *dbconnect.Tx) error {
		replayed, found, err := replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if found {
			finalized = replayed
			return nil
		}
		readiness, sealed, err := loadEarliestFailedPreparationAtOrAfterTx(
			ctx,
			tx,
			job.WorkspaceID,
			job.SessionID,
			job.PreparationAttemptID,
			true,
		)
		if err != nil {
			return err
		}
		if sealed {
			if job.InputKind == "agent_mail" {
				if err := settleSealedAgentMailTx(ctx, tx, job, readiness, now); err != nil {
					return err
				}
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
				return nil
			}
			if _, err := settleTerminalRuntimePreparationFailureTx(ctx, tx, job, readiness, now); err != nil {
				return err
			}
			finalized = runtimeDeliveryExhaustedResult()
			return nil
		}
		if job.InputKind == "agent_mail" {
			stale, err := agentMailRecipientTerminalTx(ctx, tx, job)
			if err != nil {
				return err
			}
			replayed, found, err := replayAgentMailDeliveryFinalizationTx(ctx, tx, job)
			if err != nil {
				return err
			}
			if found {
				finalized = replayed
				return nil
			}
			if stale {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
				return nil
			}
			if err := settleAgentMailDeliveryExhaustionTx(ctx, tx, job, now); err != nil {
				return err
			}
			finalized = runtimeDeliveryExhaustedResult()
			return nil
		}
		if job.InputKind == "task_notification" {
			finalized, err = finalizeTaskNotificationDeliveryTx(ctx, tx, job, now)
			return err
		}
		scope := bridgeSessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
		threadScope, err := lockThreadMutationTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		replayed, found, err = replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if found {
			finalized = replayed
			return nil
		}
		if len(job.EventIDs) == 0 {
			return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime delivery finalization requires event anchors", retryable: false}
		}
		if err := markRuntimeInputEventsProcessedByIDTx(ctx, tx, job.WorkspaceID, job.SessionID, job.EventIDs, now); err != nil {
			return err
		}
		if err := insertRuntimeDeliveryExhaustionEventWithThreadScopeTx(ctx, tx, job, threadScope, now); err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'dead_lettered',
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND status IN ('delivering', 'accepted')`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			now,
		)
		if err != nil {
			return err
		}
		finalized = runtimeDeliveryExhaustedResult()
		return nil
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if !result.Retryable && finalized.Status == RuntimeDeliveryRejected {
		result.Retryable = false
		return result, nil
	}
	return finalized, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) finalizeMCPManifestDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" || result.Status != RuntimeDeliveryRejected || !runtimeJobFinalAttempt(job) {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "MCP manifest delivery finalization identity is incomplete", retryable: false}
	}
	jobGeneration, err := strconv.ParseInt(job.MCPManifestGeneration, 10, 64)
	if err != nil || jobGeneration <= 0 {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "MCP manifest generation is invalid", retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	transitioned := false
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.finalize_mcp_manifest_delivery", func(tx *dbconnect.Tx) error {
		if err := acquireMCPManifestAcceptanceLockTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName); err != nil {
			return err
		}
		current, found, err := loadMCPManifestRowForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
		if err != nil {
			return err
		}
		if !found || current.Generation != jobGeneration {
			return nil
		}
		transitioned = current.Readiness != mcpManifestReadinessUnready || !current.Diagnostic.Valid || current.Diagnostic.String != mcpManifestDiagnosticDeliveryExhausted
		toolset, err := mcpManifestToolsetConfigTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
		if err != nil {
			return err
		}
		_, err = transitionMCPManifestDeliveryExhaustedTx(
			ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName, current, toolset, now,
		)
		return err
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if transitioned {
		logMCPManifestReadiness(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, job.MCPServerName, mcpManifestReadinessUnready, mcpManifestDiagnosticDeliveryExhausted, jobGeneration+1)
	}
	return runtimeDeliveryExhaustedResult(), nil
}

func (s *PostgreSQLRuntimeDeliveryStore) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if job.Kind != queue.KindRuntimeInput || job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime delivery replay identity is incomplete", retryable: false}
	}
	var result RuntimeDeliveryResult
	var found bool
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.replay_runtime_delivery_finalization", func(tx *dbconnect.Tx) error {
		var err error
		result, found, err = replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		return err
	})
	return result, found, err
}

func replayRuntimeDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	if job.InputKind == "task_notification" {
		return replayTaskNotificationDeliveryFinalizationTx(ctx, tx, job)
	}
	if job.InputKind == "agent_mail" {
		return replayAgentMailDeliveryFinalizationTx(ctx, tx, job)
	}
	var inboxStatus string
	err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&inboxStatus)
	if err == nil {
		switch inboxStatus {
		case "dead_lettered":
			return runtimeDeliveryExhaustedResult(), true, nil
		case "committed", "cancelled":
			return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
		case "delivering", "accepted":
			return RuntimeDeliveryResult{}, false, nil
		default:
			return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "runtime_inbox_status_invalid", message: "runtime inbox status is invalid", retryable: false}
		}
	}
	if !dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{}, false, err
	}
	exists, err := runtimeDeliveryExhaustionEventExistsTx(ctx, tx, job)
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	if exists {
		return runtimeDeliveryExhaustedResult(), true, nil
	}
	if len(job.EventIDs) == 0 {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "event-less runtime delivery finalization is unauthorized", retryable: false}
	}
	processed, err := allRuntimeInputEventsProcessedTx(ctx, tx, job)
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	if processed {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	}
	return RuntimeDeliveryResult{}, false, nil
}

func replayAgentMailDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	if sealed, err := replaySealedAgentMailTx(ctx, tx, job); err != nil {
		return RuntimeDeliveryResult{}, false, err
	} else if sealed {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, true, nil
	}
	if exists, err := runtimeDeliveryExhaustionEventExistsTx(ctx, tx, job); err != nil {
		return RuntimeDeliveryResult{}, false, err
	} else if exists {
		return runtimeDeliveryExhaustedResult(), true, nil
	}
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "agent mail runtime input id is invalid", retryable: false}
	}
	var sent bool
	var receivedProcessed bool
	var inboxCommitted bool
	if err := tx.QueryRow(ctx,
		`SELECT
			EXISTS (
				SELECT 1 FROM session_events
				 WHERE workspace_id=$1 AND session_id=$2
				   AND type='agent.thread_message_sent'
				   AND payload_json::jsonb ->> 'delivery_id'=$3
			),
			EXISTS (
				SELECT 1 FROM session_events
				 WHERE workspace_id=$1 AND session_id=$2
				   AND session_thread_id=$4
				   AND type='agent.thread_message_received'
				   AND payload_json::jsonb ->> 'delivery_id'=$3
				   AND processed_at IS NOT NULL
			),
			EXISTS (
				SELECT 1 FROM session_runtime_inbox
				 WHERE workspace_id=$1 AND session_id=$2
				   AND session_thread_id=$4
				   AND runtime_input_id='agent_mail:' || $3
				   AND status='committed'
			)`,
		job.WorkspaceID,
		job.SessionID,
		deliveryID,
		job.SessionThreadID,
	).Scan(&sent, &receivedProcessed, &inboxCommitted); err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	if !sent {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "agent mail sent envelope is missing", retryable: false}
	}
	if receivedProcessed || inboxCommitted {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	}
	return RuntimeDeliveryResult{}, false, nil
}

func settleAgentMailDeliveryExhaustionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) error {
	if err := insertRuntimeDeliveryExhaustionEventTx(ctx, tx, job, now); err != nil {
		return err
	}
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "agent mail runtime input id is invalid", retryable: false}
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_events
		    SET processed_at = COALESCE(processed_at, $5),
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $4`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		deliveryID,
		now,
	); err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'dead_lettered',
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND status IN ('delivering', 'accepted')`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
		now,
	)
	return err
}

func settleSealedAgentMailTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	readiness sessionPreparationReadiness,
	now time.Time,
) error {
	scope := bridgeSessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	sourceID := stableRuntimeID(
		"sealed_agent_mail",
		job.RuntimeInputID,
		job.PreparationAttemptID,
		readiness.PreparationAttemptID,
	)
	declarationDigest, err := sealedAgentMailDeclarationDigest(
		job.SessionThreadID,
		job.RuntimeInputID,
		job.PreparationAttemptID,
		readiness.PreparationAttemptID,
	)
	if err != nil {
		return err
	}
	if existing, ok, err := readBridgeDeclarationOperationTx(
		ctx,
		tx,
		scope,
		bridgeOpSettleSealedAgentMail,
		"sealed_agent_mail",
		sourceID,
	); err != nil {
		return err
	} else if ok {
		receipt, receiptErr := unmarshalDeclarationReceipt(existing.ReceiptJSON)
		if existing.DeclarationDigest != declarationDigest ||
			receiptErr != nil ||
			!validSealedAgentMailReceipt(receipt, job, readiness.PreparationAttemptID, sourceID, declarationDigest) {
			return runtimeDeliveryPrepareError{
				kind:      "runtime_delivery_idempotency_conflict",
				message:   "sealed agent mail disposition conflicts with the stored result",
				retryable: false,
			}
		}
		return cancelSealedAgentMailInboxTx(ctx, tx, job, now)
	}
	receipt := sealedAgentMailReceipt(job, readiness.PreparationAttemptID, sourceID, declarationDigest)
	receiptJSON, err := marshalDeclarationReceipt(receipt)
	if err != nil {
		return err
	}
	if err := insertBridgeDeclarationOperationTx(
		ctx,
		tx,
		scope,
		bridgeOpSettleSealedAgentMail,
		"sealed_agent_mail",
		sourceID,
		declarationDigest,
		receiptJSON,
		now,
	); err != nil {
		return err
	}
	return cancelSealedAgentMailInboxTx(ctx, tx, job, now)
}

func cancelSealedAgentMailInboxTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'cancelled',
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND input_kind = 'agent_mail'
		    AND status IN ('delivering', 'accepted')
		    AND EXISTS (
		        SELECT 1
		          FROM jsonb_array_elements_text(session_runtime_inbox.event_ids_json::jsonb) source_ref(event_id)
		          JOIN session_events source
		            ON source.workspace_id = session_runtime_inbox.workspace_id
		           AND source.session_id = session_runtime_inbox.session_id
		           AND source.session_thread_id = session_runtime_inbox.session_thread_id
		           AND source.event_id = source_ref.event_id
		         WHERE source.preparation_attempt_id = $4
		    )`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
		job.PreparationAttemptID,
		now,
	)
	return err
}

func replaySealedAgentMailTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	failedPreparationAttemptID := job.SealedPreparationAttemptID
	if failedPreparationAttemptID == "" {
		readiness, found, err := loadEarliestFailedPreparationAtOrAfterTx(
			ctx,
			tx,
			job.WorkspaceID,
			job.SessionID,
			job.PreparationAttemptID,
			false,
		)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		failedPreparationAttemptID = readiness.PreparationAttemptID
	}
	scope := bridgeSessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	sourceID := stableRuntimeID(
		"sealed_agent_mail",
		job.RuntimeInputID,
		job.PreparationAttemptID,
		failedPreparationAttemptID,
	)
	declarationDigest, err := sealedAgentMailDeclarationDigest(
		job.SessionThreadID,
		job.RuntimeInputID,
		job.PreparationAttemptID,
		failedPreparationAttemptID,
	)
	if err != nil {
		return false, err
	}
	existing, ok, err := readBridgeDeclarationOperationTx(
		ctx,
		tx,
		scope,
		bridgeOpSettleSealedAgentMail,
		"sealed_agent_mail",
		sourceID,
	)
	if err != nil || !ok {
		return false, err
	}
	receipt, receiptErr := unmarshalDeclarationReceipt(existing.ReceiptJSON)
	if existing.DeclarationDigest != declarationDigest ||
		receiptErr != nil ||
		!validSealedAgentMailReceipt(receipt, job, failedPreparationAttemptID, sourceID, declarationDigest) {
		return false, runtimeDeliveryPrepareError{
			kind:      "runtime_delivery_disposition_invalid",
			message:   "sealed agent mail disposition is invalid",
			retryable: false,
		}
	}
	return true, nil
}

func sealedAgentMailReceipt(
	job RuntimeJob,
	failedPreparationAttemptID string,
	sourceID string,
	declarationDigest string,
) *bridgev1.DeclarationReceipt {
	return &bridgev1.DeclarationReceipt{
		SessionThreadId:   job.SessionThreadID,
		OperationKind:     bridgeOpSettleSealedAgentMail,
		SourceKind:        "sealed_agent_mail",
		SourceId:          sourceID,
		DeclarationDigest: declarationDigest,
		SealedAgentMail: &bridgev1.SealedAgentMailStamp{
			RuntimeInputId:             job.RuntimeInputID,
			BirthPreparationAttemptId:  job.PreparationAttemptID,
			FailedPreparationAttemptId: failedPreparationAttemptID,
			Disposition:                bridgev1.SealedAgentMailDisposition_SEALED_AGENT_MAIL_DISPOSITION_SEALED_FOR_FAILED_BIRTH,
		},
	}
}

func validSealedAgentMailReceipt(
	receipt *bridgev1.DeclarationReceipt,
	job RuntimeJob,
	failedPreparationAttemptID string,
	sourceID string,
	declarationDigest string,
) bool {
	if receipt == nil {
		return false
	}
	stamp := receipt.GetSealedAgentMail()
	return receipt.GetSessionThreadId() == job.SessionThreadID &&
		receipt.GetOperationKind() == bridgeOpSettleSealedAgentMail &&
		receipt.GetSourceKind() == "sealed_agent_mail" &&
		receipt.GetSourceId() == sourceID &&
		receipt.GetDeclarationDigest() == declarationDigest &&
		len(receipt.GetEvents()) == 0 &&
		len(receipt.GetMessages()) == 0 &&
		len(receipt.GetPendingAttachmentDeltaJson()) == 0 &&
		len(receipt.GetPendingToolDeltaJson()) == 0 &&
		len(receipt.GetPrefixConsumptions()) == 0 &&
		receipt.GetRequestReschedule() == nil &&
		receipt.GetIdleCloseout() == nil &&
		receipt.GetCompactedThroughMessageSequence() == 0 &&
		len(receipt.GetChildLifecycle()) == 0 &&
		stamp.GetRuntimeInputId() == job.RuntimeInputID &&
		stamp.GetBirthPreparationAttemptId() == job.PreparationAttemptID &&
		stamp.GetFailedPreparationAttemptId() == failedPreparationAttemptID &&
		stamp.GetDisposition() == bridgev1.SealedAgentMailDisposition_SEALED_AGENT_MAIL_DISPOSITION_SEALED_FOR_FAILED_BIRTH
}

func replayTaskNotificationDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	taskID := taskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "task notification runtime input id must identify a task", retryable: false}
	}
	var inboxStatus string
	err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&inboxStatus)
	if err == nil {
		switch inboxStatus {
		case "dead_lettered":
			return runtimeDeliveryExhaustedResult(), true, nil
		case "committed", "cancelled":
			return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
		case "queued", "delivering", "accepted", "parked":
		default:
			return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "runtime_inbox_status_invalid", message: "runtime inbox status is invalid", retryable: false}
		}
	} else if !dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{}, false, err
	}
	var taskThreadID string
	var terminalResultJSON sql.NullString
	err = tx.QueryRow(ctx,
		`SELECT session_thread_id, terminal_result_json
		   FROM session_background_tasks
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND task_id = $3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		taskID,
	).Scan(&taskThreadID, &terminalResultJSON)
	if dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	}
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	if taskThreadID != job.SessionThreadID || !terminalResultJSON.Valid || terminalResultJSON.String == "" {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	}
	return RuntimeDeliveryResult{}, false, nil
}

// agentMailRecipientTerminalTx reports the mail stale: once the recipient session or thread
// is terminal the delivery is no longer current and settles accepted without a wake.
func agentMailRecipientTerminalTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	var sessionStatus string
	var lifecycleState string
	if err := tx.QueryRow(ctx,
		`SELECT status, lifecycle_state
		   FROM sessions
		  WHERE workspace_id=$1 AND id=$2
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&sessionStatus, &lifecycleState); dbconnect.IsNoRows(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	if lifecycleState == "deleted" || sessionStatus == "terminated" {
		return true, nil
	}
	var recipientStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
	).Scan(&recipientStatus); dbconnect.IsNoRows(err) {
		return true, nil
	} else if err != nil {
		return false, err
	}
	return recipientStatus == "closed_for_runtime" || recipientStatus == "terminated", nil
}

func finalizeTaskNotificationDeliveryTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (RuntimeDeliveryResult, error) {
	if err := insertRuntimeDeliveryExhaustionEventTx(ctx, tx, job, now); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'dead_lettered',
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND status IN ('delivering', 'accepted')`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
		now,
	); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	return runtimeDeliveryExhaustedResult(), nil
}

func insertRuntimeDeliveryExhaustionEventTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) error {
	scope := bridgeSessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	return insertRuntimeDeliveryExhaustionEventWithThreadScopeTx(ctx, tx, job, threadScope, now)
}

func insertRuntimeDeliveryExhaustionEventWithThreadScopeTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, threadScope threadMutationScope, now time.Time) error {
	scope := bridgeSessionScope(job.WorkspaceID, job.SessionID, job.SessionThreadID)
	visibility, sessionVisible := threadScope.publicProjection("session.error")
	message := "The session runtime exhausted delivery attempts for this input."
	payloadJSON, err := terminalPreparationFailurePayloadJSON(message)
	if err != nil {
		return err
	}
	eventID := runtimeDeliveryExhaustionEventID(job)
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'session.error', $6, $7, $8, $6, $9, $9, $9)`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		eventID,
		sequence,
		payloadJSON,
		visibility,
		sessionVisible,
		now,
	); err != nil {
		return err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return err
	}
	messageJSON, err := terminalPreparationFailureMessageJSON(scope, message)
	if err != nil {
		return err
	}
	return insertSessionMessageProjectionTx(ctx, tx, scope, eventID, "assistant", messageJSON, now)
}

func runtimeDeliveryExhaustionEventExistsTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND event_id = $3
			   AND type = 'session.error'
			   AND payload_json::jsonb #>> '{error,retry_status,type}' = 'exhausted'
		)`,
		job.WorkspaceID,
		job.SessionID,
		runtimeDeliveryExhaustionEventID(job),
	).Scan(&exists)
	return exists, err
}

func runtimeDeliveryExhaustionEventID(job RuntimeJob) string {
	digest := sha256Hex(strings.Join([]string{job.WorkspaceID, job.SessionID, job.RuntimeInputID, "runtime_delivery_exhausted"}, "\x00"))
	return "evt_runtime_exhausted_" + digest[:24]
}

func runtimeDeliveryExhaustedResult() RuntimeDeliveryResult {
	return RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    false,
		ErrorKind:    "runtime_delivery_exhausted",
		ErrorMessage: "runtime delivery attempts are exhausted",
	}
}

type runtimeInitialMCPManifestRequiredError struct {
	toolsets []MCPManifestToolsetConfig
}

func (e runtimeInitialMCPManifestRequiredError) Error() string {
	return "initial MCP manifest delivery is required"
}

func requireInitialMCPManifestReadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	toolsets, err := initialMCPManifestToolsetsTx(ctx, tx, workspaceID, sessionID)
	if err != nil || len(toolsets) == 0 {
		return err
	}
	return runtimeInitialMCPManifestRequiredError{toolsets: toolsets}
}

func (s *PostgreSQLRuntimeDeliveryStore) enqueueInitialMCPManifestUpdates(ctx context.Context, job RuntimeJob, toolsets []MCPManifestToolsetConfig, now time.Time) error {
	return s.enqueueInitialMCPManifestUpdatesWithListTimeout(ctx, job, toolsets, now, initialMCPManifestListTimeout)
}

func (s *PostgreSQLRuntimeDeliveryStore) enqueueInitialMCPManifestUpdatesWithListTimeout(
	ctx context.Context,
	job RuntimeJob,
	toolsets []MCPManifestToolsetConfig,
	now time.Time,
	listTimeout time.Duration,
) error {
	if s.MCPManifestLister == nil {
		return runtimeDeliveryPrepareError{kind: "mcp_manifest_discovery_unavailable", message: "mcp manifest lister is unavailable", retryable: true}
	}
	for _, toolset := range toolsets {
		listCtx, cancelList := context.WithTimeout(ctx, listTimeout)
		manifest, err := s.MCPManifestLister.ListMCPTools(listCtx, MCPManifestListRequest{
			WorkspaceID:   job.WorkspaceID,
			SessionID:     job.SessionID,
			MCPServerName: toolset.MCPServerName,
		})
		cancelList()
		if err != nil {
			return runtimeDeliveryPrepareError{kind: "mcp_manifest_discovery_unavailable", message: "mcp manifest discovery failed", retryable: true}
		}
		if strings.TrimSpace(manifest.ManifestETag) == "" {
			return runtimeDeliveryPrepareError{kind: "mcp_manifest_protocol_error", message: "mcp manifest etag is missing", retryable: false}
		}
		var acceptance mcpManifestAcceptance
		if err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.enqueue_initial_mcp_manifest", func(tx *dbconnect.Tx) error {
			var err error
			acceptance, err = captureMCPManifestAcceptanceTx(
				ctx, tx, job.WorkspaceID, job.SessionID, toolset.MCPServerName, manifest.ManifestETag, manifest.Tools, now,
			)
			return err
		}); err != nil {
			if status.Code(err) == codes.ResourceExhausted {
				return runtimeDeliveryPrepareError{kind: "mcp_manifest_too_large", message: "mcp manifest tools exceed the accepted byte limit", retryable: false}
			}
			return err
		}
		if !acceptance.Duplicate {
			logMCPManifestOmissions(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance.BuiltinFamily, acceptance.Omissions)
		}
		if acceptance.ManifestTooLarge {
			logMCPManifestReadiness(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, mcpManifestReadinessUnready, mcpManifestDiagnosticTooLarge, acceptance.Generation)
		}
	}
	return nil
}

type MCPManifestToolsetConfig struct {
	MCPServerName string
	BuiltinFamily string
}

func sessionAgentMCPManifestToolsetsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) ([]MCPManifestToolsetConfig, error) {
	var agentConfigJSON string
	var installedToolsJSON string
	if err := tx.QueryRow(ctx,
		`SELECT av.config_json,
		        s.installed_tools_json
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id
		  WHERE s.workspace_id = $1
		    AND s.id = $2`,
		workspaceID,
		sessionID,
	).Scan(&agentConfigJSON, &installedToolsJSON); dbconnect.IsNoRows(err) {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_session_unavailable", message: "session agent config is unavailable", retryable: true}
	} else if err != nil {
		return nil, err
	}
	config, err := effectiveBridgeRuntimeAgentConfig(agentConfigJSON, installedToolsJSON)
	if err != nil {
		return nil, runtimeDeliveryPrepareError{kind: "invalid_session_agent_config", message: "session agent config is invalid", retryable: false}
	}
	builtinFamily, err := bridgeInstalledBuiltinFamily(config)
	if err != nil {
		return nil, runtimeDeliveryPrepareError{kind: "invalid_session_agent_config", message: "session agent config is invalid", retryable: false}
	}
	seen := map[string]struct{}{}
	var toolsets []MCPManifestToolsetConfig
	for _, raw := range config.Tools {
		var tool struct {
			Type          string `json:"type"`
			MCPServerName string `json:"mcp_server_name"`
		}
		if err := json.Unmarshal(raw, &tool); err != nil {
			return nil, runtimeDeliveryPrepareError{kind: "invalid_session_agent_config", message: "session agent tool config is invalid", retryable: false}
		}
		if tool.Type != "mcp_toolset" || tool.MCPServerName != "github" {
			continue
		}
		if _, ok := seen[tool.MCPServerName]; ok {
			continue
		}
		seen[tool.MCPServerName] = struct{}{}
		toolsets = append(toolsets, MCPManifestToolsetConfig{
			MCPServerName: tool.MCPServerName,
			BuiltinFamily: builtinFamily,
		})
	}
	return toolsets, nil
}

func initialMCPManifestToolsetsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) ([]MCPManifestToolsetConfig, error) {
	toolsets, err := sessionAgentMCPManifestToolsetsTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return nil, err
	}
	var pending []MCPManifestToolsetConfig
	for _, toolset := range toolsets {
		delivered, err := mcpManifestDeliveryExistsTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName)
		if err != nil {
			return nil, err
		}
		if delivered {
			continue
		}
		pending = append(pending, toolset)
	}
	return pending, nil
}

func mcpManifestToolsetConfigTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) (MCPManifestToolsetConfig, error) {
	toolsets, err := sessionAgentMCPManifestToolsetsTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return MCPManifestToolsetConfig{}, err
	}
	for _, toolset := range toolsets {
		if toolset.MCPServerName == mcpServerName {
			return toolset, nil
		}
	}
	return MCPManifestToolsetConfig{}, status.Error(codes.FailedPrecondition, "mcp toolset config is unavailable")
}

func mcpManifestDeliveryExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, mcpServerName string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM session_mcp_manifests
			 WHERE workspace_id = $1 AND session_id = $2 AND mcp_server_name = $3
		)`,
		workspaceID,
		sessionID,
		mcpServerName,
	).Scan(&exists)
	return exists, err
}

func runtimeTaskNotificationPayloadJSON(plan *RuntimeTaskNotificationPlan, terminalStatus string, resultJSON string) (string, error) {
	if plan == nil || plan.TaskID == "" || plan.SourceToolUseEventID == "" {
		return "", runtimeDeliveryPrepareError{kind: "task_notification_result_invalid", message: "task notification source identity is incomplete", retryable: false}
	}
	payloadJSON, err := canonicalTaskNotificationPayloadJSON(plan.TaskID, plan.SourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return "", runtimeDeliveryPrepareError{kind: "task_notification_result_invalid", message: err.Error(), retryable: false}
	}
	return payloadJSON, nil
}

func runtimeTaskNotificationStatus(terminalStatus string) string {
	switch terminalStatus {
	case "completed":
		return "completed"
	case "failed", "unknown_outcome":
		return "failed"
	case "cancelled", "cancelled_by_cleanup":
		return "cancelled"
	case "expired", "stale":
		return "expired"
	default:
		return ""
	}
}

func normalizeBackgroundTaskTerminalStatus(value string) string {
	switch value {
	case "completed", "failed", "cancelled", "expired":
		return value
	default:
		return ""
	}
}

func (s *PostgreSQLRuntimeDeliveryStore) resolveRuntimeTarget(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (runtimeBindingForDelivery, error) {
	if s.TargetResolver != nil {
		return s.TargetResolver.ResolveRuntimeTarget(ctx, tx, job)
	}
	return readRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
}

func (s *PostgreSQLRuntimeDeliveryStore) sandboxStatusFreshnessWindow() time.Duration {
	if s != nil && s.SandboxStatusFreshnessWindow > 0 {
		return s.SandboxStatusFreshnessWindow
	}
	return defaultSandboxStatusFreshness
}

func (s *PostgreSQLRuntimeDeliveryStore) resourceCredentialRefreshMargin() time.Duration {
	if s != nil && s.ResourceCredentialRefreshMargin > 0 {
		return s.ResourceCredentialRefreshMargin
	}
	return defaultResourceCredentialRefreshMargin
}

func runtimeCommandPayloadForJobTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, error) {
	if job.Kind == queue.KindRuntimeConfigUpdate {
		if job.MCPServerName != "" {
			return runtimeMCPManifestCommandPayloadTx(ctx, tx, job)
		}
		return runtimeSessionConfigCommandPayloadTx(ctx, tx, job)
	}
	if job.Kind != queue.KindRuntimeInput {
		return job.PayloadJSON, job.RuntimeInputID, nil
	}
	switch job.InputKind {
	case "messages":
		payload, err := acceptedMessageCommandPayloadTx(ctx, tx, job)
		return payload, job.RuntimeInputID, err
	case "rejection":
		payload, err := marshalBridgeDataJSON(map[string]any{
			"input_kind":  "rejection",
			"reason_code": job.RejectionReasonCode,
		})
		return payload, job.RuntimeInputID, err
	case "interrupt_control":
		payload, err := interruptControlCommandPayloadTx(ctx, tx, job)
		return payload, job.RuntimeInputID, err
	case "tool_confirmation":
		payload, err := toolConfirmationCommandPayloadTx(ctx, tx, job)
		return payload, job.RuntimeInputID, err
	default:
		return job.PayloadJSON, job.RuntimeInputID, nil
	}
}

func effectiveRuntimeInputJobTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeJob, error) {
	var inputKind string
	var rejectionReason sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT input_kind, rejection_reason_code
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&inputKind, &rejectionReason)
	if dbconnect.IsNoRows(err) {
		return job, nil
	}
	if err != nil {
		return RuntimeJob{}, err
	}
	if inputKind != "rejection" {
		return job, nil
	}
	if !rejectionReason.Valid ||
		(rejectionReason.String != "runtime_command_payload_too_large" &&
			rejectionReason.String != "runtime_command_rejected") {
		return RuntimeJob{}, runtimeDeliveryPrepareError{
			kind:      "runtime_inbox_payload_conflict",
			message:   "runtime rejection inbox row has an invalid reason code",
			retryable: false,
		}
	}
	job.InputKind = "rejection"
	job.RejectionReasonCode = rejectionReason.String
	job.CommandKind = agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES
	return job, nil
}

func runtimeMCPManifestCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, error) {
	// Queue intents carry references only. The manifest row is the complete
	// delivery source; tool policy reaches the pod through its own carrier.
	row, found, err := loadMCPManifestRowForUpdateTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
	if err != nil {
		return "", "", err
	}
	if !found {
		return "", "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "MCP manifest durable row is missing", retryable: false}
	}
	payloadJSON, err := runtimeMCPManifestCommandPayload(job.WorkspaceID, job.SessionID, job.MCPServerName, row)
	if err != nil {
		return "", "", err
	}
	return payloadJSON, runtimeMCPManifestInputID(job.SessionID, job.MCPServerName, row.Generation), nil
}

func runtimeSessionConfigCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, error) {
	// Queue intents carry references only. Rebuild from the locked durable
	// session/config rows with the same policy serializer used by cold bootstrap.
	var configGeneration int64
	var approvalMode string
	var installedToolsJSON string
	var agentConfigJSON string
	err := tx.QueryRow(ctx,
		`SELECT s.config_generation, s.approval_mode, s.installed_tools_json, av.config_json
		   FROM sessions s
		   JOIN agent_versions av
		     ON av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id
		  WHERE s.workspace_id = $1
		    AND s.id = $2
		  FOR UPDATE OF s`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&configGeneration, &approvalMode, &installedToolsJSON, &agentConfigJSON)
	if dbconnect.IsNoRows(err) {
		return "", "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime config durable row is missing", retryable: false}
	}
	if err != nil {
		return "", "", err
	}
	memoryStores, err := bridgeRuntimeMemoryStoresTx(ctx, tx, string(job.WorkspaceID), job.SessionID)
	if err != nil {
		return "", "", err
	}
	settings, err := bridgeRuntimeSessionAgentSettings(approvalMode, agentConfigJSON, installedToolsJSON, memoryStores)
	if err != nil {
		return "", "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime config durable row is invalid", retryable: false}
	}
	payloadJSON, err := marshalBridgeDataJSON(map[string]any{
		"workspace_id":      job.WorkspaceID,
		"session_id":        job.SessionID,
		"config_generation": configGeneration,
		"approval_mode":     approvalMode,
		"system":            settings.System,
		"memory_stores":     settings.MemoryStores,
		"tool_policy":       settings.ToolPolicy,
	})
	if err != nil {
		return "", "", err
	}
	return payloadJSON, runtimeConfigUpdateInputID(job.SessionID, strconv.FormatInt(configGeneration, 10)), nil
}

func acceptedMessageCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	if len(job.EventIDs) == 0 || job.SessionThreadID == "" {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "message runtime input is incomplete", retryable: false}
	}
	var nextSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID,
	).Scan(&nextSequence); err != nil {
		return "", err
	}
	scope := bridgeSessionScope(string(job.WorkspaceID), job.SessionID, job.SessionThreadID)
	messages := make([]json.RawMessage, 0, len(job.EventIDs))
	for index, eventID := range job.EventIDs {
		var eventType string
		var payloadJSON string
		var createdAt time.Time
		if err := tx.QueryRow(ctx,
			`SELECT type, payload_json, created_at
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4`,
			job.WorkspaceID, job.SessionID, job.SessionThreadID, eventID,
		).Scan(&eventType, &payloadJSON, &createdAt); dbconnect.IsNoRows(err) {
			return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "message runtime input event is missing", retryable: false}
		} else if err != nil {
			return "", err
		}
		if eventType != "user.message" {
			return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "message runtime input event type is invalid", retryable: false}
		}
		timestamp := createdAt.UTC()
		identity := bridgeRequestHash("accepted_message", string(job.WorkspaceID), job.SessionID, job.SessionThreadID, eventID)
		messageJSON, err := userMessageDataJSON(scope, "msg_input_"+identity[:32], nextSequence+int64(index), payloadJSON, timestamp)
		if err != nil {
			return "", err
		}
		messages = append(messages, json.RawMessage(messageJSON))
	}
	return marshalBridgeDataJSON(map[string]any{"messages": messages})
}

func interruptControlCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	eventID, eventType, _, sequence, err := loadSingleRuntimeInputEventTx(ctx, tx, job)
	if err != nil {
		return "", err
	}
	if eventType != "user.interrupt" {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "interrupt control event type is invalid", retryable: false}
	}
	return marshalBridgeJSON(map[string]any{
		"source_event_id":          eventID,
		"interrupt_fence_sequence": sequence,
		"reason":                   nil,
	})
}

func toolConfirmationCommandPayloadTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	eventID, eventType, payloadJSON, _, err := loadSingleRuntimeInputEventTx(ctx, tx, job)
	if err != nil {
		return "", err
	}
	if eventType != "user.tool_confirmation" {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "tool confirmation event type is invalid", retryable: false}
	}
	var payload struct {
		ToolUseID   string  `json:"tool_use_id"`
		Result      string  `json:"result"`
		DenyMessage *string `json:"deny_message"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.ToolUseID == "" || (payload.Result != "allow" && payload.Result != "deny") {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "tool confirmation event payload is invalid", retryable: false}
	}
	command := map[string]any{
		"source_event_id":   eventID,
		"tool_use_event_id": payload.ToolUseID,
		"decision":          payload.Result,
	}
	if payload.Result == "deny" && payload.DenyMessage != nil {
		command["deny_message"] = *payload.DenyMessage
	}
	return marshalBridgeJSON(command)
}

func loadSingleRuntimeInputEventTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, string, string, int64, error) {
	if len(job.EventIDs) != 1 {
		return "", "", "", 0, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "control runtime input must reference exactly one event", retryable: false}
	}
	eventID := job.EventIDs[0]
	var eventType string
	var payloadJSON string
	var sequence int64
	err := tx.QueryRow(ctx,
		`SELECT type, payload_json, sequence
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		eventID,
	).Scan(&eventType, &payloadJSON, &sequence)
	if dbconnect.IsNoRows(err) {
		return "", "", "", 0, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "control runtime input event is missing", retryable: false}
	}
	if err != nil {
		return "", "", "", 0, err
	}
	return eventID, eventType, payloadJSON, sequence, nil
}

type cleanupPendingWait struct {
	ThreadID        string
	ToolUseEventID  string
	ModelToolCallID string
	ToolName        string
	InputJSON       string
	EventType       string
	MCPServerName   string
}

type pendingToolTerminal struct {
	PartStatus string
	ErrorType  string
	Message    string
}

func insertPendingToolTerminalResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, wait cleanupPendingWait, terminal pendingToolTerminal, now time.Time) (string, error) {
	payloadJSON, eventType, err := pendingToolTerminalPayloadJSON(wait, terminal)
	if err != nil {
		return "", err
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.publicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $7, $10, $10, $10)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		eventType, payloadJSON, visibility, sessionVisible, now,
	); err != nil {
		return "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return "", err
	}
	if err := insertPendingToolTerminalMessageTx(ctx, tx, scope, eventID, wait, terminal, now); err != nil {
		return "", err
	}
	return eventID, nil
}

func pendingToolTerminalPayloadJSON(wait cleanupPendingWait, terminal pendingToolTerminal) (string, string, error) {
	eventType := "agent.tool_result"
	toolUseField := "tool_use_id"
	if wait.EventType == "agent.mcp_tool_use" {
		eventType = "agent.mcp_tool_result"
		toolUseField = "mcp_tool_use_id"
	}
	payload, err := marshalBridgeJSON(map[string]any{
		"type":       eventType,
		toolUseField: wait.ToolUseEventID,
		"is_error":   true,
		"content": []map[string]any{
			{
				"type": "text",
				"text": terminal.Message,
			},
		},
	})
	return payload, eventType, err
}

func insertPendingToolTerminalMessageTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, wait cleanupPendingWait, terminal pendingToolTerminal, now time.Time) error {
	var existing string
	err := tx.QueryRow(ctx,
		`SELECT message_id
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND source_event_id = $2
		  LIMIT 1`,
		scope.GetWorkspaceId(),
		eventID,
	).Scan(&existing)
	if err == nil {
		return nil
	}
	if !dbconnect.IsNoRows(err) {
		return err
	}
	var sequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence); err != nil {
		return err
	}
	messageID := id.New("msg_")
	timestamp := now
	dataJSON, err := marshalBridgeJSON(map[string]any{
		"id":        messageID,
		"sessionId": scope.GetSessionId(),
		"role":      "assistant",
		"origin":    "agent",
		"sequence":  sequence - 1,
		"status":    "completed",
		"createdAt": timestamp,
		"updatedAt": timestamp,
		"parts": []map[string]any{
			{
				"id":             messageID + "_tool",
				"sessionId":      scope.GetSessionId(),
				"messageId":      messageID,
				"sequence":       0,
				"createdAt":      timestamp,
				"updatedAt":      timestamp,
				"type":           "tool",
				"toolCallId":     wait.ModelToolCallID,
				"toolName":       wait.ToolName,
				"toolUseEventId": wait.ToolUseEventID,
				"toolEvent":      pendingToolEventProjection(wait),
				"state": map[string]any{
					"status": terminal.PartStatus,
					"input":  cleanupRuntimeBoundedJSON(wait.InputJSON),
					"error": map[string]any{
						"type":      terminal.ErrorType,
						"message":   terminal.Message,
						"retryable": false,
					},
				},
				"completedAt": timestamp,
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, $7, $7, $8, $8)
		ON CONFLICT (workspace_id, session_id, session_thread_id, source_event_id)
		WHERE source_event_id IS NOT NULL
		DO NOTHING`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		sequence,
		dataJSON,
		eventID,
		timestamp,
	)
	return err
}

func pendingToolEventProjection(wait cleanupPendingWait) map[string]any {
	if wait.EventType == "agent.mcp_tool_use" {
		return map[string]any{"kind": "mcp", "mcpServerName": wait.MCPServerName}
	}
	return map[string]any{"kind": "tool"}
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareAgentMailCommandTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	port int,
	now time.Time,
) (RuntimeCommandPlan, error) {
	if err := lockSessionPreparationResetFenceTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return RuntimeCommandPlan{}, err
	}
	var sessionStatus string
	var lifecycleState string
	if err := tx.QueryRow(ctx,
		`SELECT status, lifecycle_state
		   FROM sessions
		  WHERE workspace_id=$1 AND id=$2
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&sessionStatus, &lifecycleState); dbconnect.IsNoRows(err) {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	} else if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if lifecycleState == "deleted" || sessionStatus == "terminated" {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	var recipientStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
	).Scan(&recipientStatus); dbconnect.IsNoRows(err) {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	} else if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if recipientStatus == "closed_for_runtime" || recipientStatus == "terminated" {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if err := requireRuntimePreparationReadyTx(ctx, tx, job.WorkspaceID, job.SessionID, s.sandboxStatusFreshnessWindow(), s.resourceCredentialRefreshMargin(), now); err != nil {
		return RuntimeCommandPlan{}, err
	}
	binding, err := s.resolveRuntimeTarget(ctx, tx, job)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "agent mail runtime input id is invalid", retryable: false}
	}
	envelope, err := loadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID, deliveryID)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if envelope.TargetThreadID != job.SessionThreadID {
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "agent mail queue target does not match the stored envelope", retryable: false}
	}
	admitted, err := admitAgentMailDeliveryTx(
		ctx,
		tx,
		runtimeScopeForDeliveryJob(job, binding),
		envelope,
		job.PreparationAttemptID,
		binding,
		now,
	)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if admitted.Terminal {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	orderedJob := job
	orderedJob.EventIDs = []string{admitted.ReceivedEventID}
	orderedJob.SequenceFrom = admitted.ReceivedSequence
	orderedJob.SequenceTo = admitted.ReceivedSequence
	repaired, err := requeueEarlierPendingRuntimeInboxTx(ctx, tx, orderedJob, now)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if repaired {
		return RuntimeCommandPlan{}, runtimePreparationRequeuedError{err: runtimeDeliveryPrepareError{
			kind:      "runtime_inbox_repair_pending",
			message:   "earlier runtime input is pending repair and was requeued before current delivery",
			retryable: true,
		}}
	}
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"delivery_id":              envelope.DeliveryID,
		"source_thread_id":         envelope.SourceThreadID,
		"source_tool_use_event_id": envelope.SourceToolUseEventID,
		"message":                  envelope.MessageJSON,
	})
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	return RuntimeCommandPlan{
		Target: RuntimePodTarget{
			Namespace: binding.Namespace,
			PodName:   binding.PodName,
			PodUID:    binding.PodUID,
			PodIP:     binding.PodIP,
			Port:      port,
		},
		Request: &agentruntimev1.RuntimeInputCommandRequest{
			RequestId:          job.JobID + ":" + job.LeaseToken,
			WorkspaceId:        job.WorkspaceID,
			SessionId:          job.SessionID,
			SessionThreadId:    job.SessionThreadID,
			BindingId:          binding.BindingID,
			BindingGeneration:  int64(binding.BindingGeneration),
			TargetPodNamespace: binding.Namespace,
			TargetPodName:      binding.PodName,
			TargetPodUid:       binding.PodUID,
			TargetPodIp:        binding.PodIP,
			RuntimeInputId:     job.RuntimeInputID,
			EventIds:           []string{admitted.ReceivedEventID},
			SequenceFrom:       admitted.ReceivedSequence,
			SequenceTo:         admitted.ReceivedSequence,
			CommandKind:        job.CommandKind,
			PayloadJson:        payloadJSON,
		},
	}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareTaskNotificationCommandTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, port int, now time.Time) (*RuntimeTaskNotificationPlan, RuntimeCommandPlan, error) {
	taskID := taskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return nil, RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "task notification runtime input id must identify a task", retryable: false}
	}
	var sessionThreadID, sourceToolUseEventID, taskStatus string
	var terminalResultJSON, terminalEventID sql.NullString
	err := tx.QueryRow(ctx, `SELECT session_thread_id, source_tool_use_event_id, status,
		terminal_result_json, terminal_event_id
		FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, taskID).Scan(
		&sessionThreadID, &sourceToolUseEventID, &taskStatus, &terminalResultJSON, &terminalEventID,
	)
	if dbconnect.IsNoRows(err) {
		return nil, RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	if sessionThreadID != job.SessionThreadID || sourceToolUseEventID == "" {
		return nil, RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "task_notification_identity_invalid", message: "task notification durable identity is invalid", retryable: false}
	}
	if terminalEventID.Valid {
		return nil, RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	if taskStatus == "running" {
		return nil, RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "task_notification_not_terminal", message: "task notification result is not terminal", retryable: true}
	}
	if !terminalResultJSON.Valid || terminalResultJSON.String == "" || !json.Valid([]byte(terminalResultJSON.String)) || !validBackgroundTaskTerminalStatus(taskStatus) {
		return nil, RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "task_notification_result_invalid", message: "task notification durable result is invalid", retryable: false}
	}
	if err := requireInitialMCPManifestReadyTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	binding, err := s.resolveRuntimeTarget(ctx, tx, job)
	if err != nil {
		var prepareErr runtimeDeliveryPrepareError
		if errors.As(err, &prepareErr) && prepareErr.kind == "runtime_binding_unavailable" {
			return nil, RuntimeCommandPlan{StaleAccepted: true}, nil
		}
		return nil, RuntimeCommandPlan{}, err
	}
	payloadJSON, err := runtimeTaskNotificationPayloadJSON(&RuntimeTaskNotificationPlan{
		TaskID: taskID, SourceToolUseEventID: sourceToolUseEventID,
	}, taskStatus, terminalResultJSON.String)
	if err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	if err := upsertRuntimeInboxDeliveryTx(ctx, tx, job, binding, now); err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	return &RuntimeTaskNotificationPlan{
			TaskID:               taskID,
			SourceToolUseEventID: sourceToolUseEventID,
			ResultJSON:           payloadJSON,
		}, RuntimeCommandPlan{
			Target: RuntimePodTarget{
				Namespace: binding.Namespace,
				PodName:   binding.PodName,
				PodUID:    binding.PodUID,
				PodIP:     binding.PodIP,
				Port:      port,
			},
			Request: &agentruntimev1.RuntimeInputCommandRequest{
				RequestId:          job.JobID + ":" + job.LeaseToken,
				WorkspaceId:        job.WorkspaceID,
				SessionId:          job.SessionID,
				SessionThreadId:    job.SessionThreadID,
				BindingId:          binding.BindingID,
				BindingGeneration:  int64(binding.BindingGeneration),
				TargetPodNamespace: binding.Namespace,
				TargetPodName:      binding.PodName,
				TargetPodUid:       binding.PodUID,
				TargetPodIp:        binding.PodIP,
				RuntimeInputId:     job.RuntimeInputID,
				EventIds:           append([]string(nil), job.EventIDs...),
				SequenceFrom:       job.SequenceFrom,
				SequenceTo:         job.SequenceTo,
				CommandKind:        job.CommandKind,
				PayloadJson:        payloadJSON,
			},
		}, nil
}

func taskNotificationTaskID(runtimeInputID string) string {
	value := strings.TrimSpace(runtimeInputID)
	for _, prefix := range []string{"task_notification:"} {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(value, prefix))
		}
	}
	return value
}

func runtimeScopeForDeliveryJob(job RuntimeJob, binding runtimeBindingForDelivery) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		RequestId:       job.JobID + ":" + job.LeaseToken,
		WorkspaceId:     job.WorkspaceID,
		SessionId:       job.SessionID,
		SessionThreadId: job.SessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         binding.BindingID,
			BindingGeneration: binding.BindingGeneration,
			TargetPodUid:      binding.PodUID,
		},
	}
}

func runtimeScopeFromCommandRequest(request *agentruntimev1.RuntimeInputCommandRequest) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		RequestId:       request.GetRequestId(),
		WorkspaceId:     request.GetWorkspaceId(),
		SessionId:       request.GetSessionId(),
		SessionThreadId: request.GetSessionThreadId(),
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         request.GetBindingId(),
			BindingGeneration: request.GetBindingGeneration(),
			TargetPodUid:      request.GetTargetPodUid(),
		},
	}
}

func commitTaskNotificationInboxTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, runtimeInputID string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'committed',
		        committed_at = COALESCE(committed_at, $5),
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND binding_id = $4
		    AND input_kind = 'task_notification'
		    AND status IN ('delivering', 'accepted', 'committed')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		runtimeInputID,
		scope.GetBinding().GetBindingId(),
		now,
	)
	return err
}

type KubernetesRuntimeTargetResolver struct {
	Snapshot func() enginekubernetes.BindingVisibilitySnapshot
	Clock    func() time.Time
}

type runtimeBindingLostError struct {
	binding runtimeBindingForDelivery
}

func (e runtimeBindingLostError) Error() string { return "runtime binding target is gone" }

// ResolveRuntimeTarget maps each candidate pod's Kubernetes binding visibility
// to one action, and the partition is load-bearing:
//
//	visibility                                    action
//	reusable                                      reuse the binding row; no new
//	                                               binding_generation
//	absent | deleted | uid_changed | ip_changed   PROVEN GONE — choose a fresh
//	                                               candidate and write the next
//	                                               binding_generation
//	snapshot_not_ready | not_ready |              AVAILABILITY only — retry without
//	  not_serving | terminating                    mutating the binding row
//
// The proven-gone set {absent, deleted, uid_changed, ip_changed} is the ONLY
// proof a pod is gone. An availability state is never such proof: while a target
// sits in one, cleanup and repair must keep retrying and must NEVER finalize a
// binding, request sandbox release, or write terminal settlements.
func (r KubernetesRuntimeTargetResolver) ResolveRuntimeTarget(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (runtimeBindingForDelivery, error) {
	if r.Snapshot == nil {
		return runtimeBindingForDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_visibility_unavailable", message: "runtime visibility snapshot is unavailable", retryable: true}
	}
	snapshot := r.Snapshot()
	if !snapshot.Ready {
		return runtimeBindingForDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_visibility_not_ready", message: "runtime pod visibility is not ready", retryable: true}
	}
	current, found, err := readOptionalRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return runtimeBindingForDelivery{}, err
	}
	if found {
		visibility := snapshot.VisibilityFor(enginekubernetes.BoundRuntimePod{
			Namespace: current.Namespace,
			PodName:   current.PodName,
			PodUID:    current.PodUID,
			PodIP:     current.PodIP,
		})
		switch visibility {
		case enginekubernetes.BindingVisibilityReusable:
			return current, nil
		case enginekubernetes.BindingVisibilityAbsent,
			enginekubernetes.BindingVisibilityDeleted,
			enginekubernetes.BindingVisibilityUIDChanged,
			enginekubernetes.BindingVisibilityIPChanged:
			return runtimeBindingForDelivery{}, runtimeBindingLostError{binding: current}
		case enginekubernetes.BindingVisibilitySnapshotNotReady,
			enginekubernetes.BindingVisibilityNotReady,
			enginekubernetes.BindingVisibilityNotServing,
			enginekubernetes.BindingVisibilityTerminating:
			return runtimeBindingForDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_binding_not_available", message: "runtime binding is not currently available: " + string(visibility), retryable: true}
		default:
			return runtimeBindingForDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_binding_not_available", message: "runtime binding visibility is not reusable", retryable: true}
		}
	}
	candidate, ok := chooseRuntimeBindingCandidate(snapshot.Candidates)
	if !ok {
		return runtimeBindingForDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_binding_candidate_unavailable", message: "runtime binding replacement candidate is unavailable", retryable: true}
	}
	now := storage.Now()
	if r.Clock != nil {
		now = r.Clock().UTC()
	}
	var generation int64
	if err := tx.QueryRow(ctx, `SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&generation); err != nil {
		return runtimeBindingForDelivery{}, err
	}
	binding := runtimeBindingForDelivery{
		BindingID:         id.New("bind_"),
		BindingGeneration: generation,
		Namespace:         candidate.Namespace,
		PodName:           candidate.PodName,
		PodUID:            candidate.PodUID,
		PodIP:             candidate.PodIP,
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		ON CONFLICT (workspace_id, session_id) DO UPDATE SET
			binding_id = EXCLUDED.binding_id,
			binding_generation = EXCLUDED.binding_generation,
			agent_runtime_namespace = EXCLUDED.agent_runtime_namespace,
			agent_runtime_pod_name = EXCLUDED.agent_runtime_pod_name,
			agent_runtime_pod_uid = EXCLUDED.agent_runtime_pod_uid,
			agent_runtime_pod_ip = EXCLUDED.agent_runtime_pod_ip,
			bound_at = EXCLUDED.bound_at,
			updated_at = EXCLUDED.updated_at`,
		job.WorkspaceID,
		job.SessionID,
		binding.BindingID,
		binding.BindingGeneration,
		binding.Namespace,
		binding.PodName,
		binding.PodUID,
		binding.PodIP,
		now,
	)
	if err != nil {
		return runtimeBindingForDelivery{}, err
	}
	return binding, nil
}

func chooseRuntimeBindingCandidate(candidates []enginekubernetes.BindingCandidate) (enginekubernetes.BindingCandidate, bool) {
	if len(candidates) == 0 {
		return enginekubernetes.BindingCandidate{}, false
	}
	sorted := append([]enginekubernetes.BindingCandidate(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		return strings.Join([]string{left.Namespace, left.PodName, left.PodUID, left.PodIP}, "\x00") <
			strings.Join([]string{right.Namespace, right.PodName, right.PodUID, right.PodIP}, "\x00")
	})
	return sorted[0], true
}

type runtimeBindingForDelivery struct {
	BindingID         string
	BindingGeneration int64
	Namespace         string
	PodName           string
	PodUID            string
	PodIP             string
}

func readRuntimeBindingForDeliveryTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (runtimeBindingForDelivery, error) {
	binding, found, err := readOptionalRuntimeBindingForDeliveryTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return runtimeBindingForDelivery{}, err
	}
	if !found {
		return runtimeBindingForDelivery{}, runtimeDeliveryPrepareError{kind: "runtime_binding_unavailable", message: "runtime binding is unavailable", retryable: true}
	}
	return binding, nil
}

func readOptionalRuntimeBindingForDeliveryTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (runtimeBindingForDelivery, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT binding_id, binding_generation, agent_runtime_namespace, agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip
		   FROM session_runtime_bindings
		  WHERE workspace_id = $1 AND session_id = $2
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	)
	var binding runtimeBindingForDelivery
	if err := row.Scan(&binding.BindingID, &binding.BindingGeneration, &binding.Namespace, &binding.PodName, &binding.PodUID, &binding.PodIP); dbconnect.IsNoRows(err) {
		return runtimeBindingForDelivery{}, false, nil
	} else if err != nil {
		return runtimeBindingForDelivery{}, false, err
	}
	if binding.BindingID == "" || binding.BindingGeneration <= 0 || binding.Namespace == "" || binding.PodName == "" || binding.PodUID == "" || binding.PodIP == "" {
		return runtimeBindingForDelivery{}, false, runtimeDeliveryPrepareError{kind: "runtime_binding_invalid", message: "runtime binding is invalid", retryable: true}
	}
	if _, err := netip.ParseAddr(binding.PodIP); err != nil {
		return runtimeBindingForDelivery{}, false, runtimeDeliveryPrepareError{kind: "runtime_binding_invalid", message: "runtime binding pod ip is invalid", retryable: true}
	}
	return binding, true, nil
}

func requireRuntimePreparationReadyTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, freshnessWindow time.Duration, resourceCredentialRefreshMargin time.Duration, now time.Time) error {
	if err := lockSessionPreparationResetFenceTx(ctx, tx, workspaceID, sessionID); err != nil {
		return err
	}
	readiness, ok, err := loadLatestSessionPreparationReadinessForUpdateTx(ctx, tx, workspaceID, sessionID)
	if err != nil {
		return err
	}
	if !ok {
		return runtimeDeliveryPrepareError{kind: "runtime_preparation_not_ready", message: "runtime preparation is unavailable", retryable: true}
	}
	if readiness.Status == "failed" {
		return runtimePreparationTerminalFailureError{readiness: readiness}
	}
	if readiness.Status != "ready" {
		return runtimeDeliveryPrepareError{kind: "runtime_preparation_not_ready", message: "runtime preparation is not ready", retryable: true}
	}
	if readiness.SandboxStatus == "failed" {
		return runtimePreparationTerminalFailureError{readiness: readiness}
	}
	if readiness.SandboxStatus != "active" {
		if sandboxStatusNeedsPreparationReset(readiness.SandboxStatus) {
			if err := resetSessionPreparationAndEnqueuePrepareTx(ctx, tx, workspaceID, sessionID, readiness, now, false); err != nil {
				return err
			}
			return runtimePreparationRequeuedError{err: runtimeDeliveryPrepareError{kind: "runtime_preparation_not_ready", message: "runtime sandbox is not active; preparation requeued", retryable: true}}
		}
		return runtimeDeliveryPrepareError{kind: "runtime_preparation_not_ready", message: "runtime sandbox is not active", retryable: true}
	}
	if resourceCredentialNeedsLiveRotation(readiness.ResourceCredentialExpiresAt, now, resourceCredentialRefreshMargin) {
		if err := resetSessionPreparationAndEnqueuePrepareTx(ctx, tx, workspaceID, sessionID, readiness, now, false); err != nil {
			return err
		}
		return runtimePreparationRequeuedError{err: runtimeDeliveryPrepareError{kind: "runtime_preparation_not_ready", message: "runtime resource materialization credential is expiring; preparation requeued", retryable: true}}
	}
	if !sandboxRefreshIsFresh(readiness.StatusRefreshedAt, now, freshnessWindow) {
		if err := resetSessionPreparationAndEnqueuePrepareTx(ctx, tx, workspaceID, sessionID, readiness, now, true); err != nil {
			return err
		}
		return runtimePreparationRequeuedError{err: runtimeDeliveryPrepareError{kind: "runtime_preparation_not_ready", message: "runtime sandbox readiness is stale; preparation requeued", retryable: true}}
	}
	return nil
}

type runtimePreparationRequeuedError struct {
	err runtimeDeliveryPrepareError
}

func (e runtimePreparationRequeuedError) Error() string {
	return e.err.Error()
}

type runtimePreparationTerminalFailureError struct {
	readiness sessionPreparationReadiness
}

func (e runtimePreparationTerminalFailureError) Error() string {
	return "runtime preparation failed: " + terminalPreparationFailureReason(e.readiness)
}

func settleTerminalRuntimePreparationFailureTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, readiness sessionPreparationReadiness, now time.Time) (bool, error) {
	taskNotification := job.InputKind == "task_notification"
	if len(job.EventIDs) == 0 && !taskNotification {
		return false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "terminal preparation settlement requires event ids", retryable: false}
	}
	threadID := job.SessionThreadID
	if !taskNotification {
		if err := lockRuntimeInputEventsForSettlementTx(ctx, tx, job); err != nil {
			return false, err
		}
		stale, err := allRuntimeInputEventsProcessedTx(ctx, tx, job)
		if err != nil {
			return false, err
		}
		if stale {
			return false, nil
		}
		threadID, err = runtimeInputSettlementThreadIDTx(ctx, tx, job)
		if err != nil {
			return false, err
		}
	}
	if threadID == "" {
		return false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "terminal preparation settlement requires a thread", retryable: false}
	}
	scope := &bridgev1.RuntimeScope{
		WorkspaceId:     job.WorkspaceID,
		SessionId:       job.SessionID,
		SessionThreadId: threadID,
	}
	if !taskNotification {
		if err := markRuntimeInputEventsProcessedByIDTx(ctx, tx, job.WorkspaceID, job.SessionID, job.EventIDs, now); err != nil {
			return false, err
		}
	}
	errorEventID, err := insertTerminalPreparationFailureEventTx(ctx, tx, scope, job, readiness, now)
	if err != nil {
		return false, err
	}
	if !taskNotification {
		if err := settleTerminalPreparationToolConfirmationsTx(ctx, tx, job.WorkspaceID, job.SessionID, job.EventIDs, errorEventID, now); err != nil {
			return false, err
		}
	}
	if taskNotification {
		_, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'committed',
			        committed_at = COALESCE(committed_at, $4),
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND status IN ('delivering', 'accepted', 'committed')`,
			job.WorkspaceID,
			job.SessionID,
			job.RuntimeInputID,
			now,
		)
		if err != nil {
			return false, err
		}
	}
	if errorEventID == "" {
		return false, runtimeDeliveryPrepareError{kind: "runtime_preparation_settlement_missing", message: "terminal preparation settlement wrote no error event", retryable: true}
	}
	return true, nil
}

func lockRuntimeInputEventsForSettlementTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) error {
	placeholders := make([]string, 0, len(job.EventIDs))
	args := []any{job.WorkspaceID, job.SessionID}
	for index, eventID := range job.EventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+3))
		args = append(args, eventID)
	}
	rows, err := tx.Query(ctx,
		`SELECT event_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id IN (`+strings.Join(placeholders, ", ")+`)
		  ORDER BY event_id
		  FOR UPDATE`,
		args...,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != len(job.EventIDs) {
		return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input events are not settleable", retryable: false}
	}
	return nil
}

func runtimeInputSettlementThreadIDTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (string, error) {
	if len(job.EventIDs) == 0 {
		if job.SessionThreadID != "" {
			return job.SessionThreadID, nil
		}
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input settlement thread is unavailable", retryable: false}
	}
	placeholders := make([]string, 0, len(job.EventIDs))
	args := []any{job.WorkspaceID, job.SessionID}
	for index, eventID := range job.EventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+3))
		args = append(args, eventID)
	}
	row := tx.QueryRow(ctx,
		`SELECT count(*), count(DISTINCT COALESCE(session_thread_id, '')), COALESCE(MIN(COALESCE(session_thread_id, '')), '')
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	var total int
	var distinctThreads int
	var eventThreadID string
	if err := row.Scan(&total, &distinctThreads, &eventThreadID); err != nil {
		return "", err
	}
	if total != len(job.EventIDs) || distinctThreads != 1 || eventThreadID == "" {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input events are not settleable", retryable: false}
	}
	if job.SessionThreadID != "" && job.SessionThreadID != eventThreadID {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input thread does not match source events", retryable: false}
	}
	return eventThreadID, nil
}

func markRuntimeInputEventsProcessedByIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, eventIDs []string, now time.Time) error {
	for _, eventID := range eventIDs {
		var revision int64
		var visibility string
		var sessionVisible bool
		var threadID string
		err := tx.QueryRow(ctx,
			`UPDATE session_events
			    SET processed_at = $4,
			        updated_at = $4,
			        revision = revision + 1
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND event_id = $3
			    AND processed_at IS NULL
			  RETURNING revision, visibility, session_visible, COALESCE(session_thread_id, '')`,
			workspaceID,
			sessionID,
			eventID,
			now,
		).Scan(&revision, &visibility, &sessionVisible, &threadID)
		if dbconnect.IsNoRows(err) {
			alreadyProcessed, exists, err := sessionEventProcessedStateByIDTx(ctx, tx, workspaceID, sessionID, eventID)
			if err != nil {
				return err
			}
			if !exists {
				return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input event is not settleable", retryable: false}
			}
			if alreadyProcessed {
				continue
			}
			return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input event is not settleable", retryable: false}
		}
		if err != nil {
			return err
		}
		if threadID == "" {
			return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime input event thread is unavailable", retryable: false}
		}
		scope := &bridgev1.RuntimeScope{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID}
		if _, err := appendSessionEventStreamChangeForRevisionTx(ctx, tx, scope, eventID, revision, visibility, sessionVisible, now); err != nil {
			return err
		}
	}
	return nil
}

func sessionEventProcessedStateByIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, eventID string) (bool, bool, error) {
	var processedAt sql.NullTime
	err := tx.QueryRow(ctx,
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		workspaceID,
		sessionID,
		eventID,
	).Scan(&processedAt)
	if dbconnect.IsNoRows(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return processedAt.Valid, true, nil
}

func insertTerminalPreparationFailureEventTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, job RuntimeJob, readiness sessionPreparationReadiness, now time.Time) (string, error) {
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	visibility, sessionVisible := threadScope.publicProjection("session.error")
	message := terminalPreparationFailureMessage(readiness)
	payloadJSON, err := terminalPreparationFailurePayloadJSON(message)
	if err != nil {
		return "", err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'session.error', $6, $7, $8, $6, $9, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		payloadJSON,
		visibility,
		sessionVisible,
		now,
	); err != nil {
		return "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return "", err
	}
	messageJSON, err := terminalPreparationFailureMessageJSON(scope, message)
	if err != nil {
		return "", err
	}
	if err := insertSessionMessageProjectionTx(ctx, tx, scope, eventID, "assistant", messageJSON, now); err != nil {
		return "", err
	}
	if job.RuntimeInputID != "" {
		_, err = tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'committed',
			        committed_at = COALESCE(committed_at, $4),
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND runtime_input_id = $3
			    AND status IN ('delivering', 'accepted', 'committed')`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			job.RuntimeInputID,
			now,
		)
		if err != nil {
			return "", err
		}
	}
	return eventID, nil
}

func settleTerminalPreparationToolConfirmationsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, eventIDs []string, resultEventID string, now time.Time) error {
	if len(eventIDs) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(eventIDs))
	args := []any{workspaceID, sessionID}
	for index, eventID := range eventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+3))
		args = append(args, eventID)
	}
	rows, err := tx.Query(ctx,
		`SELECT COALESCE(session_thread_id, ''), payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND type = 'user.tool_confirmation'
		    AND event_id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	type confirmation struct {
		threadID  string
		payload   string
		toolUseID string
	}
	confirmations := make([]confirmation, 0)
	for rows.Next() {
		var item confirmation
		if err := rows.Scan(&item.threadID, &item.payload); err != nil {
			return err
		}
		var payload struct {
			ToolUseID string `json:"tool_use_id"`
		}
		if err := json.Unmarshal([]byte(item.payload), &payload); err != nil || payload.ToolUseID == "" || item.threadID == "" {
			return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "tool confirmation event is not settleable", retryable: false}
		}
		item.toolUseID = payload.ToolUseID
		confirmations = append(confirmations, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range confirmations {
		result, err := tx.Exec(ctx,
			`UPDATE session_pending_tool_uses
			    SET status = 'cancelled',
			        result_event_id = COALESCE(result_event_id, $5),
			        resolved_at = COALESCE(resolved_at, $6),
			        updated_at = $6
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND tool_use_event_id = $4
			    AND status IN ('pending', 'resolving')`,
			workspaceID,
			sessionID,
			item.threadID,
			item.toolUseID,
			resultEventID,
			now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "tool confirmation pending row is not settleable", retryable: false}
		}
	}
	return nil
}

func terminalPreparationFailurePayloadJSON(message string) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"type": "session.error",
		"error": map[string]any{
			"type":    "unknown_error",
			"message": message,
			"retry_status": map[string]any{
				"type": "exhausted",
			},
		},
	})
}

func terminalPreparationFailureMessageJSON(scope *bridgev1.RuntimeScope, message string) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"role": "assistant",
		"content": []map[string]any{
			{
				"type": "text",
				"text": message,
			},
		},
		"session_id":        scope.GetSessionId(),
		"session_thread_id": scope.GetSessionThreadId(),
	})
}

func terminalPreparationFailureMessage(readiness sessionPreparationReadiness) string {
	reason := terminalPreparationFailureReason(readiness)
	resourcePrefix := terminalPreparationFailureResourcePrefix(readiness)
	switch reason {
	case "github_credential_required":
		return resourcePrefix + "could not be authenticated. Rotate that resource's authorization token, then send a new input to retry preparation."
	case "github_repository_unavailable":
		return resourcePrefix + "may have the wrong URL or have been deleted. Repository URLs are fixed for the session lifetime, so create a new session to correct it; if this token lacks access, rotate that resource's authorization token, then send a new input."
	default:
		return "Session preparation failed before this input could be delivered. Fix the preparation failure, then send a new input to retry."
	}
}

func terminalPreparationFailureResourcePrefix(readiness sessionPreparationReadiness) string {
	if readiness.FailureResourceID.Valid && readiness.FailureResourceURL.Valid {
		resourceID := strings.TrimSpace(readiness.FailureResourceID.String)
		resourceURL := strings.TrimSpace(readiness.FailureResourceURL.String)
		if resourceID != "" && resourceURL != "" {
			return "GitHub repository " + resourceURL + " (resource " + resourceID + ") "
		}
	}
	return "A GitHub repository "
}

func terminalPreparationFailureReason(readiness sessionPreparationReadiness) string {
	if reason := strings.TrimSpace(readiness.FailureReason.String); readiness.FailureReason.Valid && reason != "" {
		return reason
	}
	if kind := strings.TrimSpace(readiness.LastErrorKind.String); readiness.LastErrorKind.Valid && kind != "" {
		return kind
	}
	return "session_preparation_failed"
}

func readRuntimeCommandSessionThreadIDTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (string, error) {
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1 AND session_id = $2
		  ORDER BY created_at ASC, id ASC
		  LIMIT 1`,
		workspaceID,
		sessionID,
	)
	var sessionThreadID string
	if err := row.Scan(&sessionThreadID); dbconnect.IsNoRows(err) {
		return "", runtimeDeliveryPrepareError{kind: "runtime_thread_unavailable", message: "runtime command session thread is unavailable", retryable: true}
	} else if err != nil {
		return "", err
	}
	return sessionThreadID, nil
}

func allRuntimeInputEventsProcessedTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	if len(job.EventIDs) == 0 {
		return false, nil
	}
	placeholders := make([]string, 0, len(job.EventIDs))
	args := []any{job.WorkspaceID, job.SessionID}
	for index, eventID := range job.EventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+3))
		args = append(args, eventID)
	}
	row := tx.QueryRow(ctx,
		`SELECT count(*), count(processed_at)
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	var total int
	var processed int
	if err := row.Scan(&total, &processed); err != nil {
		return false, err
	}
	return total == len(job.EventIDs) && processed == len(job.EventIDs), nil
}

type runtimeInboxRepairCandidate struct {
	SessionID            string
	SessionThreadID      string
	RuntimeInputID       string
	InputKind            string
	EventIDsJSON         string
	SequenceFrom         sql.NullInt64
	SequenceTo           sql.NullInt64
	PreparationAttemptID sql.NullString
}

type taskNotificationInboxRepairCandidate struct {
	SessionID       string
	SessionThreadID string
	RuntimeInputID  string
	TaskID          string
}

func requeueEarlierPendingRuntimeInboxTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (bool, error) {
	if job.Kind != queue.KindRuntimeInput || job.InputKind == "task_notification" || job.SequenceFrom <= 0 {
		return false, nil
	}
	candidate, ok, err := earlierPendingRuntimeInboxTx(ctx, tx, job)
	if err != nil || !ok {
		return false, err
	}
	if candidate.RuntimeInputID == job.RuntimeInputID {
		return false, nil
	}
	settled, err := settleSupersededRuntimeInboxRepairCandidateTx(ctx, tx, job.WorkspaceID, candidate, now)
	if err != nil {
		return false, err
	}
	if settled {
		return true, nil
	}
	if candidate.InputKind == "agent_mail" {
		if !candidate.PreparationAttemptID.Valid || candidate.PreparationAttemptID.String == "" {
			return false, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "agent mail inbox repair has no birth preparation attempt", retryable: false}
		}
		deliveryID := strings.TrimPrefix(candidate.RuntimeInputID, "agent_mail:")
		if deliveryID == "" || deliveryID == candidate.RuntimeInputID {
			return false, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "agent mail inbox repair has an invalid runtime input id", retryable: false}
		}
		if _, err := enqueueAgentMailWakeTx(
			ctx,
			tx,
			job.WorkspaceID,
			candidate.SessionID,
			candidate.SessionThreadID,
			deliveryID,
			candidate.PreparationAttemptID.String,
			now,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	payloadJSON, err := runtimeInputRepairPayloadJSON(job.WorkspaceID, job.SessionID, candidate)
	if err != nil {
		return false, err
	}
	ws := workspace.ID(job.WorkspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, job.SessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, job.SessionID, candidate.RuntimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    payloadJSON,
		Priority:       runtimeInputRepairPriority(candidate.InputKind),
		Now:            now,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) RepairRuntimeInbox(ctx context.Context, workspaceID string, limit int) (int, error) {
	if s == nil || s.Client == nil {
		return 0, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if workspaceID == "" {
		return 0, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime inbox repair workspace is required", retryable: false}
	}
	if limit <= 0 {
		limit = defaultRuntimeInboxRepairBatch
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	repaired := 0
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.repair_runtime_inbox", func(tx *dbconnect.Tx) error {
		taskCandidates, err := pendingTaskNotificationInboxRepairCandidatesTx(ctx, tx, workspaceID, limit)
		if err != nil {
			return err
		}
		ws := workspace.ID(workspaceID)
		enqueueRequests := make([]queue.EnqueueRequest, 0, len(taskCandidates))
		for _, candidate := range taskCandidates {
			if _, err := tx.Exec(ctx, `UPDATE session_runtime_inbox
				SET status='queued', binding_id=NULL, binding_generation=NULL, target_pod_uid=NULL, updated_at=$3
				WHERE workspace_id=$1 AND runtime_input_id=$2
				  AND status IN ('queued','delivering','accepted','parked','dead_lettered')`,
				workspaceID, candidate.RuntimeInputID, now); err != nil {
				return err
			}
			request, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(
				ws, candidate.SessionID, candidate.SessionThreadID, candidate.TaskID, now,
			)
			if err != nil {
				return err
			}
			enqueueRequests = append(enqueueRequests, request)
		}
		remaining := limit - len(taskCandidates)
		if remaining == 0 {
			if _, err := queue.EnqueueBatchTx(ctx, tx, enqueueRequests); err != nil {
				return err
			}
			repaired += len(enqueueRequests)
			return nil
		}
		candidates, err := pendingRuntimeInboxRepairCandidatesTx(ctx, tx, workspaceID, remaining)
		if err != nil {
			return err
		}
		type agentMailEnqueueValidation struct {
			requestIndex         int
			jobID                string
			sessionID            string
			sessionThreadID      string
			deliveryID           string
			preparationAttemptID string
		}
		agentMailValidations := make([]agentMailEnqueueValidation, 0)
		ordinaryEnqueueCount := 0
		for _, candidate := range candidates {
			settled, err := settleSupersededRuntimeInboxRepairCandidateTx(ctx, tx, workspaceID, candidate, now)
			if err != nil {
				return err
			}
			if settled {
				repaired++
				continue
			}
			if candidate.InputKind == "agent_mail" {
				if !candidate.PreparationAttemptID.Valid || candidate.PreparationAttemptID.String == "" {
					return runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "agent mail inbox repair has no birth preparation attempt", retryable: false}
				}
				deliveryID := strings.TrimPrefix(candidate.RuntimeInputID, "agent_mail:")
				if deliveryID == "" || deliveryID == candidate.RuntimeInputID {
					return runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "agent mail inbox repair has an invalid runtime input id", retryable: false}
				}
				request, jobID, err := agentMailWakeEnqueueRequest(
					workspaceID,
					candidate.SessionID,
					candidate.SessionThreadID,
					deliveryID,
					candidate.PreparationAttemptID.String,
					now,
				)
				if err != nil {
					return err
				}
				agentMailValidations = append(agentMailValidations, agentMailEnqueueValidation{
					requestIndex:         len(enqueueRequests),
					jobID:                jobID,
					sessionID:            candidate.SessionID,
					sessionThreadID:      candidate.SessionThreadID,
					deliveryID:           deliveryID,
					preparationAttemptID: candidate.PreparationAttemptID.String,
				})
				enqueueRequests = append(enqueueRequests, request)
				continue
			}
			payloadJSON, err := runtimeInputRepairPayloadJSON(workspaceID, candidate.SessionID, candidate)
			if err != nil {
				return err
			}
			enqueueRequests = append(enqueueRequests, queue.EnqueueRequest{
				WorkspaceID:    ws,
				Kind:           queue.KindRuntimeInput,
				PartitionKey:   queue.FormatSessionPartitionKey(ws, candidate.SessionID),
				DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, candidate.SessionID, candidate.RuntimeInputID),
				PayloadVersion: 1,
				PayloadJSON:    payloadJSON,
				Priority:       runtimeInputRepairPriority(candidate.InputKind),
				Now:            now,
			})
			ordinaryEnqueueCount++
		}
		enqueued, err := queue.EnqueueBatchTx(ctx, tx, enqueueRequests)
		if err != nil {
			return err
		}
		repaired += len(taskCandidates) + ordinaryEnqueueCount
		for _, validation := range agentMailValidations {
			inserted, err := validateAgentMailWakeJob(
				enqueued[validation.requestIndex],
				validation.jobID,
				workspaceID,
				validation.sessionID,
				validation.sessionThreadID,
				validation.deliveryID,
				validation.preparationAttemptID,
			)
			if err != nil {
				return err
			}
			if inserted {
				repaired++
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return repaired, nil
}

func settleSupersededRuntimeInboxRepairCandidateTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, candidate runtimeInboxRepairCandidate, now time.Time) (bool, error) {
	fence, err := runtimeInboxRepairFenceStatusTx(ctx, tx, workspaceID, candidate)
	if err != nil {
		return false, err
	}
	nextStatus := ""
	switch fence {
	case messageInterruptFenceAllSuperseded:
		nextStatus = "cancelled"
	case messageInterruptFenceMixed:
		nextStatus = "dead_lettered"
	default:
		return false, nil
	}
	_, err = tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = $3,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND runtime_input_id = $2
		    AND status IN ('delivering', 'accepted')`,
		workspaceID,
		candidate.RuntimeInputID,
		nextStatus,
		now,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

func runtimeInboxRepairFenceStatusTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, candidate runtimeInboxRepairCandidate) (messageInterruptFenceStatus, error) {
	if candidate.InputKind != "messages" {
		return messageInterruptFenceNone, nil
	}
	eventIDs, err := runtimeInboxRepairCandidateEventIDs(candidate)
	if err != nil {
		return messageInterruptFenceNone, err
	}
	if len(eventIDs) == 0 {
		return messageInterruptFenceNone, nil
	}
	placeholders := make([]string, 0, len(eventIDs))
	args := []any{workspaceID, candidate.SessionID, candidate.SessionThreadID}
	for index, eventID := range eventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+4))
		args = append(args, eventID)
	}
	row := tx.QueryRow(ctx,
		`WITH fence AS (
		    SELECT COALESCE(MAX(sequence), 0) AS sequence
		      FROM session_events
		     WHERE workspace_id = $1
		       AND session_id = $2
		       AND session_thread_id = $3
		       AND type = 'user.interrupt'
		       AND processed_at IS NOT NULL
		)
		SELECT count(*),
		       count(*) FILTER (
		         WHERE e.type = 'user.message'
		           AND e.processed_at IS NULL
		           AND e.sequence < fence.sequence
		       ),
		       count(*) FILTER (
		         WHERE e.type = 'user.message'
		           AND e.processed_at IS NULL
		           AND e.sequence >= fence.sequence
		       ),
		       fence.sequence
		  FROM session_events e
		  CROSS JOIN fence
		 WHERE e.workspace_id = $1
		   AND e.session_id = $2
		   AND e.session_thread_id = $3
		   AND e.event_id IN (`+strings.Join(placeholders, ", ")+`)
		 GROUP BY fence.sequence`,
		args...,
	)
	var total int
	var superseded int
	var deliverable int
	var fenceSequence int64
	if err := row.Scan(&total, &superseded, &deliverable, &fenceSequence); err != nil {
		if dbconnect.IsNoRows(err) {
			return messageInterruptFenceNone, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair candidate references missing events", retryable: false}
		}
		return messageInterruptFenceNone, err
	}
	if total != len(eventIDs) {
		return messageInterruptFenceNone, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair candidate references missing events", retryable: false}
	}
	if fenceSequence <= 0 || superseded == 0 {
		return messageInterruptFenceNone, nil
	}
	if deliverable > 0 {
		return messageInterruptFenceMixed, nil
	}
	return messageInterruptFenceAllSuperseded, nil
}

func runtimeInboxRepairCandidateEventIDs(candidate runtimeInboxRepairCandidate) ([]string, error) {
	var eventIDs []string
	if err := json.Unmarshal([]byte(candidate.EventIDsJSON), &eventIDs); err != nil {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair candidate has invalid event ids", retryable: false}
	}
	return eventIDs, nil
}

func pendingTaskNotificationInboxRepairCandidatesTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	limit int,
) ([]taskNotificationInboxRepairCandidate, error) {
	rows, err := tx.Query(ctx,
		`SELECT inbox.session_id,
		        inbox.session_thread_id,
		        inbox.runtime_input_id,
		        task.task_id
		   FROM session_runtime_inbox inbox
		   JOIN session_background_tasks task
		     ON task.workspace_id = inbox.workspace_id
		    AND task.session_id = inbox.session_id
		    AND task.session_thread_id = inbox.session_thread_id
		    AND inbox.runtime_input_id = 'task_notification:' || task.task_id
		    AND task.terminal_event_id IS NULL
		  WHERE inbox.workspace_id = $1
		    AND inbox.input_kind = 'task_notification'
		    AND inbox.status IN ('queued', 'delivering', 'accepted', 'parked', 'dead_lettered')
		    AND NOT EXISTS (
		        SELECT 1 FROM queue_jobs q
		         WHERE q.workspace_id=inbox.workspace_id
		           AND q.kind='runtime_input'
		           AND q.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		           AND q.status IN ('pending','leased')
		    )
		  ORDER BY inbox.updated_at ASC, inbox.created_at ASC, inbox.runtime_input_id ASC
		  LIMIT $2
		  FOR UPDATE OF inbox SKIP LOCKED`,
		workspaceID,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	candidates := make([]taskNotificationInboxRepairCandidate, 0)
	for rows.Next() {
		var candidate taskNotificationInboxRepairCandidate
		if err := rows.Scan(
			&candidate.SessionID,
			&candidate.SessionThreadID,
			&candidate.RuntimeInputID,
			&candidate.TaskID,
		); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func pendingRuntimeInboxRepairCandidatesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, limit int) ([]runtimeInboxRepairCandidate, error) {
	rows, err := tx.Query(ctx,
		`SELECT inbox.session_id, inbox.session_thread_id, inbox.runtime_input_id, inbox.input_kind,
		        inbox.event_ids_json, inbox.sequence_from, inbox.sequence_to,
		        (
		            SELECT CASE
		                WHEN count(*) > 0
		                 AND count(*) = count(se.preparation_attempt_id)
		                 AND count(DISTINCT se.preparation_attempt_id) = 1
		                THEN min(se.preparation_attempt_id)
		            END
		              FROM jsonb_array_elements_text(inbox.event_ids_json::jsonb) event_id(value)
		              JOIN session_events se
		                ON se.workspace_id = inbox.workspace_id
		               AND se.session_id = inbox.session_id
		               AND se.session_thread_id = inbox.session_thread_id
		               AND se.event_id = event_id.value
		        ) AS preparation_attempt_id
		   FROM session_runtime_inbox inbox
			  WHERE inbox.workspace_id = $1
			    AND inbox.status IN ('delivering', 'accepted')
			    AND inbox.input_kind <> 'task_notification'
			    AND (
			        inbox.input_kind <> 'agent_mail'
			        OR EXISTS (
			            SELECT 1
			              FROM sessions session_row
			              JOIN session_threads target
			                ON target.workspace_id = session_row.workspace_id
			               AND target.session_id = session_row.id
			               AND target.id = inbox.session_thread_id
			               AND target.status NOT IN ('closed_for_runtime', 'terminated')
			             WHERE session_row.workspace_id = inbox.workspace_id
			               AND session_row.id = inbox.session_id
			               AND session_row.status <> 'terminated'
			               AND session_row.lifecycle_state <> 'deleted'
			        )
			    )
			    AND inbox.sequence_from IS NOT NULL
		    AND EXISTS (
		        SELECT 1
		          FROM jsonb_array_elements_text(inbox.event_ids_json::jsonb) event_id(value)
		          JOIN session_events se
		            ON se.workspace_id = inbox.workspace_id
		           AND se.session_id = inbox.session_id
		           AND se.session_thread_id = inbox.session_thread_id
		           AND se.event_id = event_id.value
		         WHERE se.processed_at IS NULL
		    )
		  ORDER BY inbox.updated_at ASC, inbox.sequence_from ASC, inbox.created_at ASC, inbox.runtime_input_id ASC
		  LIMIT $2
		  FOR UPDATE OF inbox SKIP LOCKED`,
		workspaceID,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	candidates := make([]runtimeInboxRepairCandidate, 0)
	for rows.Next() {
		var candidate runtimeInboxRepairCandidate
		if err := rows.Scan(
			&candidate.SessionID,
			&candidate.SessionThreadID,
			&candidate.RuntimeInputID,
			&candidate.InputKind,
			&candidate.EventIDsJSON,
			&candidate.SequenceFrom,
			&candidate.SequenceTo,
			&candidate.PreparationAttemptID,
		); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func earlierPendingRuntimeInboxTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (runtimeInboxRepairCandidate, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT inbox.session_id, inbox.session_thread_id, inbox.runtime_input_id, inbox.input_kind,
		        inbox.event_ids_json, inbox.sequence_from, inbox.sequence_to,
		        (
		            SELECT CASE
		                WHEN count(*) > 0
		                 AND count(*) = count(se.preparation_attempt_id)
		                 AND count(DISTINCT se.preparation_attempt_id) = 1
		                THEN min(se.preparation_attempt_id)
		            END
		              FROM jsonb_array_elements_text(inbox.event_ids_json::jsonb) event_id(value)
		              JOIN session_events se
		                ON se.workspace_id = inbox.workspace_id
		               AND se.session_id = inbox.session_id
		               AND se.session_thread_id = inbox.session_thread_id
		               AND se.event_id = event_id.value
		        ) AS preparation_attempt_id
		   FROM session_runtime_inbox inbox
		  WHERE inbox.workspace_id = $1
		    AND inbox.session_id = $2
		    AND inbox.session_thread_id = $3
		    AND inbox.runtime_input_id <> $4
		    AND inbox.status IN ('delivering', 'accepted')
		    AND inbox.input_kind <> 'task_notification'
		    AND inbox.sequence_from IS NOT NULL
		    AND inbox.sequence_from < $5
		    AND EXISTS (
		        SELECT 1
		          FROM jsonb_array_elements_text(inbox.event_ids_json::jsonb) event_id(value)
		          JOIN session_events se
		            ON se.workspace_id = inbox.workspace_id
		           AND se.session_id = inbox.session_id
		           AND se.session_thread_id = inbox.session_thread_id
		           AND se.event_id = event_id.value
		         WHERE se.processed_at IS NULL
		    )
		  ORDER BY inbox.sequence_from ASC, inbox.created_at ASC, inbox.runtime_input_id ASC
		  LIMIT 1
		  FOR UPDATE OF inbox`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		job.RuntimeInputID,
		job.SequenceFrom,
	)
	var candidate runtimeInboxRepairCandidate
	if err := row.Scan(
		&candidate.SessionID,
		&candidate.SessionThreadID,
		&candidate.RuntimeInputID,
		&candidate.InputKind,
		&candidate.EventIDsJSON,
		&candidate.SequenceFrom,
		&candidate.SequenceTo,
		&candidate.PreparationAttemptID,
	); dbconnect.IsNoRows(err) {
		return runtimeInboxRepairCandidate{}, false, nil
	} else if err != nil {
		return runtimeInboxRepairCandidate{}, false, err
	}
	return candidate, true, nil
}

type runtimeInputRepairPayload struct {
	WorkspaceID          string   `json:"workspace_id"`
	SessionID            string   `json:"session_id"`
	SessionThreadID      string   `json:"session_thread_id"`
	RuntimeInputID       string   `json:"runtime_input_id"`
	EventIDs             []string `json:"event_ids"`
	SequenceFrom         int64    `json:"sequence_from"`
	SequenceTo           int64    `json:"sequence_to"`
	InputKind            string   `json:"input_kind"`
	PreparationAttemptID string   `json:"preparation_attempt_id"`
}

func runtimeInputRepairPayloadJSON(workspaceID string, sessionID string, candidate runtimeInboxRepairCandidate) ([]byte, error) {
	if !candidate.PreparationAttemptID.Valid || candidate.PreparationAttemptID.String == "" {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair has no birth preparation attempt", retryable: false}
	}
	if !candidate.SequenceFrom.Valid || !candidate.SequenceTo.Valid || candidate.SequenceFrom.Int64 <= 0 || candidate.SequenceTo.Int64 < candidate.SequenceFrom.Int64 {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair candidate has invalid sequence range", retryable: false}
	}
	var eventIDs []string
	if err := json.Unmarshal([]byte(candidate.EventIDsJSON), &eventIDs); err != nil {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair candidate has invalid event ids", retryable: false}
	}
	if len(eventIDs) == 0 {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_repair_invalid", message: "runtime inbox repair candidate has no events", retryable: false}
	}
	return json.Marshal(runtimeInputRepairPayload{
		WorkspaceID:          workspaceID,
		SessionID:            sessionID,
		SessionThreadID:      candidate.SessionThreadID,
		RuntimeInputID:       candidate.RuntimeInputID,
		EventIDs:             eventIDs,
		SequenceFrom:         candidate.SequenceFrom.Int64,
		SequenceTo:           candidate.SequenceTo.Int64,
		InputKind:            candidate.InputKind,
		PreparationAttemptID: candidate.PreparationAttemptID.String,
	})
}

func runtimeInputRepairPriority(inputKind string) int {
	if inputKind == "interrupt_control" {
		return 100
	}
	return 0
}

type messageInterruptFenceStatus string

const (
	messageInterruptFenceNone          messageInterruptFenceStatus = ""
	messageInterruptFenceAllSuperseded messageInterruptFenceStatus = "all_superseded"
	messageInterruptFenceMixed         messageInterruptFenceStatus = "mixed"
)

func messageInterruptFenceStatusTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (messageInterruptFenceStatus, error) {
	if job.InputKind != "messages" || len(job.EventIDs) == 0 {
		return messageInterruptFenceNone, nil
	}
	fenceSequence, ok, err := latestProcessedInterruptFenceSequenceTx(ctx, tx, job)
	if err != nil || !ok {
		return messageInterruptFenceNone, err
	}
	placeholders := make([]string, 0, len(job.EventIDs))
	args := []any{job.WorkspaceID, job.SessionID, job.SessionThreadID, fenceSequence}
	for index, eventID := range job.EventIDs {
		placeholders = append(placeholders, "$"+strconv.Itoa(index+5))
		args = append(args, eventID)
	}
	row := tx.QueryRow(ctx,
		`SELECT count(*),
		        count(*) FILTER (
		          WHERE type = 'user.message'
		            AND processed_at IS NULL
		            AND sequence < $4
		        ),
		        count(*) FILTER (
		          WHERE type = 'user.message'
		            AND processed_at IS NULL
		            AND sequence >= $4
		        )
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id IN (`+strings.Join(placeholders, ", ")+`)`,
		args...,
	)
	var total int
	var superseded int
	var deliverable int
	if err := row.Scan(&total, &superseded, &deliverable); err != nil {
		return messageInterruptFenceNone, err
	}
	if total != len(job.EventIDs) {
		return messageInterruptFenceNone, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "message runtime input references missing events", retryable: false}
	}
	if superseded == 0 {
		return messageInterruptFenceNone, nil
	}
	if deliverable > 0 {
		return messageInterruptFenceMixed, nil
	}
	return messageInterruptFenceAllSuperseded, nil
}

func latestProcessedInterruptFenceSequenceTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (int64, bool, error) {
	row := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0)
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'user.interrupt'
		    AND processed_at IS NOT NULL`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
	)
	var sequence int64
	if err := row.Scan(&sequence); err != nil {
		return 0, false, err
	}
	return sequence, sequence > 0, nil
}

// upsertRuntimeInboxDeliveryTx writes the delivering row that anchors the
// Runtime delivery inbox recovery protocol. session_runtime_inbox is a delivery
// recovery record only — LoadContext never projects it into context.
//
//	status         meaning                                writer
//	delivering     row created before the pod command      this upsert
//	                is sent
//	accepted       pod acknowledged the command             MarkRuntimeInputAccepted
//	committed      inputs durably committed                 CommitInputs, the
//	                                                         task_notification commit
//	                                                         (CommitTaskNotificationResult),
//	                                                         and the terminal
//	                                                         preparation-failure seal
//	                                                         (settleTerminalRuntimePreparationFailureTx)
//	cancelled      superseded / retracted                   interrupt fence
//	dead_lettered  invariant/exhaustion terminal            finalization
//
// Protocol invariants: Queue ACK is allowed once the inbox row exists and the
// command is accepted (a pod crash after ACK recovers from this row). Repair
// requeues event-backed rows while their events remain unprocessed and separately
// rebuilds eventless task notifications from their retained birth queue payload.
// Committed is written by CommitInputs, by the task_notification commit
// (CommitTaskNotificationResult), and by the terminal preparation-failure seal,
// which moves a delivering|accepted row to committed after persisting its error
// event — so a preparation-failed input reaches committed without CommitInputs.
func upsertRuntimeInboxDeliveryTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, binding runtimeBindingForDelivery, now time.Time) error {
	eventIDs := job.EventIDs
	if eventIDs == nil {
		eventIDs = []string{}
	}
	events, err := json.Marshal(eventIDs)
	if err != nil {
		return err
	}
	var statusValue string
	err = tx.QueryRow(ctx,
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, rejection_reason_code, event_ids_json,
			sequence_from, sequence_to, status, binding_id, binding_generation, target_pod_uid,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'delivering', $10, $11, $12, $13, $13)
		ON CONFLICT (workspace_id, runtime_input_id) DO UPDATE SET
			status = CASE
				WHEN session_runtime_inbox.status = 'committed' THEN session_runtime_inbox.status
				WHEN session_runtime_inbox.status = 'accepted'
				  AND session_runtime_inbox.binding_id = EXCLUDED.binding_id
				  AND session_runtime_inbox.binding_generation = EXCLUDED.binding_generation
				  AND session_runtime_inbox.target_pod_uid = EXCLUDED.target_pod_uid THEN session_runtime_inbox.status
				ELSE 'delivering'
			END,
			binding_id = EXCLUDED.binding_id,
			binding_generation = EXCLUDED.binding_generation,
			target_pod_uid = EXCLUDED.target_pod_uid,
			updated_at = EXCLUDED.updated_at
		WHERE session_runtime_inbox.session_id = EXCLUDED.session_id
		  AND session_runtime_inbox.session_thread_id = EXCLUDED.session_thread_id
		  AND session_runtime_inbox.input_kind = EXCLUDED.input_kind
		  AND session_runtime_inbox.rejection_reason_code IS NOT DISTINCT FROM EXCLUDED.rejection_reason_code
		  AND session_runtime_inbox.event_ids_json = EXCLUDED.event_ids_json
		  AND session_runtime_inbox.sequence_from IS NOT DISTINCT FROM EXCLUDED.sequence_from
		  AND session_runtime_inbox.sequence_to IS NOT DISTINCT FROM EXCLUDED.sequence_to
		RETURNING status`,
		job.WorkspaceID,
		job.SessionID,
		job.SessionThreadID,
		job.RuntimeInputID,
		job.InputKind,
		sql.NullString{String: job.RejectionReasonCode, Valid: job.RejectionReasonCode != ""},
		string(events),
		sql.NullInt64{Int64: job.SequenceFrom, Valid: job.SequenceFrom > 0},
		sql.NullInt64{Int64: job.SequenceTo, Valid: job.SequenceTo > 0},
		binding.BindingID,
		binding.BindingGeneration,
		binding.PodUID,
		now,
	).Scan(&statusValue)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeDeliveryPrepareError{kind: "runtime_inbox_payload_conflict", message: "runtime input id replay payload conflicts with the durable inbox row", retryable: false}
	}
	return err
}

type RuntimePodCommandClient struct {
	TokenSource internalgrpcauth.TokenSource
	DialOptions []grpc.DialOption
}

func NewRuntimePodCommandClient(tokenSource internalgrpcauth.TokenSource, dialOptions ...grpc.DialOption) *RuntimePodCommandClient {
	return &RuntimePodCommandClient{TokenSource: tokenSource, DialOptions: append([]grpc.DialOption(nil), dialOptions...)}
}

func (c *RuntimePodCommandClient) SendRuntimeCommand(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	if c == nil || c.TokenSource == nil {
		return nil, errors.New("runtime pod command client is required")
	}
	if request == nil {
		return nil, errors.New("runtime pod command request is required")
	}
	if proto.Size(request) > sessionrpc.MaxRuntimeCommandGRPCMessageBytes {
		return nil, &runtimeCommandPayloadTooLargeError{}
	}
	if target.PodIP == "" || target.Port <= 0 {
		return nil, errors.New("runtime pod target is required")
	}
	if _, err := netip.ParseAddr(target.PodIP); err != nil {
		return nil, errors.New("runtime pod target ip is invalid")
	}
	options := append([]grpc.DialOption{}, internalgrpc.RuntimeCommandRPCDialOptions()...)
	if len(c.DialOptions) == 0 {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		options = append(options, c.DialOptions...)
	}
	options = append(options, grpc.WithPerRPCCredentials(internalgrpcauth.NewServiceAccountTokenCredentials(c.TokenSource)))
	conn, err := grpc.NewClient("passthrough:///"+net.JoinHostPort(target.PodIP, strconv.Itoa(target.Port)), options...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	client := agentruntimev1.NewAgentRuntimePodServiceClient(conn)
	switch request.GetCommandKind() {
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES:
		return client.AcceptInput(ctx, request)
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_AGENT_MAIL:
		return client.AcceptAgentMail(ctx, request)
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION:
		return client.AcceptTaskNotification(ctx, request)
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL:
		return client.Interrupt(ctx, request)
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION:
		return client.ResolveToolConfirmation(ctx, request)
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH:
		return client.ApplyRuntimeConfig(ctx, request)
	case agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION:
		return client.CleanupSession(ctx, request)
	default:
		return nil, errors.New("runtime command kind is invalid")
	}
}

type runtimeCommandPayloadTooLargeError struct{}

func (*runtimeCommandPayloadTooLargeError) Error() string {
	return "runtime command exceeds the transport fuse"
}

type runtimeDeliveryPrepareError struct {
	kind      string
	message   string
	retryable bool
}

func (e runtimeDeliveryPrepareError) Error() string {
	if e.message != "" {
		return e.message
	}
	return e.kind
}

func runtimeDeliveryResultFromPrepareError(err error) RuntimeDeliveryResult {
	var prepareErr runtimeDeliveryPrepareError
	if errors.As(err, &prepareErr) {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    prepareErr.retryable,
			ErrorKind:    prepareErr.kind,
			ErrorMessage: prepareErr.Error(),
		}
	}
	return RuntimeDeliveryResult{
		Status:       RuntimeDeliveryRejected,
		Retryable:    true,
		ErrorKind:    "runtime_reconcile_error",
		ErrorMessage: "runtime delivery reconciliation failed",
	}
}
