package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/childcontrol"
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
	MarkRuntimeInputAccepted(context.Context, RuntimeJob, RuntimeAttemptedBinding) (bool, error)
	PrepareRuntimeInputRejection(context.Context, RuntimeJob, RuntimeDeliveryResult) (bool, error)
}

type RuntimeInterruptDeliveryAuthorityStore interface {
	InterruptDeliveryAuthority(context.Context, RuntimeJob) (RuntimeInterruptDeliveryAuthority, error)
}

type RuntimeInterruptDeliveryAuthority struct {
	Active            bool
	QueueLeaseSettled bool
}

type RuntimeCleanupFinalizer interface {
	FinalizeRuntimeCleanup(context.Context, RuntimeJob) (RuntimeDeliveryResult, error)
}

type RuntimeDeliveryFinalizationStore interface {
	FinalizeRuntimeDelivery(context.Context, RuntimeJob, RuntimeDeliveryResult) (RuntimeDeliveryResult, error)
	ReplayRuntimeDeliveryFinalization(context.Context, RuntimeJob) (RuntimeDeliveryResult, bool, error)
}

// MalformedRuntimeInputLease carries only Queue-owned lease identity. Payload
// fields are intentionally absent: durable Queue keys and the canonical Inbox
// relation select any business owner.
type MalformedRuntimeInputLease struct {
	WorkspaceID  string
	JobID        string
	LeaseToken   string
	Kind         string
	PartitionKey string
	DedupeKey    string
}

type MalformedRuntimeInputCustodyResult struct {
	Handled               bool
	QueueLeaseSettled     bool
	Retry                 bool
	InterruptTerminalized bool
	CanonicalReplacement  bool
}

type RuntimeTargetResolver interface {
	ResolveRuntimeTarget(context.Context, *dbconnect.Tx, RuntimeJob) (runtimeBindingForDelivery, error)
}

type RuntimeCleanupTargetProver interface {
	CleanupTargetProvenGone(context.Context, *dbconnect.Tx, RuntimeJob, runtimeBindingForDelivery) (bool, error)
}

type RuntimeCommandSender interface {
	AcceptInput(context.Context, RuntimePodTarget, *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error)
	AcceptAgentMail(context.Context, RuntimePodTarget, *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error)
	AcceptTaskNotification(context.Context, RuntimePodTarget, *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error)
	Interrupt(context.Context, RuntimePodTarget, *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error)
	ResolveToolConfirmation(context.Context, RuntimePodTarget, *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error)
	ApplyRuntimeConfig(context.Context, RuntimePodTarget, *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error)
	CleanupSession(context.Context, RuntimePodTarget, *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error)
}

const (
	initialMCPManifestListTimeout = 180 * time.Second
	// The production closeout proof measures command admission, Tool Fiber
	// cancellation/join, durable-operation drain, Tool Result and Request End
	// persistence, and receipt return together. Thirty seconds is the one
	// caller wait around that complete path; a non-abandonable closeout that
	// finishes later converges through receipt replay or exact-lease terminal
	// arbitration instead of acquiring another timer or Runtime attempt.
	runtimeInterruptDeliveryTimeout = 30 * time.Second
)

type RuntimeCommandPlan struct {
	StaleAccepted         bool
	DeliveryAuthorityLost bool
	SettledAccepted       bool
	QueueLeaseSettled     bool
	CleanupTargetGone     bool
	Target                RuntimePodTarget
	AttemptedBinding      RuntimeAttemptedBinding
	AcceptInput           *agentruntimev1.AcceptInputRequest
	AcceptAgentMail       *agentruntimev1.AcceptAgentMailRequest
	AcceptTask            *agentruntimev1.AcceptTaskNotificationRequest
	Interrupt             *agentruntimev1.InterruptRequest
	ToolConfirmation      *agentruntimev1.ResolveToolConfirmationRequest
	RuntimeConfig         *agentruntimev1.ApplyRuntimeConfigRequest
	CleanupSession        *agentruntimev1.CleanupSessionRequest
	TaskNotification      *RuntimeTaskNotificationPlan
}

type RuntimeAttemptedBinding struct {
	BindingID    string
	Generation   int64
	TargetPodUID string
}

func (p RuntimeCommandPlan) hasCommand() bool {
	count := 0
	for _, present := range []bool{
		p.AcceptInput != nil, p.AcceptAgentMail != nil, p.AcceptTask != nil,
		p.Interrupt != nil, p.ToolConfirmation != nil, p.RuntimeConfig != nil, p.CleanupSession != nil,
	} {
		if present {
			count++
		}
	}
	return count == 1
}

func (p RuntimeCommandPlan) send(ctx context.Context, sender RuntimeCommandSender) (RuntimeDeliveryResult, error) {
	switch {
	case p.AcceptInput != nil:
		response, err := sender.AcceptInput(ctx, p.Target, p.AcceptInput)
		return runtimeResultFromAcceptInput(response), err
	case p.AcceptAgentMail != nil:
		response, err := sender.AcceptAgentMail(ctx, p.Target, p.AcceptAgentMail)
		return runtimeResultFromAcceptAgentMail(response), err
	case p.AcceptTask != nil:
		response, err := sender.AcceptTaskNotification(ctx, p.Target, p.AcceptTask)
		return runtimeResultFromAcceptTask(response), err
	case p.Interrupt != nil:
		interruptCtx, cancel := context.WithTimeout(ctx, runtimeInterruptDeliveryTimeout)
		defer cancel()
		response, err := sender.Interrupt(interruptCtx, p.Target, p.Interrupt)
		return runtimeResultFromInterrupt(response), err
	case p.ToolConfirmation != nil:
		response, err := sender.ResolveToolConfirmation(ctx, p.Target, p.ToolConfirmation)
		return runtimeResultFromToolConfirmation(response), err
	case p.RuntimeConfig != nil:
		response, err := sender.ApplyRuntimeConfig(ctx, p.Target, p.RuntimeConfig)
		return runtimeResultFromRuntimeConfig(response), err
	case p.CleanupSession != nil:
		response, err := sender.CleanupSession(ctx, p.Target, p.CleanupSession)
		return runtimeResultFromCleanup(response), err
	default:
		return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "runtime_command_plan_invalid", ErrorMessage: "runtime command request is missing"}, nil
	}
}

func invalidRuntimeResponse() RuntimeDeliveryResult {
	return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, ErrorKind: "invalid_runtime_response", ErrorMessage: "runtime response is missing or invalid"}
}

func rejectedRuntimeResponse(reason string, retryable bool) RuntimeDeliveryResult {
	return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: retryable, ErrorKind: reason, ErrorMessage: "runtime rejected operation"}
}

func runtimeFailureKind(value string) string {
	value = strings.ToLower(value)
	if index := strings.LastIndex(value, "_failure_"); index >= 0 {
		value = value[index+len("_failure_"):]
	}
	return value
}

func runtimeResultFromAcceptInput(response *agentruntimev1.AcceptInputResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.AcceptInputFailure_ACCEPT_INPUT_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromAcceptAgentMail(response *agentruntimev1.AcceptAgentMailResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.AcceptAgentMailFailure_ACCEPT_AGENT_MAIL_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromAcceptTask(response *agentruntimev1.AcceptTaskNotificationResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.AcceptTaskNotificationFailure_ACCEPT_TASK_NOTIFICATION_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromInterrupt(response *agentruntimev1.InterruptResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromToolConfirmation(response *agentruntimev1.ResolveToolConfirmationResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetAccepted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if response.GetStale() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		if rejected.GetReason() == agentruntimev1.ResolveToolConfirmationFailure_RESOLVE_TOOL_CONFIRMATION_FAILURE_SESSION_INTERRUPT_BARRIER_STALE {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale}
		}
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromRuntimeConfig(response *agentruntimev1.ApplyRuntimeConfigResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetApplied() != nil || response.GetNoResidency() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
}

func runtimeResultFromCleanup(response *agentruntimev1.CleanupSessionResponse) RuntimeDeliveryResult {
	if response == nil {
		return invalidRuntimeResponse()
	}
	if response.GetCompleted() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	}
	if response.GetDuplicate() != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	}
	if rejected := response.GetRejected(); rejected != nil {
		return rejectedRuntimeResponse(runtimeFailureKind(rejected.GetReason().String()), rejected.GetRetryable())
	}
	return invalidRuntimeResponse()
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

func (d RuntimePodDirectDeliverer) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	replacer, ok := d.Store.(interface {
		ReplaceMalformedRuntimeInputCustody(context.Context, RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error)
	})
	if !ok || replacer == nil {
		return queue.ReplaceMalformedRuntimeInputCustodyResult{}, errors.New("runtime delivery store is unavailable")
	}
	return replacer.ReplaceMalformedRuntimeInputCustody(ctx, job)
}

func (d RuntimePodDirectDeliverer) FinalizeMalformedRuntimeInputCustody(ctx context.Context, lease MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error) {
	finalizer, ok := d.Store.(interface {
		FinalizeMalformedRuntimeInputCustody(context.Context, MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error)
	})
	if !ok || finalizer == nil {
		return MalformedRuntimeInputCustodyResult{}, errors.New("runtime delivery store is unavailable")
	}
	return finalizer.FinalizeMalformedRuntimeInputCustody(ctx, lease)
}

func (d RuntimePodDirectDeliverer) RepairLostRuntimeBindings(ctx context.Context, workspaceID string) (int, error) {
	repairer, ok := d.Store.(RuntimePodLossRepairer)
	if !ok || repairer == nil {
		return 0, nil
	}
	return repairer.RepairLostRuntimeBindings(ctx, workspaceID)
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
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted, QueueLeaseSettled: plan.QueueLeaseSettled}, nil
	}
	if plan.StaleAccepted {
		status := RuntimeDeliveryDuplicate
		if plan.DeliveryAuthorityLost {
			status = RuntimeDeliveryAuthorityLost
		}
		return RuntimeDeliveryResult{Status: status, QueueLeaseSettled: plan.QueueLeaseSettled}, nil
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
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" {
		authorizer, ok := d.Store.(RuntimeInterruptDeliveryAuthorityStore)
		if !ok || authorizer == nil {
			return RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    true,
				ErrorKind:    "runtime_interrupt_authority_unavailable",
				ErrorMessage: "runtime interrupt delivery authority is unavailable",
			}, nil
		}
		authority, err := authorizer.InterruptDeliveryAuthority(ctx, job)
		if err != nil {
			return runtimeDeliveryResultFromPrepareError(err), nil
		}
		if !authority.Active {
			status := RuntimeDeliveryDuplicate
			if !authority.QueueLeaseSettled {
				status = RuntimeDeliveryAuthorityLost
			}
			return RuntimeDeliveryResult{Status: status, QueueLeaseSettled: authority.QueueLeaseSettled}, nil
		}
	}
	if d.Sender == nil {
		return runtimeDeliveryResultWithAttemptedBinding(RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_transport_unavailable",
			ErrorMessage: "runtime command sender is unavailable",
		}, plan.AttemptedBinding), nil
	}
	if !plan.hasCommand() {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "runtime_command_plan_invalid",
			ErrorMessage: "runtime command request is missing",
		}, nil
	}
	result, err := plan.send(ctx, d.Sender)
	if err != nil {
		result, deliveryErr := runtimeDeliveryResultFromSendError(err)
		result = runtimeDeliveryResultWithAttemptedBinding(result, plan.AttemptedBinding)
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
	result = runtimeDeliveryResultWithAttemptedBinding(result, plan.AttemptedBinding)
	converted, err := d.prepareRuntimeInputRejection(ctx, job, result)
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if converted {
		return d.DeliverRuntimeJob(ctx, job)
	}
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" &&
		(result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		replayer, ok := d.Store.(RuntimeDeliveryFinalizationReplayer)
		if !ok {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "interrupt_closeout_unavailable", ErrorMessage: "interrupt closeout receipt is unavailable"}, nil
		}
		replayed, found, replayErr := replayer.ReplayRuntimeDeliveryFinalization(ctx, job)
		if replayErr != nil {
			return runtimeDeliveryResultWithAttemptedBinding(runtimeDeliveryResultFromPrepareError(replayErr), plan.AttemptedBinding), nil
		}
		if !found {
			return runtimeDeliveryResultWithAttemptedBinding(RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "interrupt_closeout_pending", ErrorMessage: "interrupt closeout receipt is not committed"}, plan.AttemptedBinding), nil
		}
		return replayed, nil
	}
	if job.Kind == queue.KindRuntimeInput && (result.Status == RuntimeDeliveryAccepted || result.Status == RuntimeDeliveryDuplicate) {
		queueLeaseSettled, err := d.Store.MarkRuntimeInputAccepted(ctx, job, plan.AttemptedBinding)
		if err != nil {
			return runtimeDeliveryResultWithAttemptedBinding(runtimeDeliveryResultFromPrepareError(err), plan.AttemptedBinding), nil
		}
		if queueLeaseSettled {
			result.QueueLeaseSettled = true
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

func runtimeDeliveryResultWithAttemptedBinding(
	result RuntimeDeliveryResult,
	attempt RuntimeAttemptedBinding,
) RuntimeDeliveryResult {
	if attempt.BindingID == "" {
		return result
	}
	result.AttemptedBindingID = attempt.BindingID
	result.AttemptedBindingGeneration = attempt.Generation
	result.AttemptedTargetPodUID = attempt.TargetPodUID
	return result
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
	Client              *dbconnect.Client
	Logger              *slog.Logger
	RuntimeGRPCPort     int
	TargetResolver      RuntimeTargetResolver
	MCPManifestLister   MCPManifestLister
	AttachmentBlobStore blob.BlobStore
	Clock               func() time.Time
}

// NewPostgreSQLRuntimeDeliveryStore provides a dependency-light store for
// focused callers and tests. Production Job Runner assembly must use
// NewJobRunnerRuntimeDeliveryStore so every delivery dependency is installed.
func NewPostgreSQLRuntimeDeliveryStore(client *dbconnect.Client, runtimeGRPCPort int) *PostgreSQLRuntimeDeliveryStore {
	return &PostgreSQLRuntimeDeliveryStore{
		Client:          client,
		RuntimeGRPCPort: runtimeGRPCPort,
		Clock:           func() time.Time { return storage.Now() },
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
	store.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: bindingSnapshot}
	return store
}

const maxRuntimePreparationReentries = 2

func (s *PostgreSQLRuntimeDeliveryStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	return s.prepareRuntimeCommand(ctx, job, 0)
}

// Preparation can legitimately repair one lost binding and capture one initial
// MCP manifest before retrying the same durable job. Bound those state-driven
// re-entries explicitly so a broken repair or capture cannot recurse forever.
func (s *PostgreSQLRuntimeDeliveryStore) prepareRuntimeCommand(ctx context.Context, job RuntimeJob, reentries int) (RuntimeCommandPlan, error) {
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
		if job.Kind == queue.KindRuntimeInput {
			// Runtime input preparation and lifecycle admission share the Session
			// mutation lock before either path locks Inbox custody. This preserves
			// one lock order when a leased notification races child close.
			if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
				return err
			}
			if job.InputKind == "interrupt_control" {
				authority, err := interruptDeliveryAuthorityTx(ctx, tx, job, now)
				if err != nil {
					return err
				}
				if !authority.Active {
					plan = RuntimeCommandPlan{
						StaleAccepted:         true,
						DeliveryAuthorityLost: !authority.QueueLeaseSettled,
						QueueLeaseSettled:     authority.QueueLeaseSettled,
					}
					return nil
				}
			}
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind != "agent_mail" {
			effectiveJob, err := effectiveRuntimeInputJobTx(ctx, tx, job)
			if err != nil {
				return err
			}
			job = effectiveJob
		}
		// A reclaimed lease for an input already accepted by the exact current
		// binding is settlement work, not a new delivery attempt. Check the
		// durable identity before readiness and target-availability gates so
		// transient control-plane observations cannot exhaust accepted custody.
		if job.Kind == queue.KindRuntimeInput {
			settled, err := settleCurrentBindingAcceptedRuntimeInputTx(ctx, tx, job, now)
			if err != nil {
				return err
			}
			if settled {
				plan = RuntimeCommandPlan{SettledAccepted: true, QueueLeaseSettled: true}
				return nil
			}
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
			mailPlan, err := s.prepareAgentMailCommandTx(ctx, tx, job, port, now)
			if err != nil {
				return err
			}
			plan = mailPlan
			return nil
		}
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "task_notification" {
			taskPlan, taskCommandPlan, err := s.prepareTaskNotificationCommandTx(ctx, tx, job, port, now)
			if err != nil {
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
			if err := claimRuntimeInboxDeliveryTx(ctx, tx, inboxJob, binding, now); err != nil {
				return err
			}
		}
		payloadJSON, runtimeInputID, err := runtimeCommandPayloadForJobTx(ctx, tx, job)
		if err != nil {
			return err
		}
		plan, err = runtimeCommandPlanForPayload(job, sessionThreadID, runtimeInputID, payloadJSON, binding, port)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		var initialMCP runtimeInitialMCPManifestRequiredError
		var lostBinding runtimeBindingLostError
		if errors.As(err, &lostBinding) {
			if reentries >= maxRuntimePreparationReentries {
				return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_invariant", message: "runtime preparation did not converge after durable repair", retryable: false}
			}
			if err := s.repairLostRuntimeBinding(ctx, job.WorkspaceID, job.SessionID, lostBinding.binding, now); err != nil {
				return RuntimeCommandPlan{}, err
			}
			return s.prepareRuntimeCommand(ctx, job, reentries+1)
		} else if errors.As(err, &initialMCP) {
			initialMCPManifestToolsets = initialMCP.toolsets
		} else {
			return RuntimeCommandPlan{}, err
		}
	}
	if len(initialMCPManifestToolsets) > 0 {
		if reentries >= maxRuntimePreparationReentries {
			return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_invariant", message: "runtime preparation did not converge after manifest capture", retryable: false}
		}
		if err := s.captureInitialMCPManifests(ctx, job, initialMCPManifestToolsets, now); err != nil {
			return RuntimeCommandPlan{}, err
		}
		return s.prepareRuntimeCommand(ctx, job, reentries+1)
	}
	return plan, nil
}

// InterruptDeliveryAuthority is the final pre-send fence for an
// interrupt command. Preparation performs the same check before any binding or
// Inbox work; this second transaction prevents a lease lost after planning
// from reaching Runtime. Runtime's echoed capability and Bridge closeout
// validation remain the final fence for a command already in transport.
func (s *PostgreSQLRuntimeDeliveryStore) InterruptDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeInterruptDeliveryAuthority, error) {
	if s == nil || s.Client == nil {
		return RuntimeInterruptDeliveryAuthority{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var authority RuntimeInterruptDeliveryAuthority
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.authorize_interrupt_delivery", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		var err error
		authority, err = interruptDeliveryAuthorityTx(ctx, tx, job, now)
		return err
	})
	return authority, err
}

func interruptDeliveryAuthorityTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (RuntimeInterruptDeliveryAuthority, error) {
	if job.Kind != queue.KindRuntimeInput || job.InputKind != "interrupt_control" || job.WorkspaceID == "" || job.SessionID == "" ||
		job.SessionThreadID == "" || job.RuntimeInputID == "" || job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" {
		return RuntimeInterruptDeliveryAuthority{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "interrupt delivery authority is incomplete", retryable: false}
	}
	workspaceID := workspace.ID(job.WorkspaceID)
	if job.PartitionKey != queue.FormatSessionPartitionKey(workspaceID, job.SessionID) ||
		job.DedupeKey != queue.FormatRuntimeInputDedupeKey(workspaceID, job.SessionID, job.RuntimeInputID) {
		return RuntimeInterruptDeliveryAuthority{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "interrupt delivery authority binding is invalid", retryable: false}
	}
	live, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
		WorkspaceID: workspaceID, JobID: job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
		PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	})
	if err != nil {
		return RuntimeInterruptDeliveryAuthority{}, err
	}
	if !live {
		return RuntimeInterruptDeliveryAuthority{}, nil
	}
	barrier, barrierActive, err := activeSessionInterruptBarrierTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return RuntimeInterruptDeliveryAuthority{}, err
	}
	if !barrierActive || barrier.runtimeInputID != job.RuntimeInputID {
		settled, err := queue.CancelLeasedRuntimeInputCustodyTx(ctx, tx, queue.CancelLeasedRuntimeInputRequest{
			Lease: queue.ExactLeaseRequest{
				WorkspaceID: workspaceID, JobID: job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
				PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			},
			SessionID: job.SessionID, RuntimeInputID: job.RuntimeInputID, InputKind: job.InputKind, Now: now,
		})
		if err != nil {
			return RuntimeInterruptDeliveryAuthority{}, err
		}
		return RuntimeInterruptDeliveryAuthority{QueueLeaseSettled: settled}, nil
	}
	return RuntimeInterruptDeliveryAuthority{Active: true}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) MarkRuntimeInputAccepted(ctx context.Context, job RuntimeJob, attempt RuntimeAttemptedBinding) (bool, error) {
	if job.Kind != queue.KindRuntimeInput {
		return false, nil
	}
	if s == nil || s.Client == nil {
		return false, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" ||
		attempt.BindingID == "" || attempt.Generation <= 0 || attempt.TargetPodUID == "" {
		return false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime job identity is incomplete", retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	queueLeaseSettled := false
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.mark_runtime_input_accepted", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		if job.InputKind == "task_notification" {
			closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID)
			if err != nil {
				return err
			}
			if closing {
				settled, err := deferLeasedTaskNotificationTx(ctx, tx, job, now)
				if err != nil {
					return err
				}
				queueLeaseSettled = settled
				return nil
			}
		}
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
			attempt.BindingID,
			attempt.Generation,
			attempt.TargetPodUID,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			if job.InputKind == "task_notification" {
				replayed, found, replayErr := replayTaskNotificationDeliveryFinalizationTx(ctx, tx, job)
				if replayErr != nil {
					return replayErr
				}
				if found && replayed.Status == RuntimeDeliveryDuplicate {
					return nil
				}
			}
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_accept_missing", message: "runtime inbox row is missing for accepted input", retryable: true}
		}
		return nil
	})
	if err == nil && queueLeaseSettled {
		logRuntimeInputCustodyTransition(s.Logger, runtimeScopeFromAttempt(job, attempt), "accepted_to_parked", 1)
	}
	return queueLeaseSettled, err
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

// FinalizeRuntimeDelivery turns an exhausted runtime-input delivery into its
// durable terminal result before the Queue job is closed.
func (s *PostgreSQLRuntimeDeliveryStore) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime delivery store is unavailable", retryable: true}
	}
	if isMCPManifestRuntimeJob(job) {
		return s.finalizeMCPManifestDelivery(ctx, job, result)
	}
	if job.Kind != queue.KindRuntimeInput || job.WorkspaceID == "" || job.SessionID == "" || job.SessionThreadID == "" || job.RuntimeInputID == "" ||
		((job.InputKind == "interrupt_control" || result.Status == RuntimeDeliveryBarrierStale) && (job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "")) {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime delivery finalization identity is incomplete", retryable: false}
	}
	if result.Status != RuntimeDeliveryRejected && result.Status != RuntimeDeliveryBarrierStale {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_response", message: "runtime delivery finalization requires a rejected result", retryable: false}
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	finalized := RuntimeDeliveryResult{}
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.finalize_runtime_delivery", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		replayed, found, err := replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		if err != nil {
			return err
		}
		if found {
			finalized = replayed
			return nil
		}
		if result.Status == RuntimeDeliveryBarrierStale {
			active, err := queue.CancelLeasedRuntimeInputCustodyTx(ctx, tx, queue.CancelLeasedRuntimeInputRequest{
				Lease: queue.ExactLeaseRequest{
					WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken,
					Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
				},
				SessionID: job.SessionID, RuntimeInputID: job.RuntimeInputID, InputKind: job.InputKind, Now: now,
			})
			if err != nil {
				return err
			}
			finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale, QueueLeaseSettled: true}
			if !active {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
			}
			return nil
		}
		if job.InputKind == "interrupt_control" {
			active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
				WorkspaceID: workspace.ID(job.WorkspaceID),
				JobID:       job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
				PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			})
			if err != nil {
				return err
			}
			if !active {
				finalized = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				return nil
			}
			finalized, err = finalizeInterruptDeliveryExhaustionTx(withInterruptCloseout(ctx, job.RuntimeInputID), tx, job, now)
			return err
		}
		if err := validateRuntimeFinalizationBindingTx(ctx, tx, job, result); err != nil {
			return err
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
			    AND status IN ('queued', 'delivering', 'accepted')`,
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
	if finalized.QueueLeaseSettled {
		return finalized, nil
	}
	if !result.Retryable && finalized.Status == RuntimeDeliveryRejected {
		result.Retryable = false
		return result, nil
	}
	return finalized, nil
}

func finalizeInterruptDeliveryExhaustionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (RuntimeDeliveryResult, error) {
	if !runtimeJobFinalAttempt(job) {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{
			kind: "runtime_inbox_status_invalid", message: "interrupt delivery attempts remain", retryable: true,
		}
	}
	return finalizeInterruptDeliveryTerminalTx(ctx, tx, job, now)
}

func finalizeInterruptDeliveryTerminalTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (RuntimeDeliveryResult, error) {
	var inboxStatus string
	var inboxBindingID, inboxPodUID sql.NullString
	var inboxBindingGeneration sql.NullInt64
	if err := tx.QueryRow(ctx,
		`SELECT status, binding_id, binding_generation, target_pod_uid
		   FROM session_runtime_inbox
		  WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		  FOR UPDATE`,
		job.WorkspaceID, job.SessionID, job.RuntimeInputID,
	).Scan(&inboxStatus, &inboxBindingID, &inboxBindingGeneration, &inboxPodUID); err != nil {
		if dbconnect.IsNoRows(err) {
			return RuntimeDeliveryResult{}, invalidRuntimeFinalizationIdentity("interrupt Inbox custody is missing")
		}
		return RuntimeDeliveryResult{}, err
	}
	if inboxStatus == "queued" {
		if inboxBindingID.Valid || inboxBindingGeneration.Valid || inboxPodUID.Valid {
			return RuntimeDeliveryResult{}, invalidRuntimeFinalizationIdentity("queued interrupt Inbox has attempted binding custody")
		}
	} else if inboxStatus != "delivering" && inboxStatus != "accepted" {
		return RuntimeDeliveryResult{}, invalidRuntimeFinalizationIdentity("interrupt Inbox custody is not terminalizable")
	}

	var bindingID, podUID sql.NullString
	var bindingGeneration sql.NullInt64
	err := tx.QueryRow(ctx,
		`SELECT binding_id, binding_generation, agent_runtime_pod_uid
		   FROM session_runtime_bindings
		  WHERE workspace_id=$1 AND session_id=$2
		  FOR UPDATE`,
		job.WorkspaceID, job.SessionID,
	).Scan(&bindingID, &bindingGeneration, &podUID)
	if err != nil && !dbconnect.IsNoRows(err) {
		return RuntimeDeliveryResult{}, err
	}
	if err == nil && inboxStatus != "queued" &&
		(!inboxBindingID.Valid || inboxBindingID.String != bindingID.String ||
			!inboxBindingGeneration.Valid || inboxBindingGeneration.Int64 != bindingGeneration.Int64 ||
			!inboxPodUID.Valid || inboxPodUID.String != podUID.String) {
		return RuntimeDeliveryResult{}, invalidRuntimeFinalizationIdentity("interrupt Inbox binding conflicts with the Session fence")
	}

	var mainThreadID string
	if err := tx.QueryRow(ctx,
		`SELECT id FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND role='main'
		  FOR UPDATE`,
		job.WorkspaceID, job.SessionID,
	).Scan(&mainThreadID); err != nil {
		if dbconnect.IsNoRows(err) {
			return RuntimeDeliveryResult{}, invalidRuntimeFinalizationIdentity("interrupt Session main Thread is missing")
		}
		return RuntimeDeliveryResult{}, err
	}
	scope := &bridgev1.RuntimeScope{
		WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: mainThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId: bindingID.String, BindingGeneration: bindingGeneration.Int64, TargetPodUid: podUID.String,
		},
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	turnID, err := loadOpenDurableTurnIDTx(ctx, tx, scope)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	runtimeWriteID := stableRuntimeID("interrupt_delivery_exhausted", job.WorkspaceID, job.SessionID, job.RuntimeInputID)
	if turnID != nil {
		runtimeWriteID = *turnID
	}
	failure := runtimeTerminationFailure{
		Type: "runtime", Code: "runtime_persistence_exhausted",
		Message: "The session runtime could not complete the request.",
		Reason:  "runtime_input_commit_exhausted", Retryable: false,
	}
	failure.RetryStatus.Type = "terminal"
	failureJSON, err := marshalBridgeJSON(failure)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if _, _, err := settleRuntimeTerminationTx(
		ctx, tx, scope, threadScope, runtimeWriteID, failure, failureJSON, now,
	); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM session_runtime_bindings WHERE workspace_id=$1 AND session_id=$2`,
		job.WorkspaceID, job.SessionID,
	); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_after=NULL, cleanup_enqueued_at=NULL, cleanup_claimed_at=NULL,
		        cleanup_job_id=NULL, binding_id=NULL, binding_generation=NULL, updated_at=$3
		  WHERE workspace_id=$1 AND session_id=$2`,
		job.WorkspaceID, job.SessionID, now,
	); err != nil {
		return RuntimeDeliveryResult{}, err
	}
	return RuntimeDeliveryResult{
		Status: RuntimeDeliveryRejected, Retryable: false,
		ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "runtime delivery attempts are exhausted",
		QueueLeaseSettled: true,
	}, nil
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
	var acceptance mcpManifestAcceptance
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
		transitioned := current.Readiness != mcpManifestReadinessUnready || !current.Diagnostic.Valid || current.Diagnostic.String != mcpManifestDiagnosticDeliveryExhausted
		toolset, err := mcpManifestToolsetConfigTx(ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName)
		if err != nil {
			return err
		}
		generation, err := transitionMCPManifestDeliveryExhaustedTx(
			ctx, tx, job.WorkspaceID, job.SessionID, job.MCPServerName, current, toolset, now,
		)
		acceptance = mcpManifestAcceptance{
			PreviousGeneration: current.Generation,
			Generation:         generation,
			Readiness:          mcpManifestReadinessUnready,
			Diagnostic:         mcpManifestDiagnosticDeliveryExhausted,
			QueueCustody:       "retained",
			Transitioned:       transitioned,
		}
		return err
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	logMCPManifestTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, job.MCPServerName, acceptance, false)
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
		if job.InputKind == "interrupt_control" {
			if job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" || job.SequenceTo <= 0 {
				return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "interrupt delivery replay identity is incomplete", retryable: false}
			}
			// Replay is the first production step for every interrupt lease. Fence
			// pending messages only after locking the Session and exact live Queue
			// identity; a reclaimed worker returns an authority-loss no-op before either
			// Queue followers or Session receipt facts can change.
			if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
				return err
			}
			active, _, err := queue.CancelInterruptFencedMessagesTx(ctx, tx, queue.InterruptFenceRequest{
				Lease: queue.ExactLeaseRequest{
					WorkspaceID: workspace.ID(job.WorkspaceID),
					JobID:       job.JobID, LeaseToken: job.LeaseToken, Kind: job.Kind,
					PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
				},
				SessionID: job.SessionID, SessionThreadID: job.SessionThreadID,
				InterruptFenceSequence: job.SequenceTo,
			})
			if err != nil {
				return err
			}
			if !active {
				result = RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
				found = true
				return nil
			}
		}
		var err error
		result, found, err = replayRuntimeDeliveryFinalizationTx(ctx, tx, job)
		return err
	})
	return result, found, err
}

func (s *PostgreSQLRuntimeDeliveryStore) ReplaceMalformedRuntimeInputCustody(ctx context.Context, job RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error) {
	if s == nil || s.Client == nil || job.WorkspaceID == "" || job.SessionID == "" || job.RuntimeInputID == "" {
		return queue.ReplaceMalformedRuntimeInputCustodyResult{}, nil
	}
	now := time.Now().UTC()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	return queue.NewPostgreSQLStore(s.Client).ReplaceMalformedRuntimeInputCustody(ctx, queue.ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID:    workspace.ID(job.WorkspaceID),
		SessionID:      job.SessionID,
		RuntimeInputID: job.RuntimeInputID,
		JobID:          job.JobID,
		LeaseToken:     job.LeaseToken,
		Now:            now,
	})
}

// FinalizeMalformedRuntimeInputCustody derives business custody exclusively
// from the live Queue row's canonical keys and the unique Inbox relation. An
// interrupt uses the existing Session-wide terminal owner in the same
// transaction as the exact Queue lease; other input kinds retain the Queue
// store's existing kind-specific replacement/dead-letter policy.
func (s *PostgreSQLRuntimeDeliveryStore) FinalizeMalformedRuntimeInputCustody(
	ctx context.Context,
	lease MalformedRuntimeInputLease,
) (MalformedRuntimeInputCustodyResult, error) {
	if s == nil || s.Client == nil || lease.WorkspaceID == "" || lease.JobID == "" || lease.LeaseToken == "" ||
		lease.Kind != queue.KindRuntimeInput || lease.PartitionKey == "" || lease.DedupeKey == "" {
		return MalformedRuntimeInputCustodyResult{}, nil
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var canonical RuntimeJob
	outcome := MalformedRuntimeInputCustodyResult{}
	err := s.Client.WithWorkspaceTx(ctx, lease.WorkspaceID, "agentruntimebridge.finalize_malformed_runtime_input", func(tx *dbconnect.Tx) error {
		var kind, partitionKey, dedupeKey, queueStatus, leaseToken string
		var leaseCurrent bool
		var attemptCount, maxAttempts int32
		if err := tx.QueryRow(ctx, `SELECT kind, partition_key, dedupe_key, status,
			COALESCE(lease_token, ''), COALESCE(leased_until > clock_timestamp(), false),
			attempt_count, max_attempts
			FROM queue_jobs WHERE workspace_id=$1 AND id=$2`,
			lease.WorkspaceID, lease.JobID,
		).Scan(&kind, &partitionKey, &dedupeKey, &queueStatus, &leaseToken, &leaseCurrent, &attemptCount, &maxAttempts); dbconnect.IsNoRows(err) {
			outcome.Handled = true
			outcome.QueueLeaseSettled = true
			return nil
		} else if err != nil {
			return err
		}
		if kind != lease.Kind || partitionKey != lease.PartitionKey || dedupeKey != lease.DedupeKey {
			outcome.Handled = true
			return nil
		}
		if queueStatus == queue.StatusAcknowledged || queueStatus == queue.StatusCancelled || queueStatus == queue.StatusDeadLettered {
			outcome.Handled = true
			outcome.QueueLeaseSettled = true
			return nil
		}
		if queueStatus != queue.StatusLeased || leaseToken != lease.LeaseToken || !leaseCurrent {
			outcome.Handled = true
			return nil
		}

		rows, err := tx.Query(ctx, `SELECT session_id, session_thread_id, runtime_input_id,
			input_kind, event_ids_json, sequence_from, sequence_to
			FROM session_runtime_inbox
			WHERE workspace_id=$1
			  AND $2='session:' || workspace_id || ':' || session_id
			  AND $3='runtime_input:' || workspace_id || ':' || session_id || ':' || runtime_input_id
			ORDER BY runtime_input_id LIMIT 2`, lease.WorkspaceID, partitionKey, dedupeKey)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		var candidates int
		var eventIDsJSON string
		var sequenceFrom, sequenceTo sql.NullInt64
		for rows.Next() {
			candidates++
			if err := rows.Scan(&canonical.SessionID, &canonical.SessionThreadID, &canonical.RuntimeInputID,
				&canonical.InputKind, &eventIDsJSON, &sequenceFrom, &sequenceTo); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if candidates != 1 {
			return nil
		}
		canonical.WorkspaceID = lease.WorkspaceID
		canonical.JobID = lease.JobID
		canonical.LeaseToken = lease.LeaseToken
		canonical.Kind = lease.Kind
		canonical.PartitionKey = partitionKey
		canonical.DedupeKey = dedupeKey
		canonical.AttemptCount = attemptCount
		canonical.MaxAttempts = maxAttempts
		if canonical.InputKind != "interrupt_control" {
			return nil
		}
		var canonicalEventIDs []string
		eventIDsValid := json.Unmarshal([]byte(eventIDsJSON), &canonicalEventIDs) == nil && len(canonicalEventIDs) > 0
		sequenceValid := sequenceFrom.Valid && sequenceTo.Valid && sequenceFrom.Int64 > 0 && sequenceTo.Int64 >= sequenceFrom.Int64
		effectiveMaxAttempts := maxAttempts
		if effectiveMaxAttempts <= 0 {
			effectiveMaxAttempts = queue.DefaultMaxAttempts
		}
		if attemptCount < effectiveMaxAttempts {
			payload, err := json.Marshal(runtimeInputQueuePayload{
				WorkspaceID: canonical.WorkspaceID, SessionID: canonical.SessionID, SessionThreadID: canonical.SessionThreadID,
				RuntimeInputID: canonical.RuntimeInputID, EventIDs: canonicalEventIDs,
				SequenceFrom: sequenceFrom.Int64, SequenceTo: sequenceTo.Int64, InputKind: canonical.InputKind,
			})
			if err != nil {
				return err
			}
			updated, err := tx.Exec(ctx, `UPDATE queue_jobs
				SET payload_json=$4, payload_version=1, updated_at=$5
				WHERE workspace_id=$1 AND id=$2 AND lease_token=$3
				  AND status='leased' AND leased_until > clock_timestamp()`,
				canonical.WorkspaceID, canonical.JobID, canonical.LeaseToken, string(payload), now)
			if err != nil {
				return err
			}
			if !rowsAffected(updated) {
				return invalidRuntimeFinalizationIdentity("malformed interrupt Queue authority changed during retry preparation")
			}
			outcome.Handled = true
			outcome.Retry = true
			return nil
		}
		if err := lockRuntimeMutationSessionTx(ctx, tx, canonical.WorkspaceID, canonical.SessionID); err != nil {
			return err
		}
		var lockedThreadID, lockedInputKind, lockedEventIDsJSON string
		var lockedSequenceFrom, lockedSequenceTo sql.NullInt64
		if err := tx.QueryRow(ctx, `SELECT session_thread_id, input_kind, event_ids_json, sequence_from, sequence_to
			FROM session_runtime_inbox
			WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3 FOR UPDATE`,
			canonical.WorkspaceID, canonical.SessionID, canonical.RuntimeInputID,
		).Scan(&lockedThreadID, &lockedInputKind, &lockedEventIDsJSON, &lockedSequenceFrom, &lockedSequenceTo); err != nil {
			return err
		}
		if lockedThreadID != canonical.SessionThreadID || lockedInputKind != canonical.InputKind || lockedEventIDsJSON != eventIDsJSON ||
			lockedSequenceFrom != sequenceFrom || lockedSequenceTo != sequenceTo {
			return invalidRuntimeFinalizationIdentity("malformed runtime Inbox relation changed during finalization")
		}
		if eventIDsValid && sequenceValid {
			canonical.EventIDs = canonicalEventIDs
			canonical.SequenceFrom = sequenceFrom.Int64
			canonical.SequenceTo = sequenceTo.Int64
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, queue.ExactLeaseRequest{
			WorkspaceID: workspace.ID(canonical.WorkspaceID), JobID: canonical.JobID, LeaseToken: canonical.LeaseToken,
			Kind: canonical.Kind, PartitionKey: canonical.PartitionKey, DedupeKey: canonical.DedupeKey,
		})
		if err != nil {
			return err
		}
		if !active {
			outcome.Handled = true
			return nil
		}
		if _, err := finalizeInterruptDeliveryTerminalTx(
			withInterruptCloseout(ctx, canonical.RuntimeInputID), tx, canonical, now,
		); err != nil {
			return err
		}
		outcome.Handled = true
		outcome.QueueLeaseSettled = true
		outcome.InterruptTerminalized = true
		return nil
	})
	if err != nil || outcome.Handled || canonical.SessionID == "" {
		return outcome, err
	}
	replaced, err := queue.NewPostgreSQLStore(s.Client).ReplaceMalformedRuntimeInputCustody(ctx, queue.ReplaceMalformedRuntimeInputCustodyRequest{
		WorkspaceID: workspace.ID(canonical.WorkspaceID), SessionID: canonical.SessionID, RuntimeInputID: canonical.RuntimeInputID,
		JobID: canonical.JobID, LeaseToken: canonical.LeaseToken, Now: now,
	})
	outcome.Handled = true
	outcome.QueueLeaseSettled = replaced.DeadLettered
	outcome.CanonicalReplacement = replaced.Replaced
	return outcome, err
}

type lockedRuntimeInboxFinalization struct {
	threadID        string
	inputKind       string
	rejectionReason sql.NullString
	eventIDs        []string
	sequenceFrom    sql.NullInt64
	sequenceTo      sql.NullInt64
	status          string
}

func validateRuntimeFinalizationBindingTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	result RuntimeDeliveryResult,
) error {
	var status string
	var bindingID, targetPodUID sql.NullString
	var bindingGeneration sql.NullInt64
	if err := tx.QueryRow(ctx, `SELECT status, binding_id, binding_generation, target_pod_uid
		FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(
		&status, &bindingID, &bindingGeneration, &targetPodUID,
	); err != nil {
		if dbconnect.IsNoRows(err) {
			return invalidRuntimeFinalizationIdentity("runtime Inbox custody is missing")
		}
		return err
	}
	attemptedComplete := result.AttemptedBindingID != "" && result.AttemptedBindingGeneration > 0 && result.AttemptedTargetPodUID != ""
	attemptedEmpty := result.AttemptedBindingID == "" && result.AttemptedBindingGeneration == 0 && result.AttemptedTargetPodUID == ""
	switch status {
	case "queued":
		if !attemptedEmpty || bindingID.Valid || bindingGeneration.Valid || targetPodUID.Valid {
			return invalidRuntimeFinalizationIdentity("queued runtime Inbox conflicts with attempted binding")
		}
	case "delivering", "accepted":
		if !attemptedComplete || !bindingID.Valid || bindingID.String != result.AttemptedBindingID ||
			!bindingGeneration.Valid || bindingGeneration.Int64 != result.AttemptedBindingGeneration ||
			!targetPodUID.Valid || targetPodUID.String != result.AttemptedTargetPodUID {
			return invalidRuntimeFinalizationIdentity("runtime Inbox binding conflicts with delivery attempt")
		}
	}
	return nil
}

// Exhaustion is authorized only by the producer-owned Inbox row. This lock and
// identity check runs before any source event, task, mail, or exhaustion fact
// can be mutated; Queue payloads never reconstruct missing custody.
func lockRuntimeInboxFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (lockedRuntimeInboxFinalization, error) {
	var row lockedRuntimeInboxFinalization
	var eventIDsJSON string
	err := tx.QueryRow(ctx, `SELECT session_thread_id, input_kind, rejection_reason_code,
		event_ids_json, sequence_from, sequence_to, status
		FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
		FOR UPDATE`, job.WorkspaceID, job.SessionID, job.RuntimeInputID).Scan(
		&row.threadID, &row.inputKind, &row.rejectionReason, &eventIDsJSON,
		&row.sequenceFrom, &row.sequenceTo, &row.status,
	)
	if dbconnect.IsNoRows(err) {
		return lockedRuntimeInboxFinalization{}, invalidRuntimeFinalizationIdentity("runtime Inbox custody is missing")
	}
	if err != nil {
		return lockedRuntimeInboxFinalization{}, err
	}
	if err := json.Unmarshal([]byte(eventIDsJSON), &row.eventIDs); err != nil || row.eventIDs == nil {
		return lockedRuntimeInboxFinalization{}, invalidRuntimeFinalizationIdentity("runtime Inbox event identity is invalid")
	}
	if row.threadID != job.SessionThreadID {
		return lockedRuntimeInboxFinalization{}, invalidRuntimeFinalizationIdentity("runtime Inbox thread conflicts with Queue custody")
	}

	switch job.InputKind {
	case "agent_mail":
		if row.inputKind != "agent_mail" || len(job.EventIDs) != 0 || job.SequenceFrom != 0 || job.SequenceTo != 0 {
			return lockedRuntimeInboxFinalization{}, invalidRuntimeFinalizationIdentity("agent mail Queue identity conflicts with Inbox custody")
		}
		if row.status == "dead_lettered" {
			return row, nil
		}
		if err := validateAgentMailFinalizationIdentityTx(ctx, tx, job, row); err != nil {
			return lockedRuntimeInboxFinalization{}, err
		}
	case "task_notification":
		if row.inputKind != "task_notification" || len(job.EventIDs) != 0 || len(row.eventIDs) != 0 ||
			job.SequenceFrom != 0 || job.SequenceTo != 0 || row.sequenceFrom.Valid || row.sequenceTo.Valid {
			return lockedRuntimeInboxFinalization{}, invalidRuntimeFinalizationIdentity("task notification Queue identity conflicts with Inbox custody")
		}
		if row.status == "dead_lettered" {
			return row, nil
		}
		if err := validateTaskNotificationFinalizationIdentityTx(ctx, tx, job); err != nil {
			return lockedRuntimeInboxFinalization{}, err
		}
	default:
		kindMatches := row.inputKind == job.InputKind
		if job.InputKind == "messages" && row.inputKind == "rejection" {
			kindMatches = row.rejectionReason.Valid && (row.rejectionReason.String == "runtime_command_payload_too_large" || row.rejectionReason.String == "runtime_command_rejected")
		}
		if !kindMatches || len(job.EventIDs) == 0 || !slices.Equal(row.eventIDs, job.EventIDs) ||
			!row.sequenceFrom.Valid || !row.sequenceTo.Valid || row.sequenceFrom.Int64 != job.SequenceFrom || row.sequenceTo.Int64 != job.SequenceTo {
			return lockedRuntimeInboxFinalization{}, invalidRuntimeFinalizationIdentity("runtime Queue identity conflicts with Inbox custody")
		}
		if err := validateRuntimeFinalizationEventsTx(ctx, tx, job); err != nil {
			return lockedRuntimeInboxFinalization{}, err
		}
	}
	return row, nil
}

func validateRuntimeFinalizationEventsTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) error {
	if job.SequenceTo-job.SequenceFrom+1 != int64(len(job.EventIDs)) {
		return invalidRuntimeFinalizationIdentity("runtime Queue event range is invalid")
	}
	for index, eventID := range job.EventIDs {
		var threadID string
		var sequence int64
		err := tx.QueryRow(ctx, `SELECT session_thread_id, sequence FROM session_events
			WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3 FOR UPDATE`,
			job.WorkspaceID, job.SessionID, eventID,
		).Scan(&threadID, &sequence)
		if dbconnect.IsNoRows(err) || (err == nil && (threadID != job.SessionThreadID || sequence != job.SequenceFrom+int64(index))) {
			return invalidRuntimeFinalizationIdentity("runtime Queue event identity conflicts with durable events")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func validateAgentMailFinalizationIdentityTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, inbox lockedRuntimeInboxFinalization) error {
	deliveryID := strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")
	if deliveryID == "" || deliveryID == job.RuntimeInputID {
		return invalidRuntimeFinalizationIdentity("agent mail runtime input id is invalid")
	}
	envelope, err := loadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID, deliveryID)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return invalidRuntimeFinalizationIdentity("agent mail envelope is missing")
		}
		return err
	}
	if envelope.TargetThreadID != job.SessionThreadID || envelope.SourceThreadID == "" || envelope.SourceToolUseEventID == "" {
		return invalidRuntimeFinalizationIdentity("agent mail envelope conflicts with Queue custody")
	}
	if inbox.status == "queued" {
		if len(inbox.eventIDs) == 0 && !inbox.sequenceFrom.Valid && !inbox.sequenceTo.Valid {
			return nil
		}
	}
	receivedEventID := stableRuntimeID("agent_mail_received_event", job.WorkspaceID, job.SessionID, job.SessionThreadID, deliveryID)
	if len(inbox.eventIDs) != 1 || inbox.eventIDs[0] != receivedEventID || !inbox.sequenceFrom.Valid || !inbox.sequenceTo.Valid || inbox.sequenceFrom.Int64 != inbox.sequenceTo.Int64 {
		return invalidRuntimeFinalizationIdentity("agent mail received identity conflicts with Inbox custody")
	}
	var sequence int64
	var sourceThreadID, sourceToolUseEventID string
	err = tx.QueryRow(ctx, `SELECT sequence, payload_json::jsonb ->> 'source_thread_id',
		payload_json::jsonb ->> 'source_tool_use_event_id'
		FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		AND event_id=$4 AND type='agent.thread_message_received'
		AND payload_json::jsonb ->> 'delivery_id'=$5 FOR UPDATE`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, receivedEventID, deliveryID,
	).Scan(&sequence, &sourceThreadID, &sourceToolUseEventID)
	if dbconnect.IsNoRows(err) || (err == nil && (sequence != inbox.sequenceFrom.Int64 || sourceThreadID != envelope.SourceThreadID || sourceToolUseEventID != envelope.SourceToolUseEventID)) {
		return invalidRuntimeFinalizationIdentity("agent mail received event conflicts with durable envelope")
	}
	return err
}

func validateTaskNotificationFinalizationIdentityTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) error {
	taskID := taskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return invalidRuntimeFinalizationIdentity("task notification runtime input id is invalid")
	}
	var threadID, sourceToolUseEventID, taskStatus string
	var terminalResultJSON sql.NullString
	var sourceExists bool
	err := tx.QueryRow(ctx, `SELECT task.session_thread_id, task.source_tool_use_event_id, task.status,
		task.terminal_result_json, EXISTS (
			SELECT 1 FROM session_events source WHERE source.workspace_id=task.workspace_id
			AND source.session_id=task.session_id AND source.session_thread_id=task.session_thread_id
			AND source.event_id=task.source_tool_use_event_id
		)
		FROM session_background_tasks task
		WHERE task.workspace_id=$1 AND task.session_id=$2 AND task.task_id=$3 FOR UPDATE`,
		job.WorkspaceID, job.SessionID, taskID,
	).Scan(&threadID, &sourceToolUseEventID, &taskStatus, &terminalResultJSON, &sourceExists)
	if dbconnect.IsNoRows(err) {
		return invalidRuntimeFinalizationIdentity("task notification source is missing")
	}
	if err != nil {
		return err
	}
	if threadID != job.SessionThreadID || sourceToolUseEventID == "" || !sourceExists || !validBackgroundTaskTerminalStatus(taskStatus) ||
		!terminalResultJSON.Valid || !json.Valid([]byte(terminalResultJSON.String)) {
		return invalidRuntimeFinalizationIdentity("task notification source conflicts with Queue custody")
	}
	return nil
}

func invalidRuntimeFinalizationIdentity(message string) error {
	return runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: message, retryable: false}
}

func replayRuntimeDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	if job.InputKind == "task_notification" {
		return replayTaskNotificationDeliveryFinalizationTx(ctx, tx, job)
	}
	if job.InputKind == "agent_mail" {
		return replayAgentMailDeliveryFinalizationTx(ctx, tx, job)
	}
	inbox, err := lockRuntimeInboxFinalizationTx(ctx, tx, job)
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	switch inbox.status {
	case "dead_lettered":
		return runtimeDeliveryExhaustedResult(), true, nil
	case "committed":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "cancelled":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}, true, nil
	case "queued", "delivering", "accepted":
		return RuntimeDeliveryResult{}, false, nil
	default:
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "runtime_inbox_status_invalid", message: "runtime inbox status is invalid", retryable: false}
	}
}

func replayAgentMailDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	inbox, err := lockRuntimeInboxFinalizationTx(ctx, tx, job)
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	switch inbox.status {
	case "dead_lettered":
		return runtimeDeliveryExhaustedResult(), true, nil
	case "committed", "cancelled":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "queued", "delivering", "accepted":
		return RuntimeDeliveryResult{}, false, nil
	default:
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "runtime_inbox_status_invalid", message: "runtime inbox status is invalid", retryable: false}
	}
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
		    AND status IN ('queued', 'delivering', 'accepted')`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
		now,
	)
	return err
}

func replayTaskNotificationDeliveryFinalizationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	taskID := taskNotificationTaskID(job.RuntimeInputID)
	if taskID == "" {
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "task notification runtime input id must identify a task", retryable: false}
	}
	inbox, err := lockRuntimeInboxFinalizationTx(ctx, tx, job)
	if err != nil {
		return RuntimeDeliveryResult{}, false, err
	}
	switch inbox.status {
	case "dead_lettered":
		var errorCode string
		err := tx.QueryRow(ctx, `SELECT error_code
				FROM session_bridge_operations
				WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
				  AND operation=$4 AND source_kind=$4 AND idempotency_key=$5
				  AND ack_status=$6 AND runtime_input_id=$7
				FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.SessionThreadID,
			bridgeOpCommitTaskNotificationResult, taskID+":"+job.RuntimeInputID,
			bridgeAckRejected, job.RuntimeInputID,
		).Scan(&errorCode)
		if err == nil && taskNotificationRejectionCode(errorCode) {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
		}
		if err != nil && !dbconnect.IsNoRows(err) {
			return RuntimeDeliveryResult{}, false, err
		}
		return runtimeDeliveryExhaustedResult(), true, nil
	case "committed", "cancelled":
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "parked":
		// Deferral owns parked custody; a stale Queue lease cannot terminalize it.
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, true, nil
	case "queued", "delivering", "accepted":
	default:
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "runtime_inbox_status_invalid", message: "runtime inbox status is invalid", retryable: false}
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
		return RuntimeDeliveryResult{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "task notification source is missing", retryable: false}
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
		    AND status IN ('queued', 'delivering', 'accepted')`,
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
	payloadJSON, err := runtimeDeliveryExhaustionPayloadJSON(message)
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
	return nil
}

func runtimeDeliveryExhaustionEventID(job RuntimeJob) string {
	digest := sha256Hex(strings.Join([]string{job.WorkspaceID, job.SessionID, job.RuntimeInputID, "runtime_delivery_exhausted"}, "\x00"))
	return "evt_runtime_exhausted_" + digest[:24]
}

func runtimeDeliveryExhaustionPayloadJSON(message string) (string, error) {
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

func (s *PostgreSQLRuntimeDeliveryStore) captureInitialMCPManifests(ctx context.Context, job RuntimeJob, toolsets []MCPManifestToolsetConfig, now time.Time) error {
	return s.captureInitialMCPManifestsWithListTimeout(ctx, job, toolsets, now, initialMCPManifestListTimeout)
}

func (s *PostgreSQLRuntimeDeliveryStore) captureInitialMCPManifestsWithListTimeout(
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
			var discoveryFailure mcpManifestDiscoveryError
			if !errors.As(err, &discoveryFailure) {
				return err
			}
			acceptance, commitErr := s.captureInitialMCPManifestFailure(ctx, job, toolset, discoveryFailure.diagnostic, now)
			if commitErr != nil {
				return commitErr
			}
			logMCPManifestTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance, true)
			continue
		}
		var acceptance mcpManifestAcceptance
		if err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.enqueue_initial_mcp_manifest", func(tx *dbconnect.Tx) error {
			var err error
			acceptance, err = captureInitialMCPManifestAcceptanceTx(
				ctx, tx, job.WorkspaceID, job.SessionID, toolset, manifest, now,
			)
			return err
		}); err != nil {
			return err
		}
		logMCPManifestTransitionCommitted(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance, true)
		if !acceptance.Duplicate {
			logMCPManifestOmissions(s.Logger, ServiceNameJobRunner, job.WorkspaceID, job.SessionID, toolset.MCPServerName, acceptance.BuiltinFamily, acceptance.Omissions)
		}
	}
	return nil
}

func (s *PostgreSQLRuntimeDeliveryStore) captureInitialMCPManifestFailure(
	ctx context.Context,
	job RuntimeJob,
	toolset MCPManifestToolsetConfig,
	diagnostic string,
	now time.Time,
) (mcpManifestAcceptance, error) {
	var acceptance mcpManifestAcceptance
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.capture_initial_mcp_manifest_failure", func(tx *dbconnect.Tx) error {
		var err error
		acceptance, err = captureInitialMCPManifestUnreadyTx(ctx, tx, job.WorkspaceID, job.SessionID, toolset, diagnostic, now)
		return err
	})
	return acceptance, err
}

func captureInitialMCPManifestAcceptanceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	toolset MCPManifestToolsetConfig,
	manifest MCPManifestListResult,
	now time.Time,
) (mcpManifestAcceptance, error) {
	if err := acquireMCPManifestAcceptanceLockTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName); err != nil {
		return mcpManifestAcceptance{}, err
	}
	current, exists, err := loadMCPManifestRowForUpdateTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	if exists {
		return mcpManifestAcceptance{PreviousGeneration: current.Generation, Generation: current.Generation, Duplicate: true}, nil
	}
	filtered, omissions := filterMCPManifestCollisions(toolset.BuiltinFamily, manifest.Tools)
	toolsJSON, canonicalErr := canonicalMCPManifestToolsJSON(filtered)
	if strings.TrimSpace(manifest.ManifestETag) == "" || canonicalErr != nil || len([]byte(toolsJSON)) > MaxMcpManifestBytes {
		acceptance, err := captureInitialMCPManifestUnreadyLockedTx(ctx, tx, workspaceID, sessionID, toolset, mcpManifestDiagnosticInvalid, now)
		return acceptance, err
	}
	acceptance, err := commitMCPManifestReadyTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName, manifest.ManifestETag, toolsJSON, 1, toolset, now)
	acceptance.Readiness = mcpManifestReadinessReady
	acceptance.QueueCustody = "created"
	acceptance.Transitioned = true
	acceptance.BuiltinFamily = toolset.BuiltinFamily
	acceptance.Omissions = omissions
	return acceptance, err
}

func captureInitialMCPManifestUnreadyTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	toolset MCPManifestToolsetConfig,
	diagnostic string,
	now time.Time,
) (mcpManifestAcceptance, error) {
	if err := acquireMCPManifestAcceptanceLockTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName); err != nil {
		return mcpManifestAcceptance{}, err
	}
	current, exists, err := loadMCPManifestRowForUpdateTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName)
	if err != nil {
		return mcpManifestAcceptance{}, err
	}
	if exists {
		return mcpManifestAcceptance{PreviousGeneration: current.Generation, Generation: current.Generation, Duplicate: true}, nil
	}
	return captureInitialMCPManifestUnreadyLockedTx(ctx, tx, workspaceID, sessionID, toolset, diagnostic, now)
}

func captureInitialMCPManifestUnreadyLockedTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	toolset MCPManifestToolsetConfig,
	diagnostic string,
	now time.Time,
) (mcpManifestAcceptance, error) {
	generation, err := transitionMCPManifestUnreadyTx(ctx, tx, workspaceID, sessionID, toolset.MCPServerName, mcpManifestRow{}, false, diagnostic, toolset, now)
	return mcpManifestAcceptance{
		Generation: generation, Readiness: mcpManifestReadinessUnready, Diagnostic: diagnostic,
		QueueCustody: "created", Transitioned: true, BuiltinFamily: toolset.BuiltinFamily,
	}, err
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
	case "cancelled":
		return "cancelled"
	case "expired":
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

func runtimeCommandPlanForPayload(job RuntimeJob, sessionThreadID, runtimeInputID, contentJSON string, binding runtimeBindingForDelivery, port int) (RuntimeCommandPlan, error) {
	target := RuntimePodTarget{Namespace: binding.Namespace, PodName: binding.PodName, PodUID: binding.PodUID, PodIP: binding.PodIP, Port: port}
	attempt := RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID}
	plan := RuntimeCommandPlan{Target: target, AttemptedBinding: attempt}
	thread := func() (string, string, string, int64, string) {
		return job.WorkspaceID, job.SessionID, sessionThreadID, binding.BindingGeneration, binding.PodUID
	}
	workspaceID, sessionID, threadID, generation, podUID := thread()
	switch job.Kind {
	case queue.KindRuntimeConfigUpdate:
		configGeneration, err := runtimeGenerationFromInputID(runtimeInputID)
		if err != nil {
			return RuntimeCommandPlan{}, err
		}
		request := &agentruntimev1.ApplyRuntimeConfigRequest{WorkspaceId: workspaceID, SessionId: sessionID, BindingId: binding.BindingID, BindingGeneration: generation, TargetPodUid: podUID}
		if job.MCPServerName != "" {
			request.Config = &agentruntimev1.ApplyRuntimeConfigRequest_McpManifest{McpManifest: &agentruntimev1.RuntimeMcpManifestConfig{McpServerName: job.MCPServerName, Generation: configGeneration, ContentJson: contentJSON}}
		} else {
			request.Config = &agentruntimev1.ApplyRuntimeConfigRequest_SessionConfig{SessionConfig: &agentruntimev1.RuntimeSessionConfig{Generation: configGeneration, ContentJson: contentJSON}}
		}
		plan.RuntimeConfig = request
		return plan, nil
	case queue.KindRuntimeInput:
	default:
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "unsupported runtime command job", retryable: false}
	}
	switch job.InputKind {
	case "messages", "rejection":
		request := &agentruntimev1.AcceptInputRequest{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID, BindingId: binding.BindingID, BindingGeneration: generation, TargetPodUid: podUID, RuntimeInputId: runtimeInputID, InputOrder: job.SequenceTo}
		if job.InputKind == "messages" {
			request.Content = &agentruntimev1.AcceptInputRequest_MessagesJson{MessagesJson: contentJSON}
		} else {
			reason := agentruntimev1.AcceptInputRejectionReason_ACCEPT_INPUT_REJECTION_REASON_RUNTIME_REJECTED
			if job.RejectionReasonCode == "runtime_command_payload_too_large" {
				reason = agentruntimev1.AcceptInputRejectionReason_ACCEPT_INPUT_REJECTION_REASON_PAYLOAD_TOO_LARGE
			}
			request.Content = &agentruntimev1.AcceptInputRequest_Rejection{Rejection: &agentruntimev1.AcceptInputRejection{Reason: reason}}
		}
		plan.AcceptInput = request
	case "interrupt_control":
		var content struct {
			Origin string `json:"origin"`
		}
		if err := json.Unmarshal([]byte(contentJSON), &content); err != nil {
			return RuntimeCommandPlan{}, err
		}
		origin := agentruntimev1.InterruptOrigin_INTERRUPT_ORIGIN_USER
		if content.Origin == "agent" {
			origin = agentruntimev1.InterruptOrigin_INTERRUPT_ORIGIN_AGENT
		}
		plan.Interrupt = &agentruntimev1.InterruptRequest{
			WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID,
			BindingId: binding.BindingID, BindingGeneration: generation, TargetPodUid: podUID,
			RuntimeInputId: runtimeInputID, Origin: origin,
			InterruptLeaseRef: &agentruntimev1.InterruptLeaseRef{
				JobId: job.JobID, LeaseToken: job.LeaseToken,
				PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
			},
		}
	case "tool_confirmation":
		var content struct {
			ToolUseEventID string  `json:"tool_use_event_id"`
			Decision       string  `json:"decision"`
			DenyMessage    *string `json:"deny_message"`
		}
		if err := json.Unmarshal([]byte(contentJSON), &content); err != nil || content.ToolUseEventID == "" {
			return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "tool confirmation command is invalid", retryable: false}
		}
		decision := agentruntimev1.ToolConfirmationDecision_TOOL_CONFIRMATION_DECISION_ALLOW
		if content.Decision == "deny" {
			decision = agentruntimev1.ToolConfirmationDecision_TOOL_CONFIRMATION_DECISION_DENY
		}
		request := &agentruntimev1.ResolveToolConfirmationRequest{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: threadID, BindingId: binding.BindingID, BindingGeneration: generation, TargetPodUid: podUID, RuntimeInputId: runtimeInputID, ToolUseEventId: content.ToolUseEventID, Decision: decision}
		if content.DenyMessage != nil {
			request.DenyMessage = content.DenyMessage
		}
		plan.ToolConfirmation = request
	default:
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "unsupported runtime input kind", retryable: false}
	}
	return plan, nil
}

func runtimeGenerationFromInputID(runtimeInputID string) (int64, error) {
	value := runtimeInputID[strings.LastIndex(runtimeInputID, ":")+1:]
	generation, err := strconv.ParseInt(value, 10, 64)
	if err != nil || generation <= 0 {
		return 0, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "runtime config generation is invalid", retryable: false}
	}
	return generation, nil
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
	messages := make([]json.RawMessage, 0, len(job.EventIDs))
	for _, eventID := range job.EventIDs {
		var eventType string
		var payloadJSON string
		if err := tx.QueryRow(ctx,
			`SELECT type, payload_json
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4`,
			job.WorkspaceID, job.SessionID, job.SessionThreadID, eventID,
		).Scan(&eventType, &payloadJSON); dbconnect.IsNoRows(err) {
			return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "message runtime input event is missing", retryable: false}
		} else if err != nil {
			return "", err
		}
		if eventType != "user.message" {
			return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "message runtime input event type is invalid", retryable: false}
		}
		messageJSON, err := userMessageContextDraftJSON(payloadJSON)
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
	if eventType != "user.interrupt" && eventType != childInterruptRequestedEventType {
		return "", runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "interrupt control event type is invalid", retryable: false}
	}
	return marshalBridgeJSON(map[string]any{
		"source_event_id":          eventID,
		"interrupt_fence_sequence": sequence,
		"origin":                   map[bool]string{true: "agent", false: "user"}[eventType == childInterruptRequestedEventType],
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
	ModelRequestID  string
	ModelToolCallID string
	EventType       string
}

type pendingToolTerminal struct {
	ErrorType string
	Message   string
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
	toolUse := runtimeOrphanToolUse{
		SessionThreadID: wait.ThreadID,
		EventID:         wait.ToolUseEventID,
		EventType:       wait.EventType,
		ModelRequestID:  wait.ModelRequestID,
		ModelToolCallID: wait.ModelToolCallID,
	}
	terminalResult := runtimeTerminalToolResult{
		ErrorType: terminal.ErrorType,
		Message:   terminal.Message,
		Retryable: false,
	}
	projection, err := settleRuntimeTerminalToolPartTx(ctx, tx, scope, toolUse, terminalResult, now)
	if err != nil {
		return "", err
	}
	projectionJSON, err := marshalBridgeJSON(projection)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $11)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		eventType, payloadJSON, visibility, sessionVisible, projectionJSON, now,
	); err != nil {
		return "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
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

func (s *PostgreSQLRuntimeDeliveryStore) prepareAgentMailCommandTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	port int,
	now time.Time,
) (RuntimeCommandPlan, error) {
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
		binding,
		now,
	)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if admitted.Terminal {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	return RuntimeCommandPlan{
		Target: RuntimePodTarget{
			Namespace: binding.Namespace,
			PodName:   binding.PodName,
			PodUID:    binding.PodUID,
			PodIP:     binding.PodIP,
			Port:      port,
		},
		AttemptedBinding: RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID},
		AcceptAgentMail: &agentruntimev1.AcceptAgentMailRequest{
			WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
			BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, TargetPodUid: binding.PodUID,
			RuntimeInputId: job.RuntimeInputID, DeliveryId: envelope.DeliveryID, Content: envelope.Content,
		},
	}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareTaskNotificationCommandTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, port int, now time.Time) (*RuntimeTaskNotificationPlan, RuntimeCommandPlan, error) {
	if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
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
	closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID)
	if err != nil {
		return nil, RuntimeCommandPlan{}, err
	}
	if closing {
		settled, err := deferLeasedTaskNotificationTx(ctx, tx, job, now)
		if err != nil {
			return nil, RuntimeCommandPlan{}, err
		}
		return nil, RuntimeCommandPlan{SettledAccepted: true, QueueLeaseSettled: settled}, nil
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
	if err := claimRuntimeInboxDeliveryTx(ctx, tx, job, binding, now); err != nil {
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
			AttemptedBinding: RuntimeAttemptedBinding{BindingID: binding.BindingID, Generation: binding.BindingGeneration, TargetPodUID: binding.PodUID},
			AcceptTask: &agentruntimev1.AcceptTaskNotificationRequest{
				WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID,
				BindingId: binding.BindingID, BindingGeneration: binding.BindingGeneration, TargetPodUid: binding.PodUID,
				RuntimeInputId: job.RuntimeInputID, InputOrder: job.SequenceTo, NotificationJson: payloadJSON,
			},
		}, nil
}

// settleCurrentBindingAcceptedRuntimeInputTx consumes a reclaimed Queue lease
// without invoking Runtime again when the durable current binding already owns
// the accepted input. It deliberately does not consult Kubernetes availability:
// only the separate proven-loss transaction may transfer this custody.
func settleCurrentBindingAcceptedRuntimeInputTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	job RuntimeJob,
	now time.Time,
) (bool, error) {
	var accepted bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1
		      FROM session_runtime_inbox inbox
		      JOIN session_runtime_bindings binding
		        ON binding.workspace_id = inbox.workspace_id
		       AND binding.session_id = inbox.session_id
		       AND binding.binding_id = inbox.binding_id
		       AND binding.binding_generation = inbox.binding_generation
		       AND binding.agent_runtime_pod_uid = inbox.target_pod_uid
		     WHERE inbox.workspace_id = $1
		       AND inbox.session_id = $2
		       AND inbox.runtime_input_id = $3
		       AND inbox.status = 'accepted'
		)`,
		job.WorkspaceID,
		job.SessionID,
		job.RuntimeInputID,
	).Scan(&accepted); err != nil {
		return false, err
	}
	if !accepted {
		return false, nil
	}
	acked, err := queue.AckTx(ctx, tx, queue.AckRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID),
		JobID:       job.JobID,
		LeaseToken:  job.LeaseToken,
		Now:         now,
	})
	if err != nil {
		return false, err
	}
	if !acked {
		return false, runtimeDeliveryPrepareError{kind: "runtime_queue_lease_stale", message: "runtime input queue lease is stale", retryable: true}
	}
	return true, nil
}

// deferLeasedTaskNotificationTx transfers ownership of a leased notification
// from Queue to the dormant Runtime Inbox record. The caller holds the Session
// mutation lock, so the close fence and inbox transition share one winner with
// notification commit and resume.
func deferLeasedTaskNotificationTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (bool, error) {
	binding, err := readRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return false, err
	}
	parked, err := parkTaskNotificationInboxTx(ctx, tx, job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RuntimeInputID, binding, now)
	if err != nil {
		return false, err
	}
	if !parked {
		return false, runtimeDeliveryPrepareError{kind: "task_notification_defer_missing", message: "task notification inbox is not deferrable", retryable: true}
	}
	updated, err := queue.AckTx(ctx, tx, queue.AckRequest{
		WorkspaceID: workspace.ID(job.WorkspaceID), JobID: job.JobID, LeaseToken: job.LeaseToken, Now: now,
	})
	if err != nil {
		return false, err
	}
	if !updated {
		return false, runtimeDeliveryPrepareError{kind: "queue_authority_lost", message: "task notification Queue lease is no longer owned", retryable: true}
	}
	return true, nil
}

// parkTaskNotificationInboxTx is the sole queued/delivering/accepted-to-parked
// transition. Queue ownership remains with the caller: a leased delivery ACKs
// its exact token, while close admission targeted-cancels pending custody.
func parkTaskNotificationInboxTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	threadID string,
	runtimeInputID string,
	binding runtimeBindingForDelivery,
	now time.Time,
) (bool, error) {
	result, err := tx.Exec(ctx, `UPDATE session_runtime_inbox
		SET status='parked',
		    binding_id=COALESCE(binding_id,$6),
		    binding_generation=COALESCE(binding_generation,$7),
		    target_pod_uid=COALESCE(target_pod_uid,$8),
		    updated_at=$5
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND runtime_input_id=$4
		  AND input_kind='task_notification' AND status IN ('queued','delivering','accepted','parked')`,
		workspaceID, sessionID, threadID, runtimeInputID, now,
		binding.BindingID, binding.BindingGeneration, binding.PodUID,
	)
	if err != nil {
		return false, err
	}
	return rowsAffected(result), nil
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

func runtimeScopeFromAttempt(job RuntimeJob, attempt RuntimeAttemptedBinding) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     job.WorkspaceID,
		SessionId:       job.SessionID,
		SessionThreadId: job.SessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         attempt.BindingID,
			BindingGeneration: attempt.Generation,
			TargetPodUid:      attempt.TargetPodUID,
		},
	}
}

type KubernetesRuntimeTargetResolver struct {
	Snapshot func() enginekubernetes.BindingVisibilitySnapshot
	Clock    func() time.Time
}

func (r KubernetesRuntimeTargetResolver) BindingVisibilitySnapshot() enginekubernetes.BindingVisibilitySnapshot {
	if r.Snapshot == nil {
		return enginekubernetes.BindingVisibilitySnapshot{}
	}
	return r.Snapshot()
}

type runtimeBindingVisibilityDisposition string

const (
	runtimeBindingVisibilityReusable     runtimeBindingVisibilityDisposition = "reusable"
	runtimeBindingVisibilityProvenGone   runtimeBindingVisibilityDisposition = "proven_gone"
	runtimeBindingVisibilityAvailability runtimeBindingVisibilityDisposition = "availability"
)

func classifyRuntimeBindingVisibility(state enginekubernetes.BindingVisibilityState) runtimeBindingVisibilityDisposition {
	switch state {
	case enginekubernetes.BindingVisibilityReusable:
		return runtimeBindingVisibilityReusable
	case enginekubernetes.BindingVisibilityAbsent,
		enginekubernetes.BindingVisibilityDeleted,
		enginekubernetes.BindingVisibilityUIDChanged,
		enginekubernetes.BindingVisibilityIPChanged:
		return runtimeBindingVisibilityProvenGone
	case enginekubernetes.BindingVisibilitySnapshotNotReady,
		enginekubernetes.BindingVisibilityNotReady,
		enginekubernetes.BindingVisibilityNotServing,
		enginekubernetes.BindingVisibilityTerminating:
		return runtimeBindingVisibilityAvailability
	default:
		return runtimeBindingVisibilityAvailability
	}
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
		switch classifyRuntimeBindingVisibility(visibility) {
		case runtimeBindingVisibilityReusable:
			return current, nil
		case runtimeBindingVisibilityProvenGone:
			return runtimeBindingForDelivery{}, runtimeBindingLostError{binding: current}
		case runtimeBindingVisibilityAvailability:
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

// claimRuntimeInboxDeliveryTx binds producer-created Inbox custody to the
// selected Runtime. session_runtime_inbox is never reconstructed from a Queue
// payload; LoadContext also never projects it into context.
//
//	status         meaning                                writer
//	queued         source fact and Queue custody committed  input producer
//	delivering     existing custody bound before send       this claim
//	accepted       pod acknowledged the command             MarkRuntimeInputAccepted
//	committed      inputs durably committed                 CommitInputs, the
//	                                                         task_notification commit
//	                                                         (CommitTaskNotificationResult)
//	cancelled      superseded / retracted                   interrupt fence
//	dead_lettered  invariant/exhaustion terminal            finalization
//
// An accepted row stays owned without an active Queue job; only exact
// binding-loss reconciliation may hand it back to Queue custody.
func claimRuntimeInboxDeliveryTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, binding runtimeBindingForDelivery, now time.Time) error {
	eventIDs := job.EventIDs
	if eventIDs == nil {
		eventIDs = []string{}
	}
	events, err := json.Marshal(eventIDs)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status='delivering',binding_id=$10,binding_generation=$11,target_pod_uid=$12,updated_at=$13
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND runtime_input_id=$4
		    AND input_kind=$5 AND rejection_reason_code IS NOT DISTINCT FROM $6
		    AND event_ids_json=$7 AND sequence_from IS NOT DISTINCT FROM $8 AND sequence_to IS NOT DISTINCT FROM $9
		    AND (
		      status='queued'
		      OR (status='delivering' AND binding_id=$10 AND binding_generation=$11 AND target_pod_uid=$12)
		    )`,
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
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		var statusValue string
		var payloadMatches, bindingMatches bool
		if err := tx.QueryRow(ctx, `SELECT status,
			input_kind=$3
			  AND rejection_reason_code IS NOT DISTINCT FROM $4
			  AND event_ids_json=$5
			  AND sequence_from IS NOT DISTINCT FROM $6
			  AND sequence_to IS NOT DISTINCT FROM $7,
			COALESCE(binding_id=$8 AND binding_generation=$9 AND target_pod_uid=$10, false)
			FROM session_runtime_inbox
			WHERE workspace_id=$1 AND runtime_input_id=$2`,
			job.WorkspaceID,
			job.RuntimeInputID,
			job.InputKind,
			sql.NullString{String: job.RejectionReasonCode, Valid: job.RejectionReasonCode != ""},
			string(events),
			sql.NullInt64{Int64: job.SequenceFrom, Valid: job.SequenceFrom > 0},
			sql.NullInt64{Int64: job.SequenceTo, Valid: job.SequenceTo > 0},
			binding.BindingID,
			binding.BindingGeneration,
			binding.PodUID,
		).Scan(&statusValue, &payloadMatches, &bindingMatches); dbconnect.IsNoRows(err) {
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_custody_invalid", message: "runtime input has no producer custody", retryable: false}
		} else if err != nil {
			return err
		}
		if job.InputKind == "interrupt_control" && statusValue == "delivering" && payloadMatches && !bindingMatches {
			return runtimeDeliveryPrepareError{kind: "runtime_inbox_binding_changed", message: "runtime input binding changed during delivery", retryable: true}
		}
		return runtimeDeliveryPrepareError{kind: "runtime_inbox_payload_conflict", message: "runtime input replay conflicts with producer custody", retryable: false}
	}
	return nil
}

type RuntimePodCommandClient struct {
	TokenSource internalgrpcauth.TokenSource
	DialOptions []grpc.DialOption
}

func NewRuntimePodCommandClient(tokenSource internalgrpcauth.TokenSource, dialOptions ...grpc.DialOption) *RuntimePodCommandClient {
	return &RuntimePodCommandClient{TokenSource: tokenSource, DialOptions: append([]grpc.DialOption(nil), dialOptions...)}
}

func runtimePodCall[Request proto.Message, Response any](ctx context.Context, c *RuntimePodCommandClient, target RuntimePodTarget, request Request, invoke func(agentruntimev1.AgentRuntimePodServiceClient, context.Context, Request) (Response, error)) (Response, error) {
	var zero Response
	if c == nil || c.TokenSource == nil {
		return zero, errors.New("runtime pod command client is required")
	}
	if proto.Size(request) > sessionrpc.MaxRuntimeCommandGRPCMessageBytes {
		return zero, &runtimeCommandPayloadTooLargeError{}
	}
	if target.PodIP == "" || target.Port <= 0 {
		return zero, errors.New("runtime pod target is required")
	}
	if _, err := netip.ParseAddr(target.PodIP); err != nil {
		return zero, errors.New("runtime pod target ip is invalid")
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
		return zero, err
	}
	defer func() { _ = conn.Close() }()
	client := agentruntimev1.NewAgentRuntimePodServiceClient(conn)
	return invoke(client, ctx, request)
}

func (c *RuntimePodCommandClient) AcceptInput(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.AcceptInputRequest) (*agentruntimev1.AcceptInputResponse, error) {
		return client.AcceptInput(ctx, request)
	})
}
func (c *RuntimePodCommandClient) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
		return client.AcceptAgentMail(ctx, request)
	})
}
func (c *RuntimePodCommandClient) AcceptTaskNotification(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.AcceptTaskNotificationRequest) (*agentruntimev1.AcceptTaskNotificationResponse, error) {
		return client.AcceptTaskNotification(ctx, request)
	})
}
func (c *RuntimePodCommandClient) Interrupt(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.InterruptRequest) (*agentruntimev1.InterruptResponse, error) {
		return client.Interrupt(ctx, request)
	})
}
func (c *RuntimePodCommandClient) ResolveToolConfirmation(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.ResolveToolConfirmationRequest) (*agentruntimev1.ResolveToolConfirmationResponse, error) {
		return client.ResolveToolConfirmation(ctx, request)
	})
}
func (c *RuntimePodCommandClient) ApplyRuntimeConfig(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.ApplyRuntimeConfigRequest) (*agentruntimev1.ApplyRuntimeConfigResponse, error) {
		return client.ApplyRuntimeConfig(ctx, request)
	})
}
func (c *RuntimePodCommandClient) CleanupSession(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
	return runtimePodCall(ctx, c, target, request, func(client agentruntimev1.AgentRuntimePodServiceClient, ctx context.Context, request *agentruntimev1.CleanupSessionRequest) (*agentruntimev1.CleanupSessionResponse, error) {
		return client.CleanupSession(ctx, request)
	})
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
